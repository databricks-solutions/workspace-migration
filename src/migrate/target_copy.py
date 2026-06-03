"""Target-side file-copy primitives shared by volume_worker + models_worker.

Kept as a plain Python module (no ``# Databricks notebook source`` header)
so notebooks in ``src/migrate/`` can import it — Databricks refuses
notebook-to-notebook imports at runtime.

The pattern:

1. Upload a generic copy notebook to a known path on the target workspace
   (idempotent; ``ensure_copy_notebook_on_target``).
2. Submit a serverless notebook-task job on target that runs
   ``dbutils.fs.cp`` recursively from ``src`` to ``dst``, returning a
   JSON blob with bytes + file count (``run_target_file_copy``).

The src path must be readable from the target cluster's identity. For
managed volumes that's handled by Delta Sharing (source-side ALTER
SHARE ADD VOLUME, target reads from share-consumer path). For model
artifacts the source URI (typically ``abfss://...``) must be directly
reachable from the target SPN — either same storage account or with
a cross-account credential configured.
"""

from __future__ import annotations

import base64
import contextlib
import json
import time

from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, NotebookTask, SubmitTask
from databricks.sdk.service.workspace import ImportFormat, Language

from common.auth import AuthManager

# Workspace path where the target-side copy helper notebook is uploaded.
# All workers that need a cross-path copy share this one notebook.
TARGET_COPY_NOTEBOOK_PATH = "/Shared/cp_migration_runtime/_volume_copy"

# Target-side notebook source — uploaded once per migration run and reused
# across workers. Emits JSON with bytes_copied / file_count via notebook exit.
#
# Share-propagation race: when the source-side ``ALTER SHARE ADD VOLUME``
# runs immediately before this notebook is submitted, the consumer-side
# view of the share can lag a few seconds before the new volume is
# visible at ``/Volumes/cp_migration_share_consumer/<schema>/<volume>``.
# The first-volume-in-batch hits this almost every cross-region run
# (verified 2026-05-20 source northeurope → target westeurope:
# test_volume failed UC_VOLUME_NOT_FOUND while a later nested_volume in
# the same run succeeded). Subsequent volumes do not race because the
# share is warm. Retry the initial ``ls`` with backoff up to ~30s to
# absorb the lag.
TARGET_COPY_NOTEBOOK = """# Databricks notebook source
# Target-side helper: recursively copies files from a source path to a
# destination path via dbutils.fs.cp. Invoked by workers that need cross-
# path copies (managed volumes, model artifacts).
import json
import time

dbutils.widgets.text("src", "")
dbutils.widgets.text("dst", "")
src = dbutils.widgets.get("src")
dst = dbutils.widgets.get("dst")

total_bytes = 0
total_files = 0


def _ls_with_retry(path, attempts=8, base_delay=2.0):
    last_exc = None
    for i in range(attempts):
        try:
            return dbutils.fs.ls(path)
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if "not_found" not in msg and "does not exist" not in msg and "uc_volume_not_found" not in msg:
                raise
            if i < attempts - 1:
                time.sleep(base_delay * (1 + i))
    raise last_exc


def _copy_recursive(s, d, _entry=False):
    global total_bytes, total_files
    listing = _ls_with_retry(s) if _entry else dbutils.fs.ls(s)
    for f in listing:
        target = d.rstrip("/") + "/" + f.name.rstrip("/")
        if f.isDir():
            dbutils.fs.mkdirs(target)
            _copy_recursive(f.path, target)
        else:
            dbutils.fs.cp(f.path, target)
            total_bytes += f.size
            total_files += 1


_copy_recursive(src, dst, _entry=True)
dbutils.notebook.exit(json.dumps({"bytes_copied": total_bytes, "file_count": total_files}))
"""


def ensure_copy_notebook_on_target(auth: AuthManager) -> None:
    """Idempotently upload the copy-helper notebook to target workspace."""
    target = auth.target_client
    with contextlib.suppress(Exception):
        target.workspace.mkdirs("/Shared/cp_migration_runtime")
    # SDK requires real Enum instances here; raw strings trigger
    # ``AttributeError: 'str' object has no attribute 'value'``.
    target.workspace.import_(
        path=TARGET_COPY_NOTEBOOK_PATH,
        content=base64.b64encode(TARGET_COPY_NOTEBOOK.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )


def run_target_file_copy(
    auth: AuthManager,
    src_path: str,
    dst_path: str,
    run_name: str,
    *,
    timeout_s: int = 3600,
) -> dict:
    """Submit the copy notebook on target as a serverless job; wait + return.

    Returns ``{"bytes_copied": int, "file_count": int}`` on success.
    Raises ``RuntimeError`` on failure, timeout, or inaccessible src path.
    """
    target = auth.target_client
    submit_tasks = [
        SubmitTask(
            task_key="copy",
            notebook_task=NotebookTask(
                notebook_path=TARGET_COPY_NOTEBOOK_PATH,
                base_parameters={"src": src_path, "dst": dst_path},
            ),
            environment_key="default",
        )
    ]
    environments = [JobEnvironment(environment_key="default", spec=Environment(client="2"))]

    run = target.jobs.submit(
        run_name=run_name,
        tasks=submit_tasks,
        environments=environments,
    )
    run_id = run.run_id

    deadline = time.time() + timeout_s
    run_obj = None
    while time.time() < deadline:
        run_obj = target.jobs.get_run(run_id)
        life_cycle = getattr(run_obj.state, "life_cycle_state", None)
        if life_cycle and str(life_cycle).endswith(("TERMINATED", "SKIPPED", "INTERNAL_ERROR")):
            break
        time.sleep(5)
    else:
        raise RuntimeError(f"Target copy job timed out after {timeout_s}s (run_id={run_id})")

    result_state = getattr(run_obj.state, "result_state", None)
    if str(result_state).endswith("SUCCESS"):
        task_run_id = run_obj.tasks[0].run_id if run_obj.tasks else run_id
        out = target.jobs.get_run_output(task_run_id)
        payload = out.notebook_output.result if out.notebook_output else "{}"
        return json.loads(payload or "{}")
    msg = getattr(run_obj.state, "state_message", "") or str(result_state)
    raise RuntimeError(f"Target copy job failed ({run_name}): {msg}")
