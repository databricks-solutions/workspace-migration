# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------

# Negative-path integration assertions (X.3).
#
# Four scenarios run in parallel as independent task chains in the
# ``negative_paths_integration_test`` workflow:
#
#   X.3.1  invalid spn_client_id               -> pre_check fails (auth error)
#   X.3.2  unreachable target_workspace_url    -> pre_check fails (target error)
#   X.3.3  include_uc=false AND include_hive=false
#                                             -> discovery / migrate succeed as no-op
#
# This single assertion notebook is run once per scenario (``run_if:
# ALL_DONE`` so it runs even when the preceding task fails). The
# ``scenario`` widget selects which scenario to assert, and the notebook
# fetches the current job run's task states via the Jobs API to verify
# the expected task failed for the expected reason.

from databricks.sdk import WorkspaceClient  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("scenario", "")  # noqa: F821
dbutils.widgets.text("trigger_task_key", "")  # noqa: F821
dbutils.widgets.text("parent_run_id", "")  # noqa: F821

scenario = dbutils.widgets.get("scenario").strip()  # noqa: F821
trigger_task_key = dbutils.widgets.get("trigger_task_key").strip()  # noqa: F821
parent_run_id_raw = dbutils.widgets.get("parent_run_id").strip()  # noqa: F821

if not scenario:
    raise ValueError("test_negative_paths requires a non-empty 'scenario' widget")
if not trigger_task_key:
    raise ValueError("test_negative_paths requires a non-empty 'trigger_task_key' widget")
if not parent_run_id_raw:
    raise ValueError(
        "test_negative_paths requires a non-empty 'parent_run_id' widget — "
        "the DAB task definition must pass parent_run_id: {{job.run_id}}"
    )

# COMMAND ----------

# Parent-run-id is passed via DAB template expansion (``{{job.run_id}}``)
# in the workflow yml. This is more reliable than ``dbutils`` notebook
# context accessors — both current-run-id and root-run-id methods hit
# Py4J whitelist errors on certain runtimes.

run_id = int(parent_run_id_raw)
print(f"[{scenario}] Assertion running against job run_id={run_id}")

w = WorkspaceClient()
run = w.jobs.get_run(run_id)

# Find the trigger task. for_each task chains wrap the real task inside
# ``tasks[].state`` entries; our negative-path workflow uses plain (non-
# for-each) tasks so a straight match on ``task_key`` works.
trigger_task = None
for t in run.tasks or []:
    if t.task_key == trigger_task_key:
        trigger_task = t
        break

if trigger_task is None:
    available = [t.task_key for t in (run.tasks or [])]
    raise AssertionError(
        f"[{scenario}] trigger task {trigger_task_key!r} not found in run {run_id}. "
        f"Available: {available}"
    )

state = trigger_task.state
result_state = getattr(state, "result_state", None)
life_cycle_state = getattr(state, "life_cycle_state", None)
state_message = getattr(state, "state_message", "") or ""
# Fetch the run-output for the full error message — the task-state
# ``state_message`` is truncated on some runtimes.
try:
    output = w.jobs.get_run_output(trigger_task.run_id)
    error_text = (getattr(output, "error", "") or "") + "\n" + (getattr(output, "error_trace", "") or "")
except Exception as exc:  # noqa: BLE001
    print(f"[{scenario}] get_run_output failed (non-fatal): {exc}")
    error_text = ""

combined = f"{state_message}\n{error_text}".lower()
print(f"[{scenario}] trigger task life_cycle={life_cycle_state} result={result_state}")
print(f"[{scenario}] error excerpt: {combined[:800]}")

# COMMAND ----------

# Per-scenario expectations.
#
# For failure scenarios we assert result_state=FAILED AND a substring of
# the error message that proves the failure was for the RIGHT reason
# (not some upstream Spark crash). For X.3.3 (no-op) we assert
# result_state=SUCCESS.


def _assert_failed_with(substrings: tuple[str, ...]) -> None:
    if str(result_state) != "RunResultState.FAILED" and str(result_state) != "FAILED":
        raise AssertionError(
            f"[{scenario}] expected trigger task to FAIL, got result_state={result_state!r}, "
            f"life_cycle={life_cycle_state!r}"
        )
    if not any(s.lower() in combined for s in substrings):
        raise AssertionError(
            f"[{scenario}] trigger task failed as expected, but error text did not contain "
            f"any of {substrings!r}. Got: {combined[:2000]}"
        )
    print(f"[{scenario}] PASS — failed for the expected reason.")


def _assert_succeeded_as_noop() -> None:
    if str(result_state) not in ("RunResultState.SUCCESS", "SUCCESS"):
        raise AssertionError(
            f"[{scenario}] expected trigger task to SUCCEED as no-op, got result_state={result_state!r}, "
            f"life_cycle={life_cycle_state!r}, msg={state_message!r}"
        )
    print(f"[{scenario}] PASS — completed cleanly as no-op.")


if scenario == "X.3.1":
    # Invalid SPN: pre_check's check_source_auth / check_target_auth raise
    # during ``auth.test_connectivity()``. The aggregated failure message
    # names the check ("check_source_auth" / "check_target_auth") and
    # pre_check raises "Pre-check failed: N check(s) returned FAIL".
    _assert_failed_with(("pre-check failed", "check_source_auth", "check_target_auth"))
elif scenario == "X.3.2":
    # Unreachable target: source auth usually still works, but
    # check_target_auth / check_target_metastore fail with DNS/connect
    # errors. pre_check still raises "Pre-check failed: ... FAIL".
    _assert_failed_with(("pre-check failed", "check_target_auth", "check_target_metastore"))
elif scenario == "X.3.3":
    # Both scopes false: migrate workflow's setup_sharing short-circuits
    # (``include_uc=false``), orchestrator publishes empty task values,
    # for_each workers receive empty batches (no-op), summary succeeds.
    # The trigger task here is the migrate run_job_task — it should
    # SUCCEED.
    _assert_succeeded_as_noop()
else:
    raise ValueError(f"Unknown scenario: {scenario!r}")
