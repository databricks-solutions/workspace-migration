# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

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
# Registered Models Worker (Phase 3 Task 34).
#
# Three-phase: (a) create model shell via SDK, (b) create each version
# with ``source`` pointing at the source-side artifact URI and copy the
# artifact bytes across to the target's newly allocated version path
# (falls back to a ``validation_failed`` status if the target SPN can't
# read the source URI), (c) replay aliases per version.
#
# Artifact copy requires the target SPN to have read access to the
# source workspace's model storage URI — typically ``abfss://`` /
# ``s3://`` — either via shared storage credentials or a cross-account
# IAM role. When inaccessible, the error_message surfaces the offending
# URI so operators know what to grant.

import json
import logging
import time

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.target_copy import ensure_copy_notebook_on_target, run_target_file_copy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("models_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    parts = fqn.strip("`").split(".")
    if len(parts) != 3:
        raise ValueError(f"Malformed model FQN: {fqn}")
    return parts[0], parts[1], parts[2]


def apply_model(
    model: dict,
    *,
    auth: AuthManager,
    dry_run: bool,
) -> list[dict]:
    """Create the registered model + each version + each alias on target."""
    model_fqn = model.get("model_fqn", "")
    obj_key = f"MODEL_{model_fqn}"
    results: list[dict] = []

    start = time.time()
    if dry_run:
        results.append(
            {
                "object_name": obj_key,
                "object_type": "registered_model",
                "status": "skipped",
                "error_message": "dry_run",
                "duration_seconds": time.time() - start,
            }
        )
        return results

    try:
        catalog, schema, name = _parse_fqn(model_fqn)
    except ValueError as exc:
        results.append(
            {
                "object_name": obj_key,
                "object_type": "registered_model",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": time.time() - start,
            }
        )
        return results

    client = auth.target_client
    # 1. Create the model shell
    try:
        client.registered_models.create(
            catalog_name=catalog,
            schema_name=schema,
            name=name,
            comment=model.get("comment"),
            storage_location=model.get("storage_location"),
        )
    except Exception as exc:  # noqa: BLE001
        # IDEMPOTENT: if model already exists, continue to versions + aliases
        if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "registered_model",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": time.time() - start,
                }
            )
            return results

    # Ensure the target-side copy helper notebook is uploaded once up front
    # so repeated per-version copy calls don't each re-upload it.
    artifact_copy_available = True
    try:
        ensure_copy_notebook_on_target(auth)
    except Exception as exc:  # noqa: BLE001
        artifact_copy_available = False
        logger.warning(
            "Could not upload target copy notebook; model artifacts will not "
            "be copied for %s: %s",
            model_fqn,
            exc,
            exc_info=True,
        )

    # 2. Versions — create metadata, then copy artifacts per version
    version_errors: list[str] = []
    total_bytes = 0
    total_files = 0
    for v in model.get("versions", []):
        version_num = v.get("version", "?")
        source_uri = v.get("storage_location") or v.get("source") or ""
        created_version = None
        try:
            created_version = client.model_versions.create(
                catalog_name=catalog,
                schema_name=schema,
                model_name=name,
                source=source_uri,
                run_id=None,
            )
        except Exception as exc:  # noqa: BLE001
            if "already" not in str(exc).lower() or "exists" not in str(exc).lower():
                version_errors.append(f"v{version_num}: {exc}")
            # For idempotent re-runs we still try to fetch the existing version
            # so artifacts can be retried on a subsequent pass.
            try:
                created_version = client.model_versions.get(
                    full_name=f"{catalog}.{schema}.{name}",
                    version=int(version_num),
                )
            except Exception:  # noqa: BLE001
                created_version = None

        # Copy artifact files from source's storage_location to target's
        # allocated storage_location. Skip silently when the source URI is
        # empty, the version create/fetch failed, or the copy notebook
        # couldn't be uploaded — operators see the fallback in error_message.
        target_loc = getattr(created_version, "storage_location", None) if created_version else None
        if artifact_copy_available and source_uri and target_loc:
            try:
                res = run_target_file_copy(
                    auth,
                    src_path=source_uri,
                    dst_path=target_loc,
                    run_name=f"model_artifact_copy__{model_fqn}__v{version_num}",
                )
                total_bytes += int(res.get("bytes_copied", 0))
                total_files += int(res.get("file_count", 0))
            except Exception as exc:  # noqa: BLE001
                version_errors.append(
                    f"v{version_num} artifact copy failed (src={source_uri}): {exc}"
                )

        # 3. Aliases for this version
        for alias in v.get("aliases") or []:
            try:
                client.registered_models.set_alias(
                    full_name=f"{catalog}.{schema}.{name}",
                    alias=alias,
                    version_num=int(version_num),
                )
            except Exception as exc:  # noqa: BLE001
                version_errors.append(f"alias '{alias}' v{version_num}: {exc}")

    duration = time.time() - start
    status_msg_parts: list[str] = [f"{total_files} file(s), {total_bytes} byte(s) copied."]
    if not artifact_copy_available:
        status_msg_parts.append(
            "Artifact copy helper unavailable on target — artifacts NOT copied."
        )
    if version_errors:
        status_msg_parts.append("Errors: " + "; ".join(version_errors[:5]))
    results.append(
        {
            "object_name": obj_key,
            "object_type": "registered_model",
            "status": "validated" if not version_errors else "validation_failed",
            "error_message": " ".join(status_msg_parts),
            "duration_seconds": duration,
        }
    )
    return results


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="registered_model_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d model records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.extend(apply_model(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Models worker complete. %d validated, %d validation_failed, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "validation_failed"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
