"""Target pre-existing state / collision detection (X.4).

Extracted from :mod:`pre_check.pre_check` as a plain module so the detection
logic is unit-testable without notebook / dbutils / spark machinery.

Policy (see :attr:`common.config.MigrationConfig.on_target_collision`):

- ``fail`` (default): any collision reported produces a FAIL row in
  ``pre_check_results`` and the migrate workflow refuses to start (gate in
  :mod:`migrate.orchestrator`).
- ``skip``: each collision produces a WARN row plus a ``skipped_target_exists``
  ``migration_status`` row so workers skip the object on the next migrate
  run, leaving the target object untouched.

This module only performs the **detection** — it does not decide whether
to raise. Callers (``pre_check``) decide, based on the policy, whether the
emitted rows should count as FAIL or WARN.

Supported object types (UC core):
    - ``catalog``
    - ``schema``
    - ``managed_table`` / ``external_table``
    - ``view``
    - ``function``
    - ``volume``

Hive-target collisions are covered too: a Hive source table lands on
target as ``<hive_target_catalog>.<db>.<table>``, and that three-part FQN
is checked via ``tables.get``.

Phase 3 governance objects (shares, recipients, monitors, models,
connections, foreign catalogs, online tables) are intentionally out of
scope for v1 because those workers already run an "already exists"
tolerance (see ``docs/idempotency_audit.md``) that makes them safe to
re-apply against a pre-existing target object. If the v1 fail-fast policy
needs to be strict about those too, extend ``_PROBES`` with the relevant
SDK getters.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from databricks.sdk.errors import NotFound

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# The canonical discovery FQN is the backticked three-part form
# (e.g. "`cat`.`schema`.`name`"). UC SDK ``*.get`` endpoints want the
# dotted form ("cat.schema.name"). The helpers below translate between.
_BACKTICK_SEP = "`.`"


def _fqn_to_parts(fqn: str) -> tuple[str, ...]:
    """Parse a backticked or dotted FQN into its catalog/schema/name parts.

    Accepts the backticked discovery form or the dotted-literal variant.
    Returns a tuple of length 1, 2, or 3 depending on the input.
    """
    stripped = fqn.strip("`")
    if _BACKTICK_SEP in stripped:
        return tuple(stripped.split(_BACKTICK_SEP))
    return tuple(stripped.split("."))


def _catalog_exists(client: WorkspaceClient, name: str) -> bool:
    try:
        client.catalogs.get(name=name)
        return True
    except NotFound:  # 404 / RESOURCE_DOES_NOT_EXIST (covers ResourceDoesNotExist subclass)
        return False


def _schema_exists(client: WorkspaceClient, full_name: str) -> bool:
    try:
        client.schemas.get(full_name=full_name)
        return True
    except NotFound:
        return False


def _table_exists(client: WorkspaceClient, full_name: str) -> bool:
    try:
        client.tables.get(full_name=full_name)
        return True
    except NotFound:
        return False


def _function_exists(client: WorkspaceClient, full_name: str) -> bool:
    try:
        client.functions.get(name=full_name)
        return True
    except NotFound:
        return False


def _volume_exists(client: WorkspaceClient, full_name: str) -> bool:
    try:
        client.volumes.read(name=full_name)
        return True
    except NotFound:
        return False


# Global-namespace securables (review finding #6): a name clash here can hit
# an unrelated pre-existing object owned by someone else, so they ARE probed.
def _connection_exists(client: WorkspaceClient, name: str) -> bool:
    try:
        client.connections.get(name=name)
        return True
    except NotFound:
        return False


def _share_exists(client: WorkspaceClient, name: str) -> bool:
    try:
        client.shares.get(name=name)
        return True
    except NotFound:
        return False


def _recipient_exists(client: WorkspaceClient, name: str) -> bool:
    try:
        client.recipients.get(name=name)
        return True
    except NotFound:
        return False


def _registered_model_exists(client: WorkspaceClient, full_name: str) -> bool:
    try:
        client.registered_models.get(full_name=full_name)
        return True
    except NotFound:
        return False


# Map an object_type (as stored in discovery_inventory) to its probe fn.
# ``view`` is a separate object_type in discovery but UC treats it as a
# table for the purposes of ``tables.get``.
#
# ``mv`` is intentionally NOT probed — materialized views are out of scope
# for v1 collision detection (the mv_st_worker already tolerates
# "already exists" on target per the X.2 audit).
#
# ``st`` (streaming tables) is intentionally NOT probed either —
# streaming tables are hard-excluded from the core migration tool and are
# migrated by the future Stateful Services Phase (separate job). They
# short-circuit in ``mv_st_worker`` with ``skipped_by_stateful_service_
# migration``, so probing them here would report collisions that have no
# downstream effect. See ``docs/stateful_services_phase.md``.
_PROBES: dict[str, Callable[[WorkspaceClient, str], bool]] = {
    "catalog": _catalog_exists,
    "schema": _schema_exists,
    "managed_table": _table_exists,
    "external_table": _table_exists,
    "view": _table_exists,
    "function": _function_exists,
    "volume": _volume_exists,
    # Global-namespace securables (review finding #6).
    "connection": _connection_exists,
    "share": _share_exists,
    "recipient": _recipient_exists,
    "registered_model": _registered_model_exists,
}


# Migrated object types that are intentionally NOT collision-probed, each with
# a reason. Together with ``_PROBES``/``_HIVE_PROBES`` this must cover EVERY
# migrated object type — the coverage guard test (review finding #6) enforces
# that, so a newly-migrated type can't silently ship without a probe decision.
_NOT_PROBED_TYPES: dict[str, str] = {
    "mv": "hard-excluded from the core tool (skipped_by_stateful_service_migration)",
    "st": "hard-excluded from the core tool (skipped_by_stateful_service_migration)",
    "tag": "governance: applied idempotently to existing objects, not a collision",
    "comment": "governance: applied idempotently to existing objects, not a collision",
    "row_filter": "governance: idempotent re-apply via staging/reapply, not a collision",
    "column_mask": "governance: idempotent re-apply via staging/reapply, not a collision",
    "policy": "governance (ABAC): idempotent re-apply, not a collision",
    "monitor": "quality monitor: bound to a table; worker tolerates pre-existing",
    "foreign_catalog": "created from a connection; worker is create-if-missing",
    "provider": "Delta Sharing inbound provider; worker tolerates pre-existing",
    "online_table": "deprecated; migrated to Lakebase synced tables by a separate job",
    "vector_search_index": "stateful; the VS worker create-if-missing handles the endpoint",
}


# Hive source object types land on target as UC tables/views under
# ``hive_target_catalog`` — probe them via ``tables.get`` too.
_HIVE_PROBES: dict[str, Callable[[WorkspaceClient, str], bool]] = {
    "hive_table": _table_exists,
    "hive_view": _table_exists,
    "hive_function": _function_exists,
}


def _normalize_full_name(fqn: str) -> str:
    """Return a dotted ``catalog.schema.name`` form for the UC SDK.

    Discovery rows carry the backticked form (SQL-safe); SDK ``*.get``
    endpoints want plain dotted. Strips backticks once and rejoins.
    """
    parts = _fqn_to_parts(fqn)
    return ".".join(parts)


def _rewrite_hive_fqn(fqn: str, hive_target_catalog: str) -> str:
    """Map a ``hive_metastore.db.t`` FQN to its target UC FQN.

    Mirrors ``migrate.hive_common.rewrite_hive_namespace`` — we can't
    import that here without pulling the full migrate package into
    pre_check's dependency tree. Keep this tiny local copy in lockstep.
    """
    parts = _fqn_to_parts(fqn)
    if len(parts) == 3 and parts[0] == "hive_metastore":
        return f"{hive_target_catalog}.{parts[1]}.{parts[2]}"
    return ".".join(parts)


def unprobed_types_present(discovery_rows: list[dict]) -> list[str]:
    """Return the sorted in-scope object types present in discovery that are
    NOT collision-probed (review finding #6). pre_check surfaces these so the
    operator knows exactly what wasn't checked, instead of assuming the clean
    collision result covers everything."""
    present = {(r.get("object_type") or "") for r in discovery_rows}
    return sorted(t for t in present if t in _NOT_PROBED_TYPES)


def detect_collisions(
    *,
    target_client: WorkspaceClient,
    discovery_rows: list[dict],
    existing_status_keys: set[tuple[str, str]],
    hive_target_catalog: str = "hive_upgraded",
) -> list[dict]:
    """Return collision records for every source object already on target.

    A collision is an object where (a) its target-side FQN already exists
    on the target metastore AND (b) there is no ``migration_status`` row
    keyed by ``(object_type, object_name)``. Clause (b) is the key
    difference from X.2's idempotency-on-resume: if we have a status row,
    the object is ours (or at least previously tracked by us), so we
    don't re-probe it as a collision.

    Each record has shape::

        {
            "object_type": "managed_table",
            "source_fqn": "`cat`.`schema`.`t`",
            "target_fqn": "cat.schema.t",
            "source_type": "uc",
        }

    Arguments:
        target_client: SDK client bound to the target workspace.
        discovery_rows: raw ``discovery_inventory`` rows (dicts with
            ``object_name`` / ``object_type`` / ``source_type`` keys).
        existing_status_keys: set of ``(object_type, object_name)`` tuples
            already in ``migration_status``. These objects are ours (or at
            least previously marked by us) so we skip the collision probe
            for them — that's what makes collision detection additive to
            X.2's idempotency guarantees.
        hive_target_catalog: config value used to map
            ``hive_metastore.db.t`` source FQNs to their target UC FQN.
    """
    collisions: list[dict] = []
    for row in discovery_rows:
        object_type = row.get("object_type") or ""
        object_name = row.get("object_name") or ""
        source_type = (row.get("source_type") or "uc").lower()
        if not object_type or not object_name:
            continue
        # Skip objects we've already touched — the status row (from a
        # prior run) tells us we own the target object, not someone else.
        if (object_type, object_name) in existing_status_keys:
            continue

        if source_type == "hive":
            probe = _HIVE_PROBES.get(object_type)
            if probe is None:
                continue
            target_fqn = _rewrite_hive_fqn(object_name, hive_target_catalog)
        else:
            probe = _PROBES.get(object_type)
            if probe is None:
                continue
            target_fqn = _normalize_full_name(object_name)

        # Fail CLOSED (review finding #10): the probe maps only a genuine
        # NotFound to "absent". Any OTHER error (PermissionDenied, transient,
        # auth) must NOT be silently read as "doesn't exist → safe" — we can't
        # confirm the target is clear, so we emit a check-failure record that
        # the caller turns into a FAIL (never a silent skip).
        try:
            exists = probe(target_client, target_fqn)
        except Exception as exc:  # noqa: BLE001 — unexpected probe error → fail closed
            collisions.append(
                {
                    "object_type": object_type,
                    "source_fqn": object_name,
                    "target_fqn": target_fqn,
                    "source_type": source_type,
                    "check_failed": True,
                    "error": str(exc),
                }
            )
            continue
        if exists:
            collisions.append(
                {
                    "object_type": object_type,
                    "source_fqn": object_name,
                    "target_fqn": target_fqn,
                    "source_type": source_type,
                    "check_failed": False,
                }
            )
    return collisions


def build_skip_status_rows(collisions: list[dict]) -> list[dict]:
    """Build ``migration_status`` rows for the ``skip`` policy.

    Each row stamps ``status = skipped_target_exists`` for the (object_type,
    object_name) pair of the source object so ``get_pending_objects``
    filters the row out on the next migrate run, short-circuiting the
    worker before it touches the target object.

    Check-failure records (``check_failed=True``) are NEVER turned into skip
    rows — skipping an object whose target state we couldn't verify is exactly
    the unsafe outcome. The caller surfaces those as a FAIL instead.
    """
    return [
        {
            "object_name": c["source_fqn"],
            "object_type": c["object_type"],
            "status": "skipped_target_exists",
            "error_message": (
                f"Target FQN {c['target_fqn']} pre-exists; skipped by "
                f"on_target_collision=skip"
            ),
            "job_run_id": None,
            "task_run_id": None,
            "source_row_count": None,
            "target_row_count": None,
            "duration_seconds": None,
        }
        for c in collisions
        if not c.get("check_failed")
    ]
