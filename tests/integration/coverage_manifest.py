"""Integration-test coverage manifest — single source of truth for which
migrated object types have a real TARGET-side ground-truth assertion and
which are (still) exempt, with a reason.

Why this exists
---------------
The independent test found that 20 defects (incl. a CRITICAL silent
data-loss, #21) sailed through the integration suite because assertions
trusted the tool's self-reported ``migration_status`` counts and no test
ever re-ran a job. This manifest makes coverage *explicit and enforced*:
``tests/unit/test_int_coverage_guard.py`` fails if any migrated object type
is neither ground-truth-covered nor listed in ``COVERAGE_EXEMPT``. So when
someone adds a new migrated type to the orchestrator, CI forces a decision —
wire a ground-truth assertion or record why it's exempt.

Mirrors the ``pre_check.collision_detection._NOT_PROBED_TYPES`` /
``unprobed_types_present`` guard pattern already used for collision probes.

As each type is hardened with ``assert_target_matches_ground_truth`` in the
live int tests, MOVE it from ``COVERAGE_EXEMPT`` into ``GROUND_TRUTH_COVERED``.
"""

from __future__ import annotations

from migrate.orchestrator import BATCHED_TYPES, LIST_TYPES

# Migrated types not present in the orchestrator's BATCHED_/LIST_TYPES
# constants but still produced by the tool.
UC_EXTRA_TYPES = ("comment", "grant")
HIVE_TYPES = (
    "hive_external",
    "hive_managed_nondbfs",
    "hive_managed_dbfs_root",
    "hive_view",
    "hive_function",
    "hive_grant",
)

# The authoritative set every integration run should account for.
ALL_MIGRATED_TYPES: frozenset[str] = frozenset(
    set(BATCHED_TYPES) | set(LIST_TYPES) | set(UC_EXTRA_TYPES) | set(HIVE_TYPES)
)

# Types with a live TARGET-side ground-truth assertion (rows/values compared
# to what the seed actually created — NOT migration_status). Grows as the
# int tests are hardened.
GROUND_TRUTH_COVERED: frozenset[str] = frozenset()

# Types deliberately not ground-truth-checked, each with a reason. Shrinks as
# hardening proceeds. New migrated types MUST land here or in COVERED.
COVERAGE_EXEMPT: dict[str, str] = {
    # De-scoped from the tool's docs/scope (finding #1) — optional stateful
    # services; not part of the core migration contract under test.
    "online_table": "de-scoped (#1) — optional stateful service",
    "vector_search_index": "de-scoped (#1) — optional stateful service",
    "lfc_pipeline": "de-scoped (#1) — optional stateful service",
    "provider": "de-scoped (#1) — Delta Sharing provider, not core",
    # Pending ground-truth hardening — tracked findings drive these.
    "managed_table": "pending #21/#16 ground-truth (RLS/CM data loss) hardening",
    "external_table": "pending ground-truth hardening",
    "volume": "pending #18 external-volume ground-truth hardening",
    "mv": "pending ground-truth hardening",
    "st": "pending ground-truth hardening",
    "function": "pending ground-truth hardening",
    "view": "pending #19 dependent-view ground-truth hardening",
    "tag": "pending #7 ground-truth hardening (catalog-tag dedup)",
    "row_filter": "pending #21 ground-truth hardening (re-apply + unfiltered data)",
    "column_mask": "pending #21 ground-truth hardening (re-apply + unmasked data)",
    "policy": "pending ground-truth hardening",
    "monitor": "pending ground-truth hardening",
    "registered_model": "pending #17 ground-truth hardening (version copy)",
    "connection": "pending #6/#22 ground-truth hardening (connection discovery)",
    "foreign_catalog": "pending #6/#22 ground-truth hardening",
    "share": "pending ground-truth hardening",
    "recipient": "pending ground-truth hardening",
    "comment": "pending ground-truth hardening",
    "grant": "pending #14 ground-truth hardening (owner/grant correctness)",
    "hive_external": "pending ground-truth hardening",
    "hive_managed_nondbfs": "pending ground-truth hardening",
    "hive_managed_dbfs_root": "pending #9 ground-truth hardening (DBFS-root rehome)",
    "hive_view": "pending #9 dependent-view ground-truth hardening",
    "hive_function": "pending ground-truth hardening",
    "hive_grant": "pending #10/#13 ground-truth hardening (ownership transfer)",
}

# Migrate jobs that must have an idempotency re-run leg (findings #8/#12/#20).
RERUN_COVERED_JOBS: frozenset[str] = frozenset({"migrate_hive"})
RERUN_JOBS_IN_SCOPE: frozenset[str] = frozenset(
    {"discovery", "migrate_uc", "migrate_hive", "migrate_governance"}
)
RERUN_EXEMPT: dict[str, str] = {
    "discovery": "pending #8 re-run leg (dedup MERGE fixed in code; live re-run assert pending)",
    "migrate_uc": "pending #20 re-run leg (setup_sharing idempotency)",
    "migrate_governance": "pending re-run leg",
}
