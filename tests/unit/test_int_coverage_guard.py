"""Coverage guard for the integration suite (mirrors the collision-probe
``unprobed_types_present`` guard).

Enforces that every migrated object type is either ground-truth-covered or
explicitly exempt-with-reason, and likewise every in-scope migrate job has a
re-run (idempotency) leg or an exemption. This is the structural fix for why
20 defects slipped past the integration tests: nothing forced each migrated
type to be verified against reality, or each job to be re-run.

When a new migrated type is added to the orchestrator, this test fails until
someone records a decision in ``tests/integration/coverage_manifest.py`` —
wire a ground-truth assertion (add to GROUND_TRUTH_COVERED) or justify the
exemption (add to COVERAGE_EXEMPT).
"""

from __future__ import annotations

from tests.integration.coverage_manifest import (
    ALL_MIGRATED_TYPES,
    COVERAGE_EXEMPT,
    GROUND_TRUTH_COVERED,
    RERUN_COVERED_JOBS,
    RERUN_EXEMPT,
    RERUN_JOBS_IN_SCOPE,
)


def test_every_migrated_type_is_covered_or_exempt():
    accounted = GROUND_TRUTH_COVERED | set(COVERAGE_EXEMPT)
    missing = ALL_MIGRATED_TYPES - accounted
    assert not missing, (
        f"Migrated types with no integration coverage decision: {sorted(missing)}. "
        f"Add each to GROUND_TRUTH_COVERED (with a live assert_target_matches_"
        f"ground_truth) or COVERAGE_EXEMPT (with a reason) in coverage_manifest.py."
    )


def test_covered_and_exempt_are_disjoint():
    both = GROUND_TRUTH_COVERED & set(COVERAGE_EXEMPT)
    assert not both, f"Types both covered and exempt (remove from exempt): {sorted(both)}"


def test_exempt_reasons_are_nonempty():
    blank = [t for t, r in COVERAGE_EXEMPT.items() if not (r or "").strip()]
    assert not blank, f"Exempt types missing a reason: {sorted(blank)}"


def test_exempt_entries_are_real_migrated_types():
    # Guards against stale exemptions lingering after a type is removed/renamed.
    stale = set(COVERAGE_EXEMPT) - set(ALL_MIGRATED_TYPES)
    assert not stale, f"Exempt entries no longer migrated (remove them): {sorted(stale)}"


def test_every_in_scope_job_has_rerun_decision():
    accounted = RERUN_COVERED_JOBS | set(RERUN_EXEMPT)
    missing = RERUN_JOBS_IN_SCOPE - accounted
    assert not missing, (
        f"In-scope jobs with no idempotency re-run decision: {sorted(missing)}. "
        f"Add a re-run leg (RERUN_COVERED_JOBS) or an exemption (RERUN_EXEMPT)."
    )
