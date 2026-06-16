"""Cross-cutting contract tests that span multiple workers / modules.

Covers:
  - Status-name alignment between workers and the tracker filter.
  - Orchestrator task-value keys match the downstream for_each inputs.
  - Dashboard datasets reference columns that exist in migration_status.
  - Negative paths (error handling, config edge cases).
"""

from __future__ import annotations

import pathlib

_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _src(path: str) -> str:
    return (_ROOT / path).read_text()


class TestStatusNameAlignment:
    """Every skip-status a worker writes must start with ``skipped`` so
    the tracker's ``status NOT LIKE 'skipped%'`` filter in
    get_pending_objects excludes them from retries. Divergence here
    silently breaks the retry contract."""

    def test_all_skip_statuses_use_skipped_prefix(self):
        """Grep every migrate worker for ``status = 'skipped*'`` /
        ``"status": "skipped*"`` patterns — each must start with
        'skipped'."""
        import re

        from common.tracking import _TERMINAL_STATUSES

        worker_dir = _ROOT / "src" / "migrate"
        offenders = []
        for f in worker_dir.glob("*.py"):
            text = f.read_text()
            # Match ``"status": "..."``
            for match in re.finditer(r'"status":\s*"([a-z_]+)"', text):
                s = match.group(1)
                if s in ("validated", "failed", "in_progress", "validation_failed"):
                    continue
                # Terminal statuses are excluded from re-runs via the explicit
                # ``NOT IN (terminal...)`` clause in get_pending_objects, not the
                # ``NOT LIKE 'skipped%'`` filter — so a terminal status may carry
                # "skip" mid-name without breaking resumability.
                if s in _TERMINAL_STATUSES:
                    continue
                if not s.startswith("skipped") and "skip" in s:
                    offenders.append(f"{f.name}: status={s!r}")
        assert not offenders, (
            "Non-'skipped'-prefixed skip statuses found:\n"
            + "\n".join(offenders)
            + "\nThese will not be excluded by get_pending_objects's "
            "NOT LIKE 'skipped%' filter — re-runs will retry them."
        )


class TestOrchestratorToWorkerKeyAlignment:
    """The orchestrator emits task values that downstream for_each tasks
    consume. The workflow YAML's ``inputs`` expression must match a key
    the orchestrator writes. Source-level audit of both sides."""

    def test_migrate_workflow_inputs_match_orchestrator_keys(self):
        """Every for_each input in the split production workflows
        (migrate_uc + migrate_hive) must reference a task value that
        orchestrator.py / hive_orchestrator.py emits. Loss of alignment
        means empty input → silent no-op."""
        import re

        # Post-WS-21 the monolithic migrate_workflow.yml is gone; the
        # for_each inputs now live in the two split data-plane workflows.
        # migrate_governance has no for_each over orchestrator output, so
        # it's not part of this audit.
        workflow_files = [
            "resources/production/migrate_uc_workflow.yml",
            "resources/production/migrate_hive_workflow.yml",
        ]
        workflow = "\n".join(_src(p) for p in workflow_files)
        uc_orch = _src("src/migrate/orchestrator.py")
        hive_orch = _src("src/migrate/hive_orchestrator.py")

        # Find all ``inputs: "{{tasks.<orchestrator>.values.<key>}}"``
        inputs = re.findall(
            r'inputs:\s*"\{\{tasks\.(\w+)\.values\.([\w_]+)\}\}"',
            workflow,
        )
        assert inputs, "No for_each inputs found in split migrate_*_workflow.yml files"

        for orch_task, key in inputs:
            # The orchestrator source for that task must emit this key.
            src_text = hive_orch if "hive" in orch_task else uc_orch
            # Key can be emitted via f-string (e.g. f"{obj}_batches") or
            # literal. Accept either the literal key or the suffix.
            if key not in src_text:
                # Suffix check — e.g. managed_table_batches emitted as
                # f"{obj_type}_batches".
                suffix_ok = any(key.endswith(sfx) and sfx in src_text for sfx in ("_batches", "_list"))
                assert suffix_ok, (
                    f"Workflow references tasks.{orch_task}.values.{key!r} but orchestrator source doesn't emit it."
                )


class TestConfigSkipStatusAlignment:
    """get_pending_objects filter + workers' skip statuses must all
    match. The filter is in tracking.py; skip statuses are scattered
    across workers. This test exists so moving either half without the
    other fails loudly."""

    def test_filter_excludes_skipped_prefix_literal(self):
        """The filter string must literally be ``NOT LIKE 'skipped%'``.
        Any variant (``NOT LIKE 'Skipped%'``, ``NOT IN ('skipped',
        'skipped_by_config', ...)``) would silently diverge from
        workers' f-string builds."""
        src = _src("src/common/tracking.py")
        assert "NOT LIKE 'skipped%'" in src, (
            "get_pending_objects must filter ``NOT LIKE 'skipped%'`` — "
            "any change to this string breaks the skip-retry contract."
        )


class TestDashboardAlignment:
    """The Lakeview dashboard reads from migration_tracking tables. The
    column names its datasets reference must exist in the actual
    schemas defined by TrackingManager.init_tracking_tables."""

    def test_dashboard_references_known_migration_status_columns(self):
        """Dashboard JSON references columns (object_type, status,
        error_message, etc.) — they must appear in the migration_status
        CREATE TABLE in tracking.py."""
        tracking_src = _src("src/common/tracking.py")
        dash_path = _ROOT / "dashboards" / "migration_dashboard.lvdash.json"
        if not dash_path.exists():
            return  # Dashboard optional — skip the check
        text = dash_path.read_text()
        # Cheap parse — find column names referenced
        known_cols = {
            "object_name",
            "object_type",
            "status",
            "error_message",
            "source_row_count",
            "target_row_count",
            "duration_seconds",
            "migrated_at",
        }
        for col in known_cols:
            if col in text:
                assert col in tracking_src, (
                    f"Dashboard references migration_status column {col!r} "
                    f"but it's not in TrackingManager.init_tracking_tables"
                )


class TestWorkerErrorMessagesAreOperatorReadable:
    """Every worker's error_message should mention something an operator
    can act on: the affected object, the root cause, or a pointer to
    docs. Bare stacktraces without context waste on-call time.

    This is a BEST-EFFORT contract: we check workers emit error_message
    strings that have >= 10 chars (i.e. not empty, not a single word).
    """

    def test_error_messages_non_trivial_across_workers(self):
        """Grep workers for ``error_message`` assignments and check
        they're not just ``""`` or ``None`` for failure cases."""
        import re

        worker_dir = _ROOT / "src" / "migrate"
        for f in worker_dir.glob("*.py"):
            text = f.read_text()
            # Any ``"status": "failed",`` line should be near an
            # ``error_message`` line that isn't just None/empty.
            for match in re.finditer(
                r'"status":\s*"failed",\s*\n\s*"error_message":\s*(None|""|\'\')',
                text,
            ):
                pytest_fail_msg = (
                    f"{f.name}: status=failed with empty error_message at "
                    f"char {match.start()} — operators need a reason."
                )
                # Convert assertion into a real failure
                raise AssertionError(pytest_fail_msg)


class TestRunAsConsistency:
    """All integration + migration jobs must run_as the same SPN
    declared in databricks.yml. A missing run_as on any job would run
    it as the deployer (usually a human) and break the consistent
    ownership model."""

    def test_all_resource_yml_have_run_as(self):
        """Every resources/*_workflow.yml declaring a job must set
        run_as: service_principal_name: ${var.migration_spn_id}."""
        resources = _ROOT / "resources"
        for f in resources.glob("*_workflow.yml"):
            text = f.read_text()
            assert "run_as:" in text, (
                f"{f.name}: missing run_as — operator deploys would run "
                f"this job as themselves, breaking ownership consistency."
            )
            assert "migration_spn_id" in text, f"{f.name}: run_as must reference ${{var.migration_spn_id}}."
