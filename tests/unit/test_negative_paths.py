"""Unit tests for the negative-path integration suite (integration X.3).

Two artefacts are covered:

- ``tests/integration/test_negative_paths.py`` — assertion notebook that
  verifies each scenario's trigger task failed for the expected reason
  (or succeeded as no-op for X.3.3). Source-level checks lock in the
  per-scenario expected-substring contract.
- ``resources/negative_paths_integration_test_workflow.yml`` — YAML
  that wires up the three scenarios. Shape checks make sure all three
  chains exist, run sequentially (required because they share
  ``config.yaml``), and each has a ``run_if: ALL_DONE`` assertion task.

These are source-level regressions — the notebook runs its main body at
import and requires ``dbutils`` / a live job run to actually execute.
"""

from __future__ import annotations

import pathlib

import yaml


def _assertion_src() -> str:
    path = (
        pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "test_negative_paths.py"
    )
    return path.read_text()


def _workflow_yaml() -> dict:
    path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "resources"
        / "negative_paths_integration_test_workflow.yml"
    )
    return yaml.safe_load(path.read_text())


class TestAssertionNotebookSource:
    """Source-level guards on the shared assertion notebook."""

    def test_declares_scenario_and_trigger_task_widgets(self):
        src = _assertion_src()
        assert 'dbutils.widgets.text("scenario"' in src
        assert 'dbutils.widgets.text("trigger_task_key"' in src

    def test_fetches_current_run_via_notebook_context(self):
        """The assertion runs inside the same job run as the trigger.
        First attempt used ``ctx.currentRunId()`` / ``rootRunId()`` but
        both hit Py4J whitelist errors on the dev runtime. Switched to
        a ``parent_run_id`` widget populated via DAB's
        ``{{job.run_id}}`` template expansion at task launch — that
        path is reliable across runtimes."""
        src = _assertion_src()
        assert 'dbutils.widgets.text("parent_run_id"' in src, (
            "Assertion must declare a ``parent_run_id`` widget that the "
            "workflow yml populates via ``{{job.run_id}}``."
        )
        assert "currentRunId" not in src, (
            "Must NOT use ctx.currentRunId() — Py4J security blocks it "
            "on some runtimes. Use parent_run_id widget instead."
        )
        assert "rootRunId" not in src, (
            "Must NOT use ctx.rootRunId() — Py4J security blocks it too."
        )

    def test_inspects_trigger_task_state_and_result(self):
        src = _assertion_src()
        # Must check result_state (SUCCESS/FAILED), not just life_cycle_state.
        # A task can finish with life_cycle=TERMINATED + result=FAILED;
        # checking only life_cycle would miss real failures.
        assert "result_state" in src
        assert "get_run_output" in src, (
            "Must fall back to get_run_output for the full error text — "
            "task state_message is truncated on some runtimes."
        )

    def test_per_scenario_expected_substrings(self):
        """Each scenario asserts a specific substring of the failure
        message so we don't accept 'any failure' as a pass."""
        src = _assertion_src()
        # X.3.1 / X.3.2 must reference pre_check's check-name keys
        assert "check_source_auth" in src
        assert "check_target_auth" in src

    def test_x33_asserts_success_not_failure(self):
        """X.3.3 is a no-op, not an error. The assertion must check for
        SUCCESS — treating it as a failure scenario would hide regressions
        where the migrate unexpectedly crashes on empty scope."""
        src = _assertion_src()
        assert "_assert_succeeded_as_noop" in src
        # And must be wired to the X.3.3 branch
        assert 'scenario == "X.3.3"' in src


class TestNegativePathWorkflowShape:
    """Shape assertions on the negative-paths workflow YAML."""

    def test_defines_single_job(self):
        data = _workflow_yaml()
        jobs = data["resources"]["jobs"]
        assert list(jobs) == ["negative_paths_integration_test"]

    def test_has_three_trigger_tasks_one_per_scenario(self):
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        task_keys = {t["task_key"] for t in tasks}
        for expected in (
            "trigger_X31_pre_check",
            "trigger_X32_pre_check",
            "trigger_X33_migrate",
        ):
            assert expected in task_keys, f"missing trigger task {expected!r}"

    def test_each_scenario_has_assert_task_with_run_if_all_done(self):
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        by_key = {t["task_key"]: t for t in tasks}
        for scenario_num in ("X31", "X32", "X33"):
            assert_key = f"assert_{scenario_num}"
            assert assert_key in by_key, f"missing {assert_key}"
            task = by_key[assert_key]
            # ALL_DONE is critical — the trigger often FAILs, and without
            # run_if the assertion would be skipped (SKIPPED_UPSTREAM_FAILED)
            # and the scenario would silently not be validated.
            assert task.get("run_if") == "ALL_DONE", (
                f"{assert_key} must set run_if: ALL_DONE so it runs even when "
                "the trigger task fails (the common case for failure scenarios)."
            )

    def test_chains_run_sequentially(self):
        """The three chains rewrite the same ``config.yaml`` and a parallel
        layout would race. Each subsequent chain's setup must depend on
        the previous chain's assertion task."""
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        by_key = {t["task_key"]: t for t in tasks}

        def _deps(task_key: str) -> set[str]:
            return {d["task_key"] for d in (by_key[task_key].get("depends_on") or [])}

        assert "assert_X31" in _deps("setup_X32_config"), (
            "setup_X32_config must depend on assert_X31 — chains share config.yaml."
        )
        assert "assert_X32" in _deps("setup_X33_config")

    def test_teardown_depends_on_every_task(self):
        """Teardown restores config.yaml from the backup; it must wait for
        every chain so the restore doesn't race an in-flight trigger."""
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        by_key = {t["task_key"]: t for t in tasks}
        teardown = by_key["teardown_config"]
        dep_keys = {d["task_key"] for d in teardown.get("depends_on") or []}
        for k in by_key:
            if k == "teardown_config":
                continue
            assert k in dep_keys, f"teardown must depend on {k!r} to avoid racing the config restore"
        assert teardown.get("run_if") == "ALL_DONE"

    def test_injection_widgets_wired_to_correct_scenarios(self):
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        by_key = {t["task_key"]: t for t in tasks}
        assert by_key["setup_X31_config"]["notebook_task"]["base_parameters"].get("inject_bad_spn_id") == "true"
        assert (
            by_key["setup_X32_config"]["notebook_task"]["base_parameters"].get("inject_unreachable_target")
            == "true"
        )

    def test_x33_sets_both_scopes_false(self):
        data = _workflow_yaml()
        tasks = data["resources"]["jobs"]["negative_paths_integration_test"]["tasks"]
        by_key = {t["task_key"]: t for t in tasks}
        params = by_key["setup_X33_config"]["notebook_task"]["base_parameters"]
        assert params["include_uc"] == "false"
        assert params["include_hive"] == "false"
