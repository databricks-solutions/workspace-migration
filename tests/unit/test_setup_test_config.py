"""Unit tests for ``tests/integration/setup_test_config.py`` and its
``apply_integration_overrides`` helper — the notebook that rewrites
the workspace copy of ``config.yaml`` per-workflow and backs up the
pre-test version for teardown to restore.

The notebook runs its main block at import (similar to
hive_orchestrator), so we use source-level regression checks plus
behavioral tests against the extracted pure helper. S.14 adds a
UC → Hive → Negative-paths cycle test to catch config override
leaks between successive integration runs.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.integration._config_override import apply_integration_overrides


def _source_text() -> str:
    path = pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "setup_test_config.py"
    return path.read_text()


def _helper_source_text() -> str:
    path = pathlib.Path(__file__).resolve().parents[2] / "tests" / "integration" / "_config_override.py"
    return path.read_text()


# ---------------------------------------------------------------
# Representative baseline config.yaml — matches the structure of
# ``config.example.yaml`` / the shipped ``config.yaml`` placeholders.
# Used as the pristine baseline for every simulated invocation.
# ---------------------------------------------------------------
def _baseline_config() -> dict:
    return {
        "source_workspace_url": "https://adb-SOURCE.azuredatabricks.net",
        "target_workspace_url": "https://adb-TARGET.azuredatabricks.net",
        "spn_client_id": "REPLACE_WITH_APPLICATION_ID",
        "spn_secret_scope": "migration",
        "spn_secret_key": "spn-secret",
        "catalog_filter": "",
        "schema_filter": "",
        "dry_run": False,
        "batch_size": 50,
        "tracking_catalog": "migration_tracking",
        "tracking_schema": "cp_migration",
        "migrate_hive_dbfs_root": False,
        "hive_dbfs_target_path": "operator-provided-adls-url",
        "hive_target_catalog": "hive_upgraded",
        "iceberg_strategy": "",
        "rls_cm_strategy": "",
    }


# ---------------------------------------------------------------
# Overrides that mirror the integration workflow YAMLs. If these
# drift from ``resources/integration_tests/*_integration_test_workflow.yml`` the
# ``TestWorkflowOverrideContractsMatchYaml`` suite will fail loudly.
# ---------------------------------------------------------------
UC_OVERRIDES = dict(
    iceberg_strategy="ddl_replay",
    rls_cm_strategy="staging_copy",
    migrate_hive_dbfs_root=False,
    hive_dbfs_target_path="",
    batch_size_raw="",
    catalog_filter_raw="",
)

HIVE_OVERRIDES = dict(
    iceberg_strategy="",
    rls_cm_strategy="",
    migrate_hive_dbfs_root=True,
    hive_dbfs_target_path="",
    batch_size_raw="10",
    catalog_filter_raw="integration_test_src",
)

NEG_X31_OVERRIDES = dict(
    iceberg_strategy="",
    rls_cm_strategy="",
    migrate_hive_dbfs_root=False,
    hive_dbfs_target_path="",
    batch_size_raw="",
    catalog_filter_raw="",
    inject_bad_spn_id=True,
)


class TestSetupTestConfigSourceGuards:
    """Source-level checks — the notebook must:
    - back up config.yaml before overriding (so teardown can restore)
    - restore from the backup on subsequent invocations so per-workflow
      overrides stay authoritative (S.14)
    - write the file back via yaml.safe_dump
    """

    def test_writes_backup_to_pre_integration_test_bak(self):
        src = _source_text()
        assert ".pre-integration-test.bak" in src, (
            "setup_test_config must save the pre-test config.yaml to a "
            "``.pre-integration-test.bak`` sibling so teardown can restore."
        )

    def test_uses_yaml_safe_dump(self):
        src = _source_text()
        assert "yaml.safe_dump" in src, (
            "Must use yaml.safe_dump (not yaml.dump) to avoid !!python/"
            "object tags when writing customer-visible config."
        )

    def test_backup_before_override(self):
        """Backup must precede the file write — otherwise a mid-notebook
        crash leaves config.yaml corrupted with no way to restore."""
        src = _source_text()
        backup_idx = src.index(".pre-integration-test.bak")
        override_idx = src.index("yaml.safe_dump")
        assert backup_idx < override_idx, "Backup copy must happen BEFORE the file is overwritten."

    def test_restores_from_backup_before_overriding_when_backup_exists(self):
        """S.14: if a prior run / chained task already captured the
        backup, this invocation must RESTORE from it (not merge on top)
        so the override below starts from the pristine baseline.
        Without this, UC → Hive back-to-back runs (or chained
        negative-paths scenarios in one workflow) silently inherit
        UC-only keys the Hive workflow never opts into."""
        src = _source_text()
        # The restore must live inside the "backup already exists" branch.
        assert "shutil.copy2(backup_path, config_path)" in src, (
            "setup_test_config must restore config.yaml from the backup "
            "on subsequent invocations so per-workflow overrides remain "
            "authoritative (S.14)."
        )

    def test_delegates_transformation_to_helper(self):
        """The pure-logic transform lives in ``_config_override`` so
        unit tests can exercise it without dbutils / yaml I/O."""
        src = _source_text()
        assert "from tests.integration._config_override import apply_integration_overrides" in src


class TestSetupTestConfigHelperSourceGuards:
    """Source-level checks on the extracted helper."""

    def test_preserves_existing_hive_dbfs_target_path_when_empty_param(self):
        """Env-specific fields like hive_dbfs_target_path are operator-
        configured once post-deploy. The helper must only override
        them when the corresponding task param is non-empty, preserving
        the operator's value when the param is the default empty
        string."""
        src = _helper_source_text()
        assert "if hive_dbfs_target_path:" in src

    def test_no_legacy_drop_and_restore_residue(self):
        """Path A removed drop_and_restore + the maintenance-window
        consent flag — guard against accidental re-introduction."""
        src = _helper_source_text()
        assert "drop_and_restore" not in src
        assert "rls_cm_maintenance_window_confirmed" not in src


class TestNegativePathInjectionsSource:
    """Integration X.3: source-level checks for the negative-path
    injection widgets. They must:

    - Declare both injection widgets (bad_spn_id / unreachable_target)
      with default "false" so normal UC / Hive integration workflows
      are unaffected.
    - Apply each injection AFTER the scope overrides so the corrupted
      config is what downstream tasks read.
    """

    def test_declares_two_injection_widgets(self):
        src = _source_text()
        for name in ("inject_bad_spn_id", "inject_unreachable_target"):
            assert f'dbutils.widgets.text("{name}", "false")' in src, (
                f"Must declare injection widget {name!r} with default 'false' "
                "so the widget is optional and normal workflows are unaffected."
            )

    def test_no_legacy_bad_rls_cm_widget(self):
        """Path A removed the X.3.4 (bad_rls_cm) scenario; the widget
        must be gone."""
        src = _source_text()
        assert "inject_bad_rls_cm" not in src

    def test_bad_spn_injection_overwrites_spn_client_id(self):
        src = _helper_source_text()
        assert 'cfg["spn_client_id"] = "00000000-0000-0000-0000-000000000000"' in src, (
            "bad_spn injection must overwrite spn_client_id with a "
            "well-formed-but-wrong UUID so the SDK accepts the shape but "
            "token exchange fails at pre_check."
        )

    def test_unreachable_target_injection_overwrites_target_url(self):
        src = _helper_source_text()
        assert 'cfg["target_workspace_url"]' in src, (
            "unreachable_target injection must overwrite target_workspace_url."
        )


class TestApplyIntegrationOverridesBehavior:
    """Behavioral tests on the pure helper."""

    def test_uc_overrides_shape(self):
        cfg = apply_integration_overrides(_baseline_config(), **UC_OVERRIDES)
        assert cfg["iceberg_strategy"] == "ddl_replay"
        assert cfg["rls_cm_strategy"] == "staging_copy"
        assert cfg["migrate_hive_dbfs_root"] is False
        # batch_size + catalog_filter unset in UC workflow → baseline preserved.
        assert cfg["batch_size"] == 50
        assert cfg["catalog_filter"] == ""
        # hive_dbfs_target_path empty param → baseline operator-value kept.
        assert cfg["hive_dbfs_target_path"] == "operator-provided-adls-url"

    def test_hive_overrides_shape(self):
        cfg = apply_integration_overrides(_baseline_config(), **HIVE_OVERRIDES)
        assert cfg["iceberg_strategy"] == ""
        assert cfg["rls_cm_strategy"] == ""
        assert cfg["migrate_hive_dbfs_root"] is True
        assert cfg["batch_size"] == 10
        assert cfg["catalog_filter"] == ["integration_test_src"]
        assert cfg["hive_dbfs_target_path"] == "operator-provided-adls-url"

    def test_helper_deep_copies_baseline(self):
        """Caller's baseline dict must survive untouched so it can be
        reused for the next scenario in a chain."""
        baseline = _baseline_config()
        snapshot = {
            "batch_size": baseline["batch_size"],
            "catalog_filter": baseline["catalog_filter"],
            "rls_cm_strategy": baseline["rls_cm_strategy"],
        }
        _ = apply_integration_overrides(baseline, **UC_OVERRIDES)
        assert baseline["batch_size"] == snapshot["batch_size"]
        assert baseline["catalog_filter"] == snapshot["catalog_filter"]
        assert baseline["rls_cm_strategy"] == snapshot["rls_cm_strategy"]

    def test_inject_bad_spn_overwrites_client_id(self):
        cfg = apply_integration_overrides(
            _baseline_config(), **dict(NEG_X31_OVERRIDES)
        )
        assert cfg["spn_client_id"] == "00000000-0000-0000-0000-000000000000"
        # Non-injected fields stay pristine.
        assert cfg["target_workspace_url"] == "https://adb-TARGET.azuredatabricks.net"

    def test_inject_unreachable_target_overwrites_url(self):
        overrides = dict(NEG_X31_OVERRIDES, inject_bad_spn_id=False, inject_unreachable_target=True)
        cfg = apply_integration_overrides(_baseline_config(), **overrides)
        assert cfg["target_workspace_url"] == "https://adb-0000000000000000.0.azuredatabricks.net"
        # SPN stays pristine when only unreachable_target is injected.
        assert cfg["spn_client_id"] == "REPLACE_WITH_APPLICATION_ID"

    def test_batch_size_invalid_string_raises(self):
        bad = dict(UC_OVERRIDES, batch_size_raw="not-a-number")
        with pytest.raises(ValueError, match="batch_size must be an integer"):
            apply_integration_overrides(_baseline_config(), **bad)

    def test_batch_size_coerces_below_one_to_one(self):
        ok = dict(UC_OVERRIDES, batch_size_raw="0")
        cfg = apply_integration_overrides(_baseline_config(), **ok)
        assert cfg["batch_size"] == 1

    def test_catalog_filter_parses_csv_and_strips_empties(self):
        ok = dict(UC_OVERRIDES, catalog_filter_raw=" a , b , , c ")
        cfg = apply_integration_overrides(_baseline_config(), **ok)
        assert cfg["catalog_filter"] == ["a", "b", "c"]


class TestOverrideCycleCleanSlate:
    """S.14: the headline test.

    When UC runs then Hive runs back-to-back (realistic CI pattern),
    the Hive run's override must start from the pristine baseline
    — NEVER from the UC run's post-override config. Otherwise UC-only
    keys leak:
      - ``rls_cm_strategy=staging_copy``
      - ``iceberg_strategy=ddl_replay``

    And similarly for the negative-paths workflow whose chained
    setup_X3N tasks all share a single ``config.yaml``.

    The contract: each ``apply_integration_overrides`` call MUST be
    fed the pristine baseline, not a previous run's result.
    ``setup_test_config.py`` enforces this by restoring
    ``config.yaml`` from ``.pre-integration-test.bak`` at the top of
    every invocation. This test pins that property at the helper
    level: given the baseline each time, per-workflow overrides are
    authoritative and independent.
    """

    def test_uc_then_hive_clean_slate(self):
        baseline = _baseline_config()
        uc_cfg = apply_integration_overrides(baseline, **UC_OVERRIDES)
        # Sanity: UC run set the UC-specific keys.
        assert uc_cfg["rls_cm_strategy"] == "staging_copy"
        assert uc_cfg["iceberg_strategy"] == "ddl_replay"

        hive_cfg = apply_integration_overrides(baseline, **HIVE_OVERRIDES)
        # UC-only keys must NOT persist into the Hive run.
        assert hive_cfg["rls_cm_strategy"] == ""
        assert hive_cfg["iceberg_strategy"] == ""
        # Hive-specific keys must be set.
        assert hive_cfg["batch_size"] == 10
        assert hive_cfg["catalog_filter"] == ["integration_test_src"]
        assert hive_cfg["migrate_hive_dbfs_root"] is True

    def test_hive_then_uc_clean_slate(self):
        """Reverse order also clean — Hive-only ``batch_size=10`` and
        ``catalog_filter=[integration_test_src]`` must NOT leak into
        a subsequent UC run (UC never sets these; they should fall
        back to baseline)."""
        baseline = _baseline_config()
        hive_cfg = apply_integration_overrides(baseline, **HIVE_OVERRIDES)
        assert hive_cfg["batch_size"] == 10
        assert hive_cfg["catalog_filter"] == ["integration_test_src"]

        uc_cfg = apply_integration_overrides(baseline, **UC_OVERRIDES)
        assert uc_cfg["batch_size"] == 50, (
            "UC run does not override batch_size — leaking Hive's "
            "batch_size=10 into UC would silently change UC's batching "
            "behavior between runs."
        )
        assert uc_cfg["catalog_filter"] == "", (
            "UC run does not override catalog_filter — leaking Hive's "
            "integration_test_src filter into UC would make UC discover "
            "only that catalog on the second run, silently breaking "
            "coverage."
        )

    def test_uc_then_hive_then_negative_paths_clean_slate(self):
        """Full cycle — UC → Hive → each negative-paths scenario.
        Every scenario must start from the pristine baseline."""
        baseline = _baseline_config()

        uc = apply_integration_overrides(baseline, **UC_OVERRIDES)
        assert uc["rls_cm_strategy"] == "staging_copy"

        hive = apply_integration_overrides(baseline, **HIVE_OVERRIDES)
        assert hive["rls_cm_strategy"] == ""

        # X.3.1 — bad SPN.
        x31 = apply_integration_overrides(
            baseline,
            iceberg_strategy="",
            rls_cm_strategy="",
            migrate_hive_dbfs_root=False,
            hive_dbfs_target_path="",
            batch_size_raw="",
            catalog_filter_raw="",
            inject_bad_spn_id=True,
        )
        assert x31["spn_client_id"] == "00000000-0000-0000-0000-000000000000"
        # Must not carry Hive's batch_size / catalog_filter.
        assert x31["batch_size"] == 50
        assert x31["catalog_filter"] == ""
        # Non-SPN fields stay pristine.
        assert x31["target_workspace_url"] == "https://adb-TARGET.azuredatabricks.net"

        # X.3.2 — unreachable target.
        x32 = apply_integration_overrides(
            baseline,
            iceberg_strategy="",
            rls_cm_strategy="",
            migrate_hive_dbfs_root=False,
            hive_dbfs_target_path="",
            batch_size_raw="",
            catalog_filter_raw="",
            inject_unreachable_target=True,
        )
        # Must not carry X.3.1's corrupted SPN.
        assert x32["spn_client_id"] == "REPLACE_WITH_APPLICATION_ID", (
            "X.3.2 must start from the pristine baseline — leaking "
            "X.3.1's bad spn_client_id would cause X.3.2's pre_check "
            "to fail for the SPN reason (X.3.1's failure mode), not "
            "for the unreachable-target reason X.3.2 is asserting."
        )
        assert x32["target_workspace_url"] == "https://adb-0000000000000000.0.azuredatabricks.net"

        # X.3.3 — no injection (used to assert both scopes off, but
        # scope flags were dropped in WS-10/WS-11; workflow choice now
        # gates scope, not config).
        x33 = apply_integration_overrides(
            baseline,
            iceberg_strategy="",
            rls_cm_strategy="",
            migrate_hive_dbfs_root=False,
            hive_dbfs_target_path="",
            batch_size_raw="",
            catalog_filter_raw="",
        )
        # Must not carry prior scenarios' corruptions.
        assert x33["spn_client_id"] == "REPLACE_WITH_APPLICATION_ID"
        assert x33["target_workspace_url"] == "https://adb-TARGET.azuredatabricks.net"

    def test_chained_additive_apply_would_leak(self):
        """Negative control: demonstrate that feeding the PREVIOUS
        run's result (instead of the pristine baseline) into the
        next ``apply_integration_overrides`` call causes the exact
        leak S.14 guards against. Pins the additive-vs-authoritative
        contract — if this ever passes under the current helper,
        someone turned the helper into an additive merge."""
        baseline = _baseline_config()
        # Exercise a UC invocation for realism (its result is
        # deliberately not reused — the next call must start from the
        # same pristine baseline).
        apply_integration_overrides(baseline, **UC_OVERRIDES)
        # BAD PATTERN: feed the UC-contaminated cfg as the "baseline"
        # for Hive. The helper's authoritative overrides cover some
        # keys but not all — UC's ``catalog_filter`` carryover would
        # be the leak.
        #
        # Concretely: Hive sets catalog_filter. UC doesn't. So if we
        # do UC-on-top-of-Hive-result, UC would inherit Hive's
        # catalog_filter=[integration_test_src] — the exact leak.
        hive_then_uc_bad = apply_integration_overrides(
            apply_integration_overrides(baseline, **HIVE_OVERRIDES),
            **UC_OVERRIDES,
        )
        assert hive_then_uc_bad["catalog_filter"] == ["integration_test_src"], (
            "Sanity: additive/chained apply DOES leak Hive's "
            "catalog_filter into a subsequent UC run. This is exactly "
            "why setup_test_config.py restores from the backup before "
            "every invocation — so the real notebook never exercises "
            "this additive pattern."
        )

        # Same baseline used authoritatively → no leak.
        hive_then_uc_clean = apply_integration_overrides(baseline, **UC_OVERRIDES)
        assert hive_then_uc_clean["catalog_filter"] == ""


class TestWorkflowOverrideContractsMatchYaml:
    """Guard against drift between the UC / Hive / Negative-paths
    workflow YAML ``base_parameters`` and the UC_OVERRIDES /
    HIVE_OVERRIDES / NEG_* dicts this file uses to simulate them.

    If someone edits the workflow YAML (e.g. adds ``batch_size`` to
    UC) without updating the test overrides, the cycle tests above
    would still pass but stop reflecting reality. These checks
    tripwire that.
    """

    @staticmethod
    def _read_workflow_params(filename: str, task_key: str) -> dict:
        import yaml as _yaml

        repo_root = pathlib.Path(__file__).resolve().parents[2]
        path = repo_root / "resources" / "integration_tests" / filename
        doc = _yaml.safe_load(path.read_text())
        tasks = next(iter(doc["resources"]["jobs"].values()))["tasks"]
        for t in tasks:
            if t["task_key"] == task_key:
                return t["notebook_task"].get("base_parameters", {})
        raise AssertionError(f"task {task_key!r} not found in {filename}")

    def test_uc_workflow_params_match_test_overrides(self):
        params = self._read_workflow_params(
            "uc_integration_test_workflow.yml", "setup_test_config"
        )
        # Subset check: the test overrides must faithfully reflect the
        # YAML's scope-affecting params. We don't require exact equality
        # because the YAML also carries ``migrate_hive_dbfs_root`` etc.
        assert params.get("iceberg_strategy") == "ddl_replay"
        assert params.get("rls_cm_strategy") == "staging_copy"
        # Path A removed the maintenance-window consent flag.
        assert "rls_cm_maintenance_window_confirmed" not in params
        assert params.get("migrate_hive_dbfs_root") == "false"
        # UC does NOT pass batch_size or catalog_filter — those are the
        # keys most at risk of leaking from a prior Hive run.
        assert "batch_size" not in params
        assert "catalog_filter" not in params

    def test_hive_workflow_params_match_test_overrides(self):
        params = self._read_workflow_params(
            "hive_integration_test_workflow.yml", "setup_test_config"
        )
        assert params.get("iceberg_strategy") == ""
        assert params.get("rls_cm_strategy") == ""
        assert params.get("migrate_hive_dbfs_root") == "true"
        assert params.get("batch_size") == "10"
        assert params.get("catalog_filter") == "integration_test_src"
        # Path A removed the maintenance-window consent flag.
        assert "rls_cm_maintenance_window_confirmed" not in params

    def test_neg_x31_workflow_params_match(self):
        params = self._read_workflow_params(
            "negative_paths_integration_test_workflow.yml", "setup_X31_config"
        )
        assert params.get("inject_bad_spn_id") == "true"
        # X.3.1 must NOT carry injections from later scenarios.
        assert "inject_unreachable_target" not in params
        assert "inject_bad_rls_cm" not in params


class TestNotebookFileCycleSimulation:
    """Simulates the file-level orchestration around
    ``apply_integration_overrides`` that ``setup_test_config.py``
    performs (backup-or-restore, load, override, write). This is the
    test that exercises the S.14 fix end-to-end: successive
    invocations against a shared ``config.yaml`` path must each
    start from the pristine backup.
    """

    @staticmethod
    def _cycle_step(config_path, backup_path, overrides, *, restore_from_backup: bool):
        """One invocation of the notebook's file-level cycle.

        ``restore_from_backup=True`` mirrors the fixed notebook:
        on subsequent invocations (backup already exists), RESTORE
        config.yaml from the backup before applying overrides.

        ``restore_from_backup=False`` mirrors the PRE-S.14 notebook:
        on subsequent invocations, leave config.yaml alone (merging
        this invocation's overrides on top of the previous one's
        result). Used as a negative control to document the leak.
        """
        import os
        import shutil

        import yaml as _yaml

        if not os.path.exists(backup_path):
            shutil.copy2(config_path, backup_path)
        elif restore_from_backup:
            shutil.copy2(backup_path, config_path)
        # else: pre-S.14 behavior — leave config.yaml as-is.

        with open(config_path) as f:
            baseline_cfg = _yaml.safe_load(f) or {}

        cfg = apply_integration_overrides(baseline_cfg, **overrides)

        with open(config_path, "w") as f:
            _yaml.safe_dump(cfg, f, sort_keys=False)
        return cfg

    def test_fixed_notebook_cycle_uc_then_hive_no_leak(self, tmp_path):
        """With the S.14 fix, Hive does NOT inherit UC-only keys."""
        import yaml as _yaml

        config_path = tmp_path / "config.yaml"
        backup_path = tmp_path / "config.yaml.pre-integration-test.bak"
        config_path.write_text(_yaml.safe_dump(_baseline_config(), sort_keys=False))

        self._cycle_step(str(config_path), str(backup_path), UC_OVERRIDES, restore_from_backup=True)
        # Inspect the intermediate state — UC-specific keys set.
        with open(config_path) as f:
            after_uc = _yaml.safe_load(f)
        assert after_uc["rls_cm_strategy"] == "staging_copy"

        self._cycle_step(str(config_path), str(backup_path), HIVE_OVERRIDES, restore_from_backup=True)
        with open(config_path) as f:
            after_hive = _yaml.safe_load(f)
        # Hive run must have started from the pristine baseline —
        # UC-only keys gone.
        assert after_hive["rls_cm_strategy"] == ""
        assert after_hive["iceberg_strategy"] == ""
        # Hive-specific keys set.
        assert after_hive["batch_size"] == 10
        assert after_hive["catalog_filter"] == ["integration_test_src"]

    def test_pre_s14_notebook_cycle_leaks_into_uc(self, tmp_path):
        """Documents the pre-S.14 bug: skipping the restore can leak
        prior corruption into a later UC run, even though the UC
        workflow YAML never opts into it."""
        import yaml as _yaml

        config_path = tmp_path / "config.yaml"
        backup_path = tmp_path / "config.yaml.pre-integration-test.bak"
        config_path.write_text(_yaml.safe_dump(_baseline_config(), sort_keys=False))

        # Nothing in either the UC or Hive workflows writes
        # ``spn_client_id`` or ``target_workspace_url``. If a prior
        # negative-paths run (in the same environment) corrupted
        # those and the teardown didn't run cleanly, a later UC run
        # without the restore-from-backup would inherit the
        # corruption. Simulate that:
        self._cycle_step(
            str(config_path),
            str(backup_path),
            dict(NEG_X31_OVERRIDES),  # inject_bad_spn_id=true
            restore_from_backup=False,
        )
        # Now a "clean" UC run, pre-S.14 — backup exists but we skip
        # restore. UC's workflow does not touch spn_client_id, so the
        # corruption leaks.
        self._cycle_step(str(config_path), str(backup_path), UC_OVERRIDES, restore_from_backup=False)
        with open(config_path) as f:
            after_leaked_uc = _yaml.safe_load(f)
        assert after_leaked_uc["spn_client_id"] == "00000000-0000-0000-0000-000000000000", (
            "Pre-S.14 behavior leak: a subsequent UC run inherits "
            "the bad spn_client_id because UC's workflow doesn't "
            "overwrite it and the pre-S.14 notebook didn't restore "
            "from backup. This is the bug S.14 fixes."
        )

    def test_fixed_notebook_cycle_restores_from_backup(self, tmp_path):
        """Same negative-paths → UC scenario as above, but with the
        S.14 fix (restore_from_backup=True). The leak is gone."""
        import yaml as _yaml

        config_path = tmp_path / "config.yaml"
        backup_path = tmp_path / "config.yaml.pre-integration-test.bak"
        config_path.write_text(_yaml.safe_dump(_baseline_config(), sort_keys=False))

        self._cycle_step(
            str(config_path),
            str(backup_path),
            dict(NEG_X31_OVERRIDES),
            restore_from_backup=True,
        )
        self._cycle_step(str(config_path), str(backup_path), UC_OVERRIDES, restore_from_backup=True)

        with open(config_path) as f:
            after_uc = _yaml.safe_load(f)
        assert after_uc["spn_client_id"] == "REPLACE_WITH_APPLICATION_ID", (
            "With S.14's restore-from-backup, the UC run starts from "
            "the pristine baseline and never sees prior injections."
        )

    def test_fixed_notebook_cycle_hive_then_uc_no_catalog_filter_leak(self, tmp_path):
        """The leakier scenario: Hive's ``catalog_filter=[integration_test_src]``
        would silently narrow a subsequent UC run's discovery to a
        single catalog if the restore didn't happen."""
        import yaml as _yaml

        config_path = tmp_path / "config.yaml"
        backup_path = tmp_path / "config.yaml.pre-integration-test.bak"
        config_path.write_text(_yaml.safe_dump(_baseline_config(), sort_keys=False))

        self._cycle_step(str(config_path), str(backup_path), HIVE_OVERRIDES, restore_from_backup=True)
        self._cycle_step(str(config_path), str(backup_path), UC_OVERRIDES, restore_from_backup=True)

        with open(config_path) as f:
            after_uc = _yaml.safe_load(f)
        assert after_uc["catalog_filter"] == "", (
            "Hive-only catalog_filter must not persist into UC — "
            "doing so would silently narrow UC discovery and mask "
            "regressions in the UC suite."
        )
        assert after_uc["batch_size"] == 50
