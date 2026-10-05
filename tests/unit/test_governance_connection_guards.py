"""Guards for findings #30 (target connection SPN grant doc) and #32 (comments
task ordering after foreign catalogs)."""

from __future__ import annotations

import pathlib

import yaml

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_GUIDE = (_ROOT / "docs" / "user_guide.md").read_text()
_WORKFLOW = yaml.safe_load((_ROOT / "resources" / "production" / "migrate_governance_workflow.yml").read_text())


def _tasks() -> dict[str, dict]:
    job = next(iter(_WORKFLOW["resources"]["jobs"].values()))
    return {t["task_key"]: t for t in job["tasks"]}


class TestTargetConnectionGrantDoc:
    def test_target_recipe_grants_spn_on_recreated_connection(self):
        # Finding #30: without USE CONNECTION + CREATE FOREIGN CATALOG on the
        # operator-recreated target connection, the SPN cannot see it.
        assert "GRANT USE CONNECTION, CREATE FOREIGN CATALOG ON CONNECTION" in _GUIDE


class TestCommentsAfterForeignCatalogs:
    def test_comments_depends_on_foreign_catalogs(self):
        # Finding #32: foreign-catalog comments fail if comments runs first.
        deps = {d["task_key"] for d in _tasks()["migrate_comments"].get("depends_on", [])}
        assert "migrate_foreign_catalogs" in deps

    def test_comments_still_reachable_from_orchestrator(self):
        # migrate_foreign_catalogs -> migrate_connections -> orchestrator keeps
        # comments downstream of the orchestrator's task values.
        tasks = _tasks()
        assert {d["task_key"] for d in tasks["migrate_foreign_catalogs"]["depends_on"]} == {"migrate_connections"}
        assert {d["task_key"] for d in tasks["migrate_connections"]["depends_on"]} == {"orchestrator"}


class TestForeignCatalogGrantsTask:
    def test_fc_grants_task_runs_grants_worker_after_foreign_catalogs(self):
        # Finding #31: foreign-catalog grants replayed in governance, after creation.
        task = _tasks()["migrate_foreign_catalog_grants"]
        assert {d["task_key"] for d in task["depends_on"]} == {"migrate_foreign_catalogs"}
        assert task["notebook_task"]["notebook_path"].endswith("src/migrate/grants_worker.py")
        assert task["notebook_task"]["base_parameters"]["grant_scope"] == "foreign_catalogs"

    def test_summary_waits_for_fc_grants(self):
        deps = {d["task_key"] for d in _tasks()["summary_governance"]["depends_on"]}
        assert "migrate_foreign_catalog_grants" in deps
