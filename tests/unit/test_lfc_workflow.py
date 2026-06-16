import yaml
from pathlib import Path

def test_migrate_lfc_workflow_has_expected_tasks():
    doc = yaml.safe_load(Path("resources/production/migrate_lfc_workflow.yml").read_text())
    tasks = {t["task_key"] for t in doc["resources"]["jobs"]["migrate_lfc"]["tasks"]}
    assert {"pre_check_lfc", "orchestrator", "migrate_lfc", "summary_lfc"} <= tasks
