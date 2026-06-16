from common.tracking import _TERMINAL_STATUSES


def test_lfc_stage1_statuses_are_terminal():
    for s in ("lfc_pipeline_created_incremental", "lfc_view_created"):
        assert s in _TERMINAL_STATUSES
