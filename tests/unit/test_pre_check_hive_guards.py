"""Unit tests for the like-for-like Hive pre_check guard helpers."""

from __future__ import annotations

from pre_check.pre_check import missing_required_mounts


class TestMissingRequiredMounts:
    def test_flags_absent_mounts(self):
        target = ["/mnt/present", "/mnt/other"]
        required = ["present", "salesraw"]
        assert missing_required_mounts(target, required) == ["salesraw"]

    def test_all_present(self):
        assert missing_required_mounts(["/mnt/a", "/mnt/b"], ["a", "b"]) == []

    def test_handles_bare_and_prefixed_target_forms(self):
        # Target mounts may come back as "/mnt/x" or "x" depending on SDK shape.
        assert missing_required_mounts(["x"], ["x"]) == []
        assert missing_required_mounts(["/mnt/x/"], ["x"]) == []

    def test_empty_required_is_no_missing(self):
        assert missing_required_mounts([], []) == []


class TestPreCheckReferencesStagingKey:
    def test_source_uses_staging_path_not_target_path(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "pre_check" / "pre_check.py").read_text()
        assert "hive_dbfs_staging_path" in src
        assert "hive_dbfs_target_path" not in src
        assert "check_target_mounts" in src
        assert "check_staging_path_reachable" in src
