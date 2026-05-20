"""Unit tests for ``migrate.target_copy``.

Most of this module's logic lives inside the embedded
``TARGET_COPY_NOTEBOOK`` string (it runs on the target workspace's
serverless executor, not on the host process), so the tests assert that
the embedded source contains the contract we depend on. Functional
tests of the notebook itself require a live target workspace —
exercised in the integration suite.
"""

from __future__ import annotations

from migrate import target_copy


class TestTargetCopyNotebookContract:
    """The embedded copy-helper notebook must contain a share-
    propagation retry on the entry-point ``ls`` call. Without it, the
    first volume in each batch fails with ``UC_VOLUME_NOT_FOUND``
    because the consumer-side share view lags a few seconds behind
    ``ALTER SHARE ADD VOLUME`` (verified 2026-05-20:
    test_volume failed while a later nested_volume in the same run
    succeeded). See the comment above ``TARGET_COPY_NOTEBOOK`` for
    full background.
    """

    def test_notebook_imports_time(self):
        """``_ls_with_retry`` needs ``time.sleep`` for the backoff."""
        assert "import time" in target_copy.TARGET_COPY_NOTEBOOK

    def test_notebook_defines_ls_with_retry(self):
        assert "_ls_with_retry" in target_copy.TARGET_COPY_NOTEBOOK

    def test_notebook_retry_covers_not_found_errors(self):
        """The retry must recognise the error shapes that indicate a
        propagation lag — ``UC_VOLUME_NOT_FOUND`` plus the generic
        ``NOT_FOUND`` / ``does not exist`` variants. Catching everything
        would mask real bugs (e.g. permission errors); catching nothing
        would defeat the purpose. The check is case-insensitive."""
        src = target_copy.TARGET_COPY_NOTEBOOK.lower()
        assert "uc_volume_not_found" in src
        assert "not_found" in src
        assert "does not exist" in src

    def test_notebook_top_level_call_opts_into_retry(self):
        """``_copy_recursive`` accepts an ``_entry`` flag and the top-
        level invocation must pass ``_entry=True`` so only the first
        ``ls`` retries. Recursive descents into already-listed subdirs
        don't race and should NOT retry (would mask other failures)."""
        assert "_copy_recursive(src, dst, _entry=True)" in target_copy.TARGET_COPY_NOTEBOOK

    def test_notebook_recursive_ls_uses_plain_ls(self):
        """The recursive branch (``_entry`` defaults to False) must use
        the un-wrapped ``dbutils.fs.ls`` — a missing path inside a
        successfully-listed parent is a real error, not a propagation
        race."""
        # The notebook should contain the conditional expression that
        # selects between the retrying and non-retrying ls.
        assert "_ls_with_retry(s) if _entry else dbutils.fs.ls(s)" in target_copy.TARGET_COPY_NOTEBOOK


class TestTargetCopyNotebookPath:
    """Where the helper notebook gets uploaded — kept under a stable
    path so re-runs find the previous deploy and don't multiply
    notebooks on target."""

    def test_path_lives_under_shared(self):
        assert target_copy.TARGET_COPY_NOTEBOOK_PATH.startswith("/Shared/")

    def test_path_namespaced_by_tool(self):
        """``cp_migration_runtime`` is the tool's reserved namespace on
        target — operator wipe / inspection is a single mkdir."""
        assert "cp_migration_runtime" in target_copy.TARGET_COPY_NOTEBOOK_PATH
