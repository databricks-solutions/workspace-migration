from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestReplayGrants:
    """Tests for the grants_worker.replay_grants function."""

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_success(self, mock_execute, mock_time):
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 102.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        auth = MagicMock()
        grants = [{"principal": "data_team", "action_type": "SELECT"}]

        results = replay_grants(
            "CATALOG",
            "`my_catalog`",
            grants,
            auth=auth,
            wh_id="wh-gr-1",
            dry_run=False,
        )

        assert len(results) == 1
        assert results[0]["status"] == "validated"
        assert results[0]["object_type"] == "grant"
        assert results[0]["error_message"] is None
        mock_execute.assert_called_once()
        # Verify the SQL includes the GRANT statement
        called_sql = mock_execute.call_args[0][2]
        assert "GRANT SELECT ON CATALOG" in called_sql
        assert "`data_team`" in called_sql

    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_skips_own(self, mock_execute):
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [{"principal": "admin_user", "action_type": "OWN"}]

        results = replay_grants(
            "SCHEMA",
            "`cat`.`sch`",
            grants,
            auth=auth,
            wh_id="wh-gr-2",
            dry_run=False,
        )

        assert len(results) == 0
        mock_execute.assert_not_called()

    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_dry_run(self, mock_execute):
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [{"principal": "analysts", "action_type": "USAGE"}]

        results = replay_grants(
            "CATALOG",
            "`prod`",
            grants,
            auth=auth,
            wh_id="wh-gr-3",
            dry_run=True,
        )

        assert len(results) == 1
        assert results[0]["status"] == "skipped"
        assert results[0]["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_failure(self, mock_execute, mock_time):
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 103.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PRINCIPAL_NOT_FOUND",
            "statement_id": "s-4",
        }

        auth = MagicMock()
        grants = [{"principal": "unknown_group", "action_type": "SELECT"}]

        results = replay_grants(
            "SCHEMA",
            "`cat`.`sch`",
            grants,
            auth=auth,
            wh_id="wh-gr-4",
            dry_run=False,
        )

        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "PRINCIPAL_NOT_FOUND" in results[0]["error_message"]


class TestGrantsWorkerSecurableCoverage:
    """Contract test: grants_worker processes every UC securable type
    that ``discovery_inventory`` tracks. If a new object type is added
    to discovery without corresponding grant enumeration here, the
    test fails loud so the gap is visible in review.
    """

    def test_run_processes_all_tracked_securable_types(self):
        """The six securable types grants_worker must enumerate:
        CATALOG, SCHEMA, TABLE, VIEW, VOLUME, FUNCTION. Each is passed
        as a string to ``_process(type, fqn)`` inside ``run()``."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "grants_worker.py").read_text()
        for securable_type in ("CATALOG", "SCHEMA", "TABLE", "VIEW", "VOLUME", "FUNCTION"):
            assert f'_process("{securable_type}"' in src, (
                f"grants_worker must call _process({securable_type!r}, ...) — "
                f"missing coverage for {securable_type} grants."
            )

    def test_run_classifies_mv_and_st_as_table_for_grants(self):
        """Materialized views and streaming tables share the UC TABLE
        securable type (SHOW GRANTS ON TABLE <fqn> works for them).
        The run() dispatcher maps discovery ``object_type`` values
        ``mv``/``st`` into the table bucket — lock this in so a
        refactor doesn't drop MV/ST grant coverage."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "grants_worker.py").read_text()
        # The mapping block lists mv, st alongside managed_table / external_table
        assert '"managed_table", "external_table", "mv", "st"' in src

    def test_replay_grants_skips_owner_grants(self):
        """OWNER grants are set differently (ALTER ... OWNER TO) — the
        grants_worker has an explicit skip for them. Lock this in."""
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [
            {
                "action_type": "SELECT",
                "principal": "user1",
                "grantable": False,
            },
            {
                "action_type": "OWN",
                "principal": "user2",
                "grantable": False,
            },
        ]
        with patch("migrate.grants_worker.execute_and_poll") as mock_exec:
            mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
            results = replay_grants(
                "SCHEMA",
                "`cat`.`sch`",
                grants,
                auth=auth,
                wh_id="wh-owner",
                dry_run=False,
            )
        # Only SELECT was replayed; OWN was skipped (no result row).
        assert len(results) == 1
        assert "SELECT" in results[0]["object_name"]
        assert "user1" in results[0]["object_name"]
        sql_sent = mock_exec.call_args[0][2]
        assert sql_sent.startswith("GRANT SELECT")
        assert mock_exec.call_count == 1
