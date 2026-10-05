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

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_own_grant_transfers_ownership(self, mock_execute, mock_time):
        """Review finding #5: OWN must be applied as ALTER ... OWNER TO the
        original owner, not silently dropped."""
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}
        auth = MagicMock()
        grants = [{"principal": "admin_user", "action_type": "OWN"}]

        results = replay_grants(
            "SCHEMA", "`cat`.`sch`", grants,
            auth=auth, wh_id="wh-gr-2", dry_run=False,
        )

        assert len(results) == 1
        assert results[0]["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert sql == "ALTER SCHEMA `cat`.`sch` OWNER TO `admin_user`"

    @patch("migrate.grants_worker.execute_and_poll")
    def test_transfer_ownership_disabled_skips_own(self, mock_execute):
        """transfer_ownership=False preserves the old skip behaviour."""
        from migrate.grants_worker import replay_grants

        auth = MagicMock()
        grants = [{"principal": "admin_user", "action_type": "OWN"}]
        results = replay_grants(
            "SCHEMA", "`cat`.`sch`", grants,
            auth=auth, wh_id="wh-gr-2", dry_run=False, transfer_ownership=False,
        )
        assert len(results) == 0
        mock_execute.assert_not_called()

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_ownership_applied_after_non_own_grants(self, mock_execute, mock_time):
        """OWNER TO must run AFTER the securable's other grants so the SPN
        keeps MANAGE while it is still granting."""
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 101.0, 102.0, 103.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        auth = MagicMock()
        grants = [
            {"principal": "owner_user", "action_type": "OWN"},
            {"principal": "data_team", "action_type": "SELECT"},
        ]
        replay_grants(
            "TABLE", "`c`.`s`.`t`", grants,
            auth=auth, wh_id="wh", dry_run=False,
        )
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert "GRANT SELECT ON TABLE" in sqls[0]  # non-OWN first
        assert sqls[1] == "ALTER TABLE `c`.`s`.`t` OWNER TO `owner_user`"  # OWN last

    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_ownership_failure_is_fail_loud_not_crash(self, mock_execute, mock_time):
        """If the original owner doesn't exist on target, the ALTER fails —
        recorded as failed with a clear message, batch continues."""
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = {"state": "FAILED", "error": "PRINCIPAL_DOES_NOT_EXIST"}
        auth = MagicMock()
        grants = [{"principal": "ghost_user", "action_type": "OWN"}]
        results = replay_grants(
            "SCHEMA", "`cat`.`sch`", grants,
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "ghost_user" in results[0]["error_message"] or "PRINCIPAL" in results[0]["error_message"]

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


class TestGrantDependencySkip:
    """Finding #19: the UC grants worker must not attempt grants on an
    object that was not migrated (a failed/skipped table, or a foreign
    catalog migrate_governance hasn't created yet) — it records
    ``skipped_dependency_not_migrated`` instead of a noisy
    TABLE/CATALOG_DOES_NOT_EXIST failure. Mirrors Hive finding #9."""

    def test_grant_target_not_migrated_true(self):
        from migrate.grants_worker import _grant_target_not_migrated

        not_migrated = {"`cat`.`sch`.`orders`"}
        assert _grant_target_not_migrated("`cat`.`sch`.`orders`", not_migrated) is True

    def test_grant_target_not_migrated_false(self):
        from migrate.grants_worker import _grant_target_not_migrated

        not_migrated = {"`cat`.`sch`.`orders`"}
        assert _grant_target_not_migrated("`cat`.`sch`.`good`", not_migrated) is False

    def test_skipped_dependency_grant_row_shape(self):
        from migrate.grants_worker import _skipped_dependency_grant_row

        row = _skipped_dependency_grant_row("`cat`.`sch`.`orders`")
        assert row["status"] == "skipped_dependency_not_migrated"
        assert row["object_type"] == "grant"
        assert "`cat`.`sch`.`orders`" in row["error_message"]

    def test_run_skips_grants_for_not_migrated_table(self):
        """A table whose migration_status is not 'validated' gets no grant
        enumeration — a skipped_dependency_not_migrated row is recorded and
        list_grants is never called for it."""
        from migrate import grants_worker

        inventory = [
            MagicMock(
                catalog_name="cat",
                schema_name="sch",
                object_name="`cat`.`sch`.`good`",
                object_type="managed_table",
            ),
            MagicMock(
                catalog_name="cat",
                schema_name="sch",
                object_name="`cat`.`sch`.`orders`",
                object_type="managed_table",
            ),
        ]
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = inventory

        dbutils = MagicMock()
        cfg = MagicMock()
        cfg.dry_run = False
        cfg.transfer_ownership = True
        cfg.tracking_catalog = "trk"
        cfg.tracking_schema = "sch"

        with (
            patch.object(grants_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(grants_worker, "AuthManager"),
            patch.object(grants_worker, "TrackingManager") as mock_tm,
            patch.object(grants_worker, "find_warehouse", return_value="wh"),
            patch.object(grants_worker, "CatalogExplorer") as mock_exp,
            patch.object(grants_worker, "replay_grants", return_value=[]),
        ):
            mock_tm.return_value.not_validated_object_names.return_value = {"`cat`.`sch`.`orders`"}
            explorer = mock_exp.return_value
            explorer.list_grants.return_value = []
            grants_worker.run(dbutils, spark)

        # list_grants must NOT be called for the not-migrated table
        table_fqns_enumerated = [c.args[1] for c in explorer.list_grants.call_args_list]
        assert "`cat`.`sch`.`orders`" not in table_fqns_enumerated
        assert "`cat`.`sch`.`good`" in table_fqns_enumerated

        # a skipped_dependency_not_migrated row was recorded for orders
        recorded = [
            r
            for call in mock_tm.return_value.append_migration_status.call_args_list
            for r in call.args[0]
        ]
        skipped = [r for r in recorded if r["status"] == "skipped_dependency_not_migrated"]
        assert any("`cat`.`sch`.`orders`" in r["error_message"] for r in skipped)


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

    def test_replay_grants_transfers_owner_grants(self):
        """OWNER grants are applied as ALTER ... OWNER TO the original owner
        (review finding #5) — SELECT grant + ownership transfer both run."""
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
        # SELECT replayed AND ownership transferred to user2.
        assert len(results) == 2
        sqls = [c.args[2] for c in mock_exec.call_args_list]
        assert any("GRANT SELECT ON SCHEMA" in s for s in sqls)
        assert any(s == "ALTER SCHEMA `cat`.`sch` OWNER TO `user2`" for s in sqls)
        assert mock_exec.call_count == 2


class TestForeignCatalogGrantScope:
    """Finding #31 — foreign-catalog grants move from migrate_uc to
    migrate_governance (grant_scope=foreign_catalogs)."""

    _INVENTORY = [
        MagicMock(catalog_name="cat", schema_name="sch", object_name="`cat`.`sch`.`t`", object_type="managed_table"),
        MagicMock(catalog_name="fc", schema_name=None, object_name="fc", object_type="foreign_catalog"),
        # a comment row on the foreign catalog must not pull it into UC scope
        MagicMock(catalog_name="fc", schema_name=None, object_name="COMMENT_CATALOG_`fc`", object_type="comment"),
    ]

    def _run(self, scope, *, fc_on_target=True, fc_schemas=("dbo",), list_schemas_error=None):
        from migrate import grants_worker

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = self._INVENTORY
        dbutils = MagicMock()
        if scope is None:
            dbutils.widgets.get.side_effect = Exception("no widget")
        else:
            dbutils.widgets.get.return_value = scope
        cfg = MagicMock(dry_run=False, transfer_ownership=True, tracking_catalog="trk", tracking_schema="sch")

        with (
            patch.object(grants_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(grants_worker, "AuthManager") as mock_auth,
            patch.object(grants_worker, "TrackingManager") as mock_tm,
            patch.object(grants_worker, "find_warehouse", return_value="wh"),
            patch.object(grants_worker, "CatalogExplorer") as mock_exp,
            patch.object(grants_worker, "replay_grants", return_value=[]),
        ):
            if not fc_on_target:
                mock_auth.return_value.target_client.catalogs.get.side_effect = Exception("NOT_FOUND")
            mock_tm.return_value.not_validated_object_names.return_value = set()
            explorer = mock_exp.return_value
            explorer.list_grants.return_value = []
            if list_schemas_error:
                explorer.list_schemas.side_effect = list_schemas_error
            else:
                explorer.list_schemas.return_value = list(fc_schemas)
            grants_worker.run(dbutils, spark)

        enumerated = [(c.args[0], c.args[1]) for c in explorer.list_grants.call_args_list]
        recorded = [r for call in mock_tm.return_value.append_migration_status.call_args_list for r in call.args[0]]
        return enumerated, recorded

    def test_default_scope_is_uc_and_excludes_foreign_catalogs(self):
        enumerated, _ = self._run(None)
        assert ("CATALOG", "`cat`") in enumerated
        assert ("TABLE", "`cat`.`sch`.`t`") in enumerated
        assert not any("fc" in fqn for _, fqn in enumerated)

    def test_uc_scope_excludes_foreign_catalogs(self):
        enumerated, _ = self._run("uc")
        assert not any("fc" in fqn for _, fqn in enumerated)

    def test_foreign_catalog_scope_replays_catalog_and_schema_grants_only(self):
        enumerated, _ = self._run("foreign_catalogs", fc_schemas=("dbo", "sales"))
        assert enumerated == [("CATALOG", "`fc`"), ("SCHEMA", "`fc`.`dbo`"), ("SCHEMA", "`fc`.`sales`")]

    def test_foreign_catalog_missing_on_target_is_skipped_not_failed(self):
        enumerated, recorded = self._run("foreign_catalogs", fc_on_target=False)
        assert enumerated == []
        assert [r["status"] for r in recorded] == ["skipped_dependency_not_migrated"]
        assert "`fc`" in recorded[0]["error_message"]

    def test_schema_listing_failure_records_failed_row_but_keeps_catalog_grants(self):
        enumerated, recorded = self._run("foreign_catalogs", list_schemas_error=RuntimeError("conn down"))
        assert enumerated == [("CATALOG", "`fc`")]
        failed = [r for r in recorded if r["status"] == "failed"]
        assert len(failed) == 1 and "conn down" in failed[0]["error_message"]

    def test_foreign_catalog_schema_grants_drop_inherited(self):
        """SHOW GRANTS ON SCHEMA also returns grants inherited from the catalog
        (ObjectType=CATALOG); only direct SCHEMA grants may be replayed."""
        from migrate import grants_worker

        direct = {
            "principal": "account users",
            "action_type": "SELECT",
            "securable_type": "SCHEMA",
            "securable_fqn": "fc.dbo",
        }
        inherited = {
            "principal": "spn",
            "action_type": "USE SCHEMA",
            "securable_type": "CATALOG",
            "securable_fqn": "fc",
        }
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = self._INVENTORY
        dbutils = MagicMock()
        dbutils.widgets.get.return_value = "foreign_catalogs"
        cfg = MagicMock(dry_run=False, transfer_ownership=True, tracking_catalog="trk", tracking_schema="sch")
        with (
            patch.object(grants_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(grants_worker, "AuthManager"),
            patch.object(grants_worker, "TrackingManager"),
            patch.object(grants_worker, "find_warehouse", return_value="wh"),
            patch.object(grants_worker, "CatalogExplorer") as mock_exp,
            patch.object(grants_worker, "replay_grants", return_value=[]) as mock_replay,
        ):
            explorer = mock_exp.return_value
            explorer.list_schemas.return_value = ["dbo"]
            explorer.list_grants.side_effect = lambda st, fqn: [inherited] if st == "CATALOG" else [direct, inherited]
            grants_worker.run(dbutils, spark)

        replayed = {c.args[1]: c.args[2] for c in mock_replay.call_args_list}
        assert replayed["`fc`"] == [inherited]  # catalog grants untouched
        assert replayed["`fc`.`dbo`"] == [direct]  # inherited dropped at schema level

    def test_unknown_scope_raises(self):
        import pytest

        with pytest.raises(ValueError):
            self._run("bogus")


class TestDirectGrantsOnly:
    """Finding #33 — inherited grants must not be replayed as explicit child grants.
    Rows mirror live SHOW GRANTS output (ObjectType / ObjectKey)."""

    @staticmethod
    def _g(principal, action, obj_type, key):
        return {"principal": principal, "action_type": action, "securable_type": obj_type, "securable_fqn": key}

    def test_table_keeps_direct_drops_schema_and_catalog_inherited(self):
        from migrate.grants_worker import _direct_grants

        grants = [
            self._g("spn", "SELECT", "CATALOG", "retail_prod"),
            self._g("account users", "SELECT", "SCHEMA", "retail_prod.sales"),
            self._g("account users", "SELECT", "TABLE", "retail_prod.sales.daily_metrics"),
        ]
        kept = _direct_grants(grants, "`retail_prod`.`sales`.`daily_metrics`")
        assert kept == [grants[2]]

    def test_view_direct_grant_reported_as_table_is_kept(self):
        # Live: a direct grant on a VIEW comes back with ObjectType=TABLE.
        from migrate.grants_worker import _direct_grants

        direct = self._g("account users", "SELECT", "TABLE", "retail_prod.sales.high_value_orders")
        inherited = self._g("account users", "SELECT", "SCHEMA", "retail_prod.sales")
        assert _direct_grants([direct, inherited], "`retail_prod`.`sales`.`high_value_orders`") == [direct]

    def test_catalog_grants_are_all_direct(self):
        from migrate.grants_worker import _direct_grants

        grants = [self._g("spn", "USE CATALOG", "CATALOG", "retail_prod")]
        assert _direct_grants(grants, "`retail_prod`") == grants

    def test_key_match_ignores_backticks_and_case(self):
        from migrate.grants_worker import _direct_grants

        g = self._g("u", "READ VOLUME", "VOLUME", "Retail_Prod.Sales.Landing_Vol")
        assert _direct_grants([g], "`retail_prod`.`sales`.`landing_vol`") == [g]

    def test_run_replays_only_direct_grants_for_uc_objects(self):
        from migrate import grants_worker

        inventory = [
            MagicMock(catalog_name="cat", schema_name="sch", object_name="`cat`.`sch`.`t`", object_type="managed_table")
        ]
        rows = {
            ("CATALOG", "`cat`"): [self._g("spn", "SELECT", "CATALOG", "cat")],
            ("SCHEMA", "`cat`.`sch`"): [
                self._g("spn", "SELECT", "CATALOG", "cat"),
                self._g("users", "SELECT", "SCHEMA", "cat.sch"),
            ],
            ("TABLE", "`cat`.`sch`.`t`"): [
                self._g("spn", "SELECT", "CATALOG", "cat"),
                self._g("users", "SELECT", "SCHEMA", "cat.sch"),
                self._g("owner", "MODIFY", "TABLE", "cat.sch.t"),
            ],
        }
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = inventory
        dbutils = MagicMock()
        dbutils.widgets.get.return_value = "uc"
        cfg = MagicMock(dry_run=False, transfer_ownership=True, tracking_catalog="trk", tracking_schema="sch")
        with (
            patch.object(grants_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(grants_worker, "AuthManager"),
            patch.object(grants_worker, "TrackingManager") as mock_tm,
            patch.object(grants_worker, "find_warehouse", return_value="wh"),
            patch.object(grants_worker, "CatalogExplorer") as mock_exp,
            patch.object(grants_worker, "replay_grants", return_value=[]) as mock_replay,
        ):
            mock_tm.return_value.not_validated_object_names.return_value = set()
            mock_exp.return_value.list_grants.side_effect = lambda st, fqn: rows[(st, fqn)]
            grants_worker.run(dbutils, spark)

        replayed = {
            c.args[1]: [(g["principal"], g["action_type"]) for g in c.args[2]] for c in mock_replay.call_args_list
        }
        assert replayed == {
            "`cat`": [("spn", "SELECT")],
            "`cat`.`sch`": [("users", "SELECT")],
            "`cat`.`sch`.`t`": [("owner", "MODIFY")],
        }
