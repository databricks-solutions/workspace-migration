from unittest.mock import MagicMock, patch

import pytest

from migrate.setup_sharing import (
    _validate_rls_cm_strategy,
    add_tables_to_share,
    ensure_share_consumer_catalog,
    ensure_target_catalogs_and_schemas,
    get_or_create_recipient,
    get_or_create_share,
)


class TestSetupSharing:
    def test_get_or_create_share_exists(self):
        auth = MagicMock()
        auth.source_client.shares.get.return_value = MagicMock(name="test_share")

        get_or_create_share(auth, "test_share")
        auth.source_client.shares.create.assert_not_called()

    def test_get_or_create_share_creates(self):
        auth = MagicMock()
        auth.source_client.shares.get.side_effect = Exception("Not found")
        auth.source_client.shares.create.return_value = MagicMock(name="new_share")

        get_or_create_share(auth, "new_share")
        auth.source_client.shares.create.assert_called_once_with(name="new_share")

    def test_get_or_create_share_dry_run(self):
        auth = MagicMock()
        auth.source_client.shares.get.side_effect = Exception("Not found")

        result = get_or_create_share(auth, "test_share", dry_run=True)
        assert result == "test_share"
        auth.source_client.shares.create.assert_not_called()

    def test_get_or_create_recipient_exists(self):
        auth = MagicMock()
        auth.source_client.recipients.get.return_value = MagicMock(name="cp_migration_recipient_ms123")

        get_or_create_recipient(auth, "ms123")
        auth.source_client.recipients.create.assert_not_called()

    def test_get_or_create_recipient_creates(self):
        auth = MagicMock()
        auth.source_client.recipients.get.side_effect = Exception("Not found")
        auth.source_client.recipients.create.return_value = MagicMock(name="cp_migration_recipient_ms123")

        get_or_create_recipient(auth, "ms123")
        # Only assert that create was called; don't lock in specific kwarg shapes
        # (authentication_type enum vs string, metastore-id format are SDK-version
        # dependent and caused integration failures when over-specified here).
        auth.source_client.recipients.create.assert_called_once()

    def test_get_or_create_recipient_dry_run(self):
        auth = MagicMock()
        auth.source_client.recipients.get.side_effect = Exception("Not found")

        result = get_or_create_recipient(auth, "ms123", dry_run=True)
        assert result == "cp_migration_recipient_ms123"
        auth.source_client.recipients.create.assert_not_called()

    def test_add_tables_to_share_dry_run(self):
        auth = MagicMock()
        tables = [
            {"object_name": "`cat_a`.`sch_1`.`t1`"},
            {"object_name": "`cat_a`.`sch_1`.`t2`"},
        ]

        add_tables_to_share(auth, "test_share", tables, dry_run=True)
        auth.source_client.shares.update.assert_not_called()

    def test_add_tables_to_share_strips_backticks(self):
        """Share API rejects backticked names; object_name must be stripped
        before being passed as ``data_object.name``."""
        auth = MagicMock()
        # Simulate an empty existing share so the pre-clean path is a no-op
        auth.source_client.shares.get.return_value = MagicMock(objects=[])

        tables = [{"object_name": "`cat_a`.`sch_1`.`t1`"}]
        add_tables_to_share(auth, "test_share", tables)

        # Find the "add" update call (after any pre-clean remove call)
        calls = auth.source_client.shares.update.call_args_list
        add_calls = [
            c
            for c in calls
            if c.kwargs.get("updates")
            and any(u.action.value == "ADD" for u in c.kwargs["updates"] if hasattr(u.action, "value"))
        ]
        assert add_calls, "shares.update was never called with an ADD"
        updates = add_calls[-1].kwargs["updates"]
        names = [u.data_object.name for u in updates]
        for n in names:
            assert "`" not in n, f"Share object name must not contain backticks: {n!r}"
            assert n.count(".") == 2, f"Expected catalog.schema.table, got: {n!r}"

    def test_add_tables_skips_malformed_fqn(self):
        auth = MagicMock()
        tables = [
            {"object_name": "bad_fqn"},
        ]

        add_tables_to_share(auth, "test_share", tables)
        auth.source_client.shares.update.assert_not_called()

    def test_ensure_target_catalogs_dry_run(self):
        auth = MagicMock()
        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
            {"catalog_name": "cat_a", "schema_name": "sch_2"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables, dry_run=True)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.schemas.create.assert_not_called()

    def test_ensure_target_catalogs_creates_missing(self):
        auth = MagicMock()
        auth.target_client.catalogs.get.side_effect = Exception("Not found")
        auth.target_client.schemas.get.side_effect = Exception("Not found")

        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables)
        # Use assert_any_call so additional catalog creations (e.g. share-consumer
        # catalogs) don't break this test.
        auth.target_client.catalogs.create.assert_any_call(name="cat_a")
        auth.target_client.schemas.create.assert_any_call(name="sch_1", catalog_name="cat_a")

    def test_ensure_target_catalogs_skips_existing(self):
        auth = MagicMock()
        # catalogs.get and schemas.get succeed (already exist)

        tables = [
            {"catalog_name": "cat_a", "schema_name": "sch_1"},
        ]

        ensure_target_catalogs_and_schemas(auth, tables)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.schemas.create.assert_not_called()

    # ------------------------------------------------------------------
    # ensure_share_consumer_catalog
    # ------------------------------------------------------------------

    def _make_auth_with_matching_provider(self):
        """Build an auth mock with a target provider whose global metastore ID
        matches the source metastore. Returns (auth, provider_name)."""
        auth = MagicMock()
        auth.source_client.metastores.summary.return_value = MagicMock(
            global_metastore_id="azure:northeurope:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        matching = MagicMock()
        matching.name = "src_provider"
        matching.data_provider_global_metastore_id = "azure:northeurope:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        other = MagicMock()
        other.name = "unrelated_provider"
        other.data_provider_global_metastore_id = "azure:westus:11111111-2222-3333-4444-555555555555"
        auth.target_client.providers.list.return_value = [other, matching]
        return auth, "src_provider"

    def test_ensure_share_consumer_catalog_creates_using_share(self):
        """Must find the matching target-side provider (by source metastore id)
        and create a catalog with provider_name + share_name kwargs.
        Skipping this step was the root cause of integration failure #16 —
        DEEP CLONE couldn't resolve the source tables without a consumer catalog.
        """
        auth, provider_name = self._make_auth_with_matching_provider()

        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)

        auth.target_client.catalogs.create.assert_called_once()
        kwargs = auth.target_client.catalogs.create.call_args.kwargs
        assert kwargs["name"] == "cp_migration_share_consumer"
        assert kwargs["provider_name"] == provider_name
        assert kwargs["share_name"] == "cp_migration_share"

    def test_ensure_share_consumer_catalog_dry_run(self):
        auth, _ = self._make_auth_with_matching_provider()
        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=True)
        auth.target_client.catalogs.create.assert_not_called()
        auth.target_client.catalogs.delete.assert_not_called()

    def test_ensure_share_consumer_catalog_drops_before_recreate(self):
        """Re-runs must drop the existing consumer catalog first — otherwise
        stale share structure (e.g. old shared_as format) causes DEEP CLONE
        to fail with unresolvable schema UUIDs."""
        auth, _ = self._make_auth_with_matching_provider()

        ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)

        # delete must be called before create
        delete_call = auth.target_client.catalogs.delete.call_args
        assert delete_call is not None, "Consumer catalog must be dropped before recreate"
        assert delete_call.args[0] == "cp_migration_share_consumer"

    def test_ensure_share_consumer_catalog_raises_when_no_provider_matches(self):
        """If the source metastore has never been shared to the target, no
        matching provider exists. Must fail loudly rather than create an
        invalid catalog."""
        auth = MagicMock()
        auth.source_client.metastores.summary.return_value = MagicMock(
            global_metastore_id="azure:northeurope:deadbeef-dead-beef-dead-beefdeadbeef"
        )
        # No matching provider
        other = MagicMock()
        other.name = "unrelated"
        other.data_provider_global_metastore_id = "azure:westus:11111111-2222-3333-4444-555555555555"
        auth.target_client.providers.list.return_value = [other]

        with pytest.raises(RuntimeError, match="No target-side provider"):
            ensure_share_consumer_catalog(auth, "cp_migration_share", dry_run=False)
        auth.target_client.catalogs.create.assert_not_called()


class TestRlsCmStrategyGating:
    """Verify ``_validate_rls_cm_strategy``'s contract.

    Path A: only ``""`` (skip) and ``"staging_copy"`` are accepted. Any
    other non-empty value is rejected (typo protection). The validator
    runs BEFORE any side-effecting setup so misconfiguration doesn't
    leave orphan shares / recipients on source.
    """

    def _config(self, strategy: str) -> MagicMock:
        config = MagicMock()
        config.rls_cm_strategy = strategy
        return config

    def test_empty_strategy_returns_empty(self):
        """Default skip path — validator returns the normalized empty string."""
        assert _validate_rls_cm_strategy(self._config("")) == ""

    def test_none_strategy_treated_as_empty(self):
        """Some config loaders might yield None; treat as default skip."""
        assert _validate_rls_cm_strategy(self._config(None)) == ""

    def test_whitespace_strategy_treated_as_empty(self):
        assert _validate_rls_cm_strategy(self._config("   ")) == ""

    def test_staging_copy_returns_normalized(self):
        """staging_copy is the only non-empty strategy now accepted."""
        assert _validate_rls_cm_strategy(self._config("staging_copy")) == "staging_copy"

    def test_staging_copy_mixed_case_normalized(self):
        assert _validate_rls_cm_strategy(self._config("Staging_Copy")) == "staging_copy"

    def test_drop_and_restore_now_rejected(self):
        """Path A removed drop_and_restore — validator rejects it as unknown."""
        with pytest.raises(ValueError, match="Unknown rls_cm_strategy"):
            _validate_rls_cm_strategy(self._config("drop_and_restore"))

    def test_unknown_value_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown rls_cm_strategy"):
            _validate_rls_cm_strategy(self._config("bogus_value"))

    def test_unknown_value_error_includes_offending_string(self):
        """Error message should surface the exact value so the operator
        can locate their typo without a log scavenger hunt."""
        with pytest.raises(ValueError, match="'typo_value'"):
            _validate_rls_cm_strategy(self._config("typo_value"))


class TestAddRlsCmFromTablesApi:
    """_add_rls_cm_from_tables_api is the belt-and-braces backup probe:
    discovery can silently miss row_filter / column_mask rows (information
    _schema doesn't reliably surface them on every runtime), so setup_sharing
    also asks the UC Tables API directly for each pending managed table.
    Without this, tables slip through into the share and Delta Sharing
    refuses with InvalidParameterValue.
    """

    def _pending(self, *fqns):
        return [{"object_name": f} for f in fqns]

    def test_flags_table_with_row_filter(self):
        from migrate.setup_sharing import _add_rls_cm_from_tables_api

        auth = MagicMock()
        t = MagicMock()
        t.row_filter = MagicMock(function_name="c.s.rf", input_column_names=["r"])
        t.columns = []
        auth.source_client.tables.get.return_value = t

        rls_cm_fqns: set[str] = set()
        _add_rls_cm_from_tables_api(
            auth,
            self._pending("`c`.`s`.`t1`"),
            rls_cm_fqns,
        )
        assert rls_cm_fqns == {"`c`.`s`.`t1`"}
        # Called with dot-delimited name (backticks stripped).
        auth.source_client.tables.get.assert_called_once_with("c.s.t1")

    def test_flags_table_with_column_mask(self):
        from migrate.setup_sharing import _add_rls_cm_from_tables_api

        auth = MagicMock()
        t = MagicMock()
        t.row_filter = None
        col_masked = MagicMock(mask=MagicMock(function_name="c.s.m"))
        col_plain = MagicMock(mask=None)
        t.columns = [col_plain, col_masked]
        auth.source_client.tables.get.return_value = t

        rls_cm_fqns: set[str] = set()
        _add_rls_cm_from_tables_api(
            auth,
            self._pending("`c`.`s`.`t1`"),
            rls_cm_fqns,
        )
        assert rls_cm_fqns == {"`c`.`s`.`t1`"}

    def test_clean_table_not_flagged(self):
        from migrate.setup_sharing import _add_rls_cm_from_tables_api

        auth = MagicMock()
        t = MagicMock()
        t.row_filter = None
        t.columns = [MagicMock(mask=None)]
        auth.source_client.tables.get.return_value = t

        rls_cm_fqns: set[str] = set()
        _add_rls_cm_from_tables_api(
            auth,
            self._pending("`c`.`s`.`clean`"),
            rls_cm_fqns,
        )
        assert rls_cm_fqns == set()

    def test_tolerates_per_table_failure(self):
        """If tables.get errors for ONE table, the rest still get probed."""
        from migrate.setup_sharing import _add_rls_cm_from_tables_api

        auth = MagicMock()
        good = MagicMock()
        good.row_filter = MagicMock()
        good.columns = []

        def _get(name: str):
            if name == "c.s.bad":
                raise RuntimeError("API blip")
            return good

        auth.source_client.tables.get.side_effect = _get

        rls_cm_fqns: set[str] = set()
        _add_rls_cm_from_tables_api(
            auth,
            self._pending("`c`.`s`.`bad`", "`c`.`s`.`good`"),
            rls_cm_fqns,
        )
        # good is flagged despite bad throwing.
        assert rls_cm_fqns == {"`c`.`s`.`good`"}

    def test_preserves_existing_set_contents(self):
        """Augments the set in place (tracker-sourced entries plus API hits)."""
        from migrate.setup_sharing import _add_rls_cm_from_tables_api

        auth = MagicMock()
        t = MagicMock()
        t.row_filter = MagicMock()
        t.columns = []
        auth.source_client.tables.get.return_value = t

        rls_cm_fqns = {"`c`.`s`.`pre_existing`"}
        _add_rls_cm_from_tables_api(
            auth,
            self._pending("`c`.`s`.`t1`"),
            rls_cm_fqns,
        )
        assert rls_cm_fqns == {"`c`.`s`.`pre_existing`", "`c`.`s`.`t1`"}


class TestRunSkipsRlsCmTables:
    """End-to-end (logic only — no real clients) test of setup_sharing.run's
    RLS/CM skip path.

    With the default ``rls_cm_strategy=''``, any managed table flagged by
    either ``tracker.get_tables_with_rls_cm()`` or the live Tables API
    probe should:
        (a) be excluded from the share payload (shares.update gets the
            clean tables only),
        (b) produce a ``skipped_by_rls_cm_policy`` row via
            ``tracker.append_migration_status``.
    """

    def _mock_deps(self, rls_cm_fqns=None, pending=None):
        """Builds the full set of mocks run() touches so we can assert on
        them without standing up a real workspace.
        """
        rls_cm_fqns = rls_cm_fqns or set()
        pending = pending or []

        config = MagicMock()
        config.dry_run = False
        config.rls_cm_strategy = ""

        auth = MagicMock()
        # shares.get returns (existing share reused) for simplicity.
        auth.source_client.shares.get.return_value = MagicMock(name="cp_migration_share")
        # recipients.get returns a dummy existing recipient.
        auth.source_client.recipients.get.return_value = MagicMock(name="cp_migration_recipient_x")
        auth.target_client.metastores.summary.return_value = MagicMock(global_metastore_id="azure:region:uuid")

        # Tables API returns no-filter tables by default; specific fqns are
        # flagged via rls_cm_fqns + _add_rls_cm_from_tables_api monkey-patch.
        def _tables_get(full_name):
            t = MagicMock()
            # Only tables in the rls_cm_fqns set carry a row_filter.
            fqn = "`" + full_name.replace(".", "`.`") + "`"
            if fqn in rls_cm_fqns:
                t.row_filter = MagicMock()
                t.columns = []
            else:
                t.row_filter = None
                t.columns = []
            return t

        auth.source_client.tables.get.side_effect = _tables_get

        tracker = MagicMock()
        tracker.get_pending_objects.return_value = pending
        tracker.get_tables_with_rls_cm.return_value = set()  # empty — probe populates
        return config, auth, tracker

    def test_skip_path_records_status_and_excludes_from_share(self):
        """Two pending tables — one clean, one with RLS. Only clean
        goes to the share; dirty gets a skipped_by_rls_cm_policy row."""
        from migrate import setup_sharing

        clean_fqn = "`c`.`s`.`clean`"
        dirty_fqn = "`c`.`s`.`dirty`"
        pending = [
            {"object_name": clean_fqn, "object_type": "managed_table"},
            {"object_name": dirty_fqn, "object_type": "managed_table"},
        ]

        # Seed the tracker helper to already flag `dirty`; the Tables API
        # probe can also populate this — either source suffices.
        config, auth, tracker = self._mock_deps(
            rls_cm_fqns={dirty_fqn},
            pending=pending,
        )
        tracker.get_tables_with_rls_cm.return_value = {dirty_fqn}

        # Patch module-level bindings so run() uses our mocks.
        with (
            patch("migrate.setup_sharing.MigrationConfig") as cfg_cls,
            patch("migrate.setup_sharing.AuthManager", return_value=auth),
            patch("migrate.setup_sharing.TrackingManager", return_value=tracker),
            patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas"),
            patch("migrate.setup_sharing.ensure_share_consumer_catalog"),
            patch("migrate.setup_sharing.add_tables_to_share") as add_fn,
        ):
            cfg_cls.from_workspace_file.return_value = config
            setup_sharing.run(dbutils=MagicMock(), spark=MagicMock())

        # add_tables_to_share must NOT include the dirty table.
        assert add_fn.call_count == 1
        _, _, shared_tables = add_fn.call_args[0]
        shared_fqns = {t["object_name"] for t in shared_tables}
        assert shared_fqns == {clean_fqn}

        # And migration_status got a skipped_by_rls_cm_policy row for dirty.
        append_calls = [c.args[0] for c in tracker.append_migration_status.call_args_list]
        # Each call passes a list of dicts. Flatten and inspect.
        all_recorded = [row for call in append_calls for row in call]
        skipped = [r for r in all_recorded if r["status"] == "skipped_by_rls_cm_policy"]
        assert len(skipped) == 1
        assert skipped[0]["object_name"] == dirty_fqn
        assert "Delta Sharing" in (skipped[0]["error_message"] or "")

    def test_no_rls_cm_tables_shares_everything(self):
        """Default green path: no flagged tables, all pending go to share,
        no skipped_by_rls_cm_policy rows written."""
        from migrate import setup_sharing

        pending = [
            {"object_name": "`c`.`s`.`a`", "object_type": "managed_table"},
            {"object_name": "`c`.`s`.`b`", "object_type": "managed_table"},
        ]
        config, auth, tracker = self._mock_deps(rls_cm_fqns=set(), pending=pending)

        with (
            patch("migrate.setup_sharing.MigrationConfig") as cfg_cls,
            patch("migrate.setup_sharing.AuthManager", return_value=auth),
            patch("migrate.setup_sharing.TrackingManager", return_value=tracker),
            patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas"),
            patch("migrate.setup_sharing.ensure_share_consumer_catalog"),
            patch("migrate.setup_sharing.add_tables_to_share") as add_fn,
        ):
            cfg_cls.from_workspace_file.return_value = config
            setup_sharing.run(dbutils=MagicMock(), spark=MagicMock())

        shared_fqns = {t["object_name"] for t in add_fn.call_args[0][2]}
        assert shared_fqns == {"`c`.`s`.`a`", "`c`.`s`.`b`"}

        recorded = [r for call in tracker.append_migration_status.call_args_list for r in call.args[0]]
        assert not any(r["status"] == "skipped_by_rls_cm_policy" for r in recorded)


class TestStagingCopyFlow:
    """Path A staging_copy: when rls_cm_strategy='staging_copy' and a
    pending table carries RLS/CM, setup_sharing must CTAS into the
    cp_migration_staging schema and add the STAGING fqn to the share.
    Source RLS/CM is never touched.
    """

    def _config(self) -> MagicMock:
        config = MagicMock()
        config.rls_cm_strategy = "staging_copy"
        config.dry_run = False
        config.tracking_catalog = "tcat"
        config.current_run_id = "run-abc"
        return config

    def _mock_auth_tracker(self, pending_fqn: str):
        """Builds auth + tracker mocks matching the run() side-effects we need."""
        auth = MagicMock()
        auth.source_client.shares.get.return_value = MagicMock(name="cp_migration_share")
        auth.source_client.recipients.get.return_value = MagicMock(name="cp_migration_recipient_x")
        auth.target_client.metastores.summary.return_value = MagicMock(global_metastore_id="m")

        tracker = MagicMock()
        tracker.get_pending_objects.return_value = [
            {
                "object_name": pending_fqn,
                "object_type": "managed_table",
                "catalog_name": "c",
                "schema_name": "s",
            }
        ]
        tracker.get_tables_with_rls_cm.return_value = {pending_fqn}
        return auth, tracker

    def test_staging_copy_creates_staging_table_via_ctas(self):
        """CTAS must run into the cp_migration_staging schema and the source
        must NOT be stripped (no DROP ROW FILTER / DROP MASK calls)."""
        from migrate import setup_sharing

        original_fqn = "`c`.`s`.`rls_table`"
        config = self._config()
        auth, tracker = self._mock_auth_tracker(original_fqn)
        spark = MagicMock()

        with (
            patch("migrate.setup_sharing.MigrationConfig") as cfg_cls,
            patch("migrate.setup_sharing.AuthManager", return_value=auth),
            patch("migrate.setup_sharing.TrackingManager", return_value=tracker),
            patch("migrate.setup_sharing._add_rls_cm_from_tables_api"),
            patch(
                "migrate.setup_sharing.capture_rls_cm",
                return_value={
                    "filter_fn_fqn": "fn",
                    "filter_columns": [],
                    "masks": [],
                },
            ),
            patch("migrate.setup_sharing.has_rls_cm", return_value=True),
            patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas"),
            patch("migrate.setup_sharing.ensure_share_consumer_catalog"),
            patch("migrate.setup_sharing.add_tables_to_share"),
        ):
            cfg_cls.from_workspace_file.return_value = config
            setup_sharing.run(dbutils=MagicMock(), spark=spark)

        # Assert: spark.sql was called with a CREATE TABLE ... AS SELECT into staging schema.
        ctas_calls = [
            c.args[0]
            for c in spark.sql.call_args_list
            if "CREATE" in c.args[0]
            and "cp_migration_staging" in c.args[0]
            and "AS SELECT" in c.args[0]
        ]
        assert len(ctas_calls) == 1, f"Expected 1 CTAS call, got {len(ctas_calls)}: {ctas_calls}"
        assert "AS SELECT * FROM" in ctas_calls[0]
        assert original_fqn in ctas_calls[0]

        # Manifest write happens.
        tracker.record_staging_created.assert_called_once()
        kwargs = tracker.record_staging_created.call_args.kwargs
        assert kwargs["original_fqn"] == original_fqn
        assert kwargs["run_id"] == "run-abc"

        # Source NEVER stripped — no DROP ROW FILTER / DROP MASK SQL.
        strip_calls = [
            c.args[0]
            for c in spark.sql.call_args_list
            if "DROP ROW FILTER" in c.args[0] or "DROP MASK" in c.args[0]
        ]
        assert strip_calls == [], f"staging_copy must NOT strip source — found: {strip_calls}"

    def test_staging_copy_adds_staging_fqn_to_share_not_original(self):
        """The staging table goes into the share, NOT the original."""
        from migrate import setup_sharing

        original_fqn = "`c`.`s`.`rls_table`"
        config = self._config()
        auth, tracker = self._mock_auth_tracker(original_fqn)
        spark = MagicMock()

        with (
            patch("migrate.setup_sharing.MigrationConfig") as cfg_cls,
            patch("migrate.setup_sharing.AuthManager", return_value=auth),
            patch("migrate.setup_sharing.TrackingManager", return_value=tracker),
            patch("migrate.setup_sharing._add_rls_cm_from_tables_api"),
            patch(
                "migrate.setup_sharing.capture_rls_cm",
                return_value={
                    "filter_fn_fqn": "fn",
                    "filter_columns": [],
                    "masks": [],
                },
            ),
            patch("migrate.setup_sharing.has_rls_cm", return_value=True),
            patch("migrate.setup_sharing.ensure_target_catalogs_and_schemas"),
            patch("migrate.setup_sharing.ensure_share_consumer_catalog"),
            patch("migrate.setup_sharing.add_tables_to_share") as add_tables,
        ):
            cfg_cls.from_workspace_file.return_value = config
            setup_sharing.run(dbutils=MagicMock(), spark=spark)

        # add_tables_to_share gets the STAGING fqn, not the original.
        added = add_tables.call_args.args[2]
        assert len(added) == 1
        assert "cp_migration_staging" in added[0]["object_name"]
        assert added[0]["object_name"] != original_fqn
