"""Unit tests for Phase 3 governance workers (Tasks 28-37)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _ok() -> dict:
    return {"state": "SUCCEEDED", "statement_id": "s"}


def _fail(err: str = "ERR") -> dict:
    return {"state": "FAILED", "error": err, "statement_id": "s"}


# ---------------------------------------------------------------- Tags --


class TestTagsWorker:
    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_tag_group_to_table(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        auth = MagicMock()
        result = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "env", "tag_value": "prod"}],
            auth=auth,
            wh_id="wh-1",
            dry_run=False,
        )
        assert result["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER TABLE `c`.`s`.`t` SET TAGS" in sql
        assert "'env' = 'prod'" in sql

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_column_tag(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = _ok()

        result = apply_tag_group(
            ("COLUMN", "`c`.`s`.`t`", "ssn"),
            [{"tag_name": "pii", "tag_value": "true"}],
            auth=MagicMock(),
            wh_id="wh-1",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert "ALTER COLUMN `ssn` SET TAGS" in sql
        assert result["object_name"].endswith(".ssn")

    def test_tag_value_escapes_quotes(self):
        from migrate.tags_worker import _tag_clause

        clause = _tag_clause([("owner", "O'Brien")])
        assert clause == "('owner' = 'O''Brien')"

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_catalog_tag(self, mock_execute, mock_time):
        """Item 3.1 — CATALOG-level tags must render ``ALTER CATALOG ... SET TAGS``."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        result = apply_tag_group(
            ("CATALOG", "`prod_cat`", ""),
            [{"tag_name": "owner", "tag_value": "finance"}],
            auth=MagicMock(),
            wh_id="wh-1",
            dry_run=False,
        )
        assert result["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER CATALOG `prod_cat` SET TAGS" in sql
        assert "'owner' = 'finance'" in sql
        # No column suffix on the tracking key.
        assert result["object_name"] == "TAGS_CATALOG_`prod_cat`"

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_schema_tag(self, mock_execute, mock_time):
        """Item 3.1 — SCHEMA-level tags must render ``ALTER SCHEMA ... SET TAGS``."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        result = apply_tag_group(
            ("SCHEMA", "`c`.`gold`", ""),
            [
                {"tag_name": "tier", "tag_value": "bronze"},
                {"tag_name": "pii", "tag_value": "false"},
            ],
            auth=MagicMock(),
            wh_id="wh-1",
            dry_run=False,
        )
        assert result["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER SCHEMA `c`.`gold` SET TAGS" in sql
        # Two tags land in one ALTER statement (grouping contract).
        assert "'tier' = 'bronze'" in sql
        assert "'pii' = 'false'" in sql

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_applies_volume_tag(self, mock_execute, mock_time):
        """Item 3.1 — VOLUME-level tags must render ``ALTER VOLUME ... SET TAGS``."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        result = apply_tag_group(
            ("VOLUME", "`c`.`s`.`landing_vol`", ""),
            [{"tag_name": "retention_days", "tag_value": "30"}],
            auth=MagicMock(),
            wh_id="wh-1",
            dry_run=False,
        )
        assert result["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER VOLUME `c`.`s`.`landing_vol` SET TAGS" in sql
        assert "'retention_days' = '30'" in sql


# ---------------------------------------------------- Comments --------


class TestCommentsWorker:
    """Item 3.5 — ensure ``_emit_comment`` handles every UC securable type,
    including the two gaps that needed a narrow code fix (COLUMN, VOLUME)."""

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_emits_catalog_comment(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = _ok()

        res = _emit_comment(
            "CATALOG",
            "`c`",
            "finance domain catalog",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert sql == "COMMENT ON CATALOG `c` IS 'finance domain catalog'"

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_emits_schema_comment(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = _ok()

        res = _emit_comment(
            "SCHEMA",
            "`c`.`gold`",
            "curated gold layer",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert sql == "COMMENT ON SCHEMA `c`.`gold` IS 'curated gold layer'"

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_emits_column_comment_uses_alter_table_syntax(self, mock_execute, mock_time):
        """COMMENT ON COLUMN is NOT valid Databricks SQL — the worker must
        emit ``ALTER TABLE t ALTER COLUMN c COMMENT '...'`` instead."""
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.2]
        mock_execute.return_value = _ok()

        res = _emit_comment(
            "COLUMN",
            "`c`.`s`.`t`",
            "social security number",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
            column_name="ssn",
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER TABLE `c`.`s`.`t` ALTER COLUMN `ssn` COMMENT " in sql
        assert "'social security number'" in sql
        # COMMENT ON COLUMN must never appear — UC rejects it.
        assert "COMMENT ON COLUMN" not in sql
        assert res["object_name"] == "COMMENT_COLUMN_`c`.`s`.`t`.ssn"

    def test_column_comment_requires_column_name(self):
        import pytest

        from migrate.comments_worker import _emit_comment

        with pytest.raises(ValueError, match="column_name is required"):
            _emit_comment(
                "COLUMN",
                "`c`.`s`.`t`",
                "x",
                auth=MagicMock(),
                wh_id="wh",
                dry_run=False,
            )

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_emits_volume_comment(self, mock_execute, mock_time):
        """VOLUME comments go through the generic ``COMMENT ON VOLUME ... IS ...`` path."""
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = _ok()

        res = _emit_comment(
            "VOLUME",
            "`c`.`s`.`landing_vol`",
            "raw inbound files",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert sql == "COMMENT ON VOLUME `c`.`s`.`landing_vol` IS 'raw inbound files'"
        assert res["object_name"] == "COMMENT_VOLUME_`c`.`s`.`landing_vol`"

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_comment_escapes_single_quotes(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = _ok()

        _emit_comment(
            "TABLE",
            "`c`.`s`.`t`",
            "O'Brien's table",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert "'O''Brien''s table'" in sql

    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_comment_failure_surfaces_error(self, mock_execute, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.2]
        mock_execute.return_value = _fail("permission denied")

        res = _emit_comment(
            "VOLUME",
            "`c`.`s`.`v`",
            "x",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "permission denied" in res["error_message"]

    @patch("migrate.comments_worker.time")
    def test_comment_dry_run_skips_execution(self, mock_time):
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 100.1]

        res = _emit_comment(
            "COLUMN",
            "`c`.`s`.`t`",
            "pii",
            auth=MagicMock(),
            wh_id="wh",
            dry_run=True,
            column_name="email",
        )
        assert res["status"] == "skipped"
        assert res["error_message"] == "dry_run"


# ---------------------------------------------------- Row filters -----


class TestRowFiltersWorker:
    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_applies_row_filter(self, mock_execute, mock_time):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`", "filter_function_fqn": "c.s.region_fn", "filter_columns": ["region", "env"]},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER TABLE `c`.`s`.`t` SET ROW FILTER c.s.region_fn" in sql
        assert "ON (`region`, `env`)" in sql


# ---------------------------------------------------- Column masks ----


class TestColumnMasksWorker:
    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_applies_column_mask(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        res = apply_column_mask(
            {
                "table_fqn": "`c`.`s`.`users`",
                "column_name": "ssn",
                "mask_function_fqn": "c.s.redact_ssn",
                "mask_using_columns": ["role"],
            },
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_execute.call_args[0][2]
        assert "ALTER COLUMN `ssn` SET MASK c.s.redact_ssn" in sql
        assert "USING COLUMNS (`role`)" in sql

    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_mask_without_using_columns(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = _ok()

        apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`", "column_name": "x", "mask_function_fqn": "c.s.fn", "mask_using_columns": []},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        sql = mock_execute.call_args[0][2]
        assert "USING COLUMNS" not in sql


# ---------------------------------------------------- Policies --------


class TestPoliciesWorker:
    @patch("migrate.policies_worker.time")
    def test_posts_policy(self, mock_time):
        from migrate.policies_worker import apply_policy

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_policy(
            {"name": "p1", "on_securable_fullname": "c.s.t"},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"
        auth.target_client.api_client.do.assert_called_once()
        method, path = auth.target_client.api_client.do.call_args[0][:2]
        assert method == "POST"
        assert "/policies" in path

    @patch("migrate.policies_worker.time")
    def test_records_error_on_api_failure(self, mock_time):
        from migrate.policies_worker import apply_policy

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception("403")

        res = apply_policy({"name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "failed"
        assert "403" in res["error_message"]


# ---------------------------------------------------- Monitors --------


class TestMonitorsWorker:
    @patch("migrate.monitors_worker.time")
    def test_posts_monitor(self, mock_time):
        from migrate.monitors_worker import apply_monitor

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_monitor(
            {
                "table_fqn": "`c`.`s`.`t`",
                "definition": {
                    "table_name": "source.s.t",  # should be stripped
                    "schedule": {"quartz_cron_expression": "0 0 * * * ?"},
                    "status": "ACTIVE",  # should be stripped
                },
            },
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"
        body = auth.target_client.api_client.do.call_args.kwargs["body"]
        assert "table_name" not in body  # stripped
        assert "status" not in body
        assert "schedule" in body


# ---------------------------------------------------- Models ----------


class TestModelsWorker:
    @patch("migrate.models_worker.run_target_file_copy")
    @patch("migrate.models_worker.ensure_copy_notebook_on_target")
    @patch("migrate.models_worker.time")
    def test_creates_model_with_versions_aliases_and_artifact_copy(self, mock_time, mock_ensure, mock_copy):
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 110.0]
        mock_copy.return_value = {"bytes_copied": 12345, "file_count": 3}
        auth = MagicMock()
        auth.target_client.registered_models.create.return_value = MagicMock()
        created_version = MagicMock()
        created_version.storage_location = "abfss://target/.../m1/v1"
        auth.target_client.model_versions.create.return_value = created_version
        auth.target_client.registered_models.set_alias.return_value = MagicMock()

        results = apply_model(
            {
                "model_fqn": "c.s.m1",
                "storage_location": "abfss://source/.../m1",
                "versions": [
                    {
                        "version": 1,
                        "source": "run:/abc/art",
                        "storage_location": "abfss://source/.../m1/v1",
                        "aliases": ["prod"],
                    },
                ],
            },
            auth=auth,
            dry_run=False,
        )
        assert len(results) == 1
        assert results[0]["status"] == "validated"
        # Artifact-copy outcome surfaced in the message.
        assert "3 file(s)" in results[0]["error_message"]
        assert "12345 byte(s)" in results[0]["error_message"]
        # Copy helper called with source URI → target allocated path.
        mock_copy.assert_called_once()
        call_kwargs = mock_copy.call_args.kwargs
        assert call_kwargs["src_path"] == "abfss://source/.../m1/v1"
        assert call_kwargs["dst_path"] == "abfss://target/.../m1/v1"
        auth.target_client.registered_models.set_alias.assert_called_once_with(
            full_name="c.s.m1",
            alias="prod",
            version_num=1,
        )

    @patch("migrate.models_worker.run_target_file_copy")
    @patch("migrate.models_worker.ensure_copy_notebook_on_target")
    @patch("migrate.models_worker.time")
    def test_idempotent_on_already_exists(self, mock_time, mock_ensure, mock_copy):
        from databricks.sdk.errors import AlreadyExists

        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 101.0]
        mock_copy.return_value = {"bytes_copied": 0, "file_count": 0}
        auth = MagicMock()
        auth.target_client.registered_models.create.side_effect = AlreadyExists(
            "RESOURCE_ALREADY_EXISTS"
        )
        auth.target_client.model_versions.create.return_value = MagicMock()

        results = apply_model(
            {"model_fqn": "c.s.m1", "versions": []},
            auth=auth,
            dry_run=False,
        )
        assert results[0]["status"] == "validated"

    @patch("migrate.models_worker.run_target_file_copy")
    @patch("migrate.models_worker.ensure_copy_notebook_on_target")
    @patch("migrate.models_worker.time")
    def test_artifact_copy_failure_hard_fails(self, mock_time, mock_ensure, mock_copy):
        """L4: artifact copy failure now hard-fails (matches volume_worker).

        Previously this returned ``validation_failed`` with a warning;
        the artifact bytes are essential, not best-effort, so a copy
        failure marks the row ``failed`` so operators don't miss it.
        """
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 105.0]
        mock_copy.side_effect = RuntimeError(
            "Target copy job failed (model_artifact_copy__c.s.m1__v1): PERMISSION_DENIED"
        )
        auth = MagicMock()
        created_version = MagicMock()
        created_version.storage_location = "abfss://target/.../m1/v1"
        auth.target_client.registered_models.create.return_value = MagicMock()
        auth.target_client.model_versions.create.return_value = created_version

        results = apply_model(
            {
                "model_fqn": "c.s.m1",
                "versions": [
                    {
                        "version": 1,
                        "storage_location": "abfss://source/.../m1/v1",
                        "aliases": [],
                    }
                ],
            },
            auth=auth,
            dry_run=False,
        )
        assert results[0]["status"] == "failed"
        assert "artifact copy failed" in results[0]["error_message"]
        assert "abfss://source/.../m1/v1" in results[0]["error_message"]

    @patch("migrate.models_worker.run_target_file_copy")
    @patch("migrate.models_worker.ensure_copy_notebook_on_target")
    @patch("migrate.models_worker.time")
    def test_copy_notebook_upload_failure_skips_artifact_copy(self, mock_time, mock_ensure, mock_copy):
        """If the helper notebook can't be uploaded, artifact copy is skipped
        silently and the message flags it — model metadata still migrates."""
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 101.0]
        mock_ensure.side_effect = Exception("workspace.import_ failed")
        auth = MagicMock()
        auth.target_client.registered_models.create.return_value = MagicMock()
        auth.target_client.model_versions.create.return_value = MagicMock()

        results = apply_model(
            {
                "model_fqn": "c.s.m1",
                "versions": [{"version": 1, "storage_location": "abfss://src/v1"}],
            },
            auth=auth,
            dry_run=False,
        )
        # Still validated — artifact copy is best-effort.
        assert results[0]["status"] == "validated"
        assert "artifacts NOT copied" in results[0]["error_message"]
        mock_copy.assert_not_called()


# ---------------------------------------------------- Connections -----


class TestConnectionsWorker:
    @patch("migrate.connections_worker.time")
    def test_creates_connection_and_flags_missing_credentials(self, mock_time):
        from migrate.connections_worker import apply_connection

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.connections.create.return_value = MagicMock()

        res = apply_connection(
            {"connection_name": "snow", "connection_type": "SNOWFLAKE", "options": {"host": "acct", "password": ""}},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validation_failed"
        assert "password" in res["error_message"]

    @patch("migrate.connections_worker.time")
    def test_validated_when_no_secret_options(self, mock_time):
        from migrate.connections_worker import apply_connection

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.connections.create.return_value = MagicMock()

        res = apply_connection(
            {"connection_name": "httpapi", "connection_type": "HTTP", "options": {"url": "https://x"}},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"


# ---------------------------------------------------- Foreign catalogs


class TestForeignCatalogsWorker:
    @patch("migrate.foreign_catalogs_worker.time")
    def test_creates_foreign_catalog(self, mock_time):
        from migrate.foreign_catalogs_worker import apply_foreign_catalog

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        res = apply_foreign_catalog(
            {"catalog_name": "snow_fc", "connection_name": "snow", "options": {}},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"
        auth.target_client.catalogs.create.assert_called_once()


# ---------------------------------------------------- Online tables ---


class TestOnlineTablesWorker:
    @patch("migrate.online_tables_worker.time")
    def test_posts_online_table(self, mock_time):
        from migrate.online_tables_worker import apply_online_table

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.return_value = {"ok": True}

        res = apply_online_table(
            {"online_table_fqn": "c.s.online_t", "definition": {"spec": {"source_table_full_name": "c.s.t"}}},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"
        body = auth.target_client.api_client.do.call_args.kwargs["body"]
        assert body["name"] == "c.s.online_t"
        assert body["spec"]["source_table_full_name"] == "c.s.t"


# ---------------------------------------------------- Sharing ---------


class TestSharingWorker:
    @patch("migrate.sharing_worker.time")
    @patch("migrate.sharing_worker.execute_and_poll")
    def test_share_adds_mixed_object_types(self, mock_execute, mock_time):
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 130.0]
        mock_execute.return_value = _ok()
        auth = MagicMock()
        auth.target_client.shares.create.return_value = MagicMock()

        res = apply_share(
            {
                "share_name": "s1",
                "comment": None,
                "objects": [
                    {"name": "c.s.t", "data_object_type": "SharedDataObjectDataObjectType.TABLE"},
                    {"name": "c.s.v", "data_object_type": "VIEW"},
                    {"name": "c.s.vol", "data_object_type": "VOLUME"},
                    {"name": "c.s", "data_object_type": "SCHEMA"},
                ],
            },
            auth=auth,
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"
        sqls = [c.args[2] for c in mock_execute.call_args_list]
        assert any("ADD TABLE" in s for s in sqls)
        assert any("ADD VIEW" in s for s in sqls)
        assert any("ADD VOLUME" in s for s in sqls)
        assert any("ADD SCHEMA" in s for s in sqls)

    @patch("migrate.sharing_worker.time")
    def test_recipient_idempotent(self, mock_time):
        from migrate.sharing_worker import apply_recipient

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.recipients.create.side_effect = Exception("already exists")

        res = apply_recipient(
            {"recipient_name": "r1", "authentication_type": "DATABRICKS"},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "validated"
        assert "already existed" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    @patch("migrate.sharing_worker.execute_and_poll")
    def test_share_partial_failure_marks_validation_failed(
        self,
        mock_execute,
        mock_time,
    ):
        """If some ADD succeeds and some fails, share row should be
        ``validation_failed`` so operators know to investigate without
        halting the rest of the share pipeline."""
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 105.0]
        # First two ADDs succeed, third fails
        mock_execute.side_effect = [
            _ok(),
            _ok(),
            {"state": "FAILED", "error": "UNAUTHORIZED", "statement_id": "s"},
        ]
        auth = MagicMock()
        auth.target_client.shares.create.return_value = MagicMock()

        res = apply_share(
            {
                "share_name": "s1",
                "objects": [
                    {"name": "c.s.t1", "data_object_type": "TABLE"},
                    {"name": "c.s.t2", "data_object_type": "TABLE"},
                    {"name": "c.s.t3", "data_object_type": "TABLE"},
                ],
            },
            auth=auth,
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validation_failed"
        assert "UNAUTHORIZED" in res["error_message"]
        assert "Added 2" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    def test_provider_failure_surfaces_error(self, mock_time):
        """Create-provider failure must produce status='failed' — we
        don't silently treat API errors as success."""
        from migrate.sharing_worker import apply_provider

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.providers.create.side_effect = Exception("INVALID_ACTIVATION_URL")

        res = apply_provider(
            {"provider_name": "p1", "authentication_type": "TOKEN", "recipient_profile_str": "http://..."},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "INVALID_ACTIVATION_URL" in res["error_message"]

    @patch("migrate.sharing_worker.time")
    def test_share_dry_run_skips_api(self, mock_time):
        """Dry run must NOT hit shares.create on target."""
        from migrate.sharing_worker import apply_share

        mock_time.time.side_effect = [100.0, 100.0]
        auth = MagicMock()
        res = apply_share(
            {"share_name": "s1", "objects": []},
            auth=auth,
            wh_id="wh",
            dry_run=True,
        )
        assert res["status"] == "skipped"
        assert res["error_message"] == "dry_run"
        auth.target_client.shares.create.assert_not_called()


class TestPhase3WorkersIdempotency:
    """Cross-worker contract: each Phase 3 worker must tolerate its
    target object already existing on target (e.g. from a previous
    partial migrate). The tests below exercise idempotency per object
    type — a re-run must not fail with ``ALREADY_EXISTS`` or similar.
    """

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_tolerates_already_set_tag(self, mock_execute, mock_time):
        """Re-applying a tag that already exists must succeed (SET TAGS
        is idempotent in UC — same key/value is a no-op)."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}

        auth = MagicMock()
        tag_group = [
            {
                "securable_type": "TABLE",
                "securable_fqn": "`c`.`s`.`t`",
                "tag_name": "env",
                "tag_value": "test",
            },
        ]
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            tag_group,
            auth=auth,
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "validated"


class TestPhase3DispatchOnObjectType:
    """Every Phase 3 worker reads its ``object_type`` from the incoming
    list payload and dispatches only to the types it owns. Prevents a
    regression where e.g. tags_worker accidentally starts processing
    row_filter entries.
    """

    def test_tags_worker_only_handles_tag_rows(self):
        """Source-level check that tags_worker's per-row dispatch
        filters on object_type == 'tag'."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "tags_worker.py").read_text()
        # Either explicit object_type filtering, or reading tag_list
        # (which the orchestrator already pre-filters to tag type).
        assert "tag_list" in src or "object_type = 'tag'" in src or 'object_type == "tag"' in src

    def test_row_filters_worker_uses_row_filter_list(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "row_filters_worker.py").read_text()
        assert "row_filter_list" in src

    def test_column_masks_worker_uses_column_mask_list(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "column_masks_worker.py").read_text()
        assert "column_mask_list" in src


class TestPhase3StatusEmission:
    """Every Phase 3 worker writes to migration_status with an
    object_type matching the Phase 3 backlog (tag, row_filter,
    column_mask, policy, comment, monitor, registered_model,
    connection, foreign_catalog, share, recipient, provider,
    online_table). Locks in the naming so dashboard panels + the
    tracker's NOT-LIKE-'skipped%' filter stay aligned."""

    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_writes_object_type_tag(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        auth = MagicMock()
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [
                {
                    "securable_type": "TABLE",
                    "securable_fqn": "`c`.`s`.`t`",
                    "tag_name": "k",
                    "tag_value": "v",
                }
            ],
            auth=auth,
            wh_id="wh",
            dry_run=False,
        )
        assert res["object_type"] == "tag"

    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_row_filters_worker_writes_object_type_row_filter(self, mock_execute, mock_time):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.1]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        auth = MagicMock()
        res = apply_row_filter(
            {
                "table_fqn": "`c`.`s`.`t`",
                "filter_function_fqn": "c.s.f",
                "filter_columns": ["region"],
            },
            auth=auth,
            wh_id="wh",
            dry_run=False,
        )
        assert res["object_type"] == "row_filter"


# ---------------------------------------------------- Negative-path ----
#
# Every Phase 3 worker must turn a downstream failure into a status='failed'
# tracking row, not a raised exception. Locks in the contract so a single
# bad tag / RLS / mask / monitor doesn't halt the whole worker.


class TestPhase3WorkerErrorSurfacing:
    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_tags_worker_surfaces_failed_sql(self, mock_execute, mock_time):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "PERMISSION_DENIED: not metastore admin",
            "statement_id": "s",
        }
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "env", "tag_value": "prod"}],
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "PERMISSION_DENIED" in res["error_message"]

    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_row_filter_surfaces_missing_function(self, mock_execute, mock_time):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "ROUTINE_NOT_FOUND",
            "statement_id": "s",
        }
        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`", "filter_function_fqn": "c.s.missing_fn", "filter_columns": ["region"]},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "ROUTINE_NOT_FOUND" in res["error_message"]

    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_column_mask_surfaces_missing_function(self, mock_execute, mock_time):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 100.5]
        mock_execute.return_value = {
            "state": "FAILED",
            "error": "ROUTINE_NOT_FOUND",
            "statement_id": "s",
        }
        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`", "column_name": "ssn", "mask_function_fqn": "c.s.missing_mask"},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "ROUTINE_NOT_FOUND" in res["error_message"]

    @patch("migrate.monitors_worker.time")
    def test_monitor_surfaces_api_failure(self, mock_time):
        from migrate.monitors_worker import apply_monitor

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception("TABLE_NOT_FOUND: monitor target missing")
        res = apply_monitor(
            {"table_fqn": "`c`.`s`.`t`", "definition": {"schedule": {"quartz_cron_expression": "0 0 * * * ?"}}},
            auth=auth,
            dry_run=False,
        )
        assert res["status"] == "failed"
        assert "TABLE_NOT_FOUND" in res["error_message"]

    @patch("migrate.models_worker.time")
    def test_model_surfaces_api_failure(self, mock_time):
        from migrate.models_worker import apply_model

        mock_time.time.side_effect = [100.0, 101.0]
        auth = MagicMock()
        auth.target_client.registered_models.create.side_effect = Exception("PERMISSION_DENIED on schema")
        results = apply_model(
            {"model_fqn": "c.s.m1", "storage_location": "abfss://x@y/m1", "versions": []},
            auth=auth,
            dry_run=False,
        )
        # apply_model returns a list of results (model + versions)
        assert any(r["status"] == "failed" for r in results)
        assert any("PERMISSION_DENIED" in (r.get("error_message") or "") for r in results)


# ---------------------------------------------------- Dry-run gate ------


class TestPhase3DryRun:
    """Every worker that takes dry_run must short-circuit execute_and_poll
    and return status='skipped' / error_message='dry_run'. Missing these
    means dry_run silently hits the target."""

    @patch("migrate.tags_worker.execute_and_poll")
    @patch("migrate.tags_worker.time")
    def test_tags_worker_dry_run(self, mock_time, mock_execute):
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "k", "tag_value": "v"}],
            auth=MagicMock(),
            wh_id="wh",
            dry_run=True,
        )
        assert res["status"] == "skipped"
        assert res["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.row_filters_worker.execute_and_poll")
    @patch("migrate.row_filters_worker.time")
    def test_row_filter_dry_run(self, mock_time, mock_execute):
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`", "filter_function_fqn": "c.s.f", "filter_columns": ["r"]},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=True,
        )
        assert res["status"] == "skipped"
        mock_execute.assert_not_called()

    @patch("migrate.column_masks_worker.execute_and_poll")
    @patch("migrate.column_masks_worker.time")
    def test_column_mask_dry_run(self, mock_time, mock_execute):
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 100.0]
        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`", "column_name": "ssn", "mask_function_fqn": "c.s.f"},
            auth=MagicMock(),
            wh_id="wh",
            dry_run=True,
        )
        assert res["status"] == "skipped"
        mock_execute.assert_not_called()
