"""Unit tests for :mod:`pre_check.collision_detection` (X.4).

Mirrors the X.2 pattern — mock ``auth.target_client`` and probe each
SDK call that the detection helper issues. No spark / notebook machinery.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pre_check.collision_detection import (
    _PROBES,
    build_skip_status_rows,
    detect_collisions,
)


def _not_found_client() -> MagicMock:
    """Target client where every SDK ``*.get`` raises (target is empty)."""
    c = MagicMock()
    c.catalogs.get.side_effect = RuntimeError("NOT_FOUND")
    c.schemas.get.side_effect = RuntimeError("NOT_FOUND")
    c.tables.get.side_effect = RuntimeError("NOT_FOUND")
    c.functions.get.side_effect = RuntimeError("NOT_FOUND")
    c.volumes.read.side_effect = RuntimeError("NOT_FOUND")
    return c


def _found_client() -> MagicMock:
    """Target client where every SDK ``*.get`` succeeds (everything exists)."""
    c = MagicMock()
    # Returning MagicMock() means the probe helper treats the call as a
    # successful "exists". (A real SDK would return a typed object.)
    c.catalogs.get.return_value = MagicMock()
    c.schemas.get.return_value = MagicMock()
    c.tables.get.return_value = MagicMock()
    c.functions.get.return_value = MagicMock()
    c.volumes.read.return_value = MagicMock()
    return c


# ---------------------------------------------------------------------------
# detect_collisions — per-object-type matrix
# ---------------------------------------------------------------------------


class TestDetectCollisionsPerType:
    """For each supported UC object_type, assert that a target-present probe
    produces a collision record and a target-absent probe produces none."""

    @pytest.mark.parametrize(
        "object_type,source_fqn,expected_target",
        [
            ("catalog", "retail", "retail"),
            ("schema", "`retail`.`orders`", "retail.orders"),
            ("managed_table", "`retail`.`orders`.`t`", "retail.orders.t"),
            ("external_table", "`retail`.`orders`.`ext_t`", "retail.orders.ext_t"),
            ("view", "`retail`.`orders`.`v`", "retail.orders.v"),
            ("function", "`retail`.`orders`.`f`", "retail.orders.f"),
            ("volume", "`retail`.`orders`.`vol`", "retail.orders.vol"),
        ],
    )
    def test_target_present_emits_collision(self, object_type, source_fqn, expected_target):
        """When target has the same FQN and no status row exists, emit a
        collision record with the normalized dotted target FQN."""
        client = _found_client()
        rows = [
            {
                "object_name": source_fqn,
                "object_type": object_type,
                "source_type": "uc",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
        )
        assert len(out) == 1
        assert out[0]["object_type"] == object_type
        assert out[0]["source_fqn"] == source_fqn
        assert out[0]["target_fqn"] == expected_target

    @pytest.mark.parametrize("object_type", list(_PROBES.keys()))
    def test_target_absent_emits_nothing(self, object_type):
        """When target returns NOT_FOUND for the probe, detection is a no-op."""
        client = _not_found_client()
        rows = [
            {
                "object_name": "`c`.`s`.`n`" if object_type not in ("catalog", "schema") else "c",
                "object_type": object_type,
                "source_type": "uc",
            }
        ]
        # Also handle schema FQN shape
        if object_type == "schema":
            rows[0]["object_name"] = "`c`.`s`"
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
        )
        assert out == []


class TestCollisionsRespectExistingStatus:
    """X.2 resume compatibility: a row already in migration_status is
    **ours** (we created or are migrating it) — collision detection must
    ignore those to avoid breaking the idempotency audit's tolerance."""

    def test_skips_object_with_existing_status_row(self):
        """Even if the target has the FQN, if a status row exists for
        (object_type, object_name), we don't count it as a collision."""
        client = _found_client()
        rows = [
            {
                "object_name": "`retail`.`orders`.`t`",
                "object_type": "managed_table",
                "source_type": "uc",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys={("managed_table", "`retail`.`orders`.`t`")},
        )
        assert out == []

    def test_mixed_some_tracked_some_not(self):
        """Only untracked objects produce collision records."""
        client = _found_client()
        rows = [
            {  # tracked — skipped
                "object_name": "`c`.`s`.`tracked`",
                "object_type": "managed_table",
                "source_type": "uc",
            },
            {  # untracked — collision
                "object_name": "`c`.`s`.`untracked`",
                "object_type": "managed_table",
                "source_type": "uc",
            },
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys={("managed_table", "`c`.`s`.`tracked`")},
        )
        assert len(out) == 1
        assert out[0]["source_fqn"] == "`c`.`s`.`untracked`"


class TestCollisionProbesRoute:
    """The probe helpers hit the right SDK endpoint per object_type so
    changes to one don't silently regress another."""

    def test_catalog_probe_calls_catalogs_get(self):
        client = _found_client()
        detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": "x", "object_type": "catalog", "source_type": "uc"}],
            existing_status_keys=set(),
        )
        client.catalogs.get.assert_called_once_with(name="x")

    def test_schema_probe_calls_schemas_get_with_dotted(self):
        client = _found_client()
        detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": "`c`.`s`", "object_type": "schema", "source_type": "uc"}],
            existing_status_keys=set(),
        )
        client.schemas.get.assert_called_once_with(full_name="c.s")

    def test_table_probe_calls_tables_get_with_dotted(self):
        client = _found_client()
        detect_collisions(
            target_client=client,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"}
            ],
            existing_status_keys=set(),
        )
        client.tables.get.assert_called_once_with(full_name="c.s.t")

    def test_function_probe_calls_functions_get(self):
        client = _found_client()
        detect_collisions(
            target_client=client,
            discovery_rows=[
                {"object_name": "`c`.`s`.`f`", "object_type": "function", "source_type": "uc"}
            ],
            existing_status_keys=set(),
        )
        client.functions.get.assert_called_once_with(name="c.s.f")

    def test_volume_probe_calls_volumes_read(self):
        client = _found_client()
        detect_collisions(
            target_client=client,
            discovery_rows=[
                {"object_name": "`c`.`s`.`v`", "object_type": "volume", "source_type": "uc"}
            ],
            existing_status_keys=set(),
        )
        client.volumes.read.assert_called_once_with(name="c.s.v")


class TestCollisionHiveRewrite:
    """Hive source objects land on target under ``hive_target_catalog`` —
    detection must probe the rewritten target FQN, not the source one."""

    def test_hive_table_rewritten_to_target_catalog(self):
        client = _found_client()
        rows = [
            {
                "object_name": "`hive_metastore`.`sales`.`t`",
                "object_type": "hive_table",
                "source_type": "hive",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
            hive_target_catalog="hive_upgraded",
        )
        assert len(out) == 1
        # Source FQN is preserved; target FQN is rewritten under the
        # configured upgrade catalog.
        assert out[0]["target_fqn"] == "hive_upgraded.sales.t"
        client.tables.get.assert_called_once_with(full_name="hive_upgraded.sales.t")

    def test_hive_view_rewritten_to_target_catalog(self):
        client = _found_client()
        rows = [
            {
                "object_name": "`hive_metastore`.`sales`.`v`",
                "object_type": "hive_view",
                "source_type": "hive",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
            hive_target_catalog="custom_hive_target",
        )
        assert len(out) == 1
        assert out[0]["target_fqn"] == "custom_hive_target.sales.v"

    def test_hive_function_rewritten_and_probed_via_functions_get(self):
        client = _found_client()
        rows = [
            {
                "object_name": "`hive_metastore`.`sales`.`fn`",
                "object_type": "hive_function",
                "source_type": "hive",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
            hive_target_catalog="hive_upgraded",
        )
        assert len(out) == 1
        assert out[0]["target_fqn"] == "hive_upgraded.sales.fn"
        client.functions.get.assert_called_once_with(name="hive_upgraded.sales.fn")


class TestCollisionUnsupportedTypes:
    """Phase 3 governance types are intentionally out of scope for v1 —
    those workers already tolerate pre-existing target state per
    docs/idempotency_audit.md. Detection returns nothing for them."""

    @pytest.mark.parametrize(
        "object_type",
        [
            "share",
            "recipient",
            "provider",
            "connection",
            "foreign_catalog",
            "online_table",
            "monitor",
            "registered_model",
            "tag",
            "row_filter",
            "column_mask",
            "comment",
            "policy",
        ],
    )
    def test_phase3_types_do_not_probe(self, object_type):
        client = _not_found_client()
        rows = [
            {
                "object_name": "some_obj",
                "object_type": object_type,
                "source_type": "uc",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
        )
        assert out == []
        # And no probe call was made for these types — defensive: make
        # sure we're not accidentally hitting a shared ``*.get`` path.
        client.catalogs.get.assert_not_called()
        client.schemas.get.assert_not_called()
        client.tables.get.assert_not_called()
        client.functions.get.assert_not_called()
        client.volumes.read.assert_not_called()


# ---------------------------------------------------------------------------
# build_skip_status_rows — migration_status row shape
# ---------------------------------------------------------------------------


class TestBuildSkipStatusRows:
    """The skip policy seeds ``migration_status`` with
    ``skipped_target_exists`` rows, which get_pending_objects treats as
    terminal (added to tracking.py terminal set in this PR)."""

    def test_empty_input_yields_empty_output(self):
        assert build_skip_status_rows([]) == []

    def test_each_collision_becomes_one_status_row(self):
        rows = build_skip_status_rows(
            [
                {
                    "object_type": "managed_table",
                    "source_fqn": "`c`.`s`.`t`",
                    "target_fqn": "c.s.t",
                    "source_type": "uc",
                },
                {
                    "object_type": "schema",
                    "source_fqn": "`c`.`s`",
                    "target_fqn": "c.s",
                    "source_type": "uc",
                },
            ]
        )
        assert len(rows) == 2
        assert rows[0]["status"] == "skipped_target_exists"
        assert rows[0]["object_type"] == "managed_table"
        assert rows[0]["object_name"] == "`c`.`s`.`t`"
        assert "c.s.t" in rows[0]["error_message"]
        assert rows[1]["status"] == "skipped_target_exists"
        assert rows[1]["object_type"] == "schema"

    def test_status_row_has_all_schema_fields(self):
        """Row keys must match migration_status schema so
        TrackingManager.append_migration_status doesn't drop fields."""
        rows = build_skip_status_rows(
            [
                {
                    "object_type": "managed_table",
                    "source_fqn": "`c`.`s`.`t`",
                    "target_fqn": "c.s.t",
                    "source_type": "uc",
                }
            ]
        )
        expected_keys = {
            "object_name",
            "object_type",
            "status",
            "error_message",
            "job_run_id",
            "task_run_id",
            "source_row_count",
            "target_row_count",
            "duration_seconds",
        }
        assert set(rows[0].keys()) == expected_keys


class TestCollisionEdgeCases:
    """Corner cases that the detection helper defends against."""

    def test_empty_discovery_rows_yields_no_collisions(self):
        client = _found_client()
        assert (
            detect_collisions(
                target_client=client,
                discovery_rows=[],
                existing_status_keys=set(),
            )
            == []
        )

    def test_row_missing_object_name_is_skipped(self):
        """Defensive: malformed discovery rows shouldn't crash detection."""
        client = _found_client()
        rows = [
            {"object_name": "", "object_type": "managed_table", "source_type": "uc"},
            {"object_name": None, "object_type": "catalog", "source_type": "uc"},
        ]
        assert (
            detect_collisions(
                target_client=client,
                discovery_rows=rows,
                existing_status_keys=set(),
            )
            == []
        )

    def test_row_missing_source_type_defaults_to_uc(self):
        """Rows without source_type (legacy / test data) are treated as UC."""
        client = _found_client()
        rows = [
            {
                "object_name": "`c`.`s`.`t`",
                "object_type": "managed_table",
            }
        ]
        out = detect_collisions(
            target_client=client,
            discovery_rows=rows,
            existing_status_keys=set(),
        )
        assert len(out) == 1
        assert out[0]["source_type"] == "uc"
