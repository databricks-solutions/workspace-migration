"""Unit tests for :mod:`pre_check.collision_detection` (X.4).

Mirrors the X.2 pattern — mock ``auth.target_client`` and probe each
SDK call that the detection helper issues. No spark / notebook machinery.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from pre_check.collision_detection import (
    _NOT_PROBED_TYPES,
    _PROBES,
    build_skip_status_rows,
    detect_collisions,
    unprobed_types_present,
)


def _not_found_client() -> MagicMock:
    """Target client where every SDK ``*.get`` raises NotFound (target empty).

    Uses the real SDK ``NotFound`` (covers RESOURCE_DOES_NOT_EXIST, a subclass)
    — NOT a bare RuntimeError. After the fail-closed fix (#10) only genuine
    not-found errors map to "absent"; any other error is a check failure.
    """
    c = MagicMock()
    nf = NotFound("RESOURCE_DOES_NOT_EXIST")
    c.catalogs.get.side_effect = nf
    c.schemas.get.side_effect = nf
    c.tables.get.side_effect = nf
    c.functions.get.side_effect = nf
    c.volumes.read.side_effect = nf
    c.connections.get.side_effect = nf
    c.shares.get.side_effect = nf
    c.recipients.get.side_effect = nf
    c.registered_models.get.side_effect = nf
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


class TestCollisionProbesFailClosed:
    """Review finding #10: probes caught bare Exception and returned False,
    so a permission error on a pre-existing target read as 'absent' → 'safe',
    and the destructive migration proceeded. Probes must fail CLOSED: only a
    genuine NotFound means absent; any other error surfaces as a check failure."""

    def test_permission_denied_is_not_treated_as_absent(self):
        client = MagicMock()
        client.tables.get.side_effect = PermissionDenied("user has no permission on target")
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
            existing_status_keys=set(),
        )
        # Must NOT be silently empty (that would be fail-open). A check-failure
        # record is emitted so the caller can fail closed.
        assert len(out) == 1
        assert out[0]["check_failed"] is True
        assert "permission" in out[0]["error"].lower()

    def test_genuine_not_found_is_absent(self):
        """A real NotFound still means 'no collision' (no check_failed)."""
        client = _not_found_client()
        out = detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": "c", "object_type": "catalog", "source_type": "uc"}],
            existing_status_keys=set(),
        )
        assert out == []

    def test_real_collision_is_not_flagged_check_failed(self):
        client = _found_client()
        out = detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": "c", "object_type": "catalog", "source_type": "uc"}],
            existing_status_keys=set(),
        )
        assert len(out) == 1
        assert out[0].get("check_failed") is False

    def test_build_skip_status_rows_ignores_check_failures(self):
        """A check-failure must never become a skipped_target_exists row —
        skipping an object we couldn't verify is exactly the unsafe outcome."""
        collisions = [
            {"object_type": "managed_table", "source_fqn": "`c`.`s`.`real`",
             "target_fqn": "c.s.real", "source_type": "uc", "check_failed": False},
            {"object_type": "managed_table", "source_fqn": "`c`.`s`.`unverifiable`",
             "target_fqn": "c.s.unverifiable", "source_type": "uc", "check_failed": True,
             "error": "PermissionDenied"},
        ]
        rows = build_skip_status_rows(collisions)
        names = {r["object_name"] for r in rows}
        assert names == {"`c`.`s`.`real`"}


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
    """Types intentionally exempt from collision probing (review finding #6):
    governance objects (idempotent re-apply), hard-excluded MV/ST, and types
    whose worker tolerates pre-existing target state. Detection returns nothing
    for them. NOTE: share / recipient / connection / registered_model are NOT
    here — they are global-namespace securables and ARE probed now."""

    @pytest.mark.parametrize(
        "object_type",
        [
            "provider",
            "foreign_catalog",
            "online_table",
            "monitor",
            "tag",
            "row_filter",
            "column_mask",
            "comment",
            "policy",
            "mv",
            "st",
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


# ---------------------------------------------------------------------------
# Review finding #6 — global-namespace probes, surfacing, coverage guard
# ---------------------------------------------------------------------------


class TestGlobalNamespaceProbes:
    """connection / share / recipient / registered_model live in flat
    namespaces where a clash can hit a stranger's object — they ARE probed."""

    def _client_present(self, attr):
        c = MagicMock()
        getattr(c, attr).get.return_value = MagicMock()
        return c

    @pytest.mark.parametrize(
        "object_type,name",
        [
            ("connection", "prod_sqlserver"),
            ("share", "partner_share"),
            ("recipient", "partner_recipient"),
            ("registered_model", "`cat`.`sch`.`m`"),
        ],
    )
    def test_present_global_object_emits_collision(self, object_type, name):
        client = MagicMock()
        # All SDK getters succeed → object exists.
        out = detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": name, "object_type": object_type, "source_type": "uc"}],
            existing_status_keys=set(),
        )
        assert len(out) == 1
        assert out[0]["object_type"] == object_type
        assert out[0]["check_failed"] is False

    def test_connection_probe_fails_closed_on_permission_error(self):
        from databricks.sdk.errors import PermissionDenied

        client = MagicMock()
        client.connections.get.side_effect = PermissionDenied("no access")
        out = detect_collisions(
            target_client=client,
            discovery_rows=[{"object_name": "c", "object_type": "connection", "source_type": "uc"}],
            existing_status_keys=set(),
        )
        assert out[0]["check_failed"] is True


class TestCollisionCoverageGuard:
    """Self-enforcing rule (#6): every migrated object type must be either
    collision-probed or explicitly exempted, so a newly-migrated type can't
    silently ship without a probe decision."""

    def test_every_migrated_type_is_probed_or_exempt(self):
        from migrate.orchestrator import BATCHED_TYPES, LIST_TYPES

        migrated = set(BATCHED_TYPES) | set(LIST_TYPES)
        covered = set(_PROBES) | set(_NOT_PROBED_TYPES)
        missing = migrated - covered
        assert not missing, (
            f"Migrated object type(s) {sorted(missing)} are neither collision-probed "
            f"(_PROBES) nor exempted (_NOT_PROBED_TYPES). Add a probe (preferred) or an "
            f"exemption-with-reason in collision_detection.py."
        )

    def test_unprobed_types_present_surfaces_only_discovered(self):
        rows = [
            {"object_name": "c.s.t", "object_type": "managed_table", "source_type": "uc"},
            {"object_name": "m", "object_type": "monitor", "source_type": "uc"},
            {"object_name": "p", "object_type": "provider", "source_type": "uc"},
        ]
        # managed_table is probed (not surfaced); monitor + provider are exempt.
        assert unprobed_types_present(rows) == ["monitor", "provider"]
