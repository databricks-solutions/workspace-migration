"""RLS / CM capture + staging-FQN primitives.

Used by ``setup_sharing`` to capture row filter / column mask metadata
off the source table (so the post-migrate worker can re-apply the
same policy on the target) and to derive a deterministic staging-table
FQN for the Path A staging_copy strategy.

Plain module (no ``# Databricks notebook source`` header) so notebooks
can import it — Databricks refuses notebook-to-notebook imports at
runtime.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from common.auth import AuthManager

logger = logging.getLogger(__name__)


def _split_fqn(fqn: str) -> tuple[str, str, str]:
    """Split a backtick or unquoted three-part FQN into (catalog, schema, name)."""
    clean = fqn.strip("`")
    parts = clean.split("`.`") if "`.`" in clean else clean.split(".")
    if len(parts) != 3:
        raise ValueError(f"Malformed FQN {fqn!r}: expected three parts")
    return parts[0], parts[1], parts[2]


def _dotted(fqn: str) -> str:
    """Canonicalise an FQN to ``catalog.schema.name`` (no backticks)."""
    cat, sch, name = _split_fqn(fqn)
    return f"{cat}.{sch}.{name}"


def _backticked(fqn: str) -> str:
    cat, sch, name = _split_fqn(fqn)
    return f"`{cat}`.`{sch}`.`{name}`"


def make_staging_table_fqn(original_fqn: str, run_id: str, tracking_catalog: str) -> str:
    r"""Generate the deterministic staging-table FQN for an original table.

    Format: `<tracking_catalog>`.`cp_migration_staging`.`stg_<sha12>`
    where sha12 is the first 12 hex chars of SHA-256 over
    ``<canonical_original_fqn>|<run_id>``. Same (original, run, tracking_catalog)
    triple always hashes the same name; different runs produce different
    names so a re-run after a partial cleanup never collides.

    Canonicalization strips backticks so ``\`c\`.\`s\`.\`t\``` and ``c.s.t``
    produce the same staging name.
    """
    canonical = _dotted(original_fqn)
    digest = hashlib.sha256(f"{canonical}|{run_id}".encode()).hexdigest()[:12]
    return f"`{tracking_catalog}`.`cp_migration_staging`.`stg_{digest}`"


def capture_rls_cm(auth: AuthManager, table_fqn: str) -> dict:
    """Read the live row filter + column masks off a source table.

    Uses ``source_client.tables.get`` — authoritative and bypasses the
    discovery_inventory snapshot, so we always record what's actually
    in effect at strip time.

    Returns a dict shape:
        {
            "filter_fn_fqn": str | None,
            "filter_columns": list[str],
            "masks": [{"column": str, "fn_fqn": str, "using_columns": list[str]}],
        }
    """
    info = auth.source_client.tables.get(_dotted(table_fqn))
    captured: dict[str, Any] = {
        "filter_fn_fqn": None,
        "filter_columns": [],
        "masks": [],
    }
    rf = getattr(info, "row_filter", None)
    if rf is not None:
        fn_name = getattr(rf, "function_name", None) or ""
        input_cols = getattr(rf, "input_column_names", None) or []
        if fn_name:
            captured["filter_fn_fqn"] = fn_name
            captured["filter_columns"] = list(input_cols)
    for col in getattr(info, "columns", None) or []:
        mask = getattr(col, "mask", None)
        if mask is None:
            continue
        fn_name = getattr(mask, "function_name", None) or ""
        using_cols = getattr(mask, "using_column_names", None) or []
        if fn_name:
            captured["masks"].append(
                {
                    "column": col.name,
                    "fn_fqn": fn_name,
                    "using_columns": list(using_cols),
                }
            )
    return captured


def has_rls_cm(captured: dict) -> bool:
    """Whether the captured dict represents any policy worth carrying."""
    return bool(captured.get("filter_fn_fqn")) or bool(captured.get("masks"))
