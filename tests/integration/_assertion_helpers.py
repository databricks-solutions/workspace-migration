"""Shared assertion helpers for integration test notebooks.

Extracted from ``test_uc_end_to_end.py`` so the same helper can be used
across ``test_hive_end_to_end.py``, ``test_governance_end_to_end.py``,
``test_negative_paths.py``, etc.

The integration notebooks are Databricks notebooks executed by DAB
jobs; the bootstrap pattern at the top of each notebook puts the
bundle's ``src/`` + ``files/`` dirs on ``sys.path`` so this module can
be imported as ``from tests.integration._assertion_helpers import ...``.
Same import pattern as ``_config_override.py``.

Kept as a pure Python module (no ``# Databricks notebook source``
header) so DAB does not deploy it as a notebook.
"""

from __future__ import annotations

from typing import Any


def expect_validated(
    row: Any,
    label: str,
    error_messages: list[str],
) -> bool:
    """Assert a ``migration_status`` row is BOTH ``validated`` AND has
    an empty ``error_message``.

    Closes review finding **H11**. Previously the integration tests had
    ~20 patterns of the form::

        if row["status"] != "validated":
            error_messages.append(...)

    which silently passed when a worker recorded ``validated`` with a
    non-empty ``error_message`` (e.g. a ``WARNING: rebuilt with stale
    schema`` slipped in as info). That made "validated" mean "status
    field is the right string" rather than "the operation actually
    succeeded with no warnings". This helper enforces both.

    Arguments
    ---------
    row
        A row-like (PySpark Row, dict, or Mapping) with ``status`` and
        ``error_message`` accessible via subscript or attribute.
    label
        Short human label used in the failure message — typically the
        test-section identifier (e.g. ``"Phase 2.5.B iceberg_sales"``).
    error_messages
        The notebook's accumulating list. Appended-to on failure so the
        notebook keeps its accumulate-then-fail-at-end style.

    Returns
    -------
    True if the row passes both checks; False otherwise.

    Notes
    -----
    Workers that legitimately use ``error_message`` to carry an info
    marker under ``status='validated'`` (e.g. models_worker's "N file(s),
    M byte(s) copied." summary, sharing_worker's "already existed on
    target" idempotency note) should NOT use this helper. Inline status
    checks are correct for those sites.
    """
    _status = _safe_get(row, "status")
    _err = _safe_get(row, "error_message")
    if _status != "validated":
        error_messages.append(
            f"{label}: status={_status!r}, expected 'validated'. error_message={_err!r}"
        )
        return False
    if _err:
        error_messages.append(
            f"{label}: status='validated' but error_message is set: {_err!r} "
            "(worker recorded a warning under a passing status — see review H11)."
        )
        return False
    return True


def _safe_get(row: Any, key: str) -> Any:
    """Subscript access first, then attribute access. PySpark Rows
    support both — pure dicts only support subscript, plain objects
    only support attribute access."""
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        pass
    return getattr(row, key, None)
