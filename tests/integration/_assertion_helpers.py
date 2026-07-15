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


def assert_target_matches_ground_truth(
    spark: Any,
    fqn: str,
    error_messages: list[str],
    *,
    expected_count: int | None = None,
    expected_rows: list[tuple] | None = None,
    select_cols: list[str] | None = None,
    label: str | None = None,
) -> bool:
    """Query the TARGET table ``fqn`` DIRECTLY and compare to seed-declared
    ground truth — **never** ``migration_status``'s self-reported counts.

    This exists because the tool's own ``source_row_count``/``target_row_count``
    are measured *after* any row filter, and column masks don't change row
    counts — so a self-reported "row counts match" check passes even when the
    target silently received filtered rows or masked values (findings #16 /
    #21: ``orders`` migrated 2 of 4 rows and ``customers.email`` came across as
    ``***@***``, both under a green ``validated``). The only way to catch that
    is to read the target and compare to what the seed *actually* put in.

    Arguments
    ---------
    expected_count
        Exact number of rows the target table must have (full, unfiltered).
    expected_rows / select_cols
        Order-independent set of expected value tuples for ``select_cols`` —
        catches masked/dropped VALUES, not just row counts. Pass both together.
    label
        Human label for failure messages (defaults to ``fqn``).

    Returns True iff every requested check passed; appends precise messages
    to ``error_messages`` otherwise.
    """
    lbl = label or fqn
    ok = True

    if expected_count is not None:
        actual = _safe_get(spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").first(), "n")
        if actual != expected_count:
            ok = False
            error_messages.append(
                f"{lbl}: target row count {actual} != expected {expected_count} "
                f"(ground truth, unfiltered). Possible RLS/row-filter data loss (#16/#21)."
            )

    if expected_rows is not None:
        if not select_cols:
            error_messages.append(f"{lbl}: expected_rows given without select_cols.")
            return False
        cols = ", ".join(select_cols)
        rows = spark.sql(f"SELECT {cols} FROM {fqn}").collect()
        actual_set = {tuple(_safe_get(r, c) for c in select_cols) for r in rows}
        expected_set = {tuple(r) for r in expected_rows}
        if actual_set != expected_set:
            ok = False
            missing = expected_set - actual_set
            unexpected = actual_set - expected_set
            error_messages.append(
                f"{lbl}: target values != ground truth on {select_cols}. "
                f"missing={sorted(missing)} unexpected={sorted(unexpected)} "
                f"(masked/dropped values slip past self-reported counts — #16/#21)."
            )
    return ok


def assert_migrate_idempotent(
    workspace_client: Any,
    job_id: int,
    error_messages: list[str],
    *,
    label: str | None = None,
) -> bool:
    """Run migrate ``job_id`` a SECOND time and assert a clean result —
    guards the "re-runs are safe" contract the README/user_guide advertise.

    First runs silently insert / create; the bugs only surface on the second
    run (finding #8 discovery ``DELTA_MULTIPLE_SOURCE_ROW``, #12 hive
    ``LOCATION_OVERLAP``, #20 ``setup_sharing`` ``ResourceAlreadyExists``).
    No integration test currently runs anything twice, which is why the whole
    idempotency class went undetected. Fails if the re-run's terminal state
    isn't SUCCESS or a known non-idempotent signature appears in its message.
    """
    lbl = label or f"job {job_id}"
    bad_signatures = (
        "DELTA_MULTIPLE_SOURCE_ROW",
        "LOCATION_OVERLAP",
        "ResourceAlreadyExists",
        "ALREADY_EXISTS",
    )
    run = workspace_client.jobs.run_now_and_wait(job_id=job_id)
    state = getattr(run, "state", None)
    result = getattr(state, "result_state", None)
    result_str = getattr(result, "value", str(result))
    msg = getattr(state, "state_message", "") or ""
    if result_str != "SUCCESS":
        error_messages.append(
            f"{lbl}: re-run terminal state {result_str!r} (expected SUCCESS) — "
            f"migrate is not idempotent. message={msg!r}"
        )
        return False
    hit = [sig for sig in bad_signatures if sig.lower() in msg.lower()]
    if hit:
        error_messages.append(
            f"{lbl}: re-run reported SUCCESS but message carries non-idempotent "
            f"signature(s) {hit}: {msg!r}"
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
