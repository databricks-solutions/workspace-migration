"""Unit tests for models_worker — idempotency, error handling, artifact copy."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import AlreadyExists, PermissionDenied

from migrate import models_worker


def _model_with_one_version():
    return {
        "model_fqn": "c.s.m",
        "comment": None,
        "storage_location": None,
        "versions": [
            {"version": "1", "storage_location": "abfss://src/model/v1", "aliases": []},
        ],
    }


def _auth_with_target():
    auth = MagicMock()
    client = auth.target_client
    fake_version = MagicMock()
    fake_version.storage_location = "abfss://target/model/v1"
    client.model_versions.create.return_value = fake_version
    return auth, client


def test_apply_model_idempotent_when_model_already_exists(monkeypatch):
    """If ``registered_models.create`` raises AlreadyExists, the worker
    continues to versions + aliases rather than failing."""
    auth, client = _auth_with_target()
    client.registered_models.create.side_effect = AlreadyExists("model exists")

    monkeypatch.setattr(models_worker, "ensure_copy_notebook_on_target", lambda *a, **k: None)
    monkeypatch.setattr(
        models_worker,
        "run_target_file_copy",
        lambda *a, **k: {"bytes_copied": 0, "file_count": 0},
    )

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert len(results) == 1
    assert results[0]["status"] in ("validated", "validation_failed")
    assert results[0]["status"] != "failed"


def test_apply_model_propagates_non_already_exists(monkeypatch):
    """Any error other than AlreadyExists is recorded as ``failed``."""
    auth, client = _auth_with_target()
    client.registered_models.create.side_effect = PermissionDenied("nope")

    monkeypatch.setattr(models_worker, "ensure_copy_notebook_on_target", lambda *a, **k: None)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert "nope" in results[0]["error_message"]


def test_apply_model_hard_fails_on_artifact_copy_failure(monkeypatch):
    """L4: artifact copy failure hard-fails the model row (mirrors volume_worker).

    Previously the failure was appended to ``version_errors`` and the row
    was recorded as ``validation_failed``; we now hard-fail because the
    artifact bytes are essential, not best-effort metadata.
    """
    auth, client = _auth_with_target()
    client.registered_models.create.return_value = None

    monkeypatch.setattr(models_worker, "ensure_copy_notebook_on_target", lambda *a, **k: None)

    def boom(*args, **kwargs):
        raise RuntimeError("copy job failed")

    monkeypatch.setattr(models_worker, "run_target_file_copy", boom)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert "copy job failed" in results[0]["error_message"]
    assert "v1 artifact copy failed" in results[0]["error_message"]


def test_apply_model_version_idempotent_on_already_exists(monkeypatch):
    """If a version exists, fetch it and proceed to artifact copy."""
    auth, client = _auth_with_target()
    client.registered_models.create.return_value = None
    client.model_versions.create.side_effect = AlreadyExists("version exists")
    fake_existing = MagicMock()
    fake_existing.storage_location = "abfss://target/model/v1"
    client.model_versions.get.return_value = fake_existing

    monkeypatch.setattr(models_worker, "ensure_copy_notebook_on_target", lambda *a, **k: None)
    monkeypatch.setattr(
        models_worker,
        "run_target_file_copy",
        lambda *a, **k: {"bytes_copied": 1, "file_count": 1},
    )

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    # Idempotent path should not raise; status reflects whether the artifact
    # copy succeeded — it does here, so validated.
    assert results[0]["status"] == "validated"
    client.model_versions.get.assert_called_once()
