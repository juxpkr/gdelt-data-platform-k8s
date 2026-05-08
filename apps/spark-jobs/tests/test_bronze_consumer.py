import os
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from ingestion.gdelt_bronze_consumer import _resolve_batch_id

STARTED = datetime(2026, 5, 8, 10, 0, 0, tzinfo=timezone.utc)
EXPECTED_FALLBACK = "20260508100000"


def test_uses_env_var_when_set(monkeypatch):
    monkeypatch.setenv("SOURCE_BATCH_ID", "20260508071500")
    assert _resolve_batch_id(STARTED) == "20260508071500"


def test_falls_back_to_started_at_when_env_missing(monkeypatch):
    monkeypatch.delenv("SOURCE_BATCH_ID", raising=False)
    assert _resolve_batch_id(STARTED) == EXPECTED_FALLBACK


def test_falls_back_when_env_is_empty_string(monkeypatch):
    monkeypatch.setenv("SOURCE_BATCH_ID", "   ")
    assert _resolve_batch_id(STARTED) == EXPECTED_FALLBACK


def test_fallback_format_is_14_digits(monkeypatch):
    monkeypatch.delenv("SOURCE_BATCH_ID", raising=False)
    result = _resolve_batch_id(STARTED)
    assert len(result) == 14
    assert result.isdigit()
