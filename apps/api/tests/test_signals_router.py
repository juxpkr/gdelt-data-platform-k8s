from unittest.mock import patch
import pytest

BASE_ROW = {
    "global_event_id": "123456789",
    "event_date": "2026-05-08",
    "action_geo_fullname": "Seoul, South Korea",
    "event_code": "14",
    "event_code_name": "Protest",
    "avg_tone": -3.5,
    "goldstein_scale": -5.0,
    "num_mentions": 42,
    "num_articles": 10,
    "source_url": "https://example.com",
    "risk_score": 1.2345,
}


def _row(**overrides):
    return {**BASE_ROW, **overrides}


def test_signals_returns_list(client, mocker):
    mocker.patch("routers.signals.fetch_all", return_value=[BASE_ROW])
    r = client.get("/api/signals")
    assert r.status_code == 200
    assert "signals" in r.json()
    assert len(r.json()["signals"]) == 1


def test_signals_risk_score_passed_through(client, mocker):
    mocker.patch("routers.signals.fetch_all", return_value=[_row(risk_score=2.7183)])
    r = client.get("/api/signals")
    assert r.json()["signals"][0]["risk_score"] == pytest.approx(2.7183, rel=1e-4)


def test_signals_null_risk_score_handled(client, mocker):
    # goldstein_scale=-10이면 SQL NULLIF가 NULL 반환 → Python에서 None으로 처리
    mocker.patch("routers.signals.fetch_all", return_value=[_row(risk_score=None)])
    r = client.get("/api/signals")
    assert r.status_code == 200
    assert r.json()["signals"][0]["risk_score"] is None


def test_signals_empty_returns_empty_list(client, mocker):
    mocker.patch("routers.signals.fetch_all", return_value=[])
    r = client.get("/api/signals")
    assert r.status_code == 200
    assert r.json()["signals"] == []


def test_signals_limit_max_exceeded(client):
    r = client.get("/api/signals?limit=101")
    assert r.status_code == 422


def test_signals_db_error_returns_500(client, mocker):
    mocker.patch("routers.signals.fetch_all", side_effect=Exception("Trino is down"))
    r = client.get("/api/signals")
    assert r.status_code == 500
