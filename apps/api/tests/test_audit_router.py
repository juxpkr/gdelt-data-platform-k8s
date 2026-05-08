import pytest

SAMPLE_RUN = {
    "batch_id": "20260508100000",
    "stage": "bronze",
    "status": "success",
    "input_rows": 8647,
    "output_rows": 8647,
    "duration_seconds": 45.6,
    "error_message": None,
    "created_at": "2026-05-08 10:00:50.000000",
}


# --- GET /api/audit/runs ---

def test_get_runs_returns_list(client, mocker):
    mocker.patch("routers.audit.fetch_all", return_value=[SAMPLE_RUN])
    r = client.get("/api/audit/runs")
    assert r.status_code == 200
    assert "runs" in r.json()
    assert len(r.json()["runs"]) == 1


def test_get_runs_stage_filter_valid(client, mocker):
    mocker.patch("routers.audit.fetch_all", return_value=[SAMPLE_RUN])
    r = client.get("/api/audit/runs?stage=bronze")
    assert r.status_code == 200


def test_get_runs_stage_filter_invalid(client):
    r = client.get("/api/audit/runs?stage=xxx")
    assert r.status_code == 400


def test_get_runs_status_filter_invalid(client):
    r = client.get("/api/audit/runs?status=unknown")
    assert r.status_code == 400


def test_get_runs_limit_over_max(client):
    r = client.get("/api/audit/runs?limit=201")
    assert r.status_code == 422


def test_get_runs_empty_result(client, mocker):
    mocker.patch("routers.audit.fetch_all", return_value=[])
    r = client.get("/api/audit/runs")
    assert r.status_code == 200
    assert r.json()["runs"] == []


# --- GET /api/audit/runs/{batch_id} ---

def test_get_batch_valid_id(client, mocker):
    mocker.patch("routers.audit.fetch_all", return_value=[SAMPLE_RUN])
    r = client.get("/api/audit/runs/20260508100000")
    assert r.status_code == 200
    assert r.json()["batch_id"] == "20260508100000"
    assert len(r.json()["runs"]) == 1


def test_get_batch_invalid_format(client):
    r = client.get("/api/audit/runs/abc")
    assert r.status_code == 400


def test_get_batch_not_found(client, mocker):
    mocker.patch("routers.audit.fetch_all", return_value=[])
    r = client.get("/api/audit/runs/20260508100000")
    assert r.status_code == 404


def test_get_batch_db_error(client, mocker):
    mocker.patch("routers.audit.fetch_all", side_effect=Exception("Trino is down"))
    r = client.get("/api/audit/runs/20260508100000")
    assert r.status_code == 500
