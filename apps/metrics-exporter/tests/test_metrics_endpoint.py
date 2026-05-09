PIPELINE_ROWS = [
    {"stage": "bronze", "status": "success", "output_rows": 1000, "duration_seconds": 45.0},
    {"stage": "silver", "status": "success", "output_rows": 130,  "duration_seconds": 30.0},
    {"stage": "gold",   "status": "success", "output_rows": 130,  "duration_seconds": 15.0},
]


def test_health_returns_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_metrics_returns_200_with_gdelt_prefix(client, mocker):
    mocker.patch("main.collect_pipeline_metrics", return_value=(PIPELINE_ROWS, True))
    mocker.patch("main.collect_freshness_metrics", return_value=(300.0, 1))
    mocker.patch("main.collect_e2e_extra_metrics", return_value=(90.0, 0.13, 0))

    r = client.get("/metrics")
    assert r.status_code == 200
    assert "gdelt_" in r.text


def test_metrics_200_on_trino_failure(client, mocker):
    # 모든 collector가 실패해도 /metrics는 HTTP 200 유지, exporter_up=0 설정
    mocker.patch("main.collect_pipeline_metrics", side_effect=Exception("Trino down"))

    r = client.get("/metrics")
    assert r.status_code == 200
    assert "gdelt_exporter_up 0.0" in r.text


def test_metrics_no_batch_id_in_labels(client, mocker):
    mocker.patch("main.collect_pipeline_metrics", return_value=(PIPELINE_ROWS, True))
    mocker.patch("main.collect_freshness_metrics", return_value=(300.0, 1))
    mocker.patch("main.collect_e2e_extra_metrics", return_value=(90.0, 0.13, 0))

    r = client.get("/metrics")
    # prometheus label 형식은 key="value" — HELP 설명 문자열이 아니라 label key로 노출되는지 확인
    assert 'batch_id="' not in r.text
    assert 'error_message="' not in r.text
