from collectors.pipeline import collect_pipeline_metrics

E2E_ROWS = [
    {"stage": "bronze", "status": "success", "output_rows": 1000, "duration_seconds": 45.0},
    {"stage": "silver", "status": "success", "output_rows": 130,  "duration_seconds": 30.0},
    {"stage": "gold",   "status": "success", "output_rows": 130,  "duration_seconds": 15.0},
]


def test_e2e_complete_batch_returns_3_rows(mocker):
    mocker.patch("collectors.pipeline.fetch_all", return_value=E2E_ROWS)
    rows, e2e_available = collect_pipeline_metrics()
    assert e2e_available is True
    assert len(rows) == 3


def test_missing_stage_falls_back_to_per_stage(mocker):
    # E2E 쿼리가 빈 결과 → fallback 쿼리로 2개 stage 반환
    mocker.patch("collectors.pipeline.fetch_all", side_effect=[[], E2E_ROWS[:2]])
    rows, e2e_available = collect_pipeline_metrics()
    assert e2e_available is False
    assert len(rows) == 2


def test_failed_stage_status_preserved(mocker):
    rows_with_failure = [
        {"stage": "bronze", "status": "failed",  "output_rows": 0,   "duration_seconds": 5.0},
        {"stage": "silver", "status": "success", "output_rows": 130, "duration_seconds": 30.0},
        {"stage": "gold",   "status": "success", "output_rows": 130, "duration_seconds": 15.0},
    ]
    mocker.patch("collectors.pipeline.fetch_all", return_value=rows_with_failure)
    rows, _ = collect_pipeline_metrics()
    bronze = next(r for r in rows if r["stage"] == "bronze")
    assert bronze["status"] == "failed"


def test_output_rows_and_duration_reflected(mocker):
    mocker.patch("collectors.pipeline.fetch_all", return_value=E2E_ROWS)
    rows, _ = collect_pipeline_metrics()
    bronze = next(r for r in rows if r["stage"] == "bronze")
    assert bronze["output_rows"] == 1000
    assert bronze["duration_seconds"] == 45.0


def test_batch_id_not_in_returned_rows(mocker):
    mocker.patch("collectors.pipeline.fetch_all", return_value=E2E_ROWS)
    rows, _ = collect_pipeline_metrics()
    for row in rows:
        assert "batch_id" not in row
