import pytest
from collectors.silver_quality import collect_silver_quality_metrics

_BATCH_ROW = {"batch_id": "20260509153000"}

_QUALITY_ROW = {
    "dedup_violations":   0,
    "core_null_count":    0,
    "mention_join_ratio": 0.95,
    "gkg_coverage_ratio": 0.08,
}


def test_returns_quality_metrics_for_e2e_batch(mocker):
    mocker.patch("collectors.silver_quality.fetch_one", side_effect=[_BATCH_ROW, _QUALITY_ROW])
    result = collect_silver_quality_metrics()
    assert result["dedup_violations"]   == pytest.approx(0.0)
    assert result["mention_join_ratio"] == pytest.approx(0.95)
    assert result["gkg_coverage_ratio"] == pytest.approx(0.08)


def test_returns_safe_defaults_when_no_e2e_batch(mocker):
    mocker.patch("collectors.silver_quality.fetch_one", return_value=None)
    result = collect_silver_quality_metrics()
    assert result["dedup_violations"]   == 0.0
    assert result["core_null_count"]    == 0.0
    assert result["mention_join_ratio"] == 0.0
    assert result["gkg_coverage_ratio"] == 0.0


def test_returns_safe_defaults_when_batch_row_empty(mocker):
    mocker.patch("collectors.silver_quality.fetch_one", return_value={"batch_id": None})
    result = collect_silver_quality_metrics()
    assert result["dedup_violations"] == 0.0


def test_returns_safe_defaults_on_trino_error(mocker):
    mocker.patch("collectors.silver_quality.fetch_one", side_effect=Exception("Trino down"))
    result = collect_silver_quality_metrics()
    assert result["dedup_violations"]   == 0.0
    assert result["mention_join_ratio"] == 0.0


def test_no_batch_id_in_returned_dict(mocker):
    mocker.patch("collectors.silver_quality.fetch_one", side_effect=[_BATCH_ROW, _QUALITY_ROW])
    result = collect_silver_quality_metrics()
    assert "batch_id" not in result
    assert "error_message" not in result
