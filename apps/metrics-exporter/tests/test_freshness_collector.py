import math
import pytest
from collectors.freshness import collect_freshness_metrics, FRESHNESS_THRESHOLD_SECONDS


def test_freshness_seconds_reflected(mocker):
    mocker.patch("collectors.freshness.fetch_one", return_value={"freshness_seconds": 300.0})
    freshness, _ = collect_freshness_metrics(e2e_available=True)
    assert freshness == pytest.approx(300.0)


def test_health_1_when_fresh_and_e2e_available(mocker):
    mocker.patch("collectors.freshness.fetch_one", return_value={"freshness_seconds": 300.0})
    _, health = collect_freshness_metrics(e2e_available=True)
    assert health == 1


def test_health_0_when_freshness_exceeds_threshold(mocker):
    over = FRESHNESS_THRESHOLD_SECONDS + 1
    mocker.patch("collectors.freshness.fetch_one", return_value={"freshness_seconds": over})
    _, health = collect_freshness_metrics(e2e_available=True)
    assert health == 0


def test_health_0_when_e2e_not_available(mocker):
    # e2e_available=False이면 freshness가 짧아도 health는 0
    mocker.patch("collectors.freshness.fetch_one", return_value={"freshness_seconds": 10.0})
    _, health = collect_freshness_metrics(e2e_available=False)
    assert health == 0


def test_fetch_one_returns_none_gives_inf_freshness(mocker):
    mocker.patch("collectors.freshness.fetch_one", return_value=None)
    freshness, health = collect_freshness_metrics(e2e_available=True)
    assert math.isinf(freshness)
    assert health == 0
