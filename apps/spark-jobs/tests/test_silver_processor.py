from unittest.mock import MagicMock
import pytest

from processing.gdelt_silver_processor import _skip_reason


def _mock_df():
    return MagicMock()


def test_no_bronze_data_returns_no_bronze_data():
    assert _skip_reason({}, None) == "no_bronze_data"


def test_no_bronze_data_when_events_silver_present():
    # bronze_dfs가 비어있으면 events_silver 유무에 관계없이 no_bronze_data
    assert _skip_reason({}, _mock_df()) == "no_bronze_data"


def test_missing_events_returns_no_events():
    # mentions만 있고 events 없음
    bronze_dfs = {"mentions": _mock_df()}
    assert _skip_reason(bronze_dfs, None) == "no_events"


def test_missing_events_with_gkg_returns_no_events():
    bronze_dfs = {"mentions": _mock_df(), "gkg": _mock_df()}
    assert _skip_reason(bronze_dfs, None) == "no_events"


def test_normal_path_returns_none():
    bronze_dfs = {"events": _mock_df(), "mentions": _mock_df(), "gkg": _mock_df()}
    assert _skip_reason(bronze_dfs, _mock_df()) is None


def test_events_only_path_returns_none():
    # mentions, gkg 없어도 events만 있으면 정상 처리
    bronze_dfs = {"events": _mock_df()}
    assert _skip_reason(bronze_dfs, _mock_df()) is None
