from unittest.mock import MagicMock
from datetime import datetime, timezone

import pytest

from audit.pipeline_audit_writer import write_audit

STARTED = datetime(2026, 5, 8, 10, 0, 0, tzinfo=timezone.utc)
FINISHED = datetime(2026, 5, 8, 10, 0, 45, tzinfo=timezone.utc)


@pytest.fixture
def spark():
    return MagicMock()


def _captured_sql(spark) -> str:
    return spark.sql.call_args[0][0]


def test_success_sql(spark):
    write_audit(spark, "20260508100000", "bronze", "success", 100, 100, STARTED, FINISHED, 45.0)

    sql = _captured_sql(spark)
    assert "'bronze'" in sql
    assert "'success'" in sql
    assert "100" in sql
    assert "45.0" in sql
    assert "NULL" in sql


def test_failed_sql(spark):
    write_audit(spark, "20260508100000", "silver", "failed", 0, 0, STARTED, FINISHED, 10.0,
                error_message="something went wrong")

    sql = _captured_sql(spark)
    assert "'failed'" in sql
    assert "something went wrong" in sql


def test_error_message_quote_escape(spark):
    write_audit(spark, "20260508100000", "bronze", "failed", 0, 0, STARTED, FINISHED, 5.0,
                error_message="it's broken")

    sql = _captured_sql(spark)
    assert "it''s broken" in sql


def test_error_message_truncate(spark):
    long_msg = "x" * 600
    write_audit(spark, "20260508100000", "bronze", "failed", 0, 0, STARTED, FINISHED, 5.0,
                error_message=long_msg)

    sql = _captured_sql(spark)
    assert "x" * 500 in sql
    assert "x" * 501 not in sql


def test_no_exception_on_spark_failure(spark):
    spark.sql.side_effect = Exception("Spark cluster is down")

    # audit write 실패가 caller로 전파되지 않아야 한다
    write_audit(spark, "20260508100000", "bronze", "success", 100, 100, STARTED, FINISHED, 45.0)


def test_duration_seconds_in_sql(spark):
    write_audit(spark, "20260508100000", "gold", "success", 0, 356, STARTED, FINISHED, 88.5)

    sql = _captured_sql(spark)
    assert "88.5" in sql
