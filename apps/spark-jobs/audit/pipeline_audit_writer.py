import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def write_audit(
    spark,
    batch_id: str,
    stage: str,
    status: str,
    input_rows: int,
    output_rows: int,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    error_message: str | None = None,
) -> None:
    """nessie.audit.pipeline_batch_runs에 batch 실행 이력을 기록한다.
    실패해도 caller로 예외를 전파하지 않는다.
    """
    try:
        safe_error = (
            "'" + error_message.replace("'", "''")[:500] + "'"
            if error_message
            else "NULL"
        )
        spark.sql(f"""
            INSERT INTO nessie.audit.pipeline_batch_runs VALUES (
              '{batch_id}',
              '{stage}',
              '{status}',
              {int(input_rows)},
              {int(output_rows)},
              TIMESTAMP '{started_at.strftime('%Y-%m-%d %H:%M:%S')}',
              TIMESTAMP '{finished_at.strftime('%Y-%m-%d %H:%M:%S')}',
              {float(duration_seconds)},
              {safe_error},
              CURRENT_TIMESTAMP
            )
        """)
        logger.info(
            "Audit written: stage=%s status=%s batch_id=%s input=%d output=%d duration=%.1fs",
            stage, status, batch_id, input_rows, output_rows, duration_seconds,
        )
    except Exception as e:
        logger.warning("Audit write skipped (non-blocking): %s", e)
