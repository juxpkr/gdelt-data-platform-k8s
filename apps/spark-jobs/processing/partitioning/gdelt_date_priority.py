"""
날짜 우선순위 처리 모듈
Mentions > GKG > Events 순으로 가장 정확한 날짜 선택
"""

from pyspark.sql import DataFrame, functions as F
import logging

logger = logging.getLogger(__name__)


def create_priority_date(df: DataFrame) -> DataFrame:
    """
    우선순위 날짜 컬럼 생성
    Silver 표준: event_date 기준으로 통일
    1순위: event_date
    2순위: date_added
    3순위: source_batch_time (batch 원천 시각)
    4순위: current_date() (절대 null 방지)
    """
    logger.info("Creating priority date column...")

    df_with_priority = df.withColumn(
        "priority_date",
        F.coalesce(
            F.to_timestamp(F.col("event_date")),
            F.col("date_added"),
            F.to_timestamp(F.col("source_batch_time")),
            F.current_timestamp(),
        ),
    )

    logger.info("Priority date column created")
    return df_with_priority
