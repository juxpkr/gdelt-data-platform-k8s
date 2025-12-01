"""GDELT 3-Way Silver Processor

Bronze Layer에서 수집된 원본 데이터를 정제하고, 가공하여 Gold Layer에서 분석하기 좋은 형태로 변환
주요 기능으로는 데이터 타입 변환, null 값 처리, 칼럼 정규화, 3-Way 조인
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone


# 모듈 임포트
sys.path.append("/opt/airflow/spark-jobs")
from utils.spark_builder import get_spark_session
from utils.redis_client import redis_client
from utils.schemas.gdelt_schemas import GDELTSchemas
# from audit.lifecycle_updater import EventLifecycleUpdater

# Transformers
from processing.transformers.events_transformer import transform_events_to_silver
from processing.transformers.mentions_transformer import (
    transform_mentions_to_silver,
)
from processing.transformers.gkg_transformer import transform_gkg_to_silver

# Joiners
from processing.joiners.gdelt_three_way_joiner import (
    perform_three_way_join,
    select_final_columns,
)

# Partitioning
from processing.partitioning.gdelt_date_priority import create_priority_date
from processing.partitioning.gdelt_partition_writer import write_to_delta_lake

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_from_bronze_minio(
    spark: SparkSession, start_time_str: str, end_time_str: str
) -> DataFrame:
    """
    MinIO Bronze Layer에서 각 데이터 타입별로 데이터를 읽고,
    DataFrame의 딕셔너리 형태로 반환
    """
    logger.info(
        f"Reading Bronze data from MinIO for period: {start_time_str} to {end_time_str}"
    )
    # Bronze Layer 경로 설정
    bronze_paths = {
        "events": "s3a://warehouse/bronze/gdelt_events",
        "mentions": "s3a://warehouse/bronze/gdelt_mentions",
        "gkg": "s3a://warehouse/bronze/gdelt_gkg",
    }

    # 반환 타입을 딕셔너리로 변경 (기존: 통합 dataframe)
    dataframes = {}

    for data_type, base_path in bronze_paths.items():
        try:
            logger.info(f"Reading {data_type.upper()} data from {base_path}")

            # Delta 테이블에서 읽기 (hour 파티션 기준)
            # data_interval의 hour만 추출
            from datetime import datetime
            start_dt = datetime.strptime(start_time_str.split('+')[0], "%Y-%m-%d %H:%M:%S")
            target_hour = start_dt.hour
            target_day = start_dt.day

            bronze_df = (
                spark.read.format("delta")
                .load(base_path)
                .filter(
                    (F.col("year") == start_dt.year)
                    & (F.col("month") == start_dt.month)
                    & (F.col("day") == target_day)
                    & (F.col("hour") == target_hour)
                )
                .coalesce(4)
            )

            # count() 대신 first()로 존재 여부만 빠르게 확인하기
            if bronze_df.first():
                logger.info(f"Successfully loaded data for {data_type.upper()}.")
                dataframes[data_type] = bronze_df
            else:
                logger.warning(
                    f"No new data found for {data_type.upper()} in the given time range."
                )

        except Exception as e:
            logger.error(f"Failed to read {data_type} from Bronze: {e}")
            continue  # 하나의 타입이 실패해도 다른 타입은 계속 시도

    return dataframes


def main():
    """메인 실행 함수"""
    logger.info("Starting GDELT 3-Way Silver Processor (Refactored)...")

    spark = get_spark_session(
        "GDELT 3Way Silver Processor"
    )
    redis_client.register_driver_ui(spark, "GDELT 3Way Silver Processor")

    # Airflow가 넘겨준 인자를 sys.argv를 통해 받음
    if len(sys.argv) != 3:
        logger.error("Usage: spark-submit <script> <start_time> <end_time>")
        sys.exit(1)

    start_time_str = sys.argv[1]
    end_time_str = sys.argv[2]

    # batch_id 생성 (시간 기반)
    batch_id = (
        f"batch_{start_time_str.replace(':', '').replace('-', '').replace(' ', '_')}"
    )

    try:

        # MinIO Bronze Layer에서 데이터 읽기 (Airflow가 준 시간 인자 전달)
        bronze_dataframes = read_from_bronze_minio(spark, start_time_str, end_time_str)

        if not bronze_dataframes:
            logger.warning("No Bronze data found in MinIO. Exiting gracefully.")
            return

        # 데이터 타입별 분리 및 변환 (이제 filter가 필요 없음!)
        events_df = bronze_dataframes.get("events")
        mentions_df = bronze_dataframes.get("mentions")
        gkg_df = bronze_dataframes.get("gkg")

        # 각 데이터 타입 변환 (if df 로 None과 Enpty를 동시에 체크할 수 있음)
        events_silver = transform_events_to_silver(events_df) if events_df else None
        mentions_silver = (
            transform_mentions_to_silver(mentions_df) if mentions_df else None
        )
        gkg_silver = transform_gkg_to_silver(gkg_df) if gkg_df else None

        # Events 단독 Silver 저장
        if events_silver:
            write_to_delta_lake(
                df=events_silver,
                delta_path="s3a://warehouse/silver/gdelt_events",
                table_name="silver_events",
                partition_col="processed_at",  # Hot: 실시간 대시보드용 (수집시간)
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
                register_hive=False
            )

            logger.info("Events Silver stored successfully")

        # 3-Way 조인 수행
        logger.info("Performing 3-Way Join...")
        joined_df = perform_three_way_join(events_silver, mentions_silver, gkg_silver)

        # 조인 결과가 있을 때만 후속 처리 진행
        if joined_df and not joined_df.rdd.isEmpty():  # 조인 후에는 isEmpty() 체크 필요
            # 우선순위 날짜 생성
            joined_with_priority = create_priority_date(joined_df)

            # 최종 컬럼 선택
            final_silver_df = select_final_columns(joined_with_priority)

            # Events detailed Silver 저장
            write_to_delta_lake(
                df=final_silver_df,
                delta_path="s3a://warehouse/silver/gdelt_events_detailed",
                table_name="silver_events_detailed",
                partition_col="priority_date",  # 사건 시간 기준
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
                register_hive=False
            )

            logger.info("Silver processing complete!")

        logger.info("Silver Layer processing completed successfully!")

    except Exception as e:
        logger.error(f"Error in 3-Way Silver processing: {e}", exc_info=True)

    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    main()
