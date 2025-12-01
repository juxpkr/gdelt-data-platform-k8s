"""
파티셔닝 기반 Delta Lake Writer
- APPEND 방식으로 빠른 저장
- Hive Metastore 자동 등록
"""

from pyspark.sql import DataFrame, functions as F
import logging

logger = logging.getLogger(__name__)


def write_to_delta_lake(
    df: DataFrame, delta_path: str, table_name: str, partition_col: str, merge_key: str, register_hive: bool = True
):
    """DataFrame을 Delta Lake에 저장하고 Hive Metastore에 등록"""
    logger.info(f"Saving {table_name} to {delta_path}")

    if df.rdd.isEmpty():
        logger.warning(f"No records in DataFrame for {table_name}. Skipping write.")
        return

    spark = df.sparkSession

    # 파티션 컬럼 추가
    df_with_partitions = (
        df.withColumn("year", F.year(F.col(partition_col)))
        .withColumn("month", F.month(F.col(partition_col)))
        .withColumn("day", F.dayofmonth(F.col(partition_col)))
        .withColumn("hour", F.hour(F.col(partition_col)))
    )

    # Delta Lake에 append
    logger.info(f"Writing data to {delta_path}...")
    (
        df_with_partitions.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .option("mergeSchema", "true")
        .save(delta_path)
    )

    # Hive Metastore에 테이블 등록
    if register_hive:
        logger.info(f"Registering {table_name} in Hive Metastore...")
        try:
            from delta.tables import DeltaTable

            # 테이블이 이미 있는지 확인
            if not spark.catalog.tableExists(f"default.{table_name}"):
                # Delta Lake로 먼저 읽어서 스키마 추론 후 EXTERNAL TABLE로 등록
                delta_df = spark.read.format("delta").load(delta_path)
                delta_df.createOrReplaceTempView(f"temp_{table_name}")

                # EXTERNAL TABLE로 명시적 생성 (PLACEHOLDER 방지)
                spark.sql(f"""
                    CREATE EXTERNAL TABLE default.{table_name}
                    USING DELTA
                    LOCATION '{delta_path}'
                """)
                logger.info(f" {table_name} created as EXTERNAL TABLE in Hive Metastore")

                # temp view 삭제
                spark.catalog.dropTempView(f"temp_{table_name}")
            else:
                # 이미 있으면 메타데이터만 리프레시
                spark.sql(f"REFRESH TABLE default.{table_name}")
                logger.info(f" {table_name} refreshed in Hive Metastore")
        except Exception as e:
            logger.warning(f"Hive registration failed for {table_name}: {e}")
            logger.warning("Continuing without Hive registration...")

    logger.info(f" {table_name} saved successfully at {delta_path}")
