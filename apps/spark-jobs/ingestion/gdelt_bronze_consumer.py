import os
import sys
import time
from pathlib import Path
import logging

project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
)

from utils.spark_builder import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_TOPICS = {
    "events": os.getenv("KAFKA_TOPIC_GDELT_EVENTS", "gdelt_events_bronze"),
    "mentions": os.getenv("KAFKA_TOPIC_GDELT_MENTIONS", "gdelt_mentions_bronze"),
    "gkg": os.getenv("KAFKA_TOPIC_GDELT_GKG", "gdelt_gkg_bronze"),
}

ICEBERG_TABLES = {
    "events": "nessie.bronze.gdelt_events",
    "mentions": "nessie.bronze.gdelt_mentions",
    "gkg": "nessie.bronze.gdelt_gkg",
}

BRONZE_SCHEMA = StructType([
    StructField("data_type", StringType(), True),
    StructField("bronze_data", ArrayType(StringType()), True),
    StructField("row_number", IntegerType(), True),
    StructField("source_file", StringType(), True),
    StructField("extracted_time", StringType(), True),
    StructField("producer_timestamp", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("total_columns", IntegerType(), True),
])

DATA_TYPE_CONFIG = {
    "events": {"pk_index": 0, "merge_key": "GLOBALEVENTID"},
    "mentions": {"pk_index": 0, "merge_key": "GLOBALEVENTID"},
    "gkg": {"pk_index": 0, "merge_key": "GKGRECORDID"},
}


def ensure_table_exists(spark: SparkSession, data_type: str):
    table = ICEBERG_TABLES[data_type]
    namespace = ".".join(table.split(".")[:2])  # nessie.bronze
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            data_type       STRING,
            bronze_data     ARRAY<STRING>,
            row_number      INT,
            source_file     STRING,
            extracted_time  STRING,
            producer_timestamp STRING,
            source_url      STRING,
            total_columns   INT,
            processed_at    TIMESTAMP,
            GLOBALEVENTID   STRING,
            GKGRECORDID     STRING
        ) USING iceberg
        PARTITIONED BY (days(processed_at))
    """)


def setup_streaming_query(spark: SparkSession, data_type: str, logger):
    kafka_topic = KAFKA_TOPICS[data_type]
    table = ICEBERG_TABLES[data_type]
    checkpoint_path = f"s3a://warehouse/checkpoints/bronze/{data_type}"
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    ensure_table_exists(spark, data_type)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    config = DATA_TYPE_CONFIG[data_type]
    merge_key = config["merge_key"]
    pk_index = config["pk_index"]

    def process_micro_batch(df: DataFrame, batch_id: int):
        logger.info(f"--- Batch {batch_id} [{data_type}] ---")
        try:
            if df.isEmpty():
                return

            parsed_df = (
                df.select(from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("d"))
                .select("d.*")
                .withColumn(merge_key, F.trim(col("bronze_data").getItem(pk_index)))
                .withColumn("processed_at", to_timestamp(col("extracted_time"), "yyyy-MM-dd HH:mm:ss"))
                .filter(col(merge_key).isNotNull() & (col(merge_key) != ""))
            )

            # GLOBALEVENTID/GKGRECORDID 둘 다 컬럼으로 존재해야 스키마 맞음
            for key in ["GLOBALEVENTID", "GKGRECORDID"]:
                if key not in parsed_df.columns:
                    parsed_df = parsed_df.withColumn(key, F.lit(None).cast(StringType()))

            view_name = f"batch_{data_type}_{batch_id}"
            parsed_df.createOrReplaceGlobalTempView(view_name)

            spark.sql(f"""
                MERGE INTO {table} t
                USING global_temp.{view_name} s
                ON t.{merge_key} = s.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        except Exception as e:
            logger.error(f"!!! Batch {batch_id} [{data_type}] FAILED: {e} !!!")
            raise

    query = (
        kafka_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start()
    )
    return query


def main():
    spark = get_spark_session(
        "GDELT_Bronze_Consumer",
        extra_packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
    )

    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("GDELT_BRONZE_CONSUMER")
    logger.info("========== Starting GDELT 3-Way Bronze Consumer ==========")

    queries = []
    try:
        for data_type in ["events", "mentions", "gkg"]:
            queries.append(setup_streaming_query(spark, data_type, logger))

        while any(q.isActive for q in queries):
            time.sleep(0.5)

        for query in queries:
            if query.exception():
                for q in queries:
                    if q.isActive:
                        q.stop()
                raise query.exception()

        logger.info("========== GDELT 3-Way Bronze Consumer FINISHED ==========")
    except Exception as e:
        logger.error(f"!!! FAILED: {e} !!!")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
