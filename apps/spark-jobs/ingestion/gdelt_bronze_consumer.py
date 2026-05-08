import os
import sys
import time
from datetime import datetime, timezone
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
from audit.pipeline_audit_writer import write_audit

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
    StructField("source_batch_id", StringType(), True),
    StructField("source_batch_time", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("total_columns", IntegerType(), True),
])

DATA_TYPE_CONFIG = {
    "events":   {"pk_index": 0, "merge_key": "GLOBALEVENTID"},
    "mentions": {"pk_index": 0, "merge_key": "mention_id"},
    "gkg":      {"pk_index": 0, "merge_key": "GKGRECORDID"},
}


def ensure_table_exists(spark: SparkSession, data_type: str):
    table = ICEBERG_TABLES[data_type]
    namespace = ".".join(table.split(".")[:2])  # nessie.bronze
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            data_type        STRING,
            bronze_data      ARRAY<STRING>,
            row_number       INT,
            source_file      STRING,
            source_batch_id  STRING,
            source_batch_time STRING,
            source_url       STRING,
            total_columns    INT,
            ingested_at      TIMESTAMP,
            GLOBALEVENTID    STRING,
            GKGRECORDID      STRING,
            mention_id       STRING
        ) USING iceberg
        PARTITIONED BY (source_batch_id)
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
                .withColumn("ingested_at", F.current_timestamp())
            )

            if data_type == "mentions":
                _eid  = F.coalesce(F.trim(col("bronze_data").getItem(0)), F.lit("__NULL__"))
                _mtd  = F.coalesce(F.trim(col("bronze_data").getItem(2)), F.lit("__NULL__"))
                _murl = F.coalesce(F.trim(col("bronze_data").getItem(5)), F.lit("__NULL__"))
                parsed_df = parsed_df \
                    .withColumn("GLOBALEVENTID", F.trim(col("bronze_data").getItem(0))) \
                    .withColumn("GKGRECORDID", F.lit(None).cast(StringType())) \
                    .withColumn("mention_id", F.sha2(F.concat_ws("|", _eid, _mtd, _murl), 256)) \
                    .filter(
                        F.trim(col("bronze_data").getItem(0)).isNotNull() &
                        (F.trim(col("bronze_data").getItem(0)) != "") &
                        F.trim(col("bronze_data").getItem(5)).isNotNull() &
                        (F.trim(col("bronze_data").getItem(5)) != "")
                    )
            elif data_type == "events":
                parsed_df = parsed_df \
                    .withColumn("GLOBALEVENTID", F.trim(col("bronze_data").getItem(pk_index))) \
                    .withColumn("GKGRECORDID", F.lit(None).cast(StringType())) \
                    .withColumn("mention_id", F.lit(None).cast(StringType()))
            else:  # gkg
                parsed_df = parsed_df \
                    .withColumn("GKGRECORDID", F.trim(col("bronze_data").getItem(pk_index))) \
                    .withColumn("GLOBALEVENTID", F.lit(None).cast(StringType())) \
                    .withColumn("mention_id", F.lit(None).cast(StringType()))

            parsed_df = parsed_df.filter(col(merge_key).isNotNull() & (col(merge_key) != ""))

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
    started_at = datetime.now(timezone.utc)
    source_batch_id = os.getenv("SOURCE_BATCH_ID", "").strip()
    if source_batch_id:
        batch_id = source_batch_id
    else:
        batch_id = started_at.strftime("%Y%m%d%H%M%S")
        logger.warning(
            "SOURCE_BATCH_ID env var is missing. Falling back to job start time batch_id=%s",
            batch_id,
        )

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

        total_input = sum(
            p.get("numInputRows", 0)
            for q in queries
            for p in q.recentProgress
        )
        finished_at = datetime.now(timezone.utc)
        write_audit(
            spark, batch_id, "bronze", "success",
            total_input, total_input,
            started_at, finished_at,
            (finished_at - started_at).total_seconds(),
        )

        logger.info("========== GDELT 3-Way Bronze Consumer FINISHED ==========")
    except Exception as e:
        logger.error(f"!!! FAILED: {e} !!!")
        finished_at = datetime.now(timezone.utc)
        write_audit(
            spark, batch_id, "bronze", "failed",
            0, 0,
            started_at, finished_at,
            (finished_at - started_at).total_seconds(),
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
