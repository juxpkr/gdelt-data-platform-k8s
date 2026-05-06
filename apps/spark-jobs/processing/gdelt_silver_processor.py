import sys
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F

sys.path.append("/opt/airflow/spark-jobs")
from utils.spark_builder import get_spark_session
from processing.transformers.events_transformer import transform_events_to_silver
from processing.transformers.mentions_transformer import transform_mentions_to_silver
from processing.transformers.gkg_transformer import transform_gkg_to_silver
from processing.joiners.gdelt_three_way_joiner import perform_three_way_join, select_final_columns
from processing.partitioning.gdelt_date_priority import create_priority_date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLES = {
    "events":   "nessie.bronze.gdelt_events",
    "mentions": "nessie.bronze.gdelt_mentions",
    "gkg":      "nessie.bronze.gdelt_gkg",
}

SILVER_TABLES = {
    "events":          "nessie.silver.gdelt_events",
    "events_detailed": "nessie.silver.gdelt_events_detailed",
}


def read_from_bronze(spark: SparkSession, batch_id: str) -> dict:
    dataframes = {}
    for data_type, table in BRONZE_TABLES.items():
        try:
            df = (
                spark.table(table)
                .filter(F.col("source_batch_id") == batch_id)
                .coalesce(4)
            )
            if df.first():
                dataframes[data_type] = df
            else:
                logger.warning(f"No data for {data_type} with source_batch_id={batch_id}")
        except Exception as e:
            logger.error(f"Failed to read {data_type} from bronze: {e}")
    return dataframes


def merge_to_silver(spark: SparkSession, df: DataFrame, table: str, merge_key: str):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    view = f"silver_incoming_{table.split('.')[-1]}"
    df.withColumn("processed_at", F.current_timestamp()) \
      .createOrReplaceGlobalTempView(view)

    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 1")
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        spark.sql(f"""
            CREATE TABLE {table}
            USING iceberg
            PARTITIONED BY (days(processed_at))
            AS SELECT * FROM global_temp.{view}
        """)
        logger.info(f"Created and populated {table}")
    else:
        spark.sql(f"""
            MERGE INTO {table} t
            USING global_temp.{view} s
            ON t.{merge_key} = s.{merge_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)


def main():
    logger.info("Starting GDELT Silver Processor...")

    spark = get_spark_session("GDELT_Silver_Processor")

    if len(sys.argv) != 2:
        logger.error("Usage: script.py <source_batch_id>")
        sys.exit(1)

    batch_id = sys.argv[1]
    logger.info(f"Processing source_batch_id: {batch_id}")

    try:
        bronze_dfs = read_from_bronze(spark, batch_id)
        if not bronze_dfs:
            logger.warning("No bronze data found. Exiting.")
            return

        events_df   = bronze_dfs.get("events")
        mentions_df = bronze_dfs.get("mentions")
        gkg_df      = bronze_dfs.get("gkg")

        events_silver   = transform_events_to_silver(events_df)    if events_df   else None
        mentions_silver = transform_mentions_to_silver(mentions_df) if mentions_df else None
        gkg_silver      = transform_gkg_to_silver(gkg_df)           if gkg_df      else None

        if events_silver:
            merge_to_silver(spark, events_silver, SILVER_TABLES["events"], "global_event_id")
            logger.info("Events silver merged.")

        joined_df = perform_three_way_join(events_silver, mentions_silver, gkg_silver)
        if joined_df and not joined_df.rdd.isEmpty():
            final_df = select_final_columns(create_priority_date(joined_df))
            merge_to_silver(spark, final_df, SILVER_TABLES["events_detailed"], "global_event_id")
            logger.info("Silver events_detailed merged.")

        logger.info("Silver processing complete.")

    except Exception as e:
        logger.error(f"Silver processor failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
