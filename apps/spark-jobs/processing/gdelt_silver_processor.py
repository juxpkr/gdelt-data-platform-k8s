import os
import sys
import logging
from datetime import datetime
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


def ensure_silver_namespace(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")


def read_from_bronze(spark: SparkSession, start_time_str: str, end_time_str: str) -> dict:
    start_dt = datetime.strptime(start_time_str.split('+')[0], "%Y-%m-%d %H:%M:%S")
    end_dt   = datetime.strptime(end_time_str.split('+')[0],   "%Y-%m-%d %H:%M:%S")

    dataframes = {}
    for data_type, table in BRONZE_TABLES.items():
        try:
            df = (
                spark.table(table)
                .filter(
                    (F.col("processed_at") >= F.lit(start_dt))
                    & (F.col("processed_at") <  F.lit(end_dt))
                )
                .coalesce(4)
            )
            if df.first():
                dataframes[data_type] = df
            else:
                logger.warning(f"No data for {data_type} in [{start_time_str}, {end_time_str})")
        except Exception as e:
            logger.error(f"Failed to read {data_type} from bronze: {e}")
    return dataframes


def write_to_iceberg(df: DataFrame, table: str):
    df.writeTo(table).using("iceberg").createOrReplace()


def main():
    logger.info("Starting GDELT Silver Processor...")

    spark = get_spark_session("GDELT_Silver_Processor")

    if len(sys.argv) != 3:
        logger.error("Usage: script.py <start_time> <end_time>")
        sys.exit(1)

    start_time_str = sys.argv[1]
    end_time_str   = sys.argv[2]

    try:
        ensure_silver_namespace(spark)

        bronze_dfs = read_from_bronze(spark, start_time_str, end_time_str)
        if not bronze_dfs:
            logger.warning("No bronze data found. Exiting.")
            return

        events_df   = bronze_dfs.get("events")
        mentions_df = bronze_dfs.get("mentions")
        gkg_df      = bronze_dfs.get("gkg")

        events_silver   = transform_events_to_silver(events_df)   if events_df   else None
        mentions_silver = transform_mentions_to_silver(mentions_df) if mentions_df else None
        gkg_silver      = transform_gkg_to_silver(gkg_df)          if gkg_df      else None

        if events_silver:
            write_to_iceberg(events_silver, SILVER_TABLES["events"])
            logger.info("Events silver stored.")

        joined_df = perform_three_way_join(events_silver, mentions_silver, gkg_silver)
        if joined_df and not joined_df.rdd.isEmpty():
            final_df = select_final_columns(create_priority_date(joined_df))
            write_to_iceberg(final_df, SILVER_TABLES["events_detailed"])
            logger.info("Silver events_detailed stored.")

        logger.info("Silver processing complete.")

    except Exception as e:
        logger.error(f"Silver processor failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
