from pyspark.sql import DataFrame, functions as F
import logging

logger = logging.getLogger(__name__)


def perform_three_way_join(
    events_silver: DataFrame,
    mentions_silver: DataFrame = None,
    gkg_silver: DataFrame = None,
) -> DataFrame:
    """
    Silver events_detailed 생성.

    흐름:
      1. events dedup (global_event_id 기준)
      2. mentions + gkg를 mention_identifier=document_identifier로 join
      3. 그 결과를 global_event_id 기준으로 aggregate → event-level enrichment
      4. events_dedup LEFT JOIN enrichment → 1 row per event

    mentions는 events와 gkg를 연결하는 bridge로만 사용.
    최종 결과에는 mention row가 남지 않음.
    """
    if events_silver is None:
        raise ValueError("events_silver is required")

    # ── 1. events dedup ───────────────────────────────────────────────────────
    events_dedup = events_silver.dropDuplicates(["global_event_id"])
    logger.info(f"events_dedup ready")

    if mentions_silver is None and gkg_silver is None:
        logger.warning("No mentions or GKG data. Returning events only.")
        return events_dedup

    # ── 2. mentions slim ──────────────────────────────────────────────────────
    # bridge 역할에 필요한 컬럼만 선택
    mentions_slim = None
    if mentions_silver is not None:
        mentions_slim = mentions_silver.select(
            F.col("global_event_id"),
            F.col("mention_identifier"),
            F.col("mention_source_name"),
            F.col("mention_doc_tone"),
            F.col("mention_time_date"),
        ).filter(
            F.col("global_event_id").isNotNull() &
            F.col("mention_identifier").isNotNull()
        )

    # ── 3. gkg slim ───────────────────────────────────────────────────────────
    gkg_slim = None
    if gkg_silver is not None:
        gkg_slim = gkg_silver.select(
            F.col("document_identifier"),
            F.col("v2_persons"),
            F.col("v2_organizations"),
            F.col("v2_enhanced_themes"),
            F.col("source_common_name"),
        ).filter(F.col("document_identifier").isNotNull())

    # ── 4. mentions + gkg join → bridge 테이블 ────────────────────────────────
    if mentions_slim is not None and gkg_slim is not None:
        logger.info("Joining mentions + gkg on mention_identifier = document_identifier")
        bridge = mentions_slim.join(
            gkg_slim,
            mentions_slim["mention_identifier"] == gkg_slim["document_identifier"],
            how="left",
        ).drop(gkg_slim["document_identifier"])
    elif mentions_slim is not None:
        logger.warning("No GKG data. Bridge = mentions only.")
        bridge = mentions_slim.withColumn("v2_persons", F.lit(None).cast("string")) \
                              .withColumn("v2_organizations", F.lit(None).cast("string")) \
                              .withColumn("v2_enhanced_themes", F.lit(None).cast("string")) \
                              .withColumn("source_common_name", F.lit(None).cast("string"))
    else:
        logger.warning("No mentions data. Returning events only.")
        return events_dedup

    # ── 5. bridge → event-level aggregate ────────────────────────────────────
    logger.info("Aggregating bridge to event-level")
    event_enrichment = bridge.groupBy("global_event_id").agg(
        F.count(F.col("mention_identifier")).alias("mention_count"),
        F.avg("mention_doc_tone").alias("mention_doc_tone"),
        F.max("mention_time_date").alias("mention_time_date"),
        F.first("mention_source_name", ignorenulls=True).alias("mention_source_name"),
        # GKG 집계: 세미콜론 구분 원문을 그대로 first로 대표값 추출
        # TODO: collect_set + array_join으로 전체 합산 고려
        F.first("v2_persons", ignorenulls=True).alias("v2_persons"),
        F.first("v2_organizations", ignorenulls=True).alias("v2_organizations"),
        F.first("v2_enhanced_themes", ignorenulls=True).alias("v2_enhanced_themes"),
        F.first("source_common_name", ignorenulls=True).alias("gkg_source_name"),
    )

    # ── 6. events_dedup LEFT JOIN enrichment ─────────────────────────────────
    logger.info("Left joining events_dedup with event_enrichment")
    result = events_dedup.join(event_enrichment, on="global_event_id", how="left")

    logger.info("3-way join complete. Result is 1 row per global_event_id.")
    return result


def select_final_columns(df: DataFrame) -> DataFrame:
    """events_detailed 최종 스키마 선택"""

    events_cols = [
        F.col("global_event_id"),
        F.col("event_date"),
        F.col("actor1_code"),
        F.col("actor1_name"),
        F.col("actor1_country_code"),
        F.col("actor1_known_group_code"),
        F.col("actor1_ethnic_code"),
        F.col("actor1_religion1_code"),
        F.col("actor1_religion2_code"),
        F.col("actor1_type1_code"),
        F.col("actor1_type2_code"),
        F.col("actor1_type3_code"),
        F.col("actor2_code"),
        F.col("actor2_name"),
        F.col("actor2_country_code"),
        F.col("actor2_known_group_code"),
        F.col("actor2_ethnic_code"),
        F.col("actor2_religion1_code"),
        F.col("actor2_religion2_code"),
        F.col("actor2_type1_code"),
        F.col("actor2_type2_code"),
        F.col("actor2_type3_code"),
        F.col("is_root_event"),
        F.col("event_code"),
        F.col("event_base_code"),
        F.col("event_root_code"),
        F.col("quad_class"),
        F.col("goldstein_scale"),
        F.col("num_mentions"),
        F.col("num_sources"),
        F.col("num_articles"),
        F.col("avg_tone"),
        F.col("action_geo_type"),
        F.col("action_geo_fullname"),
        F.col("action_geo_country_code"),
        F.col("action_geo_adm1_code"),
        F.col("action_geo_adm2_code"),
        F.col("action_geo_lat"),
        F.col("action_geo_long"),
        F.col("action_geo_feature_id"),
        F.col("date_added"),
        F.col("source_url"),
    ]

    enrichment_cols = [
        F.col("mention_count"),
        F.col("mention_doc_tone"),
        F.col("mention_time_date"),
        F.col("mention_source_name"),
        F.col("v2_persons"),
        F.col("v2_organizations"),
        F.col("v2_enhanced_themes"),
        F.col("gkg_source_name"),
    ]

    # priority_date, year/month/day/hour는 있을 때만
    optional_cols = [c for c in ["priority_date", "year", "month", "day", "hour"] if c in df.columns]

    # processed_at은 merge_to_silver에서 write 시점에 추가됨 — 여기서 선택하지 않음
    meta_cols = [
        F.col("source_batch_id"),
        F.col("source_batch_time"),
        F.col("ingested_at"),
    ]

    return df.select(
        *events_cols,
        *enrichment_cols,
        *[F.col(c) for c in optional_cols],
        *meta_cols,
    )
