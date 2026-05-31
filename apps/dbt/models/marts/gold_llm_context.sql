{#
  incremental_predicates: MERGE target scan을 event_date 파티션 기준으로 제한.
  event_date 파티션이 적용되어 pruning이 실제로 작동함.
  7일 이상 과거 데이터 백필 시 중복 삽입 가능성 있음 (파티션 범위 밖).
#}
{{ config(
    materialized='incremental',
    unique_key='global_event_id',
    incremental_strategy='merge',
    incremental_predicates=["DBT_INTERNAL_DEST.event_date >= current_date - interval '7' day"],
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['event_date']"
    }
) }}

{% set batch_id = var('source_batch_id', none) %}

with silver_data as (
    select *
    from (
        select *,
               row_number() over (
                   partition by global_event_id
                   order by processed_at desc
               ) as rn
        from nessie.silver.gdelt_events_detailed
        where global_event_id is not null
          {% if batch_id %}
          and source_batch_id = '{{ batch_id }}'
          {% endif %}
    ) t
    where rn = 1
),

event_codes as (
    select * from {{ ref('event_detail_codes') }}
),

country_codes as (
    select * from {{ ref('geo_country_codes') }}
)

select
    s.global_event_id,
    s.event_date,
    s.processed_at,
    s.actor1_name,
    s.actor1_country_code,
    s.actor2_name,
    s.actor2_country_code,
    s.event_code,
    s.action_geo_fullname,
    s.source_url,
    s.num_mentions,
    s.num_articles,
    s.goldstein_scale,
    s.avg_tone,
    s.mention_source_name,
    s.mention_doc_tone,
    s.v2_persons,
    s.v2_organizations,
    s.v2_enhanced_themes,
    s.source_batch_id,
    cast(s.source_batch_time as varchar)                as source_batch_time,
    cast(current_timestamp as timestamp(6) with time zone) as gold_processed_at,

    concat(
        '날짜: ',          cast(s.event_date as varchar),
        '. 행위자1: ',     coalesce(s.actor1_name, '미상'),
        ' (',              coalesce(c1.name_kor, s.actor1_country_code, '국적불명'), ')',
        ' → 행위자2: ',   coalesce(s.actor2_name, '미상'),
        ' (',              coalesce(c2.name_kor, s.actor2_country_code, '국적불명'), ')',
        '. 이벤트: ',      coalesce(ec.description, s.event_code, 'N/A'),
        '. 장소: ',        coalesce(s.action_geo_fullname, '미상'),
        '. 골드스타인: ',  coalesce(cast(s.goldstein_scale as varchar), 'N/A'),
        '. 톤: ',          coalesce(cast(s.avg_tone as varchar), 'N/A'),
        '. 언급횟수: ',    coalesce(cast(s.num_mentions as varchar), '0'),
        '. 관련인물: ',    coalesce(s.v2_persons, '없음'),
        '. 관련조직: ',    coalesce(s.v2_organizations, '없음'),
        '. 테마: ',        coalesce(s.v2_enhanced_themes, '없음'),
        '. URL: ',         coalesce(s.source_url, '없음')
    ) as llm_content_text

from silver_data s
left join event_codes ec   on s.event_code           = ec.code
left join country_codes c1 on s.actor1_country_code  = c1.iso_code
left join country_codes c2 on s.actor2_country_code  = c2.iso_code
