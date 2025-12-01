{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://warehouse/gold',
    unique_key='global_event_id',
    pre_hook="CREATE OR REPLACE TEMPORARY VIEW view_silver_events_detailed USING DELTA OPTIONS (path 's3a://warehouse/silver/gdelt_events_detailed')"
) }}

WITH silver_data_raw AS (
    SELECT * FROM view_silver_events_detailed
),

-- 중복 제거: 가장 최신 데이터만
silver_data AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY global_event_id ORDER BY processed_at DESC) as rn
        FROM silver_data_raw
        WHERE global_event_id IS NOT NULL
    ) tmp
    WHERE rn = 1
),

-- Seeds (매핑 테이블) 참조
event_codes AS (
    SELECT * FROM {{ ref('event_detail_codes') }}
),
country_codes AS (
    SELECT * FROM {{ ref('geo_country_codes') }}
)

SELECT
    s.global_event_id,
    s.event_date,
    -- 메타데이터: 나중에 검색할 때 필터링용
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

    concat(
        '날짜: ', cast(s.event_date as string),
        '. 주요 행위자: ', coalesce(s.actor1_name, '신원 미상'),
        ' (', coalesce(c1.name_kor, s.actor1_country_code, '국적 불명'), ')',
        '이(가) ', coalesce(s.actor2_name, '대상 불명'),
        ' (', coalesce(c2.name_kor, s.actor2_country_code, '국적 불명'), ')',
        '에게 다음과 같은 행동을 했습니다: ', coalesce(ec.description, s.event_code),
        '. 장소: ', coalesce(s.action_geo_fullname, '위치 불명'),
        '. 골드스타인 척도: ', coalesce(cast(s.goldstein_scale as string), 'N/A'),
        '. 평균 톤: ', coalesce(cast(s.avg_tone as string), 'N/A'),
        '. 언급 횟수: ', coalesce(cast(s.num_mentions as string), '0'),
        '. 기사 수: ', coalesce(cast(s.num_articles as string), '0'),
        -- GKG 상세 정보
        '. 관련 인물: ', coalesce(s.v2_persons, '없음'),
        '. 관련 조직: ', coalesce(s.v2_organizations, '없음'),
        '. 주요 테마: ', coalesce(s.v2_enhanced_themes, '없음'),
        '. 언론사: ', coalesce(s.mention_source_name, '미상'),
        '. 기사 톤: ', coalesce(cast(s.mention_doc_tone as string), 'N/A'),
        '. 관련 기사 링크: ', coalesce(s.source_url, '없음')
    ) as llm_content_text

FROM silver_data s
LEFT JOIN event_codes ec ON s.event_code = ec.code
LEFT JOIN country_codes c1 ON s.actor1_country_code = c1.iso_code
LEFT JOIN country_codes c2 ON s.actor2_country_code = c2.iso_code

-- WHERE
    -- 중요도 필터링: 언급 횟수(num_mentions)가 5회 이상인 것만
    -- WHERE s.num_mentions >= 5