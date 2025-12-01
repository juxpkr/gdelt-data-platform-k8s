import os
import sys
import time
from pyspark.sql.functions import col
import pandas as pd
import json
from google import genai 
from pyspark.sql.types import StringType

from utils.spark_builder import get_spark_session

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    print("CRITICAL: GOOGLE_API_KEY environment variable is missing!")
    sys.exit(1)

# Gemini Client 초기화
client = genai.Client(api_key=GOOGLE_API_KEY)

def generate_batch_summary(df_pandas):
    """50개 뉴스를 1번의 API 호출로 처리 """
    
    # 텍스트 묶기
    combined_text = ""
    for _, row in df_pandas.iterrows():
        combined_text += f"[ID:{row['global_event_id']}] {row['llm_content_text']}\n"

    # 프롬프트 (JSON 출력 강제)
    prompt = f"""
    당신은 30년 경력의 베테랑 국제 정세 분석가입니다. 
    아래 제공된 [뉴스 데이터]를 보고 정책 입안자를 위한 '일일 정세 브리핑'을 작성하십시오.

    [요청 사항]
    1. 각 이벤트(ID)별로 단순 요약이 아닌, '사건의 배경 - 핵심 내용 - 지정학적 시사점'이 연결되도록 작성할 것.
    2. 분량은 각 ID당 한국어 2~3문장으로 알차게 구성할 것. (너무 짧은 단답형 지양)
    3. 문체는 '했습니다' 같은 서술형보다는, '~함', '~로 분석됨', '~가 예상됨' 등 명료한 보고서체(개조식)를 사용할 것.

    [응답 포맷 및 제약]
    1. 반드시 아래 JSON 포맷을 엄격히 준수할 것.
    2. JSON 데이터 외에 '```json' 마크다운이나 잡담을 절대 포함하지 말 것.

    [응답 예시]
    [
      {{"id": "123456", "summary": "미국 연준이 인플레이션 우려로 금리를 0.25%p 인상함. 이는 최근 고용 지표 호조에 따른 결정으로 보이며, 신흥국 자본 유출에 대한 경계감이 고조될 것으로 전망됨."}},
      {{"id": "789012", "summary": "러시아와 우크라이나 국경 지대에서 국지적 교전이 발생하여 긴장이 최고조에 달함. EU는 즉각적인 중재 의사를 밝혔으나, 에너지 공급망 차질 우려로 국제 유가가 급등세를 보이고 있음."}}
    ]

    [뉴스 데이터]
    {combined_text}
    """

    try:
        # 1회 호출 (gemini-2.5-flash 사용)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={'response_mime_type': 'application/json'}
        )
        
        # 결과 파싱
        data = json.loads(response.text)
        return {str(item['id']): item['summary'] for item in data}

    except Exception as e:
        print(f"ERROR in batch processing: {str(e)}")
        return {}

def main():
    spark = get_spark_session(app_name="GDELT-LLM-Summary-Generator")

    print("=" * 60)
    print(" Reading Gold Layer Data...")
    print("=" * 60)

    # Gold 데이터 읽기
    gold_path = "s3a://warehouse/gold/gold_llm_context"
    df = spark.read.format("delta").load(gold_path)

    # Global Impact Top 50 추출 (num_mentions 기준)
    print("\n Extracting TOP 50 High-Impact Events...")
    if "num_mentions" in df.columns:
        print(" Sorting by High Impact (num_mentions)...")
        pdf = df.filter(col("num_mentions") > 0) \
                .orderBy(col("num_mentions").desc(), col("event_date").desc()) \
                .limit(50).toPandas()
    else:
        print(" 'num_mentions' column missing! Sorting by Recent Date instead.")
        pdf = df.orderBy(col("event_date").desc()) \
                .limit(50).toPandas()

    if pdf.empty:
        print(" No data found in Gold Layer!")
        spark.stop()
        return

    print(f"\n Calling Gemini API for {len(pdf)} events (Batch Mode)...")
    
    # 배치 함수 호출
    summary_map = generate_batch_summary(pdf)
    
    # 결과 매핑 (ID 기준으로 요약문 합치기)
    pdf['ai_summary'] = pdf['global_event_id'].astype(str).map(summary_map).fillna("요약 실패")

    # 결과 미리보기
    print("\n" + "=" * 60)
    print(" Sample Results:")
    print("=" * 60)
    
    cols_to_show = ['event_date', 'ai_summary']
    if 'num_mentions' in pdf.columns:
        cols_to_show.insert(1, 'num_mentions')
    
    print(pdf[cols_to_show].head(3).to_string())

    print("=" * 60)

    # 결과 저장
    print("\n Saving results to Delta Lake...")
    new_schema = df.schema
    
    
    new_schema = new_schema.add("ai_summary", StringType(), True)
    
    s_df = spark.createDataFrame(pdf, schema=new_schema)
    output_path = "s3a://warehouse/gold/gdelt_ai_summaries"
    s_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)

    print(f"\n Successfully saved {len(pdf)} AI summaries to {output_path}")
    print("=" * 60)

if __name__ == "__main__":
    main()