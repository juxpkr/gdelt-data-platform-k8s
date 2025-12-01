import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from deltalake import DeltaTable
from datetime import datetime, timedelta
import pytz
import os

# 페이지 기본 세팅
st.set_page_config(
    layout="wide",
    page_title="GDELT AI Analytics Platform",
    page_icon="🌍",
    initial_sidebar_state="expanded"
)

# MinIO 접속 정보 
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

storage_options = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true"
}

# MinIO에서 데이터 가져오기
@st.cache_data(ttl=60)
def load_ai_summaries():
    """Gold Layer - AI 요약 데이터"""
    try:
        dt = DeltaTable("s3://warehouse/gold/gdelt_ai_summaries", storage_options=storage_options)
        df = dt.to_pandas()
        if 'event_date' in df.columns:
            df['event_date'] = pd.to_datetime(df['event_date'])
        return df
    except Exception as e:
        st.error(f"AI Summaries 로드 실패: {str(e)}")
        return None

@st.cache_data(ttl=60)
def load_llm_context():
    """Gold Layer - LLM 컨텍스트 데이터"""
    try:
        dt = DeltaTable("s3://warehouse/gold/gold_llm_context", storage_options=storage_options)
        df = dt.to_pandas()
        if 'event_date' in df.columns:
            df['event_date'] = pd.to_datetime(df['event_date'])
        return df
    except Exception as e:
        st.warning(f"LLM Context 로드 실패: {str(e)}")
        return None

@st.cache_data(ttl=120)
def load_silver_events():
    """Silver Layer - 정제된 이벤트 데이터"""
    try:
        dt = DeltaTable("s3://warehouse/silver/gdelt_events", storage_options=storage_options)
        df = dt.to_pandas()
        if 'event_date' in df.columns:
            df['event_date'] = pd.to_datetime(df['event_date'])
        return df
    except Exception as e:
        st.warning(f"Silver Events 로드 실패: {str(e)}")
        return None

def check_connection():
    """MinIO 연결 상태 체크"""
    try:
        load_ai_summaries()
        return True
    except:
        return False

# [사이드바] 네비게이션 & 필터
st.sidebar.title("🌍 GDELT Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "페이지 선택",
    ["대시보드", "AI 인사이트", "데이터 탐색", "파이프라인 모니터"]
)

st.sidebar.markdown("---")
st.sidebar.caption(f"**Backend:** Kubernetes (Kind)")
st.sidebar.caption(f"**Storage:** MinIO + Delta Lake")
st.sidebar.caption(f"**Engine:** Spark 3.4.3 + dbt")
st.sidebar.caption(f"**AI Model:** Gemini 2.5 Flash")

# [페이지 1] 대시보드
if page == "대시보드":
    st.title("🌍 GDELT AI Analytics Platform")
    st.markdown("### 실시간 국제 뉴스 이벤트 분석 대시보드")

    # 데이터 로드
    df_ai = load_ai_summaries()
    df_context = load_llm_context()
    df_silver = load_silver_events()

    if df_ai is None:
        st.error("데이터를 불러올 수 없습니다. MinIO 연결을 확인하세요.")
        st.stop()

    # 상단 메트릭
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_events = len(df_ai) if df_ai is not None else 0
        st.metric("처리된 이벤트", f"{total_events:,}건")

    with col2:
        if df_context is not None and 'num_mentions' in df_context.columns:
            total_mentions = int(df_context['num_mentions'].sum())
            st.metric("총 언급 수", f"{total_mentions:,}")
        else:
            st.metric("총 언급 수", "N/A")

    with col3:
        if df_ai is not None and 'event_date' in df_ai.columns:
            latest_date = df_ai['event_date'].max()
            st.metric("최신 데이터", latest_date.strftime('%Y-%m-%d') if pd.notna(latest_date) else "N/A")
        else:
            st.metric("최신 데이터", "N/A")

    with col4:
        st.metric("AI 모델", "Gemini 2.5")

    st.divider()

    # 시각화 섹션
    if df_context is not None and not df_context.empty:
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("시간별 이벤트 트렌드")
            if 'event_date' in df_context.columns:
                daily_counts = df_context.groupby('event_date').size().reset_index(name='count')
                daily_counts = daily_counts.sort_values('event_date')

                fig = px.line(
                    daily_counts,
                    x='event_date',
                    y='count',
                    labels={'event_date': '날짜', 'count': '이벤트 수'},
                    template='plotly_white'
                )
                fig.update_traces(line_color='#1f77b4', line_width=2)
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("평균 감정 점수 추이")
            if 'avg_tone' in df_context.columns and 'event_date' in df_context.columns:
                tone_by_date = df_context.groupby('event_date')['avg_tone'].mean().reset_index()
                tone_by_date = tone_by_date.sort_values('event_date')

                fig = px.area(
                    tone_by_date,
                    x='event_date',
                    y='avg_tone',
                    labels={'event_date': '날짜', 'avg_tone': '평균 감정 점수'},
                    template='plotly_white'
                )
                fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="중립")
                fig.update_traces(fillcolor='rgba(31, 119, 180, 0.3)')
                st.plotly_chart(fig, use_container_width=True)

    # 최신 AI 요약 미리보기
    st.subheader("최근 AI 요약 Top 5")
    if df_ai is not None and not df_ai.empty:
        recent = df_ai.sort_values('event_date', ascending=False).head(5)

        for idx, row in recent.iterrows():
            with st.expander(f"{row.get('event_date', 'N/A')} - Event ID: {row.get('global_event_id', 'N/A')}"):
                st.success(f"**AI 요약:** {row.get('ai_summary', '요약 없음')}")

                if 'num_mentions' in row:
                    st.caption(f"언급 횟수: {row['num_mentions']}")
                if 'avg_tone' in row:
                    tone_emoji = "😊" if row['avg_tone'] > 0 else "😞" if row['avg_tone'] < 0 else "😐"
                    st.caption(f"{tone_emoji} 감정 점수: {row['avg_tone']:.2f}")

# [페이지 2] AI 인사이트
elif page == "AI 인사이트":
    st.title("GDELT AI Insight Inspector")
    st.markdown("### Raw Data vs AI Summary 비교 분석")

    df = load_ai_summaries()

    if df is None or df.empty:
        st.error("AI 요약 데이터가 없습니다!")
        st.stop()

    # 최신순 정렬
    if 'event_date' in df.columns:
        df = df.sort_values(by='event_date', ascending=False)

    # 상단: 요약 지표
    col1, col2, col3 = st.columns(3)
    col1.metric("분석된 이벤트", f"{len(df):,}건")
    col2.metric("AI 모델", "Gemini 2.5 Flash")

    if 'num_mentions' in df.columns:
        avg_mentions = df['num_mentions'].mean()
        col3.metric("평균 언급 수", f"{avg_mentions:.1f}")

    st.divider()

    # 필터 옵션
    col_filter1, col_filter2 = st.columns([2, 1])

    with col_filter1:
        # 날짜 필터
        if 'event_date' in df.columns:
            date_options = sorted(df['event_date'].dt.date.unique(), reverse=True)
            selected_date = st.selectbox(
                "날짜 선택",
                ["전체"] + [str(d) for d in date_options]
            )

            if selected_date != "전체":
                df = df[df['event_date'].dt.date == pd.to_datetime(selected_date).date()]

    with col_filter2:
        # 정렬 기준
        sort_by = st.selectbox(
            "정렬 기준",
            ["최신순", "언급 많은순"] if 'num_mentions' in df.columns else ["최신순"]
        )

        if sort_by == "언급 많은순" and 'num_mentions' in df.columns:
            df = df.sort_values('num_mentions', ascending=False)

    st.divider()

    # 메인: Before & After 비교
    st.subheader("이벤트 상세 분석")

    # 이벤트 리스트
    if df.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        event_options = df.apply(
            lambda row: f"{row.get('event_date', 'N/A')} | ID: {row['global_event_id']} | {row.get('ai_summary', '')[:50]}...",
            axis=1
        ).tolist()

        selected_idx = st.selectbox(
            "분석할 이벤트 선택:",
            range(len(event_options)),
            format_func=lambda i: event_options[i]
        )

        if selected_idx is not None:
            row = df.iloc[selected_idx]

            # 메타 정보
            meta_col1, meta_col2, meta_col3 = st.columns(3)
            with meta_col1:
                st.info(f"**Event ID:** {row.get('global_event_id', 'N/A')}")
            with meta_col2:
                st.info(f"**날짜:** {row.get('event_date', 'N/A')}")
            with meta_col3:
                if 'num_mentions' in row:
                    st.info(f"**언급 수:** {row['num_mentions']}")

            st.divider()

            # Before & After 비교
            c1, c2 = st.columns(2)

            with c1:
                st.markdown("#### AI Summary (Output)")
                st.success(f"{row.get('ai_summary', '요약 없음')}")
                st.caption("Gemini 2.5 Flash가 생성한 한국어 요약")

            with c2:
                st.markdown("#### Raw Context (Input)")
                context_text = row.get('llm_content_text', 'No Context')
                st.text_area(
                    label="원본 데이터",
                    value=context_text,
                    height=300,
                    disabled=True,
                    label_visibility="collapsed"
                )
                st.caption("Spark + dbt가 조립한 원본 컨텍스트")

# [페이지 3] 데이터 탐색
elif page == "데이터 탐색":
    st.title("GDELT 데이터 탐색")
    st.markdown("### Silver & Gold Layer 원본 데이터 조회")

    # 레이어 선택
    layer = st.radio(
        "데이터 레이어 선택:",
        ["Gold - LLM Context", "Gold - AI Summaries", "Silver - Events"],
        horizontal=True
    )

    # 데이터 로드
    if layer == "Gold - LLM Context":
        df = load_llm_context()
        table_path = "s3://warehouse/gold/gold_llm_context"
    elif layer == "Gold - AI Summaries":
        df = load_ai_summaries()
        table_path = "s3://warehouse/gold/gdelt_ai_summaries"
    else:
        df = load_silver_events()
        table_path = "s3://warehouse/silver/events"

    if df is None or df.empty:
        st.warning(f"{layer} 데이터를 불러올 수 없습니다.")
        st.stop()

    # 데이터 정보
    col1, col2, col3 = st.columns(3)
    col1.metric("총 레코드 수", f"{len(df):,}")
    col2.metric("컬럼 수", f"{len(df.columns)}")

    if 'event_date' in df.columns:
        date_range = f"{df['event_date'].min().date()} ~ {df['event_date'].max().date()}"
        col3.metric("데이터 기간", date_range)

    st.divider()

    # 컬럼 정보
    with st.expander("테이블 스키마 보기"):
        schema_df = pd.DataFrame({
            '컬럼명': df.columns,
            '데이터 타입': [str(dtype) for dtype in df.dtypes],
            'Null 개수': [df[col].isna().sum() for col in df.columns],
            'Null 비율 (%)': [f"{(df[col].isna().sum() / len(df) * 100):.1f}" for col in df.columns]
        })
        st.dataframe(schema_df, use_container_width=True)

    st.caption(f"**테이블 경로:** `{table_path}`")

    # 필터 & 검색
    st.subheader("데이터 필터링")

    col_search1, col_search2 = st.columns(2)

    with col_search1:
        # 날짜 필터
        if 'event_date' in df.columns:
            min_date = df['event_date'].min().date()
            max_date = df['event_date'].max().date()

            date_range = st.date_input(
                "날짜 범위:",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )

            if len(date_range) == 2:
                df = df[
                    (df['event_date'].dt.date >= date_range[0]) &
                    (df['event_date'].dt.date <= date_range[1])
                ]

    with col_search2:
        # 텍스트 검색
        search_query = st.text_input("텍스트 검색 (모든 컬럼):")
        if search_query:
            mask = df.astype(str).apply(lambda row: row.str.contains(search_query, case=False, na=False).any(), axis=1)
            df = df[mask]

    # 표시할 행 수
    show_rows = st.slider("표시할 행 수:", 10, 1000, 100, step=10)

    st.divider()

    # 데이터 테이블
    st.subheader(f"데이터 미리보기 (총 {len(df):,}건)")
    st.dataframe(df.head(show_rows), use_container_width=True)

    # CSV 다운로드
    csv = df.head(show_rows).to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="CSV 다운로드",
        data=csv,
        file_name=f"gdelt_{layer.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# [페이지 4] 파이프라인 모니터
elif page == "파이프라인 모니터":
    st.title("GDELT 파이프라인 모니터")
    st.markdown("### Delta Lake 메타데이터 & 데이터 품질 체크")

    # 레이어별 상태 체크
    st.subheader("레이어별 데이터 현황")

    layers_info = []

    # Silver Layer
    df_silver = load_silver_events()
    if df_silver is not None:
        silver_info = {
            "레이어": "Silver Events",
            "경로": "s3://warehouse/silver/events",
            "레코드 수": f"{len(df_silver):,}",
            "최신 데이터": df_silver['event_date'].max().strftime('%Y-%m-%d %H:%M') if 'event_date' in df_silver.columns else "N/A",
            "상태": "정상"
        }
        layers_info.append(silver_info)
    else:
        layers_info.append({
            "레이어": "Silver Events",
            "경로": "s3://warehouse/silver/events",
            "레코드 수": "N/A",
            "최신 데이터": "N/A",
            "상태": "오류"
        })

    # Gold LLM Context
    df_context = load_llm_context()
    if df_context is not None:
        context_info = {
            "레이어": "Gold LLM Context",
            "경로": "s3://warehouse/gold/gold_llm_context",
            "레코드 수": f"{len(df_context):,}",
            "최신 데이터": df_context['event_date'].max().strftime('%Y-%m-%d %H:%M') if 'event_date' in df_context.columns else "N/A",
            "상태": "정상"
        }
        layers_info.append(context_info)
    else:
        layers_info.append({
            "레이어": "Gold LLM Context",
            "경로": "s3://warehouse/gold/gold_llm_context",
            "레코드 수": "N/A",
            "최신 데이터": "N/A",
            "상태": "오류"
        })

    # Gold AI Summaries
    df_ai = load_ai_summaries()
    if df_ai is not None:
        ai_info = {
            "레이어": "Gold AI Summaries",
            "경로": "s3://warehouse/gold/gdelt_ai_summaries",
            "레코드 수": f"{len(df_ai):,}",
            "최신 데이터": df_ai['event_date'].max().strftime('%Y-%m-%d %H:%M') if 'event_date' in df_ai.columns else "N/A",
            "상태": "정상"
        }
        layers_info.append(ai_info)
    else:
        layers_info.append({
            "레이어": "Gold AI Summaries",
            "경로": "s3://warehouse/gold/gdelt_ai_summaries",
            "레코드 수": "N/A",
            "최신 데이터": "N/A",
            "상태": "오류"
        })

    # 테이블로 표시
    layers_df = pd.DataFrame(layers_info)
    st.dataframe(layers_df, use_container_width=True, hide_index=True)

    st.divider()

    # 데이터 품질 체크
    st.subheader("데이터 품질 분석")

    if df_ai is not None and not df_ai.empty:
        quality_col1, quality_col2 = st.columns(2)

        with quality_col1:
            st.markdown("#### AI 요약 품질")

            # AI 요약 길이 분석
            if 'ai_summary' in df_ai.columns:
                df_ai['summary_length'] = df_ai['ai_summary'].astype(str).str.len()
                avg_length = df_ai['summary_length'].mean()

                st.metric("평균 요약 길이", f"{avg_length:.0f} 자")

                # 요약 실패 건수
                failed = (df_ai['ai_summary'].astype(str).str.contains('요약 실패|No Summary', na=True)).sum()
                success_rate = ((len(df_ai) - failed) / len(df_ai) * 100) if len(df_ai) > 0 else 0
                st.metric("요약 성공률", f"{success_rate:.1f}%")

        with quality_col2:
            st.markdown("#### 데이터 신선도")

            if 'event_date' in df_ai.columns:
                latest = df_ai['event_date'].max()
                now = pd.Timestamp.now()
                age_hours = (now - latest).total_seconds() / 3600

                st.metric("최신 데이터 시점", latest.strftime('%Y-%m-%d %H:%M'))

                if age_hours < 24:
                    st.success(f"신선함")
                elif age_hours < 48:
                    st.warning(f"주의 (약 {age_hours/24:.1f}일 전)")
                else:
                    st.error(f"오래됨 (약 {age_hours/24:.1f}일 전)")

    st.divider()

    # 시스템 정보
    st.subheader("시스템 정보")

    sys_col1, sys_col2 = st.columns(2)

    with sys_col1:
        st.info(f"""
        **MinIO Endpoint:** `{MINIO_ENDPOINT}`
        **Access Key:** `{MINIO_ACCESS_KEY[:5]}***`
        **Region:** `us-east-1`
        **Protocol:** `HTTP`
        """)

    with sys_col2:
        st.info(f"""
        **Kubernetes:** Kind (Local)
        **Spark Version:** 3.5
        **Delta Lake:** 2.4.0
        **dbt Core:** 1.8+
        """)

    # 접속 가이드
    with st.expander("MinIO 포트 포워딩 설정"):
        st.code("""
# MinIO 콘솔 (UI)
kubectl port-forward svc/minio-console 9001:9001 -n airflow

# MinIO API (S3)
kubectl port-forward svc/minio 9000:9000 -n airflow

# Airflow UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
        """, language="bash")

# [Footer]
st.divider()
st.caption("GDELT Data Platform | Powered by Kubernetes + Spark + Delta Lake + Gemini AI")
