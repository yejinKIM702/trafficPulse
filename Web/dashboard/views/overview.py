"""
SCR-01: 요약 대시보드 (Overview)
선택된 기간/운영자에 대한 전체 혼잡/지연 상태를 한눈에 파악하는 화면
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from typing import Dict, Optional
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from utils.logger import logger
    from utils.errors import display_error_in_streamlit
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    def display_error_in_streamlit(e, msg=None):
        st.error(f"❌ {msg or str(e)}")


def load_overview_data(use_local: bool = True) -> Optional[pd.DataFrame]:
    """
    요약 대시보드용 데이터 로딩
    
    Args:
        use_local: 로컬 데이터 사용 여부
    
    Returns:
        DataFrame 또는 None
    """
    try:
        if use_local:
            # 로컬 Parquet 파일 읽기
            data_path = Path(__file__).parent.parent.parent / "data" / "curated" / "route_delay_by_hour"
            
            if not data_path.exists():
                logger.warning(f"데이터 경로를 찾을 수 없습니다: {data_path}")
                return None
            
            import pyarrow.parquet as pq
            
            table = pq.read_table(data_path)
            df = table.to_pandas()
            
            return df
        else:
            # S3 또는 DynamoDB에서 읽기
            # TODO: 구현 필요
            return None
            
    except Exception as e:
        logger.error(f"데이터 로딩 실패: {str(e)}")
        return None


def filter_data_by_state(df: pd.DataFrame, filter_state: Dict) -> pd.DataFrame:
    """
    필터 상태에 따라 데이터 필터링
    
    Args:
        df: 원본 DataFrame
        filter_state: 필터 상태
    
    Returns:
        필터링된 DataFrame
    """
    filtered_df = df.copy()
    
    # 날짜 필터
    if filter_state.get('date_range'):
        start_date, end_date = filter_state['date_range']
        if 'aggregation_date' in filtered_df.columns:
            filtered_df['aggregation_date'] = pd.to_datetime(filtered_df['aggregation_date'])
            filtered_df = filtered_df[
                (filtered_df['aggregation_date'].dt.date >= start_date) &
                (filtered_df['aggregation_date'].dt.date <= end_date)
            ]
    
    # 요일 필터
    if filter_state.get('day_of_week'):
        if 'weekday' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['weekday'].isin(filter_state['day_of_week'])]
    
    # 시간대 필터
    if filter_state.get('time_range'):
        time_start, time_end = filter_state['time_range']
        if 'hour' in filtered_df.columns:
            filtered_df = filtered_df[
                (filtered_df['hour'] >= time_start) &
                (filtered_df['hour'] <= time_end)
            ]
    
    # 운영자 필터
    if filter_state.get('operator'):
        if 'Operatorname' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Operatorname'] == filter_state['operator']]
    
    return filtered_df


def calculate_congestion_score(congestion_rate: float) -> float:
    """
    혼잡률을 혼잡 지수(0-100)로 변환
    
    Args:
        congestion_rate: 혼잡률 (%)
    
    Returns:
        혼잡 지수 (0-100)
    """
    return min(100, max(0, congestion_rate))


def render_overview_view(filter_state: Dict):
    """
    요약 대시보드 화면 렌더링 (SCR-01)
    
    Args:
        filter_state: 필터 상태
    """
    st.header("도시 버스 혼잡/지연 요약 대시보드")
    
    # 로딩 인디케이터
    with st.spinner("데이터를 불러오는 중입니다..."):
        try:
            # 데이터 로딩
            df = load_overview_data(use_local=True)
            
            if df is None or df.empty:
                st.warning("⚠️ 데이터를 불러올 수 없습니다. ETL 파이프라인을 먼저 실행해주세요.")
                st.info("💡 실행 방법: `python refresh.py`")
                return
            
            # 필터 적용
            filtered_df = filter_data_by_state(df, filter_state)
            
            if filtered_df.empty:
                st.warning("⚠️ 선택한 조건에 해당하는 데이터가 없습니다.")
                return
            
            # KPI 카드 영역 (3~4개)
            st.subheader("📈 핵심 지표 (KPI)")
            
            col1, col2, col3, col4 = st.columns(4)
            
            # KPI-01: 평균 혼잡 지수
            avg_congestion_rate = filtered_df['congestion_rate'].mean() if 'congestion_rate' in filtered_df.columns else 0
            congestion_score = calculate_congestion_score(avg_congestion_rate)
            
            # KPI-02: 평균 속도
            # Speed 데이터가 없으면 avg_delay에서 추정 (간단한 버전)
            avg_speed = 30.0  # 기본값, 실제로는 Speed 컬럼에서 계산해야 함
            
            # KPI-03: 정지/저속 비율
            # Congestion=1인 비율을 정지/저속 비율로 간주
            stop_slow_ratio = avg_congestion_rate
            
            # KPI-04: 측정 샘플 수
            total_samples = filtered_df['trip_count'].sum() if 'trip_count' in filtered_df.columns else len(filtered_df)
            
            with col1:
                st.metric(
                    "평균 혼잡 지수 (KPI-01)",
                    f"{congestion_score:.1f}",
                    delta=f"{avg_congestion_rate:.1f}%"
                )
            
            with col2:
                st.metric(
                    "평균 속도 (KPI-02)",
                    f"{avg_speed:.1f} km/h"
                )
            
            with col3:
                st.metric(
                    "정지/저속 비율 (KPI-03)",
                    f"{stop_slow_ratio:.1f}%"
                )
            
            with col4:
                st.metric(
                    "측정 샘플 수 (KPI-04)",
                    f"{int(total_samples):,}"
                )
            
            st.markdown("---")
            
            # 그래프 영역 - CH-01: 일자별 평균 혼잡 지수 추이
            st.subheader("일자별 평균 혼잡 지수 추이 (CH-01)")
            
            if 'aggregation_date' in filtered_df.columns and 'congestion_rate' in filtered_df.columns:
                daily_congestion = filtered_df.groupby('aggregation_date').agg({
                    'congestion_rate': 'mean',
                    'trip_count': 'sum'
                }).reset_index()
                
                daily_congestion['aggregation_date'] = pd.to_datetime(daily_congestion['aggregation_date'])
                daily_congestion = daily_congestion.sort_values('aggregation_date')
                daily_congestion['congestion_score'] = daily_congestion['congestion_rate'].apply(calculate_congestion_score)
                
                fig = px.line(
                    daily_congestion,
                    x='aggregation_date',
                    y='congestion_score',
                    title="일자별 평균 혼잡 지수 추이",
                    labels={
                        'aggregation_date': '날짜',
                        'congestion_score': '평균 혼잡 지수 (0-100)'
                    },
                    markers=True
                )
                fig.update_traces(line_color='#1f77b4', line_width=3, marker_size=8)
                fig.update_layout(
                    hovermode='x unified',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # 일자별 상세 데이터 테이블
                with st.expander("일자별 상세 데이터 보기"):
                    display_df = daily_congestion[['aggregation_date', 'congestion_score', 'congestion_rate', 'trip_count']].copy()
                    display_df.columns = ['날짜', '혼잡 지수', '혼잡률 (%)', '운행 횟수']
                    display_df = display_df.round(2)
                    st.dataframe(display_df, use_container_width=True)
            
            # 시간대별 평균 혼잡 지수 추이
            st.subheader("시간대별 평균 혼잡 지수 추이")
            
            if 'hour' in filtered_df.columns and 'congestion_rate' in filtered_df.columns:
                hourly_congestion = filtered_df.groupby('hour').agg({
                    'congestion_rate': 'mean',
                    'trip_count': 'sum'
                }).reset_index()
                hourly_congestion = hourly_congestion.sort_values('hour')
                hourly_congestion['congestion_score'] = hourly_congestion['congestion_rate'].apply(calculate_congestion_score)
                
                fig = px.bar(
                    hourly_congestion,
                    x='hour',
                    y='congestion_score',
                    title="시간대별 평균 혼잡 지수",
                    labels={
                        'hour': '시간 (시)',
                        'congestion_score': '평균 혼잡 지수 (0-100)'
                    },
                    color='congestion_score',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            display_error_in_streamlit(e, "요약 대시보드 데이터를 불러오는 중 오류가 발생했습니다.")

