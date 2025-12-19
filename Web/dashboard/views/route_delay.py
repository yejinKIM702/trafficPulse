"""
F-09: 노선별 지연 분석 화면
선택한 날짜 범위와 노선에 대해 일별/시간대별 평균 지연 그래프를 보여줍니다.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from typing import Dict, Optional
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from utils.logger import logger
    from utils.errors import display_error_in_streamlit
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    def display_error_in_streamlit(e, msg=None):
        import streamlit as st
        st.error(f"❌ {msg or str(e)}")


def load_route_delay_data(use_local: bool = True) -> Optional[pd.DataFrame]:
    """
    노선별 지연 데이터 로딩
    
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
            
            # PySpark 없이 pandas로 직접 읽기 (간단한 버전)
            # 실제로는 PySpark를 사용하거나, 미리 변환된 CSV를 사용할 수 있음
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
    
    # 노선 필터
    if filter_state.get('selected_routes') and len(filter_state['selected_routes']) > 0:
        if 'Journey_Pattern_ID' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Journey_Pattern_ID'].isin(filter_state['selected_routes'])]
    
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


def render_route_delay_view(filter_state: Dict):
    """
    노선별 지연 분석 화면 렌더링
    
    Args:
        filter_state: 필터 상태
    """
    st.header("노선별 지연 분석")
    
    try:
        # 로딩 인디케이터
        with st.spinner("데이터를 불러오는 중입니다..."):
            # 데이터 로딩
            df = load_route_delay_data(use_local=True)
        
        if df is None or df.empty:
            st.warning("⚠️ 데이터를 불러올 수 없습니다. ETL 파이프라인을 먼저 실행해주세요.")
            st.info("💡 실행 방법: `python -m etl.spark_etl`")
            return
        
        # 필터 적용
        filtered_df = filter_data_by_state(df, filter_state)
        
        if filtered_df.empty:
            st.warning("⚠️ 선택한 필터 조건에 해당하는 데이터가 없습니다.")
            return
        
        # KPI 카드
        col1, col2, col3, col4 = st.columns(4)
        
        avg_delay = filtered_df['avg_delay'].mean() if 'avg_delay' in filtered_df.columns else 0
        max_delay = filtered_df['max_delay'].max() if 'max_delay' in filtered_df.columns else 0
        total_trips = filtered_df['trip_count'].sum() if 'trip_count' in filtered_df.columns else 0
        congestion_rate = filtered_df['congestion_rate'].mean() if 'congestion_rate' in filtered_df.columns else 0
        
        with col1:
            st.metric("평균 지연 시간", f"{avg_delay:.1f}초")
        with col2:
            st.metric("최대 지연 시간", f"{max_delay:.1f}초")
        with col3:
            st.metric("총 운행 횟수", f"{int(total_trips):,}회")
        with col4:
            st.metric("평균 혼잡률", f"{congestion_rate:.1f}%")
        
        st.markdown("---")
        
        # 일별 평균 지연 그래프
        st.subheader("일별 평균 지연 시간")
        
        if 'aggregation_date' in filtered_df.columns and 'avg_delay' in filtered_df.columns:
            daily_delay = filtered_df.groupby('aggregation_date')['avg_delay'].mean().reset_index()
            daily_delay['aggregation_date'] = pd.to_datetime(daily_delay['aggregation_date'])
            daily_delay = daily_delay.sort_values('aggregation_date')
            
            fig = px.line(
                daily_delay,
                x='aggregation_date',
                y='avg_delay',
                title="일별 평균 지연 시간 추이",
                labels={'aggregation_date': '날짜', 'avg_delay': '평균 지연 시간 (초)'}
            )
            fig.update_traces(line_color='#1f77b4', line_width=2)
            st.plotly_chart(fig, use_container_width=True)
        
        # 시간대별 평균 지연 그래프
        st.subheader("시간대별 평균 지연 시간")
        
        if 'hour' in filtered_df.columns and 'avg_delay' in filtered_df.columns:
            hourly_delay = filtered_df.groupby('hour')['avg_delay'].mean().reset_index()
            hourly_delay = hourly_delay.sort_values('hour')
            
            fig = px.bar(
                hourly_delay,
                x='hour',
                y='avg_delay',
                title="시간대별 평균 지연 시간",
                labels={'hour': '시간 (시)', 'avg_delay': '평균 지연 시간 (초)'}
            )
            fig.update_traces(marker_color='#ff7f0e')
            st.plotly_chart(fig, use_container_width=True)
        
        # 노선별 지연 비교
        st.subheader("노선별 지연 비교")
        
        if 'Journey_Pattern_ID' in filtered_df.columns and 'avg_delay' in filtered_df.columns:
            route_delay = filtered_df.groupby('Journey_Pattern_ID')['avg_delay'].mean().reset_index()
            route_delay = route_delay.sort_values('avg_delay', ascending=False).head(10)
            
            fig = px.bar(
                route_delay,
                x='Journey_Pattern_ID',
                y='avg_delay',
                title="노선별 평균 지연 시간 (상위 10개)",
                labels={'Journey_Pattern_ID': '노선 ID', 'avg_delay': '평균 지연 시간 (초)'}
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # 데이터 테이블
        with st.expander("상세 데이터 보기"):
            st.dataframe(filtered_df.head(100))
            
    except Exception as e:
        display_error_in_streamlit(e, "노선별 지연 분석 데이터를 불러오는 중 오류가 발생했습니다.")

