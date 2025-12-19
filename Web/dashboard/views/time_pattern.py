"""
SCR-04: 시간대 패턴 비교 화면
두 개의 시간대(예: 출근 vs 퇴근) 간 혼잡/지연 패턴을 비교하는 화면
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from typing import Dict, Optional, Tuple
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


def load_time_pattern_data(use_local: bool = True) -> Optional[pd.DataFrame]:
    """
    시간대 패턴 데이터 로딩
    
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
    
    # 노선 필터
    if filter_state.get('selected_routes') and len(filter_state['selected_routes']) > 0:
        if 'Journey_Pattern_ID' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Journey_Pattern_ID'].isin(filter_state['selected_routes'])]
    
    # 운영자 필터
    if filter_state.get('operator'):
        if 'Operatorname' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Operatorname'] == filter_state['operator']]
    
    return filtered_df


def render_time_pattern_view(filter_state: Dict):
    """
    시간대 패턴 비교 화면 렌더링 (SCR-04)
    
    Args:
        filter_state: 필터 상태
    """
    st.header("시간대 패턴 비교")
    
    try:
        # FIL-07, FIL-08: 시간대 A/B 선택 필터
        col1, col2 = st.columns(2)
        
        time_of_day_options = ["06-10", "07-09", "10-16", "16-20", "17-19", "20-24", "00-06"]
        
        with col1:
            st.subheader("시간대 A (FIL-07)")
            time_a = st.selectbox(
                "시간대 A 선택",
                options=time_of_day_options,
                index=1,  # 기본값: 07-09 (출근 시간)
                key="time_a"
            )
            time_a_start, time_a_end = map(int, time_a.split("-"))
            time_a_label = st.text_input("라벨", "출근 시간", key="time_a_label")
        
        with col2:
            st.subheader("시간대 B (FIL-08)")
            time_b = st.selectbox(
                "시간대 B 선택",
                options=time_of_day_options,
                index=4,  # 기본값: 17-19 (퇴근 시간)
                key="time_b"
            )
            time_b_start, time_b_end = map(int, time_b.split("-"))
            time_b_label = st.text_input("라벨", "퇴근 시간", key="time_b_label")
        
        # 로딩 인디케이터
        with st.spinner("데이터를 불러오는 중입니다..."):
            # 데이터 로딩
            df = load_time_pattern_data(use_local=True)
        
        if df is None or df.empty:
            st.warning("⚠️ 데이터를 불러올 수 없습니다. ETL 파이프라인을 먼저 실행해주세요.")
            st.info("💡 실행 방법: `python -m etl.spark_etl`")
            return
        
        # 필터 적용
        filtered_df = filter_data_by_state(df, filter_state)
        
        if filtered_df.empty:
            st.warning("⚠️ 선택한 필터 조건에 해당하는 데이터가 없습니다.")
            return
        
        # 시간대별 데이터 필터링
        if 'hour' not in filtered_df.columns:
            st.error("시간대 정보가 데이터에 없습니다.")
            return
        
        time_a_data = filtered_df[
            (filtered_df['hour'] >= time_a_start) & 
            (filtered_df['hour'] <= time_a_end)
        ]
        
        time_b_data = filtered_df[
            (filtered_df['hour'] >= time_b_start) & 
            (filtered_df['hour'] <= time_b_end)
        ]
        
        if time_a_data.empty or time_b_data.empty:
            st.warning("⚠️ 선택한 시간대에 데이터가 없습니다.")
            return
        
        # 시간대 A/B 요약 KPI
        st.subheader("시간대 A/B 요약 KPI")
        
        col_a1, col_a2, col_b1, col_b2 = st.columns(4)
        
        # KPI-A-01, KPI-A-02: 시간대 A 평균 혼잡 지수, 평균 속도
        time_a_congestion = time_a_data['congestion_rate'].mean() if 'congestion_rate' in time_a_data.columns else 0
        time_a_congestion_score = min(100, max(0, time_a_congestion))
        time_a_speed = 30.0  # 기본값, 실제로는 Speed 컬럼에서 계산
        
        # KPI-B-01, KPI-B-02: 시간대 B 평균 혼잡 지수, 평균 속도
        time_b_congestion = time_b_data['congestion_rate'].mean() if 'congestion_rate' in time_b_data.columns else 0
        time_b_congestion_score = min(100, max(0, time_b_congestion))
        time_b_speed = 30.0  # 기본값
        
        with col_a1:
            st.metric(
                f"{time_a_label} 평균 혼잡 지수 (KPI-A-01)",
                f"{time_a_congestion_score:.1f}"
            )
        with col_a2:
            st.metric(
                f"{time_a_label} 평균 속도 (KPI-A-02)",
                f"{time_a_speed:.1f} km/h"
            )
        with col_b1:
            st.metric(
                f"{time_b_label} 평균 혼잡 지수 (KPI-B-01)",
                f"{time_b_congestion_score:.1f}",
                delta=f"{time_b_congestion_score - time_a_congestion_score:.1f}"
            )
        with col_b2:
            st.metric(
                f"{time_b_label} 평균 속도 (KPI-B-02)",
                f"{time_b_speed:.1f} km/h",
                delta=f"{time_b_speed - time_a_speed:.1f}"
            )
        
        st.markdown("---")
        
        # CH-06: 노선별 시간대 A/B 혼잡 지수 비교 (그룹 바 차트)
        st.subheader("노선별 시간대 A/B 혼잡 지수 비교 (CH-06)")
        
        if 'Journey_Pattern_ID' in filtered_df.columns and 'congestion_rate' in filtered_df.columns:
            # 노선별 집계
            route_a = time_a_data.groupby('Journey_Pattern_ID')['congestion_rate'].mean().reset_index()
            route_a['congestion_score'] = route_a['congestion_rate'].apply(lambda x: min(100, max(0, x)))
            route_a['time_period'] = time_a_label
            
            route_b = time_b_data.groupby('Journey_Pattern_ID')['congestion_rate'].mean().reset_index()
            route_b['congestion_score'] = route_b['congestion_rate'].apply(lambda x: min(100, max(0, x)))
            route_b['time_period'] = time_b_label
            
            # 공통 노선만 선택
            common_routes = set(route_a['Journey_Pattern_ID']) & set(route_b['Journey_Pattern_ID'])
            route_a = route_a[route_a['Journey_Pattern_ID'].isin(common_routes)]
            route_b = route_b[route_b['Journey_Pattern_ID'].isin(common_routes)]
            
            # 그룹 바 차트
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name=time_a_label,
                x=route_a['Journey_Pattern_ID'],
                y=route_a['congestion_score'],
                marker_color='#1f77b4'
            ))
            
            fig.add_trace(go.Bar(
                name=time_b_label,
                x=route_b['Journey_Pattern_ID'],
                y=route_b['congestion_score'],
                marker_color='#ff7f0e'
            ))
            
            fig.update_layout(
                title="노선별 시간대 A/B 혼잡 지수 비교",
                xaxis_title="노선/운영자 (Journey_Pattern_ID)",
                yaxis_title="혼잡 지수 (congestion_score)",
                barmode='group',
                height=500,
                xaxis={'tickangle': -45}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # 비교 테이블
            comparison_df = pd.merge(
                route_a[['Journey_Pattern_ID', 'congestion_score']],
                route_b[['Journey_Pattern_ID', 'congestion_score']],
                on='Journey_Pattern_ID',
                suffixes=(f'_{time_a_label}', f'_{time_b_label}')
            )
            comparison_df.columns = ['노선 ID', f'{time_a_label} 혼잡 지수', f'{time_b_label} 혼잡 지수']
            comparison_df = comparison_df.round(2)
            
            with st.expander("노선별 상세 비교 데이터"):
                st.dataframe(comparison_df, use_container_width=True)
        
            
    except Exception as e:
        display_error_in_streamlit(e, "시간대 패턴 데이터를 불러오는 중 오류가 발생했습니다.")

