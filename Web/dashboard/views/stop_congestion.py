"""
SCR-03: 구간(세그먼트)별 혼잡 Top N 화면
특정 기간/시간대에 어느 구간(segment_id)이 가장 혼잡한지를 파악하는 화면
"""
import streamlit as st
import pandas as pd
import plotly.express as px
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


def load_stop_congestion_data(use_local: bool = True) -> Optional[pd.DataFrame]:
    """
    정류장별 혼잡도 데이터 로딩
    
    Args:
        use_local: 로컬 데이터 사용 여부
    
    Returns:
        DataFrame 또는 None
    """
    try:
        if use_local:
            # 로컬 Parquet 파일 읽기
            data_path = Path(__file__).parent.parent.parent / "data" / "curated" / "stop_congestion_hourly"
            
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


def render_stop_congestion_view(filter_state: Dict):
    """
    구간별 혼잡 Top N 화면 렌더링 (SCR-03)
    
    Args:
        filter_state: 필터 상태
    """
    st.header("🚏 구간(세그먼트)별 혼잡 Top N")
    
    try:
        # FIL-06: Top N 설정
        col_left, col_right = st.columns([1, 3])
        
        with col_left:
            top_n = st.slider(
                "Top N 선택 (FIL-06)",
                min_value=5,
                max_value=50,
                value=10,
                help="혼잡 상위 N개 구간 표시"
            )
        
        # 로딩 인디케이터
        with st.spinner("데이터를 불러오는 중입니다..."):
            # 데이터 로딩
            df = load_stop_congestion_data(use_local=True)
        
        if df is None or df.empty:
            st.warning("⚠️ 데이터를 불러올 수 없습니다. ETL 파이프라인을 먼저 실행해주세요.")
            st.info("💡 실행 방법: `python -m etl.spark_etl`")
            return
        
        # 필터 적용
        filtered_df = filter_data_by_state(df, filter_state)
        
        if filtered_df.empty:
            st.warning("⚠️ 선택한 필터 조건에 해당하는 데이터가 없습니다.")
            return
        
        # 정류장별 혼잡도 집계
        if 'Stop_ID' in filtered_df.columns and 'congestion_rate' in filtered_df.columns:
            stop_congestion = filtered_df.groupby('Stop_ID').agg({
                'congestion_rate': 'mean',
                'congestion_count': 'sum',
                'total_count': 'sum',
                'avg_speed': 'mean',
                'avg_delay': 'mean'
            }).reset_index()
            
            stop_congestion = stop_congestion.sort_values('congestion_rate', ascending=False).head(top_n)
            
            # KPI 카드
            col1, col2, col3 = st.columns(3)
            
            avg_congestion = stop_congestion['congestion_rate'].mean()
            max_congestion = stop_congestion['congestion_rate'].max()
            total_congestion_events = stop_congestion['congestion_count'].sum()
            
            with col1:
                st.metric("평균 혼잡률", f"{avg_congestion:.1f}%")
            with col2:
                st.metric("최대 혼잡률", f"{max_congestion:.1f}%")
            with col3:
                st.metric("총 혼잡 발생", f"{int(total_congestion_events):,}회")
            
            st.markdown("---")
            
            # CH-05: 구간 혼잡 바 차트
            with col_right:
                st.subheader(f"구간별 혼잡 지수 Top {top_n} (CH-05)")
                
                # 혼잡 지수 계산 (0-100)
                stop_congestion['congestion_score'] = stop_congestion['congestion_rate'].apply(
                    lambda x: min(100, max(0, x))
                )
                
                fig = px.bar(
                    stop_congestion,
                    x='Stop_ID',
                    y='congestion_score',
                    title=f"구간별 혼잡 지수 Top {top_n}",
                    labels={
                        'Stop_ID': '구간 ID (segment_id)',
                        'congestion_score': '혼잡 지수 (0-100)'
                    },
                    color='congestion_score',
                    color_continuous_scale='Reds'
                )
                fig.update_xaxes(tickangle=-45)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # TB-02: 구간 혼잡 Top N 테이블
            st.subheader("구간 혼잡 Top N 테이블 (TB-02)")
            
            # Rank 추가
            stop_congestion['Rank'] = range(1, len(stop_congestion) + 1)
            
            display_df = stop_congestion[['Rank', 'Stop_ID', 'avg_speed', 'congestion_rate', 
                                         'congestion_score', 'total_count']].copy()
            display_df.columns = ['Rank', '구간 ID (segment_id)', '평균 속도 (avg_speed)', 
                                 '혼잡률 (%)', '혼잡 지수 (congestion_score)', '샘플 수 (sample_count)']
            display_df = display_df.round(2)
            
            st.dataframe(display_df, use_container_width=True)
            
            # MAP-01: (옵션) 간단 지도 시각화
            st.subheader("Top N 혼잡 구간 위치 (MAP-01)")
            st.info("💡 지도 시각화는 향후 확장 기능입니다. 구간별 위치 정보가 필요합니다.")
            
            # 정류장별 속도 vs 혼잡도 산점도
            st.subheader("정류장별 속도 vs 혼잡도")
            
            fig = px.scatter(
                stop_congestion,
                x='avg_speed',
                y='congestion_rate',
                size='total_count',
                hover_data=['Stop_ID'],
                title="정류장별 평균 속도 vs 혼잡률",
                labels={'avg_speed': '평균 속도 (km/h)', 'congestion_rate': '혼잡률 (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("필요한 컬럼이 데이터에 없습니다.")
            
    except Exception as e:
        display_error_in_streamlit(e, "정류장 혼잡도 데이터를 불러오는 중 오류가 발생했습니다.")

