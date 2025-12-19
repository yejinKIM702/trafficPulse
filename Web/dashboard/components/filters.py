"""
F-08: 대시보드 공통 필터 제공
Streamlit 화면 상단에 날짜 범위, 노선 선택, 시간대 선택 등의 공통 필터 UI를 제공합니다.
"""
import streamlit as st
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from utils.logger import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


def get_filter_state() -> Dict:
    """
    필터 상태를 반환
    
    Returns:
        필터 상태 딕셔너리
    """
    if 'filter_state' not in st.session_state:
        st.session_state.filter_state = {
            'date_range': None,
            'selected_routes': [],
            'time_range': None,
            'time_of_day': None,  # FIL-03: 시간대 필터
            'day_of_week': None,  # FIL-04: 요일 필터
            'operator': None
        }
    
    return st.session_state.filter_state


def render_filters(
    available_routes: Optional[List[str]] = None,
    available_operators: Optional[List[str]] = None
) -> Dict:
    """
    필터 UI 렌더링
    
    Args:
        available_routes: 사용 가능한 노선 목록
        available_operators: 사용 가능한 운영자 목록
    
    Returns:
        필터 상태 딕셔너리
    """
    st.sidebar.header("🔍 필터 설정")
    
    filter_state = get_filter_state()
    
    # 운영자 선택
    if available_operators:
        selected_operator = st.sidebar.selectbox(
            "운영자 선택",
            options=["전체"] + available_operators,
            index=0 if filter_state.get('operator') is None else 
                  (available_operators.index(filter_state['operator']) + 1 
                   if filter_state['operator'] in available_operators else 0)
        )
        filter_state['operator'] = None if selected_operator == "전체" else selected_operator
    else:
        filter_state['operator'] = None
    
    # 날짜 범위 선택
    st.sidebar.subheader("📅 날짜 범위")
    
    # 기본 날짜 범위 설정 (최근 7일)
    default_start = date(2017, 11, 30)
    default_end = date(2018, 1, 27)
    
    date_range = st.sidebar.date_input(
        "날짜 범위 선택",
        value=(default_start, default_end),
        min_value=default_start,
        max_value=default_end
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        filter_state['date_range'] = date_range
    elif isinstance(date_range, date):
        filter_state['date_range'] = (date_range, date_range)
    else:
        filter_state['date_range'] = (default_start, default_end)
    
    # 노선 선택
    if available_routes:
        st.sidebar.subheader("🚌 노선 선택")
        selected_routes = st.sidebar.multiselect(
            "노선 선택 (복수 선택 가능)",
            options=available_routes,
            default=filter_state.get('selected_routes', [])
        )
        filter_state['selected_routes'] = selected_routes
    else:
        filter_state['selected_routes'] = []
    
    # 시간대 선택 (FIL-03: 드롭다운/멀티 선택)
    st.sidebar.subheader("⏰ 시간대 선택 (FIL-03)")
    
    time_of_day_options = ["전체", "00-06", "06-10", "10-16", "16-20", "20-24"]
    
    selected_time_of_day = st.sidebar.multiselect(
        "시간대 선택 (복수 선택 가능)",
        options=time_of_day_options,
        default=filter_state.get('time_of_day', ["전체"]) if filter_state.get('time_of_day') else ["전체"]
    )
    
    if "전체" in selected_time_of_day or len(selected_time_of_day) == 0:
        filter_state['time_of_day'] = None
        filter_state['time_range'] = (0, 23)  # 전체 시간대
    else:
        filter_state['time_of_day'] = selected_time_of_day
        # 선택된 시간대를 time_range로 변환
        min_hour = 0
        max_hour = 23
        for time_range_str in selected_time_of_day:
            if "-" in time_range_str:
                start, end = map(int, time_range_str.split("-"))
                min_hour = min(min_hour, start)
                max_hour = max(max_hour, end)
        filter_state['time_range'] = (min_hour, max_hour)
    
    # 요일 선택 (FIL-04: 체크박스/멀티)
    st.sidebar.subheader("📆 요일 선택 (FIL-04)")
    
    day_of_week_options = ["전체", "월", "화", "수", "목", "금", "토", "일"]
    day_mapping = {"월": 1, "화": 2, "수": 3, "목": 4, "금": 5, "토": 6, "일": 7}
    
    selected_days = st.sidebar.multiselect(
        "요일 선택 (복수 선택 가능)",
        options=day_of_week_options,
        default=filter_state.get('day_of_week', ["전체"]) if filter_state.get('day_of_week') else ["전체"]
    )
    
    if "전체" in selected_days or len(selected_days) == 0:
        filter_state['day_of_week'] = None
    else:
        filter_state['day_of_week'] = [day_mapping[d] for d in selected_days if d in day_mapping]
    
    # 필터 상태 저장
    st.session_state.filter_state = filter_state
    
    # 필터 요약 표시
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 필터 요약")
    
    if filter_state['date_range']:
        st.sidebar.write(f"**날짜**: {filter_state['date_range'][0]} ~ {filter_state['date_range'][1]}")
    
    if filter_state['selected_routes']:
        st.sidebar.write(f"**노선**: {len(filter_state['selected_routes'])}개 선택")
    else:
        st.sidebar.write("**노선**: 전체")
    
    if filter_state.get('time_of_day'):
        st.sidebar.write(f"**시간대**: {', '.join(filter_state['time_of_day'])}")
    elif filter_state.get('time_range'):
        st.sidebar.write(f"**시간**: {filter_state['time_range'][0]:02d}:00 ~ {filter_state['time_range'][1]:02d}:00")
    
    if filter_state.get('day_of_week'):
        day_names = {1: "월", 2: "화", 3: "수", 4: "목", 5: "금", 6: "토", 7: "일"}
        selected_day_names = [day_names[d] for d in filter_state['day_of_week'] if d in day_names]
        st.sidebar.write(f"**요일**: {', '.join(selected_day_names)}")
    else:
        st.sidebar.write("**요일**: 전체")
    
    if filter_state['operator']:
        st.sidebar.write(f"**운영자**: {filter_state['operator']}")
    
    return filter_state


def clear_filters():
    """필터 초기화"""
    if 'filter_state' in st.session_state:
        st.session_state.filter_state = {
            'date_range': None,
            'selected_routes': [],
            'time_range': None,
            'time_of_day': None,
            'day_of_week': None,
            'operator': None
        }

