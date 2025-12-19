"""
Dublin Bus GPS Data를 활용한 AWS·NoSQL 기반 도시 버스 혼잡/지연 모니터링 BI 대시보드
Streamlit 메인 애플리케이션
"""
import streamlit as st
from pathlib import Path
import sys

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from dashboard.components.filters import render_filters
    from dashboard.views.overview import render_overview_view
    from dashboard.views.route_delay import render_route_delay_view
    from dashboard.views.stop_congestion import render_stop_congestion_view
    from dashboard.views.time_pattern import render_time_pattern_view
    from utils.logger import logger
    from utils.errors import display_error_in_streamlit
except ImportError:
    # 상대 경로로 시도
    from components.filters import render_filters
    from views.overview import render_overview_view
    from views.route_delay import render_route_delay_view
    from views.stop_congestion import render_stop_congestion_view
    from views.time_pattern import render_time_pattern_view
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.logger import logger
    from utils.errors import display_error_in_streamlit


# 페이지 설정
st.set_page_config(
    page_title="Dublin Bus 혼잡/지연 모니터링",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 메인 타이틀
st.title("🚌 도시 버스 혼잡/지연 모니터링 BI 대시보드")
st.markdown("**시스템명**: Dublin Bus 혼잡/지연 모니터링 BI 대시보드")
st.markdown("---")

# 사이드바 필터 렌더링
try:
    # 사용 가능한 노선 및 운영자 목록 (실제로는 데이터에서 동적으로 가져와야 함)
    available_routes = None  # TODO: 데이터에서 동적으로 로드
    available_operators = ["A", "B"]  # 데이터에서 확인된 운영자
    
    filter_state = render_filters(
        available_routes=available_routes,
        available_operators=available_operators
    )
    
except Exception as e:
    logger.error(f"필터 렌더링 오류: {str(e)}")
    filter_state = {
        'date_range': None,
        'selected_routes': [],
        'time_range': None,
        'time_of_day': None,
        'day_of_week': None,
        'operator': None
    }

# 메인 컨텐츠 영역 - 화면 정의서에 따른 탭 구성
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 요약 대시보드 (SCR-01)",
    "🚌 노선별 분석",
    "🚏 구간별 혼잡 Top N (SCR-03)",
    "⏰ 시간대 패턴 비교 (SCR-04)"
])

with tab1:
    try:
        render_overview_view(filter_state)
    except Exception as e:
        display_error_in_streamlit(e)

with tab2:
    try:
        render_route_delay_view(filter_state)
    except Exception as e:
        display_error_in_streamlit(e)

with tab3:
    try:
        render_stop_congestion_view(filter_state)
    except Exception as e:
        display_error_in_streamlit(e)

with tab4:
    try:
        render_time_pattern_view(filter_state)
    except Exception as e:
        display_error_in_streamlit(e)

# 푸터
st.markdown("---")
st.markdown("### ℹ️ 정보")
st.info("""
**프로젝트**: Dublin Bus GPS Data를 활용한 AWS·NoSQL 기반 도시 버스 혼잡/지연 모니터링 BI 대시보드

**기술 스택**: AWS S3, PySpark, DynamoDB, Streamlit

**데이터 출처**: Dublin Bus GPS Data

**업데이트**: ETL 파이프라인을 실행하여 최신 데이터를 반영할 수 있습니다.
""")

