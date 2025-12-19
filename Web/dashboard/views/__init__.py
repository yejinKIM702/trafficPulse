"""대시보드 뷰 모듈"""
from dashboard.views.overview import render_overview_view
from dashboard.views.route_delay import render_route_delay_view
from dashboard.views.stop_congestion import render_stop_congestion_view
from dashboard.views.time_pattern import render_time_pattern_view

__all__ = [
    'render_overview_view',
    'render_route_delay_view',
    'render_stop_congestion_view',
    'render_time_pattern_view'
]
