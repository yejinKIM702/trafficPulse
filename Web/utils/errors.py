"""
에러 핸들링 유틸리티 모듈
"""
from typing import Optional
import streamlit as st
from utils.logger import logger


class ETLException(Exception):
    """ETL 프로세스 관련 예외"""
    pass


class DataValidationException(ETLException):
    """데이터 검증 실패 예외"""
    pass


class AWSConnectionException(ETLException):
    """AWS 연결 실패 예외"""
    pass


def handle_error(error: Exception, user_message: Optional[str] = None):
    """
    에러를 로깅하고 사용자 친화적인 메시지를 반환
    
    Args:
        error: 발생한 예외
        user_message: 사용자에게 표시할 메시지 (None이면 기본 메시지 사용)
    """
    # 에러 로깅
    logger.error(f"Error occurred: {type(error).__name__}: {str(error)}", exc_info=True)
    
    # 사용자 메시지 결정
    if user_message is None:
        if isinstance(error, DataValidationException):
            user_message = "데이터 검증 중 오류가 발생했습니다. 데이터 형식을 확인해주세요."
        elif isinstance(error, AWSConnectionException):
            user_message = "AWS 서비스 연결에 실패했습니다. 자격 증명을 확인해주세요."
        else:
            user_message = "처리 중 오류가 발생했습니다. 로그를 확인해주세요."
    
    return user_message


def display_error_in_streamlit(error: Exception, user_message: Optional[str] = None):
    """
    Streamlit에서 에러를 표시
    
    Args:
        error: 발생한 예외
        user_message: 사용자에게 표시할 메시지
    """
    message = handle_error(error, user_message)
    st.error(f"❌ {message}")
    st.exception(error)

