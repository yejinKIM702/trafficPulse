"""
로깅 유틸리티 모듈
"""
import sys
from pathlib import Path
from loguru import logger
from config.config import LOG_DIR, LOG_LEVEL

# 로그 디렉토리 생성
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 기본 로거 설정 제거
logger.remove()

# 콘솔 로거 추가
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=LOG_LEVEL,
    colorize=True
)

# 파일 로거 추가
logger.add(
    LOG_DIR / "app_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level=LOG_LEVEL,
    rotation="00:00",
    retention="30 days",
    compression="zip"
)

__all__ = ["logger"]

