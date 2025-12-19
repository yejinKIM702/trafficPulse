"""
프로젝트 설정 파일
AWS 리소스 및 경로 설정을 관리합니다.
"""
import os
from pathlib import Path

# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent.parent
DATASET_PATH = PROJECT_ROOT.parent / "Dataset" / "bus"

# AWS 설정
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "dublin-bus-data")
S3_RAW_PREFIX = "raw/gps/"
S3_CURATED_PREFIX = "curated/"

# DynamoDB 설정
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "DublinBusSummary")

# Spark 설정
SPARK_APP_NAME = "DublinBusETL"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# 데이터 처리 설정
# Speed 기반 혼잡도 판단 기준 (km/h)
CONGESTION_SPEED_THRESHOLD = 5  # 5km/h 이하는 혼잡으로 간주
# 지연 시간 계산을 위한 기준 속도 (km/h)
NORMAL_SPEED = 30  # 정상 속도 가정 (실제로는 노선별로 다를 수 있음)

# 로그 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = PROJECT_ROOT / "logs"

# Streamlit 설정
STREAMLIT_PORT = 8501
STREAMLIT_HOST = "localhost"

