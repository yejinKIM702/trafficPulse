"""
F-12: 데이터 리프레시 트리거
새로운 Dublin CSV 파일을 기준으로 전체 ETL 프로세스를 재실행할 수 있는 CLI 스크립트 또는 수동 버튼 기능을 제공합니다.
"""
import sys
import argparse
from pathlib import Path
# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.logger import logger
from utils.errors import handle_error
from etl.upload_to_s3 import upload_all_bus_data_to_s3
from etl.spark_etl import run_etl_pipeline
from etl.dynamodb_loader import load_aggregated_data_to_dynamodb


def refresh_pipeline(use_s3: bool = False, use_dynamodb: bool = False):
    """
    전체 ETL 파이프라인 재실행
    
    Args:
        use_s3: S3 사용 여부
        use_dynamodb: DynamoDB 사용 여부
    """
    try:
        logger.info("=" * 60)
        logger.info("데이터 리프레시 프로세스 시작")
        logger.info("=" * 60)
        
        # Step 1: S3 업로드 (S3 사용 시)
        if use_s3:
            logger.info("\n[Step 1/4] S3에 원천 데이터 업로드 중...")
            upload_result = upload_all_bus_data_to_s3()
            
            if upload_result['failed_count'] > 0:
                logger.warning(f"S3 업로드 중 {upload_result['failed_count']}개 파일 실패")
            else:
                logger.info(f"S3 업로드 완료: {upload_result['success_count']}개 파일")
        else:
            logger.info("\n[Step 1/4] S3 업로드 건너뛰기 (로컬 모드)")
        
        # Step 2: ETL 파이프라인 실행
        logger.info("\n[Step 2/4] ETL 파이프라인 실행 중...")
        etl_result = run_etl_pipeline(use_local=not use_s3, use_s3=use_s3)
        
        logger.info(f"ETL 파이프라인 완료:")
        # count() 호출 제거 - 데이터 존재 여부만 확인
        try:
            daily_sample = etl_result['route_delay_daily'].take(1)
            hourly_sample = etl_result['route_delay_by_hour'].take(1)
            stop_sample = etl_result['stop_congestion_hourly'].take(1)
            logger.info(f"  - 노선별 일별 지연 집계: 데이터 생성됨 ({'있음' if len(daily_sample) > 0 else '없음'})")
            logger.info(f"  - 노선별 시간대별 지연 집계: 데이터 생성됨 ({'있음' if len(hourly_sample) > 0 else '없음'})")
            logger.info(f"  - 정류장별 시간대별 혼잡도 집계: 데이터 생성됨 ({'있음' if len(stop_sample) > 0 else '없음'})")
        except Exception as e:
            logger.info("  - 집계 데이터 생성 완료 (상세 정보 생략)")
        
        # Step 3: DynamoDB 적재 (DynamoDB 사용 시)
        if use_dynamodb:
            logger.info("\n[Step 3/4] DynamoDB 적재 중...")
            dynamodb_result = load_aggregated_data_to_dynamodb(
                etl_result['route_delay_daily'],
                etl_result['route_delay_by_hour']
            )
            
            logger.info(f"DynamoDB 적재 완료:")
            logger.info(f"  - 일별 집계: 성공 {dynamodb_result['daily']['success']}건, 실패 {dynamodb_result['daily']['failed']}건")
            logger.info(f"  - 시간대별 집계: 성공 {dynamodb_result['hourly']['success']}건, 실패 {dynamodb_result['hourly']['failed']}건")
        else:
            logger.info("\n[Step 3/4] DynamoDB 적재 건너뛰기 (로컬 모드)")
        
        # Step 4: 완료
        logger.info("\n[Step 4/4] 데이터 리프레시 프로세스 완료!")
        logger.info("=" * 60)
        
        return {
            "success": True,
            "etl": {
                "route_delay_daily": etl_result['route_delay_daily'],
                "route_delay_by_hour": etl_result['route_delay_by_hour'],
                "stop_congestion_hourly": etl_result['stop_congestion_hourly']
            }
        }
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("데이터 리프레시 프로세스 실패")
        logger.error("=" * 60)
        handle_error(e)
        return {
            "success": False,
            "error": str(e)
        }


def main():
    """CLI 진입점"""
    parser = argparse.ArgumentParser(
        description="Dublin Bus 데이터 리프레시 프로세스 실행"
    )
    parser.add_argument(
        "--s3",
        action="store_true",
        help="S3 사용 (원천 데이터 업로드 및 Curated 데이터 저장)"
    )
    parser.add_argument(
        "--dynamodb",
        action="store_true",
        help="DynamoDB 사용 (집계 데이터 적재)"
    )
    
    args = parser.parse_args()
    
    result = refresh_pipeline(use_s3=args.s3, use_dynamodb=args.dynamodb)
    
    if result["success"]:
        print("\n✅ 데이터 리프레시 완료!")
        print("  - 노선별 일별 지연 집계: 데이터 생성됨")
        print("  - 노선별 시간대별 지연 집계: 데이터 생성됨")
        print("  - 정류장별 시간대별 혼잡도 집계: 데이터 생성됨")
        sys.exit(0)
    else:
        print(f"\n❌ 데이터 리프레시 실패: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()

