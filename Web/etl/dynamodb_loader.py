"""
F-07: DynamoDB 요약 테이블 적재
BI 조회 최적화를 위해 노선 × 시간대 × 날짜 기준의 요약 지표를 DynamoDB에 upsert합니다.
"""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config import DYNAMODB_TABLE_NAME, AWS_REGION
from utils.logger import logger
from utils.errors import AWSConnectionException, handle_error


def create_dynamodb_table_if_not_exists():
    """
    DynamoDB 테이블이 없으면 생성
    """
    try:
        dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
        
        # 테이블 존재 여부 확인
        try:
            dynamodb.describe_table(TableName=DYNAMODB_TABLE_NAME)
            logger.info(f"DynamoDB 테이블 '{DYNAMODB_TABLE_NAME}'가 이미 존재합니다.")
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # 테이블이 없으면 생성
                logger.info(f"DynamoDB 테이블 '{DYNAMODB_TABLE_NAME}' 생성 중...")
                
                dynamodb.create_table(
                    TableName=DYNAMODB_TABLE_NAME,
                    KeySchema=[
                        {
                            'AttributeName': 'route_id',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'time_bucket',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'route_id',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'time_bucket',
                            'AttributeType': 'S'
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'  # 온디맨드 모드
                )
                
                # 테이블이 활성화될 때까지 대기
                waiter = dynamodb.get_waiter('table_exists')
                waiter.wait(TableName=DYNAMODB_TABLE_NAME)
                
                logger.info(f"DynamoDB 테이블 '{DYNAMODB_TABLE_NAME}' 생성 완료")
            else:
                raise
                
    except NoCredentialsError:
        error = AWSConnectionException("AWS 자격 증명을 찾을 수 없습니다.")
        handle_error(error)
        raise error
    except ClientError as e:
        error = AWSConnectionException(f"DynamoDB 테이블 생성 실패: {str(e)}")
        handle_error(error)
        raise error
    except Exception as e:
        error = Exception(f"예상치 못한 오류: {str(e)}")
        handle_error(error)
        raise error


def convert_spark_row_to_dict(row) -> Dict[str, Any]:
    """
    Spark Row를 딕셔너리로 변환
    
    Args:
        row: Spark Row 객체
    
    Returns:
        딕셔너리
    """
    row_dict = row.asDict()
    
    # None 값을 처리하고 타입 변환
    result = {}
    for key, value in row_dict.items():
        if value is None:
            result[key] = None
        elif isinstance(value, (int, float)):
            result[key] = float(value) if isinstance(value, float) else int(value)
        else:
            result[key] = str(value)
    
    return result


def prepare_dynamodb_item(row_dict: Dict[str, Any], aggregation_type: str = "hourly") -> Dict[str, Any]:
    """
    DynamoDB 아이템 준비
    
    Args:
        row_dict: Spark Row에서 변환된 딕셔너리
        aggregation_type: 집계 타입 ('daily' 또는 'hourly')
    
    Returns:
        DynamoDB 아이템
    """
    # route_id 생성 (PK)
    route_id = row_dict.get('Journey_Pattern_ID', 'unknown')
    
    # time_bucket 생성 (SK)
    # 형식: YYYY-MM-DD 또는 YYYY-MM-DD-HH
    date_str = str(row_dict.get('aggregation_date', ''))
    
    if aggregation_type == "hourly":
        hour = row_dict.get('hour', 0)
        time_bucket = f"{date_str}-{hour:02d}"
    else:
        time_bucket = date_str
    
    # DynamoDB 아이템 구성
    item = {
        'route_id': route_id,
        'time_bucket': time_bucket,
        'operator': row_dict.get('Operatorname', 'unknown'),
        'avg_delay': row_dict.get('avg_delay', 0.0),
        'max_delay': row_dict.get('max_delay', 0.0),
        'min_delay': row_dict.get('min_delay', 0.0),
        'trip_count': row_dict.get('trip_count', 0),
        'congestion_count': row_dict.get('congestion_count', 0),
        'congestion_rate': row_dict.get('congestion_rate', 0.0),
        'updated_at': datetime.utcnow().isoformat()
    }
    
    return item


def load_dataframe_to_dynamodb(df: DataFrame, aggregation_type: str = "hourly", batch_size: int = 25):
    """
    Spark DataFrame을 DynamoDB에 적재
    
    Args:
        df: Spark DataFrame
        aggregation_type: 집계 타입 ('daily' 또는 'hourly')
        batch_size: 배치 크기 (DynamoDB 최대 25개)
    """
    try:
        # DynamoDB 클라이언트 생성
        dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
        
        # 테이블 생성 확인
        create_dynamodb_table_if_not_exists()
        
        # DataFrame을 리스트로 변환
        rows = df.collect()
        total_rows = len(rows)
        
        logger.info(f"DynamoDB 적재 시작: {total_rows}건 ({aggregation_type})")
        
        # 배치로 처리
        success_count = 0
        failed_count = 0
        
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            
            try:
                # 배치 아이템 준비
                batch_items = []
                for row in batch:
                    row_dict = convert_spark_row_to_dict(row)
                    item = prepare_dynamodb_item(row_dict, aggregation_type)
                    
                    # DynamoDB 형식으로 변환
                    dynamodb_item = {
                        'route_id': {'S': item['route_id']},
                        'time_bucket': {'S': item['time_bucket']},
                        'operator': {'S': item['operator']},
                        'avg_delay': {'N': str(item['avg_delay'])},
                        'max_delay': {'N': str(item['max_delay'])},
                        'min_delay': {'N': str(item['min_delay'])},
                        'trip_count': {'N': str(item['trip_count'])},
                        'congestion_count': {'N': str(item['congestion_count'])},
                        'congestion_rate': {'N': str(item['congestion_rate'])},
                        'updated_at': {'S': item['updated_at']}
                    }
                    
                    batch_items.append({
                        'PutRequest': {
                            'Item': dynamodb_item
                        }
                    })
                
                # 배치 쓰기
                dynamodb.batch_write_item(
                    RequestItems={
                        DYNAMODB_TABLE_NAME: batch_items
                    }
                )
                
                success_count += len(batch)
                
                if (i + batch_size) % 100 == 0:
                    logger.info(f"진행 중: {min(i + batch_size, total_rows)}/{total_rows}")
                    
            except Exception as e:
                logger.error(f"배치 적재 실패 (인덱스 {i}-{i+len(batch)}): {str(e)}")
                failed_count += len(batch)
        
        logger.info(f"DynamoDB 적재 완료: 성공 {success_count}건, 실패 {failed_count}건")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "total": total_rows
        }
        
    except NoCredentialsError:
        error = AWSConnectionException("AWS 자격 증명을 찾을 수 없습니다.")
        handle_error(error)
        raise error
    except ClientError as e:
        error = AWSConnectionException(f"DynamoDB 적재 실패: {str(e)}")
        handle_error(error)
        raise error
    except Exception as e:
        error = Exception(f"예상치 못한 오류: {str(e)}")
        handle_error(error)
        raise error


def load_aggregated_data_to_dynamodb(
    route_delay_daily: DataFrame,
    route_delay_by_hour: DataFrame
):
    """
    집계된 데이터를 DynamoDB에 적재
    
    Args:
        route_delay_daily: 노선별 일별 지연 집계
        route_delay_by_hour: 노선별 시간대별 지연 집계
    """
    try:
        logger.info("DynamoDB 적재 프로세스 시작")
        
        # 일별 집계 적재
        logger.info("일별 집계 적재 중...")
        daily_result = load_dataframe_to_dynamodb(route_delay_daily, aggregation_type="daily")
        
        # 시간대별 집계 적재
        logger.info("시간대별 집계 적재 중...")
        hourly_result = load_dataframe_to_dynamodb(route_delay_by_hour, aggregation_type="hourly")
        
        logger.info("DynamoDB 적재 프로세스 완료")
        
        return {
            "daily": daily_result,
            "hourly": hourly_result
        }
        
    except Exception as e:
        logger.error(f"DynamoDB 적재 프로세스 실패: {str(e)}")
        raise


if __name__ == "__main__":
    """CLI 실행 예시"""
    from etl.spark_etl import run_etl_pipeline
    
    try:
        # ETL 파이프라인 실행
        result = run_etl_pipeline(use_local=True, use_s3=False)
        
        # DynamoDB 적재
        load_result = load_aggregated_data_to_dynamodb(
            result['route_delay_daily'],
            result['route_delay_by_hour']
        )
        
        print("\nDynamoDB 적재 완료!")
        print(f"  일별 집계: 성공 {load_result['daily']['success']}건, 실패 {load_result['daily']['failed']}건")
        print(f"  시간대별 집계: 성공 {load_result['hourly']['success']}건, 실패 {load_result['hourly']['failed']}건")
        
    except Exception as e:
        print(f"\n프로세스 실패: {str(e)}")
        import sys
        sys.exit(1)

