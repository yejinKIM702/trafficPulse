"""
F-01: 원천 데이터 업로드 (S3 Raw)
로컬 Dublin Bus GPS CSV 파일을 AWS S3 Raw 영역에 업로드합니다.
"""
import boto3
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config import S3_BUCKET_NAME, S3_RAW_PREFIX, DATASET_PATH, AWS_REGION
from utils.logger import logger
from utils.errors import AWSConnectionException, handle_error


def upload_file_to_s3(local_file_path: Path, s3_key: str) -> bool:
    """
    단일 파일을 S3에 업로드
    
    Args:
        local_file_path: 로컬 파일 경로
        s3_key: S3 객체 키 (경로)
    
    Returns:
        업로드 성공 여부
    """
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        logger.info(f"Uploading {local_file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")
        s3_client.upload_file(str(local_file_path), S3_BUCKET_NAME, s3_key)
        
        logger.info(f"Successfully uploaded {local_file_path.name}")
        return True
        
    except NoCredentialsError:
        error = AWSConnectionException("AWS 자격 증명을 찾을 수 없습니다.")
        handle_error(error)
        raise error
    except ClientError as e:
        error = AWSConnectionException(f"S3 업로드 실패: {str(e)}")
        handle_error(error)
        raise error
    except Exception as e:
        error = Exception(f"예상치 못한 오류: {str(e)}")
        handle_error(error)
        raise error


def upload_all_bus_data_to_s3() -> dict:
    """
    Dataset/bus 폴더의 모든 CSV 파일을 S3 Raw에 업로드
    
    Returns:
        업로드 결과 딕셔너리 (성공/실패 파일 목록)
    """
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"데이터셋 경로를 찾을 수 없습니다: {DATASET_PATH}")
    
    csv_files = list(DATASET_PATH.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"{DATASET_PATH}에 CSV 파일이 없습니다.")
        return {"success": [], "failed": []}
    
    logger.info(f"총 {len(csv_files)}개의 CSV 파일을 업로드합니다.")
    
    success_files = []
    failed_files = []
    
    for csv_file in csv_files:
        try:
            # S3 키 생성: raw/gps/{파일명}
            s3_key = f"{S3_RAW_PREFIX}{csv_file.name}"
            
            if upload_file_to_s3(csv_file, s3_key):
                success_files.append(csv_file.name)
            else:
                failed_files.append(csv_file.name)
                
        except Exception as e:
            logger.error(f"파일 업로드 실패 {csv_file.name}: {str(e)}")
            failed_files.append(csv_file.name)
    
    result = {
        "success": success_files,
        "failed": failed_files,
        "total": len(csv_files),
        "success_count": len(success_files),
        "failed_count": len(failed_files)
    }
    
    logger.info(f"업로드 완료: 성공 {len(success_files)}개, 실패 {len(failed_files)}개")
    
    return result


if __name__ == "__main__":
    """CLI 실행 예시"""
    try:
        result = upload_all_bus_data_to_s3()
        print(f"\n업로드 결과:")
        print(f"  총 파일: {result['total']}개")
        print(f"  성공: {result['success_count']}개")
        print(f"  실패: {result['failed_count']}개")
        
        if result['failed']:
            print(f"\n실패한 파일:")
            for file in result['failed']:
                print(f"  - {file}")
                
    except Exception as e:
        logger.error(f"업로드 프로세스 실패: {str(e)}")
        raise

