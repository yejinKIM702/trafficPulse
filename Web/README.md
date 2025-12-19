# Dublin Bus GPS Data를 활용한 AWS·NoSQL 기반 도시 버스 혼잡/지연 모니터링 BI 대시보드

## 📋 프로젝트 개요

Dublin Bus GPS 데이터를 활용하여 도시 버스의 혼잡도와 지연 시간을 분석하고 시각화하는 BI 대시보드 프로젝트입니다.

### 주요 기능

- **ETL 파이프라인**: PySpark를 사용한 데이터 처리 및 집계
- **AWS 통합**: S3 데이터 저장, DynamoDB 집계 데이터 적재
- **BI 대시보드**: Streamlit 기반 인터랙티브 대시보드
  - 노선별 지연 분석
  - 정류장별 혼잡도 분석
  - 시간대별 패턴 비교

## 🏗️ 프로젝트 구조

```
Web/
├── config/              # 설정 파일
│   └── config.py       # 프로젝트 설정
├── etl/                # ETL 파이프라인
│   ├── upload_to_s3.py          # F-01: S3 업로드
│   ├── spark_etl.py             # F-02~F-06: Spark ETL
│   └── dynamodb_loader.py       # F-07: DynamoDB 적재
├── dashboard/          # Streamlit 대시보드
│   ├── app.py                  # 메인 애플리케이션
│   ├── components/             # 공통 컴포넌트
│   │   └── filters.py          # F-08: 필터
│   └── views/                  # 뷰 모듈
│       ├── route_delay.py      # F-09: 노선별 지연
│       ├── stop_congestion.py  # F-10: 정류장 혼잡도
│       └── time_pattern.py     # F-11: 시간대 패턴
├── utils/              # 유틸리티
│   ├── logger.py       # 로깅
│   └── errors.py       # F-13: 에러 핸들링
├── refresh.py          # F-12: 데이터 리프레시
├── requirements.txt    # Python 패키지 의존성
└── README.md          # 프로젝트 문서
```

## 🚀 시작하기

### 1. 환경 설정

#### 필수 요구사항
- Python 3.8 이상
- Java 8 이상 (PySpark 필요)
- AWS 계정 및 자격 증명 (S3/DynamoDB 사용 시)

#### 패키지 설치

```bash
cd Web
pip install -r requirements.txt
```

#### 환경 변수 설정

`.env` 파일을 생성하고 다음 내용을 설정하세요:

```env
# AWS 설정
AWS_REGION=ap-northeast-2
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# S3 버킷 설정
S3_BUCKET_NAME=dublin-bus-data

# DynamoDB 테이블 설정
DYNAMODB_TABLE_NAME=DublinBusSummary

# Spark 설정
SPARK_MASTER=local[*]

# 로그 레벨
LOG_LEVEL=INFO
```

### 2. ETL 파이프라인 실행

#### 로컬 모드 (S3/DynamoDB 미사용)

```bash
# 전체 파이프라인 실행
python -m etl.spark_etl

# 또는 데이터 리프레시 스크립트 사용
python refresh.py
```

#### AWS 모드 (S3/DynamoDB 사용)

```bash
# S3 사용
python refresh.py --s3

# S3 + DynamoDB 사용
python refresh.py --s3 --dynamodb
```

### 3. 대시보드 실행

```bash
streamlit run dashboard/app.py
```

브라우저에서 `http://localhost:8501`로 접속하세요.

## 📊 데이터 처리 흐름

```
1. 데이터 수집
   Dataset/bus/*.csv → S3 Raw (F-01)

2. 데이터 검증 및 변환
   S3 Raw → PySpark 로딩/검증 (F-02)
   → 시간 파생 컬럼 생성 (F-03)
   → Speed 기반 혼잡도/지연 지표 생성
   → 노선별 지연 집계 (F-04)
   → 정류장별 혼잡도 집계 (F-05)

3. 데이터 저장
   → S3 Curated (Parquet) (F-06)
   → DynamoDB 요약 테이블 (F-07)

4. BI 대시보드
   Streamlit 필터 (F-08)
   → 노선별 지연 분석 (F-09)
   → 정류장 혼잡 Top N (F-10)
   → 시간대 패턴 비교 (F-11)
```

## 🔧 주요 기능 설명

### F-01: 원천 데이터 업로드 (S3 Raw)
- 로컬 CSV 파일을 AWS S3 Raw 영역에 업로드

### F-02: Raw 데이터 로딩 및 검증
- PySpark로 CSV 데이터 로딩
- 필수 컬럼 검증 및 데이터 품질 확인

### F-03: 시간 파생 컬럼 생성
- Timestamp에서 date, hour, weekday 등 파생 컬럼 생성

### F-04: 노선별 지연 집계
- 노선 × 날짜 × 시간대 기준으로 지연 지표 집계
- 평균/최대/최소 지연, 지연 발생 비율 계산

### F-05: 정류장별 혼잡도 집계
- 정류장 × 시간대 기준으로 혼잡도 집계
- 혼잡 발생 비율 및 건수 계산

### F-06: Curated 데이터 S3 저장
- 집계 결과를 Parquet 포맷으로 S3 Curated 영역에 저장

### F-07: DynamoDB 요약 테이블 적재
- BI 조회 최적화를 위해 DynamoDB에 요약 지표 저장

### F-08: 대시보드 공통 필터
- 날짜 범위, 노선, 시간대, 운영자 필터 제공

### F-09: 노선별 지연 분석
- 일별/시간대별 평균 지연 그래프
- 노선별 지연 비교

### F-10: 정류장 혼잡 Top N
- 상위 N개 정류장의 혼잡도 랭킹
- 정류장별 상세 정보

### F-11: 시간대 패턴 비교
- 두 시간대(예: 출근/퇴근)의 지연·혼잡 지표 비교

### F-12: 데이터 리프레시 트리거
- CLI 스크립트로 전체 ETL 프로세스 재실행

### F-13: 로그 및 오류 표시
- ETL 및 대시보드 오류 로깅
- 사용자 친화적인 오류 메시지 표시

## 📝 데이터 구조

### 입력 데이터 (CSV)
- `Timestamp`: 측정 시각
- `Longitude`, `Latitude`: GPS 좌표
- `Speed`: 순간 속도 (km/h)
- `Operatorname`: 운영자 식별자
- 기타 네트워크 품질 지표

### 파생 지표
- `Congestion`: Speed 기반 혼잡도 플래그 (0/1)
- `Delay`: Speed 기반 지연 시간 추정 (초)
- `Journey_Pattern_ID`: 노선 ID (Operatorname + CellID)
- `Stop_ID`: 정류장 ID (CellID 기반)

## 🛠️ 기술 스택

- **데이터 처리**: PySpark 3.5+
- **클라우드**: AWS (S3, DynamoDB)
- **대시보드**: Streamlit 1.28+
- **시각화**: Plotly 5.17+
- **언어**: Python 3.8+

## 📌 주의사항

1. **로컬 모드**: 기본적으로 로컬 파일 시스템을 사용합니다. AWS 서비스를 사용하려면 `--s3`, `--dynamodb` 플래그를 사용하세요.

2. **데이터 경로**: ETL 실행 후 `Web/data/curated/` 디렉토리에 Parquet 파일이 생성됩니다.

3. **DynamoDB 테이블**: DynamoDB를 사용하는 경우, 테이블이 자동으로 생성됩니다 (온디맨드 모드).

4. **메모리**: 대용량 데이터 처리 시 Spark 메모리 설정을 조정해야 할 수 있습니다.

## 🔍 문제 해결

### ETL 실행 오류
- Java가 설치되어 있는지 확인
- Spark 경로가 올바르게 설정되어 있는지 확인
- 데이터 파일 경로가 올바른지 확인

### 대시보드 데이터 없음
- ETL 파이프라인을 먼저 실행했는지 확인
- `Web/data/curated/` 디렉토리에 Parquet 파일이 있는지 확인

### AWS 연결 오류
- AWS 자격 증명이 올바르게 설정되어 있는지 확인
- S3 버킷 및 DynamoDB 테이블 권한 확인

## 📄 라이선스

이 프로젝트는 교육/연구 목적으로 작성되었습니다.

## 👤 작성자

Dublin Bus GPS Data 분석 프로젝트

