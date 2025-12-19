# 🚀 프로젝트 실행 가이드

이 문서는 Dublin Bus GPS Data 분석 프로젝트를 실행하기 위한 단계별 가이드입니다.

## 📋 사전 요구사항 확인

### 1. Python 설치 확인

터미널에서 다음 명령어로 Python 버전을 확인하세요:

```bash
python3 --version
# Python 3.8 이상이어야 합니다
```

Python이 설치되어 있지 않다면:
- macOS: `brew install python3`
- 또는 [python.org](https://www.python.org/downloads/)에서 다운로드

### 2. Java 설치 확인 (PySpark 필요)

PySpark는 Java가 필요합니다. 다음 명령어로 확인:

```bash
java -version
# Java 8 이상이어야 합니다
```

Java가 설치되어 있지 않다면:
- macOS: `brew install openjdk@11`
- 또는 [Oracle Java](https://www.oracle.com/java/technologies/downloads/)에서 다운로드

### 3. 프로젝트 디렉토리 확인

```bash
cd /Users/kim-yejin/trafficPulse/Web
pwd
# /Users/kim-yejin/trafficPulse/Web 이어야 합니다
```

---

## 🔧 1단계: 패키지 설치

### 가상 환경 생성 (권장)

```bash
cd /Users/kim-yejin/trafficPulse/Web

# 가상 환경 생성
python3 -m venv venv

# 가상 환경 활성화
source venv/bin/activate  # macOS/Linux
# 또는
# venv\Scripts\activate  # Windows
```

### 패키지 설치

```bash
# requirements.txt의 패키지 설치
pip install -r requirements.txt
```

설치가 완료되면 다음 패키지들이 설치됩니다:
- PySpark (데이터 처리)
- Streamlit (대시보드)
- boto3 (AWS SDK)
- pandas, numpy (데이터 처리)
- plotly (시각화)
- 기타 의존성

**예상 설치 시간**: 5-10분 (인터넷 속도에 따라 다름)

---

## ⚙️ 2단계: 환경 설정 (선택사항)

### 로컬 모드 (기본값, AWS 불필요)

**로컬 모드에서는 추가 설정이 필요 없습니다!** 바로 실행할 수 있습니다.

### AWS 모드 (S3/DynamoDB 사용 시)

AWS를 사용하려면 환경 변수를 설정해야 합니다.

#### 방법 1: 환경 변수로 설정

```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="ap-northeast-2"
export S3_BUCKET_NAME="dublin-bus-data"
export DYNAMODB_TABLE_NAME="DublinBusSummary"
```

#### 방법 2: .env 파일 생성 (권장)

```bash
cd /Users/kim-yejin/trafficPulse/Web
cat > .env << EOF
AWS_REGION=ap-northeast-2
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
S3_BUCKET_NAME=dublin-bus-data
DYNAMODB_TABLE_NAME=DublinBusSummary
SPARK_MASTER=local[*]
LOG_LEVEL=INFO
EOF
```

**참고**: `.env` 파일은 Git에 커밋하지 마세요 (이미 .gitignore에 포함됨)

---

## 🏃 3단계: ETL 파이프라인 실행

### 로컬 모드로 실행 (권장, 처음 시작 시)

```bash
cd /Users/kim-yejin/trafficPulse/Web

# 전체 ETL 파이프라인 실행
python refresh.py
```

또는 직접 실행:

```bash
python -m etl.spark_etl
```

**예상 실행 시간**: 2-5분 (데이터 크기에 따라 다름)

### 실행 결과 확인

ETL이 성공적으로 완료되면:
- `Web/data/curated/` 디렉토리에 Parquet 파일이 생성됩니다
- 콘솔에 집계 결과가 표시됩니다

```bash
# 생성된 데이터 확인
ls -lh data/curated/
# route_delay_daily/
# route_delay_by_hour/
# stop_congestion_hourly/
```

### AWS 모드로 실행 (선택사항)

```bash
# S3 사용
python refresh.py --s3

# S3 + DynamoDB 사용
python refresh.py --s3 --dynamodb
```

---

## 📊 4단계: 대시보드 실행

### Streamlit 대시보드 시작

```bash
cd /Users/kim-yejin/trafficPulse/Web

# 대시보드 실행
streamlit run dashboard/app.py
```

### 브라우저 접속

터미널에 다음과 같은 메시지가 표시됩니다:

```
You can now view your Streamlit app in your browser.

Local URL: http://localhost:8501
Network URL: http://192.168.x.x:8501
```

브라우저에서 `http://localhost:8501`로 접속하세요.

### 대시보드 사용법

1. **필터 설정** (왼쪽 사이드바)
   - 날짜 범위 선택
   - 노선 선택
   - 시간대 선택
   - 운영자 선택

2. **탭 전환**
   - **노선별 지연 분석**: 일별/시간대별 지연 그래프
   - **정류장 혼잡도**: 상위 N개 정류장 혼잡도
   - **시간대 패턴 비교**: 출근/퇴근 시간대 비교

3. **데이터 새로고침**
   - ETL을 다시 실행한 후 브라우저를 새로고침하세요

---

## 🔍 문제 해결

### 문제 1: Java 오류

**증상**: `JAVA_HOME is not set` 또는 Java 관련 오류

**해결**:
```bash
# Java 경로 확인
/usr/libexec/java_home

# JAVA_HOME 설정 (macOS)
export JAVA_HOME=$(/usr/libexec/java_home)
echo 'export JAVA_HOME=$(/usr/libexec/java_home)' >> ~/.zshrc
source ~/.zshrc
```

### 문제 2: PySpark 메모리 부족

**증상**: `OutOfMemoryError` 또는 Spark 작업 실패

**해결**: `config/config.py`에서 Spark 메모리 설정 조정

```python
# spark_etl.py의 create_spark_session() 함수 수정
.config("spark.driver.memory", "2g")
.config("spark.executor.memory", "2g")
```

### 문제 3: 데이터를 찾을 수 없음

**증상**: `FileNotFoundError` 또는 데이터 경로 오류

**해결**:
```bash
# 데이터 경로 확인
ls -la ../Dataset/bus/*.csv

# 경로가 맞는지 확인
python -c "from config.config import DATASET_PATH; print(DATASET_PATH)"
```

### 문제 4: 대시보드에 데이터가 표시되지 않음

**증상**: 대시보드에서 "데이터를 불러올 수 없습니다" 메시지

**해결**:
1. ETL 파이프라인을 먼저 실행했는지 확인
2. `Web/data/curated/` 디렉토리에 Parquet 파일이 있는지 확인
3. ETL을 다시 실행:
   ```bash
   python refresh.py
   ```

### 문제 5: 패키지 설치 오류

**증상**: `pip install` 실패

**해결**:
```bash
# pip 업그레이드
pip install --upgrade pip

# 개별 패키지 설치 시도
pip install pyspark streamlit boto3 pandas numpy plotly pyarrow loguru
```

### 문제 6: AWS 연결 오류

**증상**: AWS 관련 오류 (로컬 모드 사용 시 무시 가능)

**해결**: 
- 로컬 모드로 실행 (`python refresh.py` - 플래그 없이)
- AWS를 사용하지 않으면 환경 변수 설정 불필요

---

## 📝 실행 체크리스트

실행 전 확인사항:

- [ ] Python 3.8+ 설치됨
- [ ] Java 8+ 설치됨
- [ ] `requirements.txt` 패키지 설치 완료
- [ ] `Dataset/bus/` 폴더에 CSV 파일 존재
- [ ] ETL 파이프라인 실행 완료
- [ ] `data/curated/` 디렉토리에 Parquet 파일 생성됨
- [ ] Streamlit 대시보드 실행 가능

---

## 🎯 빠른 시작 (요약)

```bash
# 1. 디렉토리 이동
cd /Users/kim-yejin/trafficPulse/Web

# 2. 패키지 설치
pip install -r requirements.txt

# 3. ETL 실행
python refresh.py

# 4. 대시보드 실행
streamlit run dashboard/app.py
```

---

## 💡 추가 팁

### 성능 최적화

- 대용량 데이터 처리 시 Spark 메모리 설정 조정
- 로컬 모드에서는 작은 데이터셋으로 먼저 테스트

### 로그 확인

```bash
# 로그 파일 확인
tail -f logs/app_*.log
```

### 데이터 새로고침

새로운 데이터를 추가한 후:
```bash
python refresh.py
```

그 다음 브라우저에서 대시보드를 새로고침하세요.

---

## 🆘 도움이 필요하신가요?

문제가 계속되면:
1. 로그 파일 확인 (`Web/logs/` 디렉토리)
2. 에러 메시지 전체 내용 확인
3. Python 버전 및 패키지 버전 확인:
   ```bash
   python --version
   pip list
   ```

