# ⚡ 빠른 시작 가이드

## 1분 안에 프로젝트 실행하기

### 필수 확인사항

```bash
# Python 버전 확인 (3.8+ 필요)
python3 --version

# Java 버전 확인 (8+ 필요, PySpark용)
java -version
```

### 실행 순서

```bash
# 1. 프로젝트 디렉토리로 이동
cd /Users/kim-yejin/trafficPulse/Web

# 2. 패키지 설치 (처음 한 번만)
pip install -r requirements.txt

# 3. ETL 파이프라인 실행 (데이터 처리)
python refresh.py

# 4. 대시보드 실행 (새 터미널 창에서)
streamlit run dashboard/app.py
```

### 브라우저 접속

터미널에 표시된 URL로 접속:
- 기본: `http://localhost:8501`

---

## ⚠️ 문제 발생 시

### Java 오류
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

### 데이터 없음 오류
- ETL을 먼저 실행했는지 확인
- `python refresh.py` 실행

### 패키지 오류
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

자세한 내용은 `SETUP_GUIDE.md`를 참고하세요.

