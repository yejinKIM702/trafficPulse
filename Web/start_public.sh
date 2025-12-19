#!/bin/bash
# 공개 URL로 Streamlit 대시보드 실행 스크립트

echo "🚀 Streamlit 대시보드를 공개 URL로 실행합니다..."
echo ""
echo "📝 ngrok 계정이 필요합니다:"
echo "   1. https://ngrok.com 에서 무료 계정 생성"
echo "   2. 대시보드에서 인증 토큰 복사"
echo "   3. 다음 명령어 실행: ngrok config add-authtoken YOUR_TOKEN"
echo ""
echo "계속하려면 Enter를 누르세요..."
read

cd /Users/kim-yejin/trafficPulse/Web
source venv/bin/activate

# Streamlit을 백그라운드로 실행
echo "📊 Streamlit 대시보드 시작 중..."
streamlit run dashboard/app.py --server.address=0.0.0.0 --server.port=8501 &
STREAMLIT_PID=$!

sleep 3

# ngrok 터널 시작
echo "🌐 ngrok 터널 시작 중..."
ngrok http 8501

# 종료 시 정리
trap "kill $STREAMLIT_PID 2>/dev/null; pkill ngrok 2>/dev/null; exit" INT TERM

