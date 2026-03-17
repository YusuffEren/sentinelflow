@echo off
echo Starting SentinelFlow FULL SYSTEM (Big Data Mode)...

:: 1. Start Backend (API + WebSocket)
echo Launching Backend...
start "SentinelFlow Backend" cmd /k "set PYTHONPATH=%CD%\src && python -m sentinelflow.api.app"

:: 2. Start Kafka Ingestor (Bridge)
echo Launching Ingestor...
start "SentinelFlow Kafka Ingestor" cmd /k "set PYTHONPATH=%CD%\src && python src\sentinelflow\ingestor\kafka_consumer.py --kafka-servers localhost:9092 --topic transactions"

:: 3. Start Kafka Generator (Producer)
echo Launching Generator...
start "SentinelFlow Generator" cmd /k "set PYTHONPATH=%CD%\src && python src\sentinelflow\generator\main.py --kafka-servers localhost:9092 --topic transactions --delay 0.1"

:: 4. Start Frontend (Next.js)
echo Launching Frontend...
cd sentinelflow-web
start "SentinelFlow Dashboard" cmd /k "npm run dev"

echo ===================================================
echo SYSTEM LAUNCHED! 🚀
echo Backend: http://localhost:8000
echo Frontend: http://localhost:3000
echo Data Flow: Generator -> Kafka -> Ingestor -> API
echo ===================================================
pause
