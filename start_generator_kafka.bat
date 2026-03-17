@echo off
echo Starting SentinelFlow Traffic Generator (Big Data Mode)...
echo Target: Kafka (localhost:9092)
cd c:\Users\yusuf\Desktop\sentinelflow
set PYTHONPATH=%CD%\src
python src\sentinelflow\generator\main.py --kafka-servers localhost:9092 --topic transactions --delay 0.5
pause
