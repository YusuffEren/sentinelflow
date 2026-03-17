@echo off
echo Starting SentinelFlow Kafka Ingestor...
cd c:\Users\yusuf\Desktop\sentinelflow
set PYTHONPATH=%CD%\src
python src\sentinelflow\ingestor\kafka_consumer.py
pause
