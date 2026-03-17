@echo off
echo Starting SentinelFlow Traffic Generator...
cd c:\Users\yusuf\Desktop\sentinelflow
set PYTHONPATH=%CD%\src
python src\sentinelflow\generator\http_gen.py
pause
