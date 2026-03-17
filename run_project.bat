@echo off
echo Starting SentinelFlow System...

:: Start Backend (FastAPI)
echo Launching Backend...
start "SentinelFlow Backend" cmd /k "set PYTHONPATH=%CD%\src && python -m sentinelflow.api.app"

:: Start Frontend (Next.js)
echo Launching Frontend...
cd sentinelflow-web
start "SentinelFlow Frontend" cmd /k "npm run dev"

echo.
echo ===================================================
echo Project is running!
echo Backend: http://localhost:8000
echo Frontend: http://localhost:3000
echo ===================================================
pause
