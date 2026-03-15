@echo off
cd /d "%~dp0site"
if "%PORT%"=="" set PORT=8080
echo Starting AI Exposure viewer at http://localhost:%PORT%
start "" "http://localhost:%PORT%"
python -m http.server %PORT%
pause
