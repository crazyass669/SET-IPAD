@echo off
cd /d "%~dp0"
python update_financials.py %*
echo.
pause
