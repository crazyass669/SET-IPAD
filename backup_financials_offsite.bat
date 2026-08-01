@echo off
cd /d "%~dp0"
set /p DEST="วางโฟลเดอร์ปลายทาง (เช่น D:\Backups\SET_Dashboard) แล้วกด Enter: "
python backup_financials_offsite.py "%DEST%"
echo.
pause
