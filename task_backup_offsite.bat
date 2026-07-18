@echo off
REM เรียกจาก Windows Task Scheduler — สำรอง financials.db/set_prices.db/ฯลฯ
REM ไปยัง OneDrive ทุกสัปดาห์ ดู PLAN_universe_data_health.txt งาน 4
cd /d "%~dp0"
"C:\Users\joeki\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe" backup_financials_offsite.py "C:\Users\joeki\OneDrive\SET_Dashboard_Backup" >> logs\task_backup_offsite.log 2>&1
