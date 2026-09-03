@echo off
REM เรียกจาก Windows Task Scheduler — สำรอง financials.db/set_prices.db/ฯลฯ
REM ไปยังโฟลเดอร์ mirror ของ Google Drive for Desktop ทุกสัปดาห์ (จ./พฤ. 23:00 + at log on)
REM ดู PLAN_universe_data_health.txt งาน 4  (ย้ายจาก OneDrive -> Google Drive 2026-09-04)
cd /d "%~dp0"
"C:\Users\joeki\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe" backup_financials_offsite.py "C:\SET_Dashboard_backup" --min-age-days 3 >> logs\task_backup_offsite.log 2>&1
