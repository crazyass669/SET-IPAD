@echo off
REM เรียกจาก Windows Task Scheduler — อัพเดทงบการเงินครบ (DR + ไทย + DR + US/HK
REM ที่ค้นบ่อย + snapshot) หลังฤดูงบไตรมาส (ก.พ./พ.ค./ส.ค./พ.ย.)
cd /d "%~dp0"
"C:\Users\joeki\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe" update_financials.py >> logs\task_update_financials_quarterly.log 2>&1
