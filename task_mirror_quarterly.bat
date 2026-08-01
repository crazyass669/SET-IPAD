@echo off
REM เรียกจาก Windows Task Scheduler — refresh mirror US/HK ทั้งตลาด (force)
REM ไตรมาสละครั้ง หลังฤดูงบ ดู PLAN_universe_data_health.txt ส่วนที่ 3
cd /d "%~dp0"
set PY="C:\Users\joeki\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe"
%PY% mirror_finnomena.py force >> logs\task_mirror_quarterly.log 2>&1
%PY% build_snapshot.py >> logs\task_mirror_quarterly.log 2>&1
