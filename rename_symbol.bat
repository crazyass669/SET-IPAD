@echo off
cd /d "%~dp0"
set /p OLDSYM="ชื่อย่อเดิม (เช่น PSTC) แล้วกด Enter: "
set /p NEWSYM="ชื่อย่อใหม่ (เช่น POWER) แล้วกด Enter: "
python rename_symbol.py "%OLDSYM%" "%NEWSYM%"
echo.
pause
