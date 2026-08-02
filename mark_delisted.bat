@echo off
cd /d "%~dp0"
set /p SYM="ชื่อย่อหุ้นที่เพิกถอน/ถูกควบรวมหายไป (เช่น BPP) แล้วกด Enter: "
set /p REASON="เหตุผล (เช่น ควบรวมเข้า BANPU 31 ก.ค. 2569) แล้วกด Enter: "
python mark_delisted.py "%SYM%" "%REASON%"
echo.
pause
