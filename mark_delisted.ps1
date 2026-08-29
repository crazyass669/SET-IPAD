$SYM = Read-Host "ชื่อย่อหุ้นที่เพิกถอน/ถูกควบรวมหายไป (เช่น BPP)"
$REASON = Read-Host "เหตุผล (เช่น ควบรวมเข้า BANPU 31 ก.ค. 2569)"
python "$PSScriptRoot\mark_delisted.py" $SYM $REASON
Write-Host ""
Read-Host "กด Enter เพื่อปิดหน้าต่าง"
