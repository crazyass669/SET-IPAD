$OLDSYM = Read-Host "ชื่อย่อเดิม (เช่น PSTC)"
$NEWSYM = Read-Host "ชื่อย่อใหม่ (เช่น POWER)"
python "$PSScriptRoot\rename_symbol.py" $OLDSYM $NEWSYM
Write-Host ""
Read-Host "กด Enter เพื่อปิดหน้าต่าง"
