# สร้างชอร์ตคัต "SET Dashboard" บน Desktop ของเครื่องนี้ ให้ชี้ไปที่ start.bat
# ในโฟลเดอร์นี้ พร้อมไอคอนกราฟ (static/icons/app.ico) — รันสคริปต์นี้ได้จากเครื่องไหนก็ได้
# (หาตำแหน่งโฟลเดอร์ตัวเองอัตโนมัติ ไม่ต้องแก้ path)

$root = $PSScriptRoot
$desktop = [Environment]::GetFolderPath('Desktop')

$WshShell = New-Object -ComObject WScript.Shell
$shortcut = $WshShell.CreateShortcut("$desktop\SET Dashboard.lnk")
$shortcut.TargetPath = "$root\start.bat"
$shortcut.WorkingDirectory = $root
$shortcut.IconLocation = "$root\static\icons\app.ico"
$shortcut.Description = "เปิด SET Dashboard"
$shortcut.Save()

Write-Host "สร้างชอร์ตคัตแล้วที่: $desktop\SET Dashboard.lnk"
