# SET Dashboard

## ⚠️ ห้ามลบ/เขียนทับไฟล์ข้อมูลสะสมเหล่านี้โดยเด็ดขาด

ไฟล์ต่อไปนี้อยู่ที่ root โปรเจกต์ เป็น local-only (.gitignore กันไว้แล้ว ไม่ขึ้น GitHub)
เก็บข้อมูลสะสมที่ **สร้างใหม่ไม่ได้เต็มรูปแบบ**:

- `financials.db` (~700MB) — งบการเงิน Yahoo/Finnomena/SET company-highlight/SET P&L
  รายไตรมาส — Finnomena ไม่มี API ทางการ, SET P&L รายไตรมาสดึงย้อนหลังสดได้แค่
  ~2 ปีล่าสุด (ของเก่ากว่านั้นที่เคยสะสมไว้ ถ้าหายคือหายถาวร)
- `set_prices.db` (~320MB) — ราคาหุ้นไทย + หุ้นเพิกถอน (survivorship bias fix —
  Yahoo/SET ดึงหุ้น delisted ใหม่ไม่ได้แล้ว)
- `sec_filings.db` — insider / ผู้ถือหุ้นใหญ่
- `delisted_log.json` — ประวัติหุ้นเข้า/ออก
- `us_prices.db` / `hk_prices.db` / `jp_prices.db` (~440MB รวม) — ราคาหุ้นดัชนี
  US/HK/JP — regenerate ได้ผ่าน backfill script แต่ช้ามาก (หลายชั่วโมง)

**ห้ามเด็ดขาด**: `rm` / `os.remove` / `DROP TABLE` / เปิดไฟล์โหมด `w` (truncate) /
`git clean` / `git checkout --` หรือคำสั่งอื่นใดที่ลบ-เขียนทับ-ล้างข้อมูลไฟล์เหล่านี้
โดยไม่ได้รับการยืนยันจากผู้ใช้ชัดเจนก่อนทุกครั้ง แม้จะเป็นแค่ "ทดสอบ" หรือ
"อยากได้ DB ว่างลองของ" ก็ตาม — เคยเกิดเหตุลบ `financials.db` โดยไม่ตั้งใจระหว่าง
ทดสอบมาแล้ว 1 ครั้ง (กู้จาก backup ทัน แต่เสี่ยงเสียของจริง)

ถ้าต้องการ DB ว่าง/สภาพแวดล้อมทดสอบ ให้ **copy ไฟล์ไป scratchpad ก่อนเสมอ** แล้ว
ทดสอบกับสำเนานั้น ห้ามแตะไฟล์จริงที่ root โดยตรง

Backup นอกเครื่องอยู่ที่ `C:\Users\joeki\OneDrive\SET_Dashboard_Backup\`
(อัตโนมัติทุกจันทร์/พฤหัส 23:00 + ตอน log on ถ้าของเดิมเกิน 3 วัน ผ่าน Windows Task
Scheduler "Backup_Offsite_Weekly") — แต่ backup อาจ lag ได้ถึงหลายวัน **ห้ามใช้การมี
backup เป็นข้ออ้างในการเสี่ยงลบไฟล์จริง**
