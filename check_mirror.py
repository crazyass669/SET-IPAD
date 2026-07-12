# -*- coding: utf-8 -*-
"""เช็คสถานะการโหลดงบ Finnomena (mirror_finnomena.py) — รัน: python check_mirror.py

อ่านอย่างเดียว ปลอดภัยแม้ mirror กำลังรันอยู่ | เกณฑ์ตัดสิน:
เขียนล่าสุด < 3 นาที = กำลังโหลด | 3-22 นาที = น่าจะพักเบรกกันแบน (โค้ดพักสุ่ม
5-20 นาที ทุกๆ 800-1,500 ตัว) | > 22 นาที ทั้งที่โปรเซสยังอยู่ = ค้างผิดปกติ"""
import io
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "financials.db")

# --- โปรเซส mirror ยังรันอยู่ไหม (เทียบ CommandLine ตรงๆ ผ่าน PowerShell/CIM) ---
try:
    out = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "(Get-CimInstance Win32_Process | Where-Object {$_.Name -like 'python*' "
         "-and $_.CommandLine -like '*mirror_finnomena*'}).ProcessId"],
        capture_output=True, text=True, timeout=30).stdout.split()
    pids = [p for p in out if p.strip().isdigit()]
except Exception:
    pids = []
print(f"โปรเซส mirror: {'✅ รันอยู่ (PID ' + ', '.join(pids) + ')' if pids else '❌ ไม่ได้รัน'}")

if not os.path.exists(DB):
    print("ยังไม่มี financials.db")
    sys.exit(0)

# --- เขียนล่าสุดเมื่อไหร่ (ดูทั้งไฟล์หลักและ -wal) ---
mtimes = [os.path.getmtime(DB)]
for suf in ("-wal", "-shm"):
    if os.path.exists(DB + suf):
        mtimes.append(os.path.getmtime(DB + suf))
last = datetime.fromtimestamp(max(mtimes))
idle_min = (datetime.now() - last).total_seconds() / 60
print(f"เขียน DB ล่าสุด: {last:%H:%M:%S} ({idle_min:.0f} นาทีที่แล้ว)")

# --- นับความคืบหน้าใน DB ---
con = sqlite3.connect(DB)
con.execute("PRAGMA busy_timeout=5000")
rows = con.execute(
    "SELECT symbol, payload LIKE '%\"empty\": true%', payload LIKE '%\"schema\": 2%' "
    "FROM financials WHERE source='finnomena_q' AND symbol LIKE 'FINN:%'").fetchall()
meta = con.execute("SELECT value FROM meta WHERE key='finnomena_mirror_last'").fetchone()
con.close()

per_ex = {}
n_empty = n_v2 = 0
for sym, is_empty, is_v2 in rows:
    ex = sym.split(":")[1]
    per_ex[ex] = per_ex.get(ex, 0) + 1
    n_empty += is_empty
    n_v2 += is_v2 and not is_empty
print(f"เก็บแล้วรวม {len(rows):,} ตัว = " +
      " | ".join(f"{ex} {n:,}" for ex, n in sorted(per_ex.items())) +
      f" | ไม่มีงบ {n_empty:,}")
print(f"schema ใหม่ (มี ratios/valuation): {n_v2:,} | schema เก่า (รอรอบเติม field): "
      f"{len(rows) - n_empty - n_v2:,}")
if meta:
    m = json.loads(meta[0])
    print(f"รอบที่จบล่าสุด: {m.get('at')} (มีงบ {m.get('ok')} ไม่มีงบ {m.get('empty')} พลาด {m.get('fail')})")

# --- สรุปสถานะ ---
if not pids:
    print("\n>> สรุป: mirror ไม่ได้รันอยู่ — สั่ง `python mirror_finnomena.py` เพื่อรัน/ต่อจากเดิม")
elif idle_min < 3:
    print("\n>> สรุป: ✅ กำลังโหลดตามปกติ")
elif idle_min <= 22:
    print("\n>> สรุป: 💤 น่าจะกำลังพักเบรกกันแบน (สุ่ม 5-20 นาที) — เดี๋ยวไปต่อเอง")
else:
    print("\n>> สรุป: ⚠️ นิ่งเกิน 22 นาทีทั้งที่โปรเซสยังอยู่ — ผิดปกติ "
          "ดูข้อความล่าสุดในหน้าต่างที่รัน mirror ถ้าค้างจริง Ctrl+C แล้วรันใหม่ (ต่อจากเดิมได้)")
