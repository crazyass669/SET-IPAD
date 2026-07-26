# -*- coding: utf-8 -*-
"""สร้าง factor_snapshot (ตารางปัจจัยต่อหุ้น) จากงบใน financials.db — รัน:
    python build_snapshot.py

คำนวณ growth/ratio/valuation-percentile/cash-cycle/risk ของหุ้นไทย+DR ทุกตัว
ที่มีงบ Yahoo เก็บลงตาราง factor_snapshot ให้ Screener อ่านไปกรองได้เร็ว
รันซ้ำได้เสมอ (เขียนทับทั้งตาราง) — ควรรันหลัง mirror งบ Finnomena จบทุกรอบ
เป็น local-only เหมือน financials.db (ห้าม push ขึ้น GitHub)
"""
import io
import os
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from sources import factor_snapshot as snap   # noqa: E402

t0 = time.time()
print("[Snapshot] เริ่มคำนวณ factor snapshot...", flush=True)


def cb(done, total, msg):
    print(f"[Snapshot] {msg}", flush=True)


res = snap.build_snapshot(BASE, callback=cb)
dt = time.time() - t0
print(f"[Snapshot] หลัก: หุ้นไทย {res['set']} + DR {res['dr']} = {res['total']} แถว "
      f"({dt:.0f} วิ) @ {res['at']}", flush=True)

# mirror US/HK (นอก universe หลัก) — opt-in ใน Screener แยกตลาด
# ข้ามได้ด้วย argument 'nomirror' ถ้าต้องการอัพเดตเฉพาะตัวหลักให้เร็ว
if len(sys.argv) > 1 and sys.argv[1] == "nomirror":
    print("[Snapshot] ข้าม mirror US/HK (nomirror)", flush=True)
else:
    t1 = time.time()
    print("[Snapshot] กำลังสร้าง mirror US/HK (หุ้นนอก universe หลัก, งบ >=12 ไตรมาส)...", flush=True)
    mres = snap.build_mirror_snapshot(BASE, callback=lambda ex, n, name: print(f"[Snapshot] mirror {ex} {n}...", flush=True))
    print(f"[Snapshot] mirror: US {mres.get('US', 0)} + HK {mres.get('HK', 0)} แถว "
          f"({time.time() - t1:.0f} วิ)", flush=True)
