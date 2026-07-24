# -*- coding: utf-8 -*-
"""ทยอยโหลดงบรายไตรมาส Finnomena ทั้งตลาดเก็บลง financials.db (namespace FINN:)

ใช้:
    python mirror_finnomena.py              โหลดทั้งหมด TH -> HK -> US
    python mirror_finnomena.py TH,HK        เฉพาะบางตลาด
    python mirror_finnomena.py US 2000      จำกัดรอบนี้ 2,000 ตัว (ทยอยเป็นก้อน)

หยุดกลางทางได้เสมอ (Ctrl+C / ปิดเครื่อง) — รันใหม่จะข้ามตัวที่เก็บแล้วต่อจากเดิมอัตโนมัติ
ก่อนเริ่มทุกรอบระบบสำรอง financials.db ไว้ใน financials_backups/ ให้เอง
ข้อมูลชุดนี้อยู่เฉพาะบนเครื่อง (gitignore ไว้แล้ว) ห้ามขึ้น GitHub
"""
import io
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")   # คอนโซล Windows แสดงไทย
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import run_log                     # noqa: E402
from sources import financials_store as fs   # noqa: E402

args = [a for a in sys.argv[1:]]
force = any(a.lower() == "force" for a in args)      # Mode F: ยิงซ้ำทั้งตลาดดึงงวดใหม่
pos = [a for a in args if a.lower() != "force"]      # argument ที่เหลือ (ตลาด / limit)
exchanges = tuple((pos[0] if len(pos) > 0 else "TH,HK,US").upper().split(","))
limit = int(pos[1]) if len(pos) > 1 else None

if force:
    print("[FinnMirror] โหมด FORCE — ยิงซ้ำทุกหุ้นที่มีงบ เพื่อดึงงวดใหม่มา merge "
          "(ข้ามเฉพาะตัวที่ไม่มีงบ) · เหมาะรันหลัง earnings season", flush=True)
try:
    result = fs.mirror_finnomena(BASE, exchanges=exchanges, limit=limit, force=force)
    print("[FinnMirror] อย่าลืมรัน  python build_snapshot.py  เพื่อรีเฟรช Screener", flush=True)
    run_log.record_run(BASE, "mirror_finnomena", True,
                        f"มีงบ {result['ok']} · ไม่มีงบ {result['empty']} · พลาด {result['fail']}")
except Exception as e:
    run_log.record_run(BASE, "mirror_finnomena", False, str(e))
    raise
