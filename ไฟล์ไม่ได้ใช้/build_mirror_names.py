# -*- coding: utf-8 -*-
"""ดึงชื่อบริษัทของหุ้น US/HK ที่มีงบการเงินใน mirror — เก็บ mirror_names.json

Finnomena /stock/list ให้ชื่อ (en_name) ทั้งตลาดในคำขอเดียว (2 requests ~10 วิ)
เก็บเฉพาะ ticker ที่มีงบจริงใน financials.db (FINN:US/HK ที่ไม่ใช่ marker 'ไม่มีงบ')
ใช้โชว์ 'ticker · ชื่อบริษัท' ในรายการ browse + datalist หน้างบการเงิน

รัน:  python build_mirror_names.py
เป็น local-only (อ่าน financials.db) · ชื่อบริษัทแทบไม่เปลี่ยน รันนานๆ ครั้งพอ
"""
import io
import json
import os
import sqlite3
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from sources import financials_store as fs   # noqa: E402

OUT = os.path.join(BASE, "mirror_names.json")


def _tickers_with_financials(ex):
    """ticker ในตลาด ex ที่มีงบจริงใน mirror (ไม่ใช่ marker 'ไม่มีงบ')"""
    con = sqlite3.connect(os.path.join(BASE, "financials.db"))
    con.execute("PRAGMA busy_timeout=5000")
    try:
        rows = con.execute(
            "SELECT symbol FROM financials WHERE source='finnomena_q' "
            "AND symbol LIKE ? AND payload NOT LIKE '%\"empty\": true%'", (f"FINN:{ex}:%",)).fetchall()
    finally:
        con.close()
    return {r[0].split(":", 2)[2] for r in rows}


t0 = time.time()
out = {}
for ex in ("US", "HK"):
    print(f"[Names] ดึงรายชื่อ {ex} ...", flush=True)
    rows = (fs._finn_get(f"/stock/list?exchange={ex}", timeout=120) or {}).get("data") or []
    name_map = {(r.get("name") or "").upper(): (r.get("en_name") or "").strip()
                for r in rows if r.get("name")}
    have = _tickers_with_financials(ex)
    out[ex] = {t: name_map[t] for t in have if name_map.get(t)}
    print(f"[Names] {ex}: มีงบ {len(have)} ตัว | ได้ชื่อ {len(out[ex])} ตัว", flush=True)

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False)
size = os.path.getsize(OUT) / 1024
print(f"\n✅ บันทึก mirror_names.json ({size:.0f} KB) · US {len(out['US'])} + HK {len(out['HK'])} "
      f"({time.time() - t0:.0f} วิ)", flush=True)
