# -*- coding: utf-8 -*-
"""
repair_div_yield.py — ซ่อมข้อมูล fundamentals ที่เสียจากบั๊ก ×100 เดิมใน
sources/yahoo.py (heuristic เก่าพองหุ้น div_yield ต่ำจริงให้ผิด 100 เท่า)

รันครั้งเดียวหลัง deploy โค้ดที่แก้แล้ว: ดึง fundamentals ใหม่จาก SET API
(primary, เร็ว ~20 วิ) มา patch mkt_cap/pe/pbv/div_yield ใน set_data.json
โดยไม่ต้อง Full Refresh ราคาทั้งหมด

รัน:  python repair_div_yield.py
"""
import json
import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

from core.store import OUT_FILE, _atomic_write_json
from sources.set_api import fetch_fundamentals


def main():
    path = os.path.join(BASE, OUT_FILE)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    stocks = data["stocks"]
    tickers = [s["ticker"] for s in stocks]

    print(f"ดึง fundamentals ใหม่จาก SET API ({len(tickers)} หุ้น)...")
    fresh = fetch_fundamentals(tickers)
    print(f"ได้ {len(fresh)}/{len(tickers)} ตัว")

    changed = []
    for s in stocks:
        f = fresh.get(s["ticker"])
        if not f:
            continue
        old_dy = s.get("div_yield")
        new_dy = f.get("div_yield")
        if old_dy is not None and new_dy is not None and abs(old_dy - new_dy) > 0.5:
            changed.append((s["symbol"], old_dy, new_dy))
        s["mkt_cap"]   = f.get("mkt_cap")
        s["pe"]        = f.get("pe")
        s["pbv"]       = f.get("pbv")
        s["div_yield"] = f.get("div_yield")

    _atomic_write_json(path, data)
    print(f"\nบันทึก {path} แล้ว")
    print(f"div_yield ที่เปลี่ยนเกิน 0.5 จุด: {len(changed)} ตัว")
    for sym, old, new in sorted(changed, key=lambda x: -(x[1] or 0))[:20]:
        print(f"  {sym:<10} {old} -> {new}")


if __name__ == "__main__":
    main()
