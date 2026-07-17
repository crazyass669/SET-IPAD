# -*- coding: utf-8 -*-
"""sync_th_descriptions.py — ดึง longBusinessSummary จาก Yahoo Finance + แปลไทย
สำหรับหุ้นไทยทั้งหมด (~930 ตัว) เก็บลง dr_descriptions.json (ไฟล์เดียวกับ DR — เก็บ
คีย์เป็น symbol เปล่าไม่มี .BK) ให้หน้า "งบการเงิน" (SET tab) โชว์กล่อง "📖 เกี่ยวกับบริษัท"
ได้แบบเดียวกับ DR

รัน:  python sync_th_descriptions.py [--force]
  --force = ดึงใหม่ทุกตัวแม้มีอยู่แล้ว (ปกติข้ามตัวที่มีอยู่แล้วและอายุ < 180 วัน)

Resume ได้ — เซฟทีละตัวลงไฟล์ทันที (dr_descriptions.fetch_one เซฟทุก call) ถ้าถูก
interrupt กลางทาง รันใหม่จะข้ามตัวที่ทำสำเร็จไปแล้วเอง
"""
import json
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
sys.stdout.reconfigure(encoding="utf-8")

from sources import dr_descriptions

FORCE = "--force" in sys.argv
SLEEP_SEC = 0.3   # กันยิง Yahoo/Google Translate ถี่เกินไปจนโดนบล็อกชั่วคราว


def main():
    stocks = json.load(open(os.path.join(BASE_DIR, "set_data.json"), encoding="utf-8"))["stocks"]
    symbols = [s["symbol"] for s in stocks]
    print(f"หุ้นไทยทั้งหมด {len(symbols)} ตัว")

    store = dr_descriptions.load_all(BASE_DIR)
    now = time.time()
    max_age = 180 * 86400
    todo = []
    for sym in symbols:
        cached = store.get(sym)
        if (not FORCE and cached and cached.get("th")
                and cached.get("fetched_ts") and (now - cached["fetched_ts"]) < max_age):
            continue
        todo.append(sym)
    print(f"ต้องดึงใหม่ {len(todo)} ตัว (ข้าม {len(symbols) - len(todo)} ตัวที่มีอยู่แล้ว/ยังไม่หมดอายุ)")

    ok = fail = 0
    fails = []
    t0 = time.time()
    for i, sym in enumerate(todo):
        try:
            record, err = dr_descriptions.fetch_one(BASE_DIR, sym, market="TH", force=FORCE)
        except Exception as e:
            record, err = None, str(e)
        if record:
            ok += 1
        else:
            fail += 1
            fails.append((sym, err))
            print(f"  [{i + 1}/{len(todo)}] {sym}: {err}")
        if (i + 1) % 20 == 0 or i + 1 == len(todo):
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_min = (len(todo) - i - 1) / rate / 60 if rate > 0 else 0
            print(f"[{i + 1}/{len(todo)}] ok={ok} fail={fail} · {elapsed:.0f}s ผ่านมา · เหลือ ~{eta_min:.1f} นาที")
        time.sleep(SLEEP_SEC)

    print(f"\nเสร็จแล้ว: ok={ok} fail={fail} จากทั้งหมด {len(todo)} ตัวที่ดึงใหม่")
    if fails:
        print("ตัวที่ล้มเหลว (มักเป็นหุ้นไม่มี business summary ให้ เช่น REIT/กองทุน):")
        for sym, err in fails[:50]:
            print(f"  {sym}: {err}")


if __name__ == "__main__":
    main()
