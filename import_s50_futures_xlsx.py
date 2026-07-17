# -*- coding: utf-8 -*-
"""import_s50_futures_xlsx.py — นำเข้าประวัติ S50 Futures net position จาก
"S50 Futures Data.xlsx" (คอลัมน์: วันที่, ต่างชาติ, สถาบัน, ในประเทศ — หน่วยสัญญา)
เข้าไป merge กับ s50_flow_data.json ที่ /api/market-flow-s50 ใช้อยู่แล้ว

รันครั้งเดียว: TFEX ไม่มี API ประวัติเปิดให้ดึงย้อนหลัง (ดูคอมเมนต์ใน app.py
_fetch_flow_tfex_today) ไฟล์นี้ทำให้ข้อมูลย้อนหลังกระโดดจาก 1 วัน เป็นตั้งแต่ 2020
ทันที ไม่ต้องรอสะสมเองทีละวันอีกหลายปี — จากนี้ endpoint จะดึงเติมเฉพาะวันใหม่ต่อจากนี้
"""
import json
import os
import sys

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
XLSX_PATH = os.path.join(BASE_DIR, "S50 Futures Data.xlsx")
JSON_PATH = os.path.join(BASE_DIR, "s50_flow_data.json")


def main():
    df = pd.read_excel(XLSX_PATH, header=0)
    df.columns = [c.strip() for c in df.columns]
    expected = ["วันที่", "ต่างชาติ", "สถาบัน", "ในประเทศ"]
    if list(df.columns) != expected:
        sys.exit(f"คอลัมน์ไม่ตรงที่คาด: {list(df.columns)} (ต้องการ {expected})")

    rows_by_date = {}
    try:
        with open(JSON_PATH, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = r0
    except Exception:
        pass

    n_new = 0
    for _, r in df.iterrows():
        date = r["วันที่"].strftime("%Y-%m-%d")
        row = {
            "date": date,
            "fund": float(r["สถาบัน"]),
            "foreign": float(r["ต่างชาติ"]),
            "retail": float(r["ในประเทศ"]),
        }
        if date not in rows_by_date:
            n_new += 1
        rows_by_date[date] = row   # xlsx เป็นแหล่งที่เชื่อถือได้กว่า (ประวัติทางการ) — ทับได้

    rows = sorted(rows_by_date.values(), key=lambda x: x["date"])
    from datetime import datetime
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({"rows": rows, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")},
                  f, ensure_ascii=False, indent=1)

    print(f"นำเข้าเสร็จ: {len(df)} แถวจาก xlsx, ใหม่ {n_new} วัน, รวมทั้งหมด {len(rows)} วัน "
          f"({rows[0]['date']} ถึง {rows[-1]['date']})")


if __name__ == "__main__":
    main()
