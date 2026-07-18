# -*- coding: utf-8 -*-
"""
Migrate set_history.json -> set_prices.db (SQLite) พร้อม parity check

รัน:  python migrate_history.py
ปลอดภัย: อ่าน JSON อย่างเดียว ไม่แก้/ลบไฟล์เดิม — รันซ้ำได้ (INSERT OR REPLACE)
"""
import json
import os
import random
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

from core import store


def main():
    src = os.path.join(BASE, store.HISTORY_FILE)
    if not os.path.exists(src):
        print(f"ไม่พบ {store.HISTORY_FILE} — ไม่มีอะไรให้ migrate")
        return 1

    print(f"อ่าน {store.HISTORY_FILE}...")
    t0 = time.perf_counter()
    with open(src, encoding="utf-8") as f:
        history = json.load(f)
    stocks = history.get("stocks", {})
    n_json_rows = sum(len(v["dates"]) for v in stocks.values())
    print(f"  {len(stocks)} tickers, {n_json_rows:,} rows ({time.perf_counter()-t0:.1f}s)")

    print("เขียนลง SQLite...")
    t0 = time.perf_counter()
    store.upsert_history_dict(BASE, history)
    print(f"  เสร็จใน {time.perf_counter()-t0:.1f}s -> {store.DB_FILE} "
          f"({os.path.getsize(store._db_path(BASE))/1e6:.1f} MB)")

    # ---------- parity check ----------
    print("Parity check...")
    con = store._connect(BASE)
    try:
        n_db_rows = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        n_db_tickers = con.execute(
            "SELECT COUNT(DISTINCT ticker) FROM prices").fetchone()[0]
    finally:
        con.close()
    ok = True
    if n_db_rows < n_json_rows:
        # DB อาจมี rows มากกว่า JSON ได้ (จาก quick update หลัง migrate) แต่ห้ามน้อยกว่า
        print(f"  ❌ rows: DB {n_db_rows:,} < JSON {n_json_rows:,}")
        ok = False
    else:
        print(f"  ✅ rows: DB {n_db_rows:,} >= JSON {n_json_rows:,}")
    print(f"  {'✅' if n_db_tickers >= len(stocks) else '❌'} tickers: "
          f"DB {n_db_tickers} vs JSON {len(stocks)}")

    # สุ่มเทียบ 20 ตัวแบบเต็ม series
    sample = random.sample(sorted(stocks), min(20, len(stocks)))
    bad = 0
    for tk in sample:
        db = store.get_series(BASE, tk)
        js = stocks[tk]
        same = (db is not None
                and db["dates"] == js["dates"]
                and all(abs(a - b) < 1e-9 for a, b in zip(db["closes"], js["closes"]))
                and db["volumes"] == js["volumes"])
        if not same:
            print(f"  ❌ series mismatch: {tk}")
            bad += 1
    if bad == 0:
        print(f"  ✅ series ตรงกัน 100% ({len(sample)} ตัวอย่างสุ่ม)")
    ok = ok and bad == 0 and n_db_tickers >= len(stocks)

    print("\n" + ("✅ MIGRATION OK" if ok else "❌ MIGRATION FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
