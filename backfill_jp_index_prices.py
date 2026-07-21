# -*- coding: utf-8 -*-
"""backfill_jp_index_prices.py — ดึงราคา OHLC ย้อนหลังของสมาชิกดัชนี Nikkei 225
(~225 ตัว) ลง jp_prices.db

รันครั้งเดียว (หรือรันซ้ำเพื่อ backfill เพิ่ม) ก่อนผูก gap-update เข้า Quick Update
ใช้: python backfill_jp_index_prices.py [ปีย้อนหลัง]   ค่า default = 2
"""
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from sources import jp_index_membership
from sources.yahoo import fetch_all_batch
from core import jp_store


def main():
    years = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    period = f"{years}y"

    tickers = jp_index_membership.all_tickers(BASE_DIR)
    if not tickers:
        print("ไม่พบรายชื่อดัชนีใน data/jp_index_membership.json — รัน sync_membership ก่อน")
        return

    print(f"=== Backfill ราคา {len(tickers)} ticker (Nikkei 225) period={period} ===")

    def cb(done, total, msg):
        print(f"  [{done}/{total}] {msg}")

    data = fetch_all_batch(tickers, callback=cb, period=period)
    print(f"ดึงได้ {len(data)}/{len(tickers)} ตัว")

    missing = sorted(set(tickers) - set(data))
    if missing:
        print(f"ตัวที่ดึงไม่ได้ ({len(missing)}): {', '.join(missing)}")

    jp_store.init_db(BASE_DIR)
    jp_store.upsert_bars(BASE_DIR, data)
    print(f"บันทึกลง {jp_store.DB_FILE} เสร็จแล้ว")


if __name__ == "__main__":
    main()
