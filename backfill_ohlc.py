# -*- coding: utf-8 -*-
"""backfill_ohlc.py — เติม OHLC + Adj Close ให้ set_prices.db (ครั้งเดียวหลังเพิ่มคอลัมน์)

ดึงราคา period=max ใหม่ทุกตัวผ่าน yfinance (ตัวเดิม) แล้ว upsert ทับ — แถวเดิมได้
open/high/low/adj_close เติมเข้าไป (close/volume คงเดิม) ทำเป็น chunk ละ ~80 ตัว
เพื่อคุมหน่วยความจำ + มี progress + resume ได้ (INSERT OR REPLACE idempotent)

รัน: python backfill_ohlc.py
"""
import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import store                       # noqa: E402
from sources.yahoo import fetch_all_batch     # noqa: E402
from set_data_fetcher import load_set_symbols  # noqa: E402


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def _already_done(base_dir):
    """ticker ที่มี adj_close แล้ว — ข้ามตอน resume"""
    import sqlite3
    con = sqlite3.connect(store._db_path(base_dir))
    try:
        return {r[0] for r in con.execute(
            "SELECT DISTINCT ticker FROM prices WHERE adj_close IS NOT NULL")}
    finally:
        con.close()


def main():
    syms = load_set_symbols(BASE)
    all_tickers = [s["ticker"] for s in syms]
    done_set = _already_done(BASE)
    tickers = [t for t in all_tickers if t not in done_set]
    total = len(tickers)
    log(f"เริ่ม backfill OHLC/adj_close: เหลือ {total} ตัว (ข้ามที่ทำแล้ว {len(done_set)})")

    prog_path = os.path.join(BASE, "backfill_ohlc.progress")
    CHUNK = 60
    done = 0
    filled = 0
    for i in range(0, total, CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            data = fetch_all_batch(chunk, period="max")
        except Exception as e:
            log(f"  chunk {i}-{i+len(chunk)} error: {e}")
            continue
        # เขียนเฉพาะ prices DB (ไม่แตะ set_data.json/set_history.json)
        store.DUAL_WRITE_JSON = False
        store.upsert_bars(BASE, data)
        done += len(chunk)
        filled += len(data)
        msg = f"{done}/{total} (chunk got {len(data)}/{len(chunk)})"
        log("  " + msg)
        try:
            with open(prog_path, "w", encoding="utf-8") as pf:
                pf.write(f"{time.strftime('%H:%M:%S')} {msg}\n")
        except Exception:
            pass

    # ตรวจผล: กี่ % ของแถวมี adj_close แล้ว
    import sqlite3
    con = sqlite3.connect(store._db_path(BASE))
    n_tot = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    n_adj = con.execute("SELECT count(*) FROM prices WHERE adj_close IS NOT NULL").fetchone()[0]
    n_ohlc = con.execute("SELECT count(*) FROM prices WHERE high IS NOT NULL").fetchone()[0]
    con.close()
    log(f"เสร็จ: {filled}/{total} ตัวมีข้อมูล | rows adj_close={n_adj}/{n_tot} "
        f"({n_adj*100//max(1,n_tot)}%) ohlc={n_ohlc}/{n_tot}")


if __name__ == "__main__":
    main()
