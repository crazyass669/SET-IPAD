# -*- coding: utf-8 -*-
"""sources/set_daily_val.py — สะสม PE/PBV/BVPS/DivYield "รายวัน" ทางการจาก SET.or.th
(/api/set/stock/<sym>/historical-trading) ลง financials.db เอง (ดู
PLAN_set_api_expansion.txt งาน #4B)

ทำไมต้องสะสมเอง: endpoint historical-trading ล็อคความลึกไว้ ~118 แท่งซื้อขาย (~6 เดือน)
ตายตัว ไม่รับ param วันที่ใดๆ (ยืนยันแล้ว 2026-08-23) — เก็บทุกรอบ sync แบบ upsert
(ไม่ลบของเก่า) ผ่านไป 1 ปีก็จะมีประวัติทางการ 1 ปีที่ SET เองไม่ให้ย้อนหลัง
pattern เดียวกับ SET P&L รายไตรมาส (source 'set_qpl') ที่ดึงสดได้แค่ ~2 ปี — ดู CLAUDE.md

เก็บใน financials.db (local-only อยู่แล้ว) ตาราง set_daily_valuation — คนละ concern กับ
set_company_master (ข้อมูลบริษัท ไม่ใช่ราคา/valuation รายวัน) เลยแยกตาราง/โมดูล
PRIMARY KEY(symbol, date) → upsert รอบใหม่ทับค่าเดิมของวันเดียวกัน (SET แก้ย้อนหลังได้
เช่นตอนประกาศงบใหม่ EPS เปลี่ยน PE ทั้งชุดขยับ) แต่ไม่แตะแถววันอื่น
"""
import sqlite3
from datetime import datetime, timezone

from sources import financials_store as fs

TABLE = "set_daily_valuation"

_COLS = ("close", "pe", "pbv", "book_value_per_share", "dividend_yield", "market_cap")


def _connect(base_dir):
    con = sqlite3.connect(fs._db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_table(base_dir):
    con = _connect(base_dir)
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}(
              symbol TEXT, date TEXT,
              close REAL, pe REAL, pbv REAL, book_value_per_share REAL,
              dividend_yield REAL, market_cap REAL,
              synced_at TEXT,
              PRIMARY KEY(symbol, date)
            )
        """)
        con.commit()
    finally:
        con.close()


def _th_symbols(base_dir):
    """หุ้นสามัญไทยทั้งหมด (SET+mai) — แหล่งเดียวกับ set_company._th_symbols"""
    from set_data_fetcher import load_set_symbols
    return [s["symbol"] for s in load_set_symbols(base_dir)]


def upsert_rows(base_dir, symbol, rows, con=None):
    """เขียน rows (จาก set_api.fetch_historical_trading[_batch]) ของหุ้น 1 ตัวลง DB แบบ
    upsert — วันเดิมทับค่าใหม่, วันใหม่ต่อท้าย, วันเก่าที่ SET เลิกส่งมาแล้ว "คงไว้" (นี่คือ
    จุดประสงค์ทั้งหมด — สะสมประวัติเกิน 6 เดือนที่ SET ให้) คืนจำนวนแถวที่เขียน
    con: reuse connection เดิมได้ (bulk sync วนหลายร้อยตัว)"""
    if not rows:
        return 0
    sym = symbol[:-3] if symbol.endswith(".BK") else symbol
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    owns = con is None
    if con is None:
        init_table(base_dir)
        con = _connect(base_dir)
    n = 0
    try:
        for r in rows:
            d = (r.get("date") or "")[:10]
            if not d:
                continue
            con.execute(f"""
                INSERT INTO {TABLE}(symbol, date, close, pe, pbv,
                    book_value_per_share, dividend_yield, market_cap, synced_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    close=excluded.close, pe=excluded.pe, pbv=excluded.pbv,
                    book_value_per_share=excluded.book_value_per_share,
                    dividend_yield=excluded.dividend_yield, market_cap=excluded.market_cap,
                    synced_at=excluded.synced_at
            """, (sym, d, r.get("close"), r.get("pe"), r.get("pbv"),
                  r.get("book_value_per_share"), r.get("dividend_yield"),
                  r.get("market_cap"), now))
            n += 1
        if owns:
            con.commit()
    finally:
        if owns:
            con.close()
    return n


def get_series(base_dir, symbol):
    """ประวัติ PE/PBV/BVPS/DivYield รายวันที่สะสมไว้ของหุ้น 1 ตัว (เก่า->ใหม่) — คืน [] ถ้า
    ยังไม่เคยสะสม คืนโครง row เดียวกับ set_api.fetch_historical_trading (nำไปใช้ต่อได้ทันที)"""
    if not fs.db_exists(base_dir):
        return []
    sym = symbol[:-3] if symbol.endswith(".BK") else symbol
    con = _connect(base_dir)
    try:
        cur = con.execute(f"""SELECT date, close, pe, pbv, book_value_per_share,
            dividend_yield, market_cap FROM {TABLE} WHERE symbol=? ORDER BY date""", (sym,))
        return [{"date": r[0], "close": r[1], "pe": r[2], "pbv": r[3],
                 "book_value_per_share": r[4], "dividend_yield": r[5], "market_cap": r[6]}
                for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()


def get_symbol_meta(base_dir, symbol):
    """{count, date_from, date_to, synced_at} ของหุ้น 1 ตัว — None ถ้ายังไม่เคยสะสม"""
    if not fs.db_exists(base_dir):
        return None
    sym = symbol[:-3] if symbol.endswith(".BK") else symbol
    con = _connect(base_dir)
    try:
        row = con.execute(f"""SELECT COUNT(*), MIN(date), MAX(date), MAX(synced_at)
            FROM {TABLE} WHERE symbol=?""", (sym,)).fetchone()
        if not row or not row[0]:
            return None
        return {"count": row[0], "date_from": row[1], "date_to": row[2], "synced_at": row[3]}
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def get_meta(base_dir):
    """สรุปทั้งตาราง — ใช้ในหน้า Data Health: {symbol_count, row_count, oldest_date,
    newest_date, updated_at, max_span_days} คืนค่า 0/None ถ้ายังไม่เคย sync"""
    empty = {"symbol_count": 0, "row_count": 0, "oldest_date": None,
             "newest_date": None, "updated_at": None, "max_span_days": 0}
    if not fs.db_exists(base_dir):
        return empty
    con = _connect(base_dir)
    try:
        row = con.execute(f"""SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date), MAX(date),
            MAX(synced_at) FROM {TABLE}""").fetchone()
        if not row or not row[1]:
            return empty
        span = con.execute(f"""SELECT MAX(julianday(mx) - julianday(mn)) FROM
            (SELECT MIN(date) mn, MAX(date) mx FROM {TABLE} GROUP BY symbol)""").fetchone()
        return {"symbol_count": row[0], "row_count": row[1], "oldest_date": row[2],
                "newest_date": row[3], "updated_at": row[4],
                "max_span_days": int(span[0]) if span and span[0] is not None else 0}
    except sqlite3.OperationalError:
        return empty
    finally:
        con.close()


def _latest_date(base_dir, con):
    row = con.execute(f"SELECT MAX(date) FROM {TABLE}").fetchone()
    return row[0] if row else None


def sync_all(base_dir, callback=None, skip_up_to_date=True):
    """ดึง historical-trading ทั้งกระดาน (พารัลเลล ~1-2 นาที/931 ตัว) แล้ว upsert สะสม —
    wire เข้า run_quick_update() (staleness gate: ข้ามตัวที่ date ล่าสุดใน DB = วันล่าสุด
    ของทั้งตารางแล้ว = sync ไปวันนี้แล้ว) + ปุ่มมือในหน้า Data Health

    คืน (จำนวนหุ้นที่เขียน, จำนวนหุ้นที่พยายาม, จำนวนแถวใหม่รวม)
    raise ถ้า fetch_historical_trading_batch ได้ < 50% (ปล่อย ValueError ให้ caller)"""
    from sources.set_api import fetch_historical_trading_batch
    init_table(base_dir)
    all_syms = _th_symbols(base_dir)

    targets = all_syms
    if skip_up_to_date and fs.db_exists(base_dir):
        con = _connect(base_dir)
        try:
            latest = _latest_date(base_dir, con)
            if latest:
                have_today = {r[0] for r in con.execute(
                    f"SELECT symbol FROM {TABLE} WHERE date=?", (latest,)).fetchall()}
                targets = [s for s in all_syms
                           if (s[:-3] if s.endswith(".BK") else s) not in have_today]
        finally:
            con.close()

    if not targets:
        if callback:
            callback(1, 1, "PE/PBV รายวัน: สะสมครบวันล่าสุดแล้ว ไม่มีอะไรต้องดึง")
        return 0, 0, 0

    data = fetch_historical_trading_batch(targets, callback=callback)
    now_syms = written = rows_written = 0
    con = _connect(base_dir)
    try:
        for sym, rows in data.items():
            rows_written += upsert_rows(base_dir, sym, rows, con=con)
            written += 1
        con.commit()
    finally:
        con.close()
    now_syms = written
    if callback:
        callback(1, 1, f"PE/PBV รายวัน: สะสม {now_syms}/{len(targets)} ตัว (+{rows_written} แถว)")
    return now_syms, len(targets), rows_written
