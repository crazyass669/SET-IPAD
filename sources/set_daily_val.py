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
import logging
import sqlite3
import threading
from datetime import datetime, timezone

from sources import financials_store as fs

TABLE = "set_daily_valuation"

_COLS = ("close", "pe", "pbv", "book_value_per_share", "dividend_yield", "market_cap")

# กัน sync_all() หลายจุดเรียกพร้อมกัน (tier 0 อัตโนมัติใน run_quick_update, ปุ่มมือ
# /api/set-daily-val/sync, safety-net ใน _run_quick) — ทั้ง 3 จุดเขียนตารางเดียวกันใน
# financials.db ด้วย transaction เดียวยาวทั้งลูป มีแค่ busy_timeout=5000ms กันไม่พอถ้าชนกัน
# จริง (code review 2026-08-27)
_sync_lock = threading.Lock()


def _connect(base_dir):
    """reuse pragma เดียวกับ financials_store._connect ตรงๆ (คนละไฟล์เดิมเขียนซ้ำ — เสี่ยง
    WAL/busy_timeout สองโมดูลนี้ค่อยๆ เพี้ยนต่างกันถ้าแก้จุดเดียวไม่ครบ)"""
    return fs._connect(base_dir)


def _bare(symbol):
    """ตัด .BK ออกถ้ามี — เก็บในตารางนี้แบบ bare symbol เสมอ (ใช้ซ้ำแทนการเขียน
    `symbol[:-3] if symbol.endswith(".BK") else symbol` ซ้ำ 4 จุดในไฟล์นี้)"""
    return symbol[:-3] if symbol.endswith(".BK") else symbol


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
    """หุ้นสามัญไทยทั้งหมด (SET+mai) — reuse set_company._th_symbols ตรงๆ (เดิมเขียนซ้ำ
    ทั้งที่ comment บอกว่า "แหล่งเดียวกัน" อยู่แล้ว — code review 2026-08-27)"""
    from sources.set_company import _th_symbols as _sc_th_symbols
    return _sc_th_symbols(base_dir)


def upsert_rows(base_dir, symbol, rows, con=None):
    """เขียน rows (จาก set_api.fetch_historical_trading[_batch]) ของหุ้น 1 ตัวลง DB แบบ
    upsert — วันเดิมทับค่าใหม่, วันใหม่ต่อท้าย, วันเก่าที่ SET เลิกส่งมาแล้ว "คงไว้" (นี่คือ
    จุดประสงค์ทั้งหมด — สะสมประวัติเกิน 6 เดือนที่ SET ให้) คืนจำนวนแถวที่เขียน
    con: reuse connection เดิมได้ (bulk sync วนหลายร้อยตัว)"""
    if not rows:
        return 0
    sym = _bare(symbol)
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
    sym = _bare(symbol)
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


def latest_fundamentals_map(base_dir, tickers, ref_date=None, max_staleness_days=7):
    """{ticker: {"mkt_cap": int|None, "pe": float|None, "pbv": float|None,
                 "div_yield": float|None}} จากแถว date ล่าสุดของแต่ละ symbol ในตาราง —
    normalize ให้ตรงกับ set_api.fetch_fundamentals (int mkt_cap, round 2) เป๊ะ

    ยืนยัน 2026-08-27 (เทียบ 29 ตัวหลังตลาดปิด): แถวล่าสุดของ historical-trading ==
    /highlight-data ทุก field (Δ 0.00%) → ใช้ตารางนี้เป็นแหล่งหลักของ fundamentals ใน
    Quick Update แทนการยิง highlight-data ~930 req/วันซ้ำ
    (ดู services/refresh.py::_fetch_fundamentals_with_fallback tier 0)

    tickers: list "PTT.BK" (มี/ไม่มี .BK ก็ได้) — key ผลลัพธ์ตรงกับที่ส่งเข้ามา
    ref_date: 'YYYY-MM-DD' วันอ้างอิงความสด (ปกติ = วันราคาปิดล่าสุดในเครื่อง) —
              ตัวเทียบจริงคือ max(ref_date, MAX(date) ในตาราง) · SET historical-trading
              เองตามหลังวันเทรด ~1 วัน (ดู _probe_latest_available) แม้ sync_all จะทันทุกรอบ
              ตารางก็จะช้ากว่า ref_date ~1 วันเป็นปกติ — guard เลยต้องหลวมพอ
    max_staleness_days: ข้าม symbol ที่แถวล่าสุดเก่ากว่า ref เกินจำนวนวันนี้ (calendar) —
              กันเอาค่าค้างของหุ้นพักเทรด/เพิกถอน หรือทั้งตารางที่ค้างจริง ๆ มาใช้
              (ตัวที่ถูกข้ามจะตกไป tier 1 highlight-data เอง)
    """
    if not fs.db_exists(base_dir):
        return {}
    want = {}
    for t in tickers:
        want[_bare(t)] = t
    con = _connect(base_dir)
    try:
        table_max = _latest_date(base_dir, con)
        rows = con.execute(f"""
            SELECT v.symbol, v.pe, v.pbv, v.dividend_yield, v.market_cap, v.date
            FROM {TABLE} v
            JOIN (SELECT symbol, MAX(date) mx FROM {TABLE} GROUP BY symbol) m
              ON m.symbol = v.symbol AND m.mx = v.date
        """).fetchall()
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()

    ref = max([x for x in (ref_date, table_max) if x], default=None)
    cutoff = None
    if ref:
        from datetime import date, timedelta
        try:
            y, m, d = map(int, ref[:10].split("-"))
            cutoff = (date(y, m, d) - timedelta(days=max_staleness_days)).isoformat()
        except Exception:
            cutoff = None

    out = {}
    for sym, pe, pbv, dy, mc, d in rows:
        tick = want.get(sym)
        if not tick:
            continue
        if cutoff and (d or "") < cutoff:
            continue
        out[tick] = {
            "mkt_cap":   int(mc)             if mc  is not None else None,
            "pe":        round(float(pe), 2)  if pe  is not None else None,
            "pbv":       round(float(pbv), 2) if pbv is not None else None,
            "div_yield": round(float(dy), 2)  if dy  is not None else None,
        }
    return out


def get_symbol_meta(base_dir, symbol):
    """{count, date_from, date_to, synced_at} ของหุ้น 1 ตัว — None ถ้ายังไม่เคยสะสม"""
    if not fs.db_exists(base_dir):
        return None
    sym = _bare(symbol)
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


def _probe_latest_available(default=None):
    """วันล่าสุดที่ SET /historical-trading "มีให้ดึงจริง" — โพรบจากหุ้นอ้างอิงตัวเดียว
    (PTT: บลูชิพ ไม่เคยพักเทรด) 1 request · endpoint นี้ตามหลังวันเทรด ~1 วัน (หลังตลาด
    ปิด 17:00+ ยังคืนแค่เมื่อวาน — วัดจริง 2026-08-27) เดิม gate เทียบ MAX(date) ตัวเอง
    พอทุก symbol มี MAX แล้ว targets ว่าง ไม่ advance จนกว่าจะมีหุ้นเข้าใหม่/rename มากระตุ้น
    (เปราะ) · โพรบนี้ให้ "วันล่าสุดที่ดึงได้จริง" มาเทียบตรง ๆ — ไม่มีวันใหม่ = ไม่ยิงทิ้งเปล่า,
    มีวันใหม่ = ดึงทั้งกระดาน · คืน default ถ้าโพรบล้ม (คง gate เดิม)"""
    try:
        from sources.set_api import fetch_historical_trading, _bootstrap_headers
        ctx, hdr = _bootstrap_headers()
        rows = fetch_historical_trading("PTT", ctx=ctx, hdr=hdr)
        if rows and rows[-1].get("date"):
            return rows[-1]["date"]
    except Exception as e:
        # เดิมกลืน exception เงียบสนิท (ต่างจาก except-block อื่นในไฟล์นี้ที่ log เตือน
        # ทุกจุด) — ถ้า endpoint เปลี่ยน schema/PTT ถูกพักเทรดถาวร โพรบจะล้มไปเรื่อยๆ โดย
        # ไม่มีร่องรอยใน log เลยว่าทำไม gate ถึงกลับไปใช้ MAX(date) ตัวเอง (code review
        # 2026-08-27)
        logging.warning(f"[set_daily_val] probe PTT ล้มเหลว ({e}) — fallback default={default}")
    return default


def sync_all(base_dir, callback=None, skip_up_to_date=True, probe=True):
    """ดึง historical-trading ทั้งกระดาน (พารัลเลล ~1-2 นาที/931 ตัว) แล้ว upsert สะสม —
    wire เข้า run_quick_update() + ปุ่มมือในหน้า Data Health

    staleness gate: ข้าม symbol ที่มีแถวของ "วันล่าสุดที่ SET ให้ดึงได้จริง" แล้ว
    (โพรบจาก PTT — ดู _probe_latest_available) · probe=False → เทียบ MAX(date) ตัวเอง
    แบบเดิม (ไม่ยิง network เพิ่ม — ใช้กับ opportunistic upsert ที่ต้องเร็ว/ไม่พึ่ง network)

    คืน (จำนวนหุ้นที่เขียน, จำนวนหุ้นที่พยายาม, จำนวนแถวใหม่รวม)
    raise ถ้า fetch_historical_trading_batch ได้ < 50% (ปล่อย ValueError ให้ caller)

    Guard 2 ชั้น (code review 2026-08-27 — มี 3 จุดเรียกฟังก์ชันนี้: tier 0 อัตโนมัติใน
    run_quick_update, ปุ่มมือ /api/set-daily-val/sync, safety-net ใน _run_quick):
      1) ไม่มี financials.db เลย (เช่น CI fresh checkout — เป็น local-only ไม่ commit ขึ้น
         GitHub) → ข้าม ไม่สร้าง DB ว่างๆ ขึ้นมาแค่เพื่อยิง sweep เต็ม ~931 ตัวทิ้งเปล่า —
         ต้องเช็คก่อน init_table() เสมอ ไม่งั้น init_table เองก็สร้างไฟล์ขึ้นมาก่อนแล้ว
         เช็คซ้ำทีหลังจะเจอว่า "มีไฟล์แล้ว" เสมอ (self-defeating)
      2) มีอีกจุดกำลัง sync_all() อยู่แล้ว (_sync_lock ไม่ว่าง) → ข้ามรอบนี้แทนที่จะรอ/ชนกัน
         (ทั้ง sync_all แต่ละรอบเป็น transaction เดียวยาวทั้งลูป เสี่ยง "database is locked"
         หรือยิง SET.or.th ซ้ำ ~931 ตัวสองรอบพร้อมกันถ้าไม่กัน) — รอบถัดไปจะ sync เอง
    """
    if not fs.db_exists(base_dir):
        if callback:
            callback(1, 1, "PE/PBV รายวัน: ยังไม่มี financials.db — ข้าม (Full Refresh/sync งบก่อน)")
        return 0, 0, 0
    if not _sync_lock.acquire(blocking=False):
        logging.info("[set_daily_val] sync_all: มีอีกจุดกำลัง sync อยู่แล้ว — ข้ามรอบนี้")
        if callback:
            callback(1, 1, "PE/PBV รายวัน: มี sync อีกจุดกำลังรันอยู่ — ข้ามรอบนี้")
        return 0, 0, 0

    try:
        from sources.set_api import fetch_historical_trading_batch
        init_table(base_dir)
        all_syms = _th_symbols(base_dir)

        targets = all_syms
        if skip_up_to_date:
            con = _connect(base_dir)
            try:
                latest = _latest_date(base_dir, con)
            finally:
                con.close()
            if latest:
                avail = _probe_latest_available(default=latest) if probe else latest
                if avail < latest:          # โพรบเพี้ยน/ตอบวันเก่า — อย่าถอยหลัง
                    avail = latest
                con = _connect(base_dir)
                try:
                    have = {r[0] for r in con.execute(
                        f"SELECT symbol FROM {TABLE} WHERE date=?", (avail,)).fetchall()}
                finally:
                    con.close()
                targets = [s for s in all_syms if _bare(s) not in have]

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
    finally:
        _sync_lock.release()
