# -*- coding: utf-8 -*-
"""sources/financials_store.py — เก็บงบการเงินฉบับเต็ม (ทุก field) ของทุกหุ้น
ไว้ถาวรใน SQLite ทั้งจาก Yahoo Finance และ SET.or.th

Pattern เดียวกับ sources/sec_store.py: DB แยกไฟล์ต่อโดเมนข้อมูล, table meta
เก็บ timestamp sync ล่าสุด, sync_all() เป็นจุดเข้าเดียวสำหรับ full bulk sync

เก็บเป็น JSON blob ทั้งก้อนต่อ (symbol, source) แทนที่จะ normalize เป็น column
ตายตัว เพราะจำนวน/ชื่อ field ของ Yahoo (~234) และ SET.or.th (~34) ไม่คงที่และ
เปลี่ยนได้ตาม API version — เก็บแบบยืดหยุ่นไว้ปลอดภัยกว่า
"""
import json
import math
import os
import random
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests

from sources.set_api import _bootstrap_headers, _get_json
from sources.dr_universe import _DR_STATIC, load_dr_universe

# project root (โฟลเดอร์แม่ของ sources/) — ใช้หา dr_universe_auto.json ตอน resolve
# yf ticker ของ DR โดยไม่ต้องส่ง base_dir ผ่านทุก caller
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_FILE = "financials.db"


def _db_path(base_dir):
    return os.path.join(base_dir, DB_FILE)


def db_exists(base_dir):
    return os.path.exists(_db_path(base_dir))


def _connect(base_dir):
    con = sqlite3.connect(_db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_db(base_dir):
    con = _connect(base_dir)
    try:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS financials(
              symbol TEXT, source TEXT, payload TEXT, synced_at TEXT,
              PRIMARY KEY(symbol, source)
            );
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS dividends(
              symbol TEXT, market TEXT, ex_date TEXT, dps REAL, synced_at TEXT,
              PRIMARY KEY(symbol, market, ex_date)
            );
            CREATE TABLE IF NOT EXISTS calendar_events(
              symbol TEXT, market TEXT, type TEXT, date TEXT, confidence TEXT,
              source TEXT, detail TEXT, synced_at TEXT,
              PRIMARY KEY(symbol, market, type, date)
            );
            CREATE TABLE IF NOT EXISTS mirror_ondemand(
              symbol TEXT, market TEXT, payload TEXT, synced_at TEXT,
              PRIMARY KEY(symbol, market)
            );
        """)
        con.commit()
    finally:
        con.close()


BACKUP_DIR = "financials_backups"
BACKUP_KEEP = 3   # ลดจาก 5 หลังเพิ่ม mirror ทั้งตลาด — DB โตเป็นหลายร้อย MB ต่อชุด

def backup_db(base_dir):
    """สำรอง financials.db แยกไว้อีกชุดใน financials_backups/ กันไฟล์หลักเสีย
    ใช้ sqlite backup API (ได้สำเนา consistent แม้กำลังถูกเขียนอยู่ — ปลอดภัยกว่า copy ไฟล์ตรงๆ)
    ชื่อไฟล์ตามวันที่ รันซ้ำวันเดียวกันเขียนทับชุดของวันนั้น เก็บล่าสุด 5 ชุดแล้วลบเก่าทิ้ง"""
    if not db_exists(base_dir):
        return None
    bdir = os.path.join(base_dir, BACKUP_DIR)
    os.makedirs(bdir, exist_ok=True)
    dest = os.path.join(bdir, f"financials_{datetime.now().strftime('%Y-%m-%d')}.db")
    src = sqlite3.connect(_db_path(base_dir))
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    files = sorted(f for f in os.listdir(bdir)
                   if f.startswith("financials_") and f.endswith(".db"))
    for f in files[:-BACKUP_KEEP]:
        try:
            os.remove(os.path.join(bdir, f))
        except OSError:
            pass
    return dest


def _get_meta(base_dir, key, default=None):
    if not db_exists(base_dir):
        return default
    con = _connect(base_dir)
    try:
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default
    finally:
        con.close()


def _set_meta(base_dir, key, value):
    con = _connect(base_dir)
    try:
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))
        con.commit()
    finally:
        con.close()


def _dr_key(symbol):
    """namespace symbol ของหุ้น DR แยกจากหุ้นไทยตอนเก็บ/อ่าน DB — กัน symbol ชนกัน
    เช่น 'META' มีทั้งหุ้นไทย mai (META Corporation) และ underlying ของ DR (Meta Platforms
    สหรัฐฯ) ถ้าไม่แยก namespace จะเขียนทับข้อมูลกันไปมาเวลา sync คนละรอบ"""
    return f"DR:{symbol}"


def _get_raw_payload(base_dir, symbol, source):
    """คืน payload ดิบ (ไม่มี synced_at ปน) — ใช้ภายในสำหรับ merge ตอน upsert เท่านั้น"""
    if not db_exists(base_dir):
        return None
    con = _connect(base_dir)
    try:
        row = con.execute(
            "SELECT payload FROM financials WHERE symbol=? AND source=?",
            (symbol, source)).fetchone()
    finally:
        con.close()
    return json.loads(row[0]) if row else None


def _merge_yahoo_payload(old, new):
    """ผสานปี/field ใหม่เข้ากับของเก่า — Yahoo ให้ย้อนหลังแค่ ~4-5 ปีต่อครั้งเสมอ
    ถ้า replace ทั้งก้อนทุกรอบ sync ปีเก่าที่หลุดจาก response รอบใหม่จะหายจาก DB
    ไปเรื่อยๆ ทั้งที่เราต้องการสะสมประวัติไว้เอง"""
    if old is None:
        return new
    merged = dict(new)   # metadata (name/yf/currency/type) ใช้ค่าล่าสุดเสมอ
    for section in ("income", "balance", "cashflow", "ratios", "valuation"):
        merged_section = dict(old.get(section, {}))          # เริ่มจากของเก่าทั้งหมด
        for field, dates in new.get(section, {}).items():
            merged_section[field] = {**merged_section.get(field, {}), **dates}  # ผสาน วันที่ใหม่ทับวันที่ซ้ำ (เผื่อ restatement)
        merged[section] = merged_section
    return merged


def _merge_set_payload(old, new):
    """ผสานปีใหม่เข้ากับของเก่าโดย key เป็นปี — SET.or.th ให้แค่ 4 ปีเต็ม+ไตรมาสล่าสุดเสมอ"""
    if old is None:
        return new
    by_year = {e["year"]: e for e in old.get("entries", [])}
    for e in new.get("entries", []):
        by_year[e["year"]] = e   # ปีใหม่ทับปีเดิมถ้าซ้ำปี (เผื่อ restatement), ปีเก่าที่ไม่อยู่ใน response รอบนี้ยังอยู่
    merged = dict(new)
    merged["entries"] = [by_year[y] for y in sorted(by_year.keys())]
    return merged


def upsert(base_dir, symbol, source, payload, is_dr=False):
    """เขียน payload ลง DB แบบ 'ผสานกับของเก่า' ไม่ใช่เขียนทับทั้งก้อน — สะสม
    ประวัติย้อนหลังไปเรื่อยๆ แม้ Yahoo/SET.or.th จะให้ย้อนหลังจำกัดแค่ ~4-5 ปีต่อครั้งก็ตาม

    is_dr=True เก็บภายใต้ namespace แยก (ดู _dr_key) กัน symbol ชนกับหุ้นไทยที่ชื่อซ้ำ"""
    init_db(base_dir)
    key = _dr_key(symbol) if is_dr else symbol
    old = _get_raw_payload(base_dir, key, source)
    if source in ("yahoo", "yahoo_q", "finnomena_q"):   # โครงสร้างเดียวกัน merge สะสมแบบเดียวกัน
        merged = _merge_yahoo_payload(old, payload)
    elif source == "set":
        merged = _merge_set_payload(old, payload)
    else:
        merged = payload
    con = _connect(base_dir)
    try:
        con.execute(
            "INSERT OR REPLACE INTO financials(symbol, source, payload, synced_at) VALUES (?,?,?,?)",
            (key, source, json.dumps(merged, ensure_ascii=False),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        con.commit()
    finally:
        con.close()


def get_names_bulk(base_dir, prefix, sources=("yahoo_q", "yahoo")):
    """คืน {รหัสดิบ: ชื่อบริษัท} ของทุก symbol ใต้ namespace prefix (เช่น 'FINN:JP:') ที่มีชื่อ
    จริงจาก payload — query เดียวจบ ไม่เปิด connection ต่อ ticker เหมือนเรียก get() วนลูป
    (ใช้เติมชื่อบริษัทให้ <market>_index_metrics.json — ดู
    sources/index_metrics_common.py::_compute_all_rows ตัวที่ dr_universe ไม่ได้ curate ไว้
    ข้อมูลนี้มีอยู่แล้วในเครื่องจาก sync_mirror_yahoo_index ไม่ต้องยิง Yahoo เพิ่ม)
    sources เรียงจากแหล่งที่เชื่อถือได้สุดก่อน (yahoo_q ทับ yahoo ถ้ามีทั้งคู่)"""
    if not db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        placeholders = ",".join("?" * len(sources))
        rows = con.execute(
            f"SELECT symbol, source, payload FROM financials WHERE symbol LIKE ? AND source IN ({placeholders})",
            (prefix + "%", *sources)).fetchall()
    finally:
        con.close()
    rank = {s: i for i, s in enumerate(sources)}
    best = {}   # raw -> (rank, name)
    for symbol, source, payload in rows:
        raw = symbol[len(prefix):]
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            continue
        name = data.get("name")
        if not name or name == raw:
            continue
        cur = best.get(raw)
        if cur is None or rank[source] < cur[0]:
            best[raw] = (rank[source], name)
    return {raw: v[1] for raw, v in best.items()}


def get(base_dir, symbol, source, is_dr=False, market=None):
    if not db_exists(base_dir):
        return None
    key = _dr_key(symbol) if is_dr else symbol
    con = _connect(base_dir)
    try:
        row = con.execute(
            "SELECT payload, synced_at FROM financials WHERE symbol=? AND source=?",
            (key, source)).fetchone()
        # fallback: งบ finnomena_q ที่ไม่มี key รายตัว ให้ไปอ่านจาก mirror ทั้งตลาด
        # (namespace FINN:{ex}:{name}) — ทำให้ screener/หน้างบใช้งบ 16 ปีที่โหลดไว้ได้
        # โดยไม่ต้อง sync รายตัวซ้ำ; ข้าม marker 'ไม่มีงบ' (คืน None เหมือนไม่มีข้อมูล)
        # ส่ง market ต่อเสมอ — กัน _finn_mirror_keys เดาข้าม HK ผิดตอน market='JP'
        # (ดูคอมเมนต์เต็มใน _finn_mirror_keys)
        if row is None and source == "finnomena_q":
            cands = _finn_mirror_keys(symbol, is_dr=is_dr, market=market)
            for mkey in cands:
                mrow = con.execute(
                    "SELECT payload, synced_at FROM financials WHERE symbol=? AND source=?",
                    (mkey, source)).fetchone()
                if mrow:
                    payload = json.loads(mrow[0])
                    if payload.get("empty"):
                        return None
                    row = mrow
                    break
    finally:
        con.close()
    if not row:
        return None
    data = json.loads(row[0])
    data["synced_at"] = row[1]
    return data


def get_meta_summary(base_dir):
    con_exists = db_exists(base_dir)
    last_sync = _get_meta(base_dir, "last_full_sync_at")
    count = 0
    if con_exists:
        con = _connect(base_dir)
        try:
            count = con.execute("SELECT COUNT(DISTINCT symbol) FROM financials").fetchone()[0]
        finally:
            con.close()
    return {"last_synced_at": last_sync, "symbol_count": count}


def get_synced_symbols(base_dir, source, is_dr=False):
    """คืน set ของ symbol ที่มีข้อมูล source นี้อยู่ใน DB แล้ว (ไม่ว่าจะดึงตอน
    bulk sync หรือ fallback สดตอน user ค้นหาเดี่ยวๆ ก็นับว่า 'มีแล้ว')
    is_dr=True คืนเฉพาะ symbol ฝั่ง DR (namespace 'DR:'), False คืนเฉพาะฝั่งหุ้นไทย"""
    if not db_exists(base_dir):
        return set()
    con = _connect(base_dir)
    try:
        rows = con.execute("SELECT symbol FROM financials WHERE source=?", (source,)).fetchall()
    finally:
        con.close()
    if is_dr:
        return {r[0][3:] for r in rows if r[0].startswith("DR:")}
    # ตัด 'FINN:{ex}:{name}' ออกด้วย — namespace นี้เก็บงบ mirror US/HK ไว้ใต้ is_dr=False
    # เช่นกัน (ดู sync_mirror_yahoo_index) เพื่อให้ _factors_for lookup เจอ ไม่ใช่หุ้นไทยจริง
    return {r[0] for r in rows if not r[0].startswith("DR:") and not r[0].startswith("FINN:")}


def get_synced_map(base_dir, is_dr=False):
    """คืน {(symbol, source): synced_at(datetime)} ของ universe ฝั่งนั้น (ไทย หรือ DR)
    ใช้ทำ incremental sync — ข้ามคู่ (หุ้น, แหล่ง) ที่เพิ่งดึงไปไม่นาน"""
    if not db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        rows = con.execute("SELECT symbol, source, synced_at FROM financials").fetchall()
    finally:
        con.close()
    out = {}
    for sym, src, ts in rows:
        if sym.startswith("FINN:"):
            continue   # mirror ทั้งตลาด — ไม่เกี่ยวกับ sync รายตัว
        is_dr_row = sym.startswith("DR:")
        if is_dr_row != is_dr:
            continue
        key = sym[3:] if is_dr_row else sym
        try:
            out[(key, src)] = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            pass   # ไม่มี timestamp = ถือว่าเก่ามาก (ไม่ใส่ map -> จะถูกดึงใหม่)
    return out


def get_coverage(base_dir, symbols, sources=("yahoo", "set"), is_dr=False):
    """เทียบ universe ที่ควรมี (symbols) กับที่มีจริงใน DB ต่อ source
    คืน {source: {"covered": n, "total": n, "missing": [...]}}"""
    symbols = sorted({s.upper().strip().replace(".BK", "") for s in symbols})
    out = {}
    for src in sources:
        have = get_synced_symbols(base_dir, src, is_dr=is_dr)
        missing = [s for s in symbols if s not in have]
        out[src] = {
            "covered": len(symbols) - len(missing),
            "total": len(symbols),
            "missing": missing,
        }
    return out


# ============================================================
# Fetch — Yahoo Finance (ทุก field ไม่กรอง)
# ============================================================

def _df_to_dict_full(df):
    """แปลง DataFrame ของ yfinance เป็น dict {row_label: {date_str: value}} — เก็บทุก field"""
    if df is None:
        return {}
    try:
        if df.empty:
            return {}
    except Exception:
        return {}
    out = {}
    try:
        cols = sorted(df.columns, key=str)
    except Exception:
        cols = list(df.columns)
    for idx in df.index:
        row = {}
        for col in cols:
            try:
                label = col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col)[:10]
                val = df.loc[idx, col]
                fval = float(val)
                if not math.isnan(fval) and not math.isinf(fval):
                    row[label] = fval
            except Exception:
                pass
        if row:
            out[str(idx)] = row
    return out


def fetch_yahoo_full(symbol, is_dr=False, market=None, session=None):
    """ดึงงบการเงินเต็มทุก field จาก Yahoo Finance — คืน dict พร้อมเก็บลง DB

    is_dr=True บอกชัดเจนว่า symbol นี้มาจาก universe หุ้นต่างประเทศ (DR) — ต้องระบุ
    มาจาก caller เท่านั้น ห้าม auto-detect จาก _DR_STATIC เฉยๆ เพราะบาง symbol ชื่อ
    ชนกับหุ้นไทย (เช่น 'META' มีทั้งหุ้นไทย mai และ underlying ของ DR)

    market: ใช้เดา yf ticker เฉพาะตอน is_dr=True แต่ symbol ไม่อยู่ใน DR universe ที่
    curate ไว้ (หุ้น mirror US/HK ทั่วไป) — ไม่งั้นจะพลาดไปดึงเป็นหุ้นไทย .BK (ดู
    resolve_yf_ticker ใน dr_descriptions.py ที่ใช้ logic เดียวกัน)

    session: ใส่ requests.Session() ร่วมกันตอนยิงหลาย ticker พร้อมกัน (เช่น
    sync_mirror_yahoo_index) เพื่อให้ crumb/cookie ใช้ซ้ำชุดเดียว — ไม่งั้น yf.Ticker()
    เปล่าแต่ละครั้งจะขอ crumb ใหม่ของตัวเอง ยิ่งยิง thread ขนานเยอะยิ่งโดน Yahoo
    มองว่าผิดปกติแล้วเพิกถอน crumb ทั้ง IP (ดู fetch_market_caps_parallel ใน
    sources/yahoo.py ที่ใช้ pattern นี้อยู่แล้ว)"""
    import yfinance as yf

    sym = symbol.upper().strip()
    # ใช้ universe รวม (static + auto-sync) — underlying ที่ถูกเพิ่มอัตโนมัติจาก SET
    # ต้อง resolve yf ticker ได้ด้วย ไม่งั้นจะพลาดไปดึงเป็นหุ้นไทย .BK
    dr_entry = next((s for s in load_dr_universe(_PROJECT_ROOT) if s["sym"] == sym), None) if is_dr else None
    if dr_entry:
        yf_ticker, stock_type, stock_name = dr_entry["yf"], "dr", dr_entry["name"]
    elif is_dr and market == "US":
        yf_ticker, stock_type, stock_name = sym, "dr", sym
    elif is_dr and market == "HK":
        yf_ticker, stock_type, stock_name = sym.zfill(4) + ".HK", "dr", sym
    elif is_dr and market == "JP":
        yf_ticker, stock_type, stock_name = sym + ".T", "dr", sym
    else:
        yf_ticker, stock_type, stock_name = sym + ".BK", "set", sym

    t = yf.Ticker(yf_ticker, session=session)

    income = {}
    for attr in ("income_stmt", "financials"):
        income = _df_to_dict_full(getattr(t, attr, None))
        if income:
            break

    balance = _df_to_dict_full(t.balance_sheet)
    cashflow = _df_to_dict_full(t.cashflow)

    if not income and not balance and not cashflow:
        raise ValueError(f"ไม่พบข้อมูลงบการเงินสำหรับ {sym} ({yf_ticker})")

    currency, full_name = "—", stock_name
    try:
        fi = t.fast_info
        currency = getattr(fi, "currency", None) or "—"
    except Exception:
        pass
    try:
        info = t.info
        full_name = info.get("longName") or info.get("shortName") or stock_name
        if currency == "—":
            currency = info.get("financialCurrency") or info.get("currency") or "—"
    except Exception:
        pass

    return {
        "sym": sym, "yf": yf_ticker, "name": full_name,
        "type": stock_type, "currency": currency,
        "income": income, "balance": balance, "cashflow": cashflow,
    }


def fetch_yahoo_quarterly(symbol, is_dr=False, market=None, session=None):
    """ดึงงบการเงิน 'รายไตรมาส' เต็มทุก field จาก Yahoo — โครงสร้างเดียวกับ fetch_yahoo_full
    (section -> field -> {วันสิ้นงวด: ค่า}) เก็บใต้ source 'yahoo_q' และ merge สะสม
    ได้ด้วยกลไกเดิม — Yahoo ให้ครั้งละ ~5-6 ไตรมาส สะสมทุกรอบ sync ประวัติจะยาวขึ้นเอง
    หมายเหตุ: HK/EU รายงานครึ่งปีตามกฎตลาด, JP มักได้ไม่ครบ — ได้เท่าที่ตลาดนั้นมีจริง

    market: ดูคอมเมนต์ใน fetch_yahoo_full — เดา yf ticker ตอนไม่อยู่ใน DR universe
    session: ดูคอมเมนต์ session ใน fetch_yahoo_full — ใส่ตอนยิงหลาย ticker พร้อมกัน"""
    import yfinance as yf

    sym = symbol.upper().strip()
    dr_entry = next((s for s in load_dr_universe(_PROJECT_ROOT) if s["sym"] == sym), None) if is_dr else None
    if dr_entry:
        yf_ticker, stock_type, stock_name = dr_entry["yf"], "dr", dr_entry["name"]
    elif is_dr and market == "US":
        yf_ticker, stock_type, stock_name = sym, "dr", sym
    elif is_dr and market == "HK":
        yf_ticker, stock_type, stock_name = sym.zfill(4) + ".HK", "dr", sym
    elif is_dr and market == "JP":
        yf_ticker, stock_type, stock_name = sym + ".T", "dr", sym
    else:
        yf_ticker, stock_type, stock_name = sym + ".BK", "set", sym

    t = yf.Ticker(yf_ticker, session=session)

    income = {}
    for attr in ("quarterly_income_stmt", "quarterly_financials"):
        income = _df_to_dict_full(getattr(t, attr, None))
        if income:
            break
    balance  = _df_to_dict_full(t.quarterly_balance_sheet)
    cashflow = _df_to_dict_full(t.quarterly_cashflow)

    if not income and not balance and not cashflow:
        raise ValueError(f"ไม่พบงบรายไตรมาสสำหรับ {sym} ({yf_ticker})")

    currency, full_name = "—", stock_name
    try:
        fi = t.fast_info
        currency = getattr(fi, "currency", None) or "—"
    except Exception:
        pass

    return {
        "sym": sym, "yf": yf_ticker, "name": full_name,
        "type": stock_type, "currency": currency, "period": "quarter",
        "income": income, "balance": balance, "cashflow": cashflow,
    }


def fetch_dividends(sym, market=None, session=None):
    """ดึงประวัติปันผล (ex-date -> DPS) จาก yfinance ย้อนสูงสุดที่ Yahoo มี (มักเกิน 20 ปี
    สำหรับหุ้นไทย) — ใช้ ticker เดียวกับ resolve_yf_ticker (dr_descriptions.py) เพื่อให้
    TH/US/HK resolve เป็น ticker เดียวกับที่หน้าอื่นในโปรเจกต์ใช้อยู่แล้ว
    yfinance ปรับ dividends ตาม split ให้อัตโนมัติแล้ว (ต่างจาก EPS ของ Finnomena ที่ไม่ปรับ
    — ดูคอมเมนต์ใน dividend_stats.py ตอนคำนวณ payout ratio)
    session: ดูคอมเมนต์ session ใน fetch_yahoo_full — ใส่ตอนยิงหลาย ticker พร้อมกัน
    คืน list [{ "ex_date": "YYYY-MM-DD", "dps": float }, ...] เรียงตามวันที่"""
    import yfinance as yf
    from sources.dr_descriptions import resolve_yf_ticker

    sym = sym.upper().strip()
    yf_ticker, _is_etf = resolve_yf_ticker(_PROJECT_ROOT, sym, market=market)
    if not yf_ticker:
        yf_ticker = sym + ".BK"

    series = yf.Ticker(yf_ticker, session=session).dividends
    if series is None or series.empty:
        return []
    out = []
    for ts, dps in series.items():
        if dps is None or float(dps) <= 0:
            continue
        out.append({"ex_date": ts.strftime("%Y-%m-%d"), "dps": round(float(dps), 6)})
    out.sort(key=lambda r: r["ex_date"])
    return out


def _dividends_meta_key(sym, market):
    return f"dividends_synced:{market}:{sym}"


def save_dividends(base_dir, sym, market, rows):
    """เก็บ/merge ประวัติปันผลลง DB (upsert ทีละแถวตาม ex_date — ไม่ลบของเก่าที่ยัง valid)
    เก็บ synced_at ต่อ symbol ลงตาราง meta เสมอ **แม้ rows ว่าง** (หุ้นที่ไม่เคยจ่ายปันผลเลยเป็น
    เรื่องปกติ ไม่ใช่ไม่เคย sync) — เหมือนบั๊กที่แก้ไปแล้วใน save_calendar_events (ดู
    _calendar_meta_key) ไม่งั้น get_dividends จะมองว่า "ไม่เคย sync" ตลอดกาลแล้วดึงสดซ้ำทุกครั้ง"""
    init_db(base_dir)
    sym = sym.upper().strip()
    market = (market or "TH").upper()
    con = _connect(base_dir)
    try:
        now = datetime.now().isoformat()
        if rows:
            con.executemany(
                "INSERT INTO dividends(symbol, market, ex_date, dps, synced_at) VALUES (?,?,?,?,?) "
                "ON CONFLICT(symbol, market, ex_date) DO UPDATE SET dps=excluded.dps, synced_at=excluded.synced_at",
                [(sym, market, r["ex_date"], r["dps"], now) for r in rows]
            )
        con.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_dividends_meta_key(sym, market), now)
        )
        con.commit()
    finally:
        con.close()


def get_dividends(base_dir, sym, market=None):
    """คืน (rows, synced_at ล่าสุด) ของหุ้นตัวหนึ่งจาก DB local
    คืน (None, None) ต่อเมื่อ 'ไม่เคย sync จริงๆ' เท่านั้น — ถ้า sync แล้วแต่ไม่มีปันผลเลย
    คืน ([], synced_at) เพื่อไม่ให้ caller ดึงสดซ้ำ (synced_at มาจาก meta table ไม่ใช่ max(rows)
    เพราะ rows อาจว่างตอนหุ้นไม่เคยจ่ายปันผล)"""
    if not db_exists(base_dir):
        return None, None
    init_db(base_dir)   # กัน DB เก่าที่ยังไม่มีตาราง dividends (เพิ่มเข้ามาทีหลัง financials/meta)
    sym = sym.upper().strip()
    market = (market or "TH").upper()
    con = _connect(base_dir)
    try:
        rows = con.execute(
            "SELECT ex_date, dps, synced_at FROM dividends WHERE symbol=? AND market=? ORDER BY ex_date",
            (sym, market)).fetchall()
        meta_row = con.execute(
            "SELECT value FROM meta WHERE key=?", (_dividends_meta_key(sym, market),)).fetchone()
    finally:
        con.close()
    synced_at = meta_row[0] if meta_row else (max(r[2] for r in rows) if rows else None)
    if not rows and synced_at is None:
        return None, None
    return [{"ex_date": r[0], "dps": r[1]} for r in rows], synced_at


def _fetch_set_corporate_actions(sym):
    """XD/XM/XB ที่ประกาศทางการจาก SET.or.th — /api/set/stock/<sym>/corporate-action
    (internal API เดียวกับ set_api.py, ไม่มีสัญญา — พบตอนสำรวจ 2026-07-20) หุ้นไทยเท่านั้น
    แปลงเป็น calendar_events แถวเดียวต่อ 1 เหตุการณ์ — ข้อมูลนี้เป็นของจริงที่ประกาศแล้วเสมอ
    (ไม่ใช่ประมาณการ) จึงถือว่า confidence='confirmed' เสมอ ต่างจาก earnings ที่มาจาก yfinance
    (ดู fetch_earnings_calendar)

    caType='XD' (เงินปันผล/สิทธิผู้ถือหุ้นทั่วไป) -> 'xd' (ex-date) + 'pay' (payment date ถ้ามี)
    caType='XM' (นัดประชุมผู้ถือหุ้น) -> 'xm' ที่ meetingDate (วันประชุมจริง — สำคัญกว่า xdate/
    record date ที่เป็นแค่วันตัดสิทธิ์เข้าประชุม ไม่ใช่วันที่ต้องทำอะไรจริง) ข้ามถ้าไม่มี meetingDate
    caType='XB' (สิทธิจองซื้อหุ้นเพิ่มทุน/บริษัทในเครือ) -> 'xb' ที่ xdate (ex-rights date — ต้องถือ
    หุ้นก่อนวันนี้ถึงจะได้สิทธิ์) + 'xb_pay' ที่ paymentDate ถ้ามี (วันชำระค่าจองซื้อ — แยก type จาก
    'pay' ของ XD เพราะ key ซ้ำ (symbol,market,type,date) จะชนกันถ้าวันที่บังเอิญตรงกัน)"""
    from sources.set_api import _bootstrap_headers, _get_json
    import urllib.parse

    ctx, hdr = _bootstrap_headers()
    d = _get_json(ctx, hdr, f"/api/set/stock/{urllib.parse.quote(sym)}/corporate-action")
    out = []
    for row in (d or []):
        ca = row.get("caType")
        if ca == "XD":
            dps = row.get("dividend")
            detail = f"เงินปันผล {dps} บาท/หุ้น" if dps is not None else "สิทธิประโยชน์ XD"
            xdate = (row.get("xdate") or "")[:10]
            if xdate:
                out.append({"type": "xd", "date": xdate, "confidence": "confirmed",
                            "source": "set.or.th", "detail": detail})
            pay_date = (row.get("paymentDate") or "")[:10]
            if pay_date:
                out.append({"type": "pay", "date": pay_date, "confidence": "confirmed",
                            "source": "set.or.th", "detail": detail})
        elif ca == "XM":
            meeting_date = (row.get("meetingDate") or "")[:10]
            if not meeting_date:
                continue   # ไม่มีวันประชุมจริง = ยังไม่มีประโยชน์ต่อปฏิทิน
            agenda = (row.get("agenda") or "").strip()
            if len(agenda) > 80:
                agenda = agenda[:80] + "…"
            detail = (row.get("meetingType") or "ประชุมผู้ถือหุ้น") + (f" — {agenda}" if agenda else "")
            out.append({"type": "xm", "date": meeting_date, "confidence": "confirmed",
                        "source": "set.or.th", "detail": detail})
        elif ca == "XB":
            ratio = row.get("ratio")
            detail = row.get("benefitType") or "สิทธิจองซื้อหลักทรัพย์"
            if ratio:
                detail += f" (อัตราส่วน {ratio})"
            xdate = (row.get("xdate") or "")[:10]
            if xdate:
                out.append({"type": "xb", "date": xdate, "confidence": "confirmed",
                            "source": "set.or.th", "detail": detail})
            pay_date = (row.get("paymentDate") or "")[:10]
            if pay_date:
                out.append({"type": "xb_pay", "date": pay_date, "confidence": "confirmed",
                            "source": "set.or.th", "detail": "ชำระค่าจองซื้อ — " + detail})
    return out


def fetch_earnings_calendar(sym, market=None):
    """วันประกาศงบล่วงหน้าจาก yfinance get_earnings_dates — ใช้ได้ทั้ง 3 ตลาด แต่หุ้นไทย
    ตัวเล็กมักไม่มี/คลาดเคลื่อน จึงติด confidence='estimated' เสมอ (ต่างจาก XD ของ SET.or.th
    ที่เป็น confirmed) เก็บเฉพาะแถวที่ยังไม่มี Reported EPS (แปลว่ายังไม่ประกาศ = อนาคต)"""
    import math
    import yfinance as yf
    from sources.dr_descriptions import resolve_yf_ticker

    sym = sym.upper().strip()
    yf_ticker, _is_etf = resolve_yf_ticker(_PROJECT_ROOT, sym, market=market)
    if not yf_ticker:
        yf_ticker = sym + ".BK"

    out = []
    try:
        df = yf.Ticker(yf_ticker).get_earnings_dates(limit=8)
    except Exception:
        return out
    if df is None or df.empty:
        return out
    for ts, row in df.iterrows():
        reported = row.get("Reported EPS")
        if reported is not None and not (isinstance(reported, float) and math.isnan(reported)):
            continue   # มีผลจริงแล้ว = อดีต ไม่ใช่ปฏิทินล่วงหน้า
        est = row.get("EPS Estimate")
        detail = f"คาดการณ์ EPS {round(float(est), 2)}" if est is not None and not (isinstance(est, float) and math.isnan(est)) else "วันประกาศงบ (ประมาณการ)"
        out.append({"type": "earnings", "date": ts.strftime("%Y-%m-%d"), "confidence": "estimated",
                    "source": "yahoo", "detail": detail})
    return out


def fetch_calendar_events(sym, market=None):
    """รวม XD/pay (SET.or.th, หุ้นไทยเท่านั้น) + earnings (yfinance, ทุกตลาด) เป็นชุดเดียว
    ไม่ throw ถ้าแหล่งใดแหล่งหนึ่งพัง — คืนเท่าที่ดึงได้ (ปฏิทินไม่ควร fail ทั้งก้อนเพราะแหล่ง
    เดียวล่ม)"""
    mkt = (market or "TH").upper()
    events = []
    if mkt in ("TH", "SET"):
        try:
            events.extend(_fetch_set_corporate_actions(sym.upper().strip()))
        except Exception as e:
            print(f"[Calendar] SET corporate-action ล้มเหลวสำหรับ {sym}: {e}")
    try:
        events.extend(fetch_earnings_calendar(sym, market=mkt))
    except Exception as e:
        print(f"[Calendar] yfinance earnings ล้มเหลวสำหรับ {sym}: {e}")
    return events


def _calendar_meta_key(sym, market):
    return f"calendar_synced:{market}:{sym}"


def save_calendar_events(base_dir, sym, market, rows):
    """เก็บ/merge เหตุการณ์ปฏิทินลง DB (upsert ตาม symbol+market+type+date)
    เก็บ synced_at ต่อ symbol ลงตาราง meta เสมอ **แม้ rows ว่าง** (ไม่มี event ในอนาคตเป็นเรื่อง
    ปกติ ไม่ใช่ไม่เคย sync) — ไม่งั้น get_calendar_events จะมองว่า "ไม่เคย sync" ตลอดกาลแล้วดึงสด
    ซ้ำทุกครั้งที่เปิดหน้า Calendar (พบจากรีวิวโค้ด 2026-07-20 — เสี่ยงโดน rate limit เพราะ
    Promise.all ยิงทั้ง watchlist พร้อมกัน)"""
    init_db(base_dir)
    sym = sym.upper().strip()
    market = (market or "TH").upper()
    con = _connect(base_dir)
    try:
        now = datetime.now().isoformat()
        if rows:
            con.executemany(
                "INSERT INTO calendar_events(symbol, market, type, date, confidence, source, detail, synced_at) "
                "VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(symbol, market, type, date) DO UPDATE SET "
                "confidence=excluded.confidence, source=excluded.source, detail=excluded.detail, synced_at=excluded.synced_at",
                [(sym, market, r["type"], r["date"], r["confidence"], r["source"], r.get("detail"), now) for r in rows]
            )
        con.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_calendar_meta_key(sym, market), now)
        )
        con.commit()
    finally:
        con.close()


def get_all_calendar_events(base_dir, from_date=None, market=None):
    """คืน event ปฏิทินทั้งหมดที่เคย sync ไว้ใน local DB ข้ามทุกหุ้น (ไม่ใช่แค่ watchlist) —
    ใช้กับตัวกรอง "ทั้งหมดที่มีข้อมูล"/"ตามตลาด" ในหน้า Calendar อ่านจาก cache เท่านั้น ไม่ fetch สด
    (fetch สดทีละหุ้นทำที่ get_calendar_events/fetch_calendar_events ผ่าน endpoint ปกติ)"""
    if not db_exists(base_dir):
        return []
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        q = "SELECT symbol, market, type, date, confidence, source, detail FROM calendar_events WHERE 1=1"
        params = []
        if from_date:
            q += " AND date>=?"
            params.append(from_date)
        if market:
            q += " AND market=?"
            params.append(market.upper())
        q += " ORDER BY date"
        rows = con.execute(q, params).fetchall()
    finally:
        con.close()
    return [{"symbol": s, "market": m, "type": t, "date": d, "confidence": c, "source": src, "detail": det}
            for s, m, t, d, c, src, det in rows]


def get_calendar_events(base_dir, sym, market=None, from_date=None):
    """คืน (rows, synced_at ล่าสุด) ของหุ้นตัวหนึ่งจาก DB local — default กรองเฉพาะวันที่ >=
    from_date (ปฏิทินย้อนหลังไม่มีประโยชน์ ต่างจากประวัติปันผลที่ต้องเก็บย้อนยาว)
    คืน (None, None) ต่อเมื่อ 'ไม่เคย sync จริงๆ' เท่านั้น — ถ้า sync แล้วแต่ไม่มี event ในอนาคต
    คืน ([], synced_at) เพื่อไม่ให้ caller ดึงสดซ้ำ (ดู synced_at มาจาก meta table ไม่ใช่ max(rows)
    เพราะ rows อาจว่างตอนไม่มี event เลย)"""
    if not db_exists(base_dir):
        return None, None
    init_db(base_dir)
    sym = sym.upper().strip()
    market = (market or "TH").upper()
    con = _connect(base_dir)
    try:
        if from_date:
            rows = con.execute(
                "SELECT type, date, confidence, source, detail FROM calendar_events "
                "WHERE symbol=? AND market=? AND date>=? ORDER BY date",
                (sym, market, from_date)).fetchall()
        else:
            rows = con.execute(
                "SELECT type, date, confidence, source, detail FROM calendar_events "
                "WHERE symbol=? AND market=? ORDER BY date",
                (sym, market)).fetchall()
        meta_row = con.execute(
            "SELECT value FROM meta WHERE key=?", (_calendar_meta_key(sym, market),)).fetchone()
    finally:
        con.close()
    synced_at = meta_row[0] if meta_row else None
    if not rows and synced_at is None:
        return None, None
    return [{"type": r[0], "date": r[1], "confidence": r[2], "source": r[3], "detail": r[4]} for r in rows], synced_at


def save_mirror_ondemand(base_dir, sym, market, payload):
    """เก็บ header (ราคา/return/RS/stage/sector ฯลฯ) ของหุ้น mirror US/HK นอกดัชนีหลักที่ดึงแบบ
    on-demand ตอนเปิด Tearsheet — cache กัน fetch Yahoo ซ้ำทุกครั้งที่เปิดหน้าเดิมวันเดียวกัน
    (ดู sources/mirror_ondemand.py) upsert ทับของเดิมเสมอ (ไม่เก็บประวัติ ต่างจาก dividends/
    calendar_events)"""
    init_db(base_dir)
    sym = sym.upper().strip()
    market = market.upper()
    con = _connect(base_dir)
    try:
        now = datetime.now().isoformat()
        con.execute(
            "INSERT INTO mirror_ondemand(symbol, market, payload, synced_at) VALUES (?,?,?,?) "
            "ON CONFLICT(symbol, market) DO UPDATE SET payload=excluded.payload, synced_at=excluded.synced_at",
            (sym, market, json.dumps(payload, ensure_ascii=False), now))
        con.commit()
    finally:
        con.close()


def get_mirror_ondemand(base_dir, sym, market, stale_days=1):
    """คืน (payload, stale) ของหุ้น mirror on-demand ที่เคย cache ไว้ — (None, True) ถ้าไม่เคย
    cache เลย stale=True เมื่อ synced_at เก่าเกิน stale_days (ราคาต้องสดกว่า calendar/dividends
    เพราะผู้ใช้เปิดดูตรงๆ คาดหวังราคาวันนี้)"""
    if not db_exists(base_dir):
        return None, True
    init_db(base_dir)
    sym = sym.upper().strip()
    market = market.upper()
    con = _connect(base_dir)
    try:
        row = con.execute(
            "SELECT payload, synced_at FROM mirror_ondemand WHERE symbol=? AND market=?",
            (sym, market)).fetchone()
    finally:
        con.close()
    if not row:
        return None, True
    payload, synced_at = row
    stale = True
    try:
        stale = (datetime.now() - datetime.fromisoformat(synced_at)) > timedelta(days=stale_days)
    except ValueError:
        stale = True
    return json.loads(payload), stale


def compute_quarterly_growth(payload_yahoo_q):
    """การเติบโตรายไตรมาสจากงบ quarterly (source 'yahoo_q'):
    - rev_qoq / profit_qoq  : ไตรมาสล่าสุด vs ไตรมาสก่อนหน้า (%)
    - rev_yoy_q / profit_yoy_q: ไตรมาสล่าสุด vs ไตรมาสเดียวกันของปีก่อน (%)
      (ตัวหลังกัน seasonality — ธุรกิจจำนวนมากมี high/low season ทำให้ QoQ เพี้ยนตามฤดู)
    ฐานติดลบ/ศูนย์ -> None (เปอร์เซ็นต์ไม่มีความหมาย เช่นพลิกจากขาดทุน)"""
    inc = (payload_yahoo_q or {}).get("income", {})

    # ฐานที่เกือบศูนย์ทำให้ % โตพุ่งมหาศาลจนไร้ความหมาย (เจอบ่อยในหุ้น OTC/ข้อมูลเพี้ยน)
    # cap ที่ ±3000% -> None: กัน outlier ทำลายการเรียงใน screener แต่ยังเก็บ turnaround
    # จริง (เช่นกำไรโต ~1800%) ไว้ได้
    _GROWTH_CAP = 3000.0

    def _cap(pct):
        return None if (pct is None or abs(pct) > _GROWTH_CAP) else pct

    def series(*names):
        for n in names:
            row = inc.get(n)
            if row:
                return sorted((d, v) for d, v in row.items() if v is not None)
        return []

    def qoq(vals):
        if len(vals) < 2:
            return None
        prev, last = vals[-2][1], vals[-1][1]
        if not prev or prev <= 0 or last is None:
            return None
        return _cap(round((last - prev) / prev * 100, 2))

    def yoy(vals):
        """หาไตรมาสที่ห่างจากงวดล่าสุด ~1 ปี (330-400 วัน) — ทนกรณีไตรมาสขาดหาย"""
        if len(vals) < 2:
            return None
        from datetime import date
        try:
            ld = date.fromisoformat(vals[-1][0][:10])
        except Exception:
            return None
        base = None
        for d, v in vals[:-1]:
            try:
                gap = (ld - date.fromisoformat(d[:10])).days
            except Exception:
                continue
            if 330 <= gap <= 400:
                base = v
        last = vals[-1][1]
        if not base or base <= 0 or last is None:
            return None
        return _cap(round((last - base) / base * 100, 2))

    def rising_streak(vals):
        """นับจำนวน 'ก้าว' ติดต่อกันจากงวดล่าสุดย้อนหลัง ที่ค่าเพิ่มขึ้นทุกงวด
        เช่นค่า [8,9,10,12] -> 3 (โต 3 ไตรมาสติด) — 0 = งวดล่าสุดไม่โต"""
        n = 0
        for i in range(len(vals) - 1, 0, -1):
            a, b = vals[i - 1][1], vals[i][1]
            if a is None or b is None or b <= a:
                break
            n += 1
        return n

    def yoy_series(vals):
        """คำนวณ YoY-Q (%) ของทุกงวดที่หา 'ไตรมาสเดียวกันปีก่อน' ได้ — คืน list เรียงเก่า→ใหม่"""
        from datetime import date
        out = []
        for i in range(len(vals)):
            try:
                di = date.fromisoformat(vals[i][0][:10])
            except Exception:
                continue
            base = None
            for d, v in vals[:i]:
                try:
                    gap = (di - date.fromisoformat(d[:10])).days
                except Exception:
                    continue
                if 330 <= gap <= 400:
                    base = v
            last = vals[i][1]
            if base and base > 0 and last is not None:
                out.append((vals[i][0], round((last - base) / base * 100, 2)))
        return out

    def accel_streak(vals):
        """การเร่งตัว (CANSLIM): นับไตรมาสติดกันจากงวดล่าสุด ที่ %YoY-Q สูงขึ้นกว่างวดก่อน
        (โตเร็วขึ้นเรื่อยๆ ไม่ใช่แค่โต) — ต้องสะสมงบไตรมาสหลายงวดก่อนค่าจึงจะเริ่มขึ้น"""
        return rising_streak(yoy_series(vals))

    rev = series("Total Revenue", "Operating Revenue")
    ni  = series("Net Income", "Net Income Common Stockholders")

    # Net margin รายไตรมาส (NI/Rev เป็น %) — จับคู่เฉพาะงวดที่มีทั้งคู่และรายได้ > 0
    rev_map = dict(rev)
    margin = [(d, round(v / rev_map[d] * 100, 2)) for d, v in ni
              if d in rev_map and rev_map[d] and rev_map[d] > 0]

    def margin_yoyq_delta():
        """margin งวดล่าสุด − margin ไตรมาสเดียวกันปีก่อน (จุด% — เทียบค่าตรงๆ ไม่ใช่ % growth)"""
        if len(margin) < 2:
            return None
        from datetime import date
        try:
            ld = date.fromisoformat(margin[-1][0][:10])
        except Exception:
            return None
        base = None
        for d, v in margin[:-1]:
            try:
                gap = (ld - date.fromisoformat(d[:10])).days
            except Exception:
                continue
            if 330 <= gap <= 400:
                base = v
        return round(margin[-1][1] - base, 2) if base is not None else None

    def ttm_margin_delta():
        """TTM margin (4 ไตรมาสล่าสุด) − TTM ชุดก่อนหน้า (จุด%) — ต้องมี ≥8 ไตรมาส
        (เริ่มมีค่าเองเมื่อสะสมงบไตรมาสครบ — Yahoo ให้ครั้งละ ~5-6)"""
        common = sorted(set(rev_map) & {d for d, _ in ni})
        if len(common) < 8:
            return None
        ni_map = dict(ni)
        last4, prev4 = common[-4:], common[-8:-4]
        def m(ds):
            r = sum(rev_map[d] for d in ds)
            return round(sum(ni_map[d] for d in ds) / r * 100, 2) if r > 0 else None
        a, b = m(last4), m(prev4)
        return round(a - b, 2) if (a is not None and b is not None) else None

    return {
        "rev_qoq": qoq(rev), "profit_qoq": qoq(ni),
        "rev_yoy_q": yoy(rev), "profit_yoy_q": yoy(ni),
        "rev_qoq_streak": rising_streak(rev),          # รายได้โตติดต่อกันกี่ไตรมาส
        "profit_qoq_streak": rising_streak(ni),        # กำไรโตติดต่อกันกี่ไตรมาส
        "margin_qoq_streak": rising_streak(margin),    # net margin เพิ่มติดต่อกันกี่ไตรมาส
        "rev_accel_streak": accel_streak(rev),         # YoY-Q รายได้เร่งขึ้นติดกันกี่ไตรมาส
        "profit_accel_streak": accel_streak(ni),       # YoY-Q กำไรเร่งขึ้นติดกันกี่ไตรมาส
        "margin_yoyq_delta": margin_yoyq_delta(),      # margin เทียบไตรมาสเดียวกันปีก่อน (จุด%)
        "ttm_margin_delta": ttm_margin_delta(),        # TTM margin ขยาย (จุด%) — รอสะสม 8 ไตรมาส
        "latest_quarter": (rev[-1][0][:10] if rev else (ni[-1][0][:10] if ni else None)),
        "quarters_available": max(len(rev), len(ni)) if (rev or ni) else 0,
    }


# ============================================================
# Fetch — SET.or.th (ทุก field จาก company-highlight)
# ============================================================

def fetch_set_full(symbol, ctx=None, hdr=None):
    """ดึงงบการเงินเต็มจาก SET.or.th company-highlight — คืน dict พร้อมเก็บลง DB
    ctx/hdr: ส่งจากภายนอกได้เพื่อ reuse cookie เดียวกันข้ามหลาย request (bulk sync)"""
    sym = symbol.upper().strip()
    if sym.endswith(".BK"):
        sym = sym[:-3]
    if ctx is None or hdr is None:
        ctx, hdr = _bootstrap_headers()
    data = _get_json(ctx, hdr, f"/api/set/stock/{sym}/company-highlight?lang=th")
    if not data:
        raise ValueError(f"ไม่พบข้อมูลงบการเงินสำหรับ {sym} จาก SET.or.th")
    entries = [x["financialData"] for x in data if x.get("financialData")]
    if not entries:
        raise ValueError(f"ไม่พบข้อมูลงบการเงินสำหรับ {sym} จาก SET.or.th")
    return {"sym": sym, "entries": entries}


# ============================================================
# Fetch — Finnomena (งบรายไตรมาสย้อนยาว ~20 ปี / ~80 ไตรมาส)
# ครอบคลุม: หุ้นไทยทั้งตลาด (SET+mai) + หุ้น US ทั้งตลาด + หุ้นฮ่องกง
# — ใช้เป็นแหล่ง backfill ประวัติยาว ส่วน Yahoo (yahoo_q) เป็นตัวอัพเดท
# ต่อเนื่อง + ตรวจเทียบ (API ภายในของ Finnomena อาจเปลี่ยนได้ในอนาคต
# แต่ข้อมูลที่ merge เก็บใน DB แล้วอยู่ถาวรไม่หายตามแหล่ง)
# ============================================================

_FINN_BASE = "https://www.finnomena.com/market-info/api/public"
# เวอร์ชันโครง payload ของ finnomena_q — ขยับเมื่อเพิ่ม field ที่เก็บ แล้ว mirror
# รอบถัดไปจะ re-fetch ตัวที่เป็นโครงเก่าให้เอง (ตัวที่บันทึกว่า 'ไม่มีงบ' ไม่ยิงซ้ำ)
# v2: เพิ่ม section ratios (GPM/NPM/ROA/ROE/DE/SGA%/Cash Cycle) + valuation
#     (Close/MktCap/PE/PBV/EV-EBITDA/DivYield/BVPS) + D&A ใน cashflow
FINN_SCHEMA = 2
_finn_ids = {}          # (exchange, name) -> security_id จาก /stock/list
_finn_ids_lock = threading.Lock()


def _finn_get(path, timeout=30):
    import urllib.request as _ur
    from core.net import ssl_context
    ctx = ssl_context()
    hdr = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0",
           "Accept": "application/json",
           "Referer": "https://www.finnomena.com/stock/"}
    with _ur.urlopen(_ur.Request(_FINN_BASE + path, headers=hdr),
                     context=ctx, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def _finn_load_ids():
    """โหลด mapping (ตลาด, ชื่อหุ้น) -> security_id ทั้ง 3 ตลาดครั้งเดียวต่อโปรเซส
    (~35,600 ตัว, 3 requests) — ตอน bulk sync จะได้ยิงเฉพาะ /stock/summary ต่อหุ้น"""
    with _finn_ids_lock:
        if _finn_ids:
            return _finn_ids
        for ex in ("TH", "US", "HK"):
            d = _finn_get(f"/stock/list?exchange={ex}", timeout=90)
            for row in d.get("data") or []:
                nm = (row.get("name") or "").upper()
                if nm and row.get("security_id"):
                    _finn_ids[(ex, nm)] = row["security_id"]
        return _finn_ids


def _finn_resolve(symbol, is_dr=False):
    """หา (exchange, ชื่อใน Finnomena, ชื่อบริษัท, ชนิด) ของ symbol — raise ถ้าตลาดไม่รองรับ"""
    sym = symbol.upper().strip()
    if not is_dr:
        return "TH", sym, sym, "set"
    e = next((s for s in load_dr_universe(_PROJECT_ROOT) if s["sym"] == sym), None)
    if not e:
        raise ValueError(f"{sym} ไม่อยู่ใน DR universe")
    yf_t = (e.get("yf") or "").upper()
    if "." not in yf_t:
        return "US", yf_t, e["name"], "dr"          # BRK-B ใช้รูปขีดกลางตรงกับ Finnomena อยู่แล้ว
    if yf_t.endswith(".HK"):
        return "HK", yf_t[:-3], e["name"], "dr"
    raise ValueError(f"{sym} ({yf_t}) อยู่ตลาดที่ Finnomena ไม่มีข้อมูล (มีเฉพาะ TH/US/HK)")


def _finn_mirror_keys(symbol, is_dr=False, market=None):
    """คืน list ของ FINN:{ex}:{name} ที่ 'อาจจะ' เป็น key ใน mirror ของ symbol นี้
    (ดู _mirror_key / mirror_finnomena) — ใช้ให้ get(source='finnomena_q') fallback
    ไปอ่านงบจาก mirror ทั้งตลาดได้ โดยไม่ต้อง sync finnomena_q รายตัวซ้ำ

    HK ในรายชื่อ Finnomena มีทั้งแบบเติม 0 นำหน้าและไม่เติม — คืนหลายตัวเลือกให้ลองครบ
    คืน [] ถ้าตลาดไม่รองรับ (ETF / นอก TH-US-HK) — เหมือน _finn_resolve ที่ raise

    market='JP': คืน [] เสมอโดยไม่เดา — Finnomena ไม่มีข้อมูลตลาดญี่ปุ่นเลย (รองรับแค่
    TH/US/HK) แต่รหัสหุ้น JP เป็นตัวเลข 4 หลักเหมือนหุ้น HK พอดี (พิสูจน์แล้วว่าชนกันจริง
    40 ตัวใน Nikkei 225 เช่น 9983/8316/6098) ถ้าไม่กันไว้ตรงนี้ branch 'ไม่อยู่ใน DR
    universe' ด้านล่างจะเดาลอง FINN:HK:{รหัส} แล้วเจอข้อมูลจริงของ 'หุ้น HK คนละตัว'
    พอดี ทำให้หน้างบการเงินโชว์งบบริษัท HK ผิดตัวแทนโดยไม่มี error ใดๆ เตือนเลย"""
    if market == "JP":
        return []
    sym = symbol.upper().strip()
    in_universe = True
    if is_dr:   # ETF ไม่มีงบการเงิน — กันเผลอดึง FINN key ของตราสารอื่นที่ชื่อชนกัน
        e = next((s for s in load_dr_universe(_PROJECT_ROOT) if s["sym"] == sym), None)
        if e and e.get("etf"):
            return []
        in_universe = e is not None

    keys = []
    if in_universe:
        try:
            ex, name, _stock_name, _stype = _finn_resolve(sym, is_dr=is_dr)
            keys.append(_mirror_key(ex, name))
            if ex == "HK":
                for alt in (name.lstrip("0") or name, name.zfill(4)):
                    k = _mirror_key(ex, alt)
                    if k not in keys:
                        keys.append(k)
        except ValueError:
            pass
    else:
        # is_dr แต่ไม่อยู่ใน DR universe = หุ้น mirror US/HK ทั่วไป (นอกพอร์ต) — ลอง key ตรงๆ
        keys += [_mirror_key("US", sym), _mirror_key("HK", sym)]
        if sym.isdigit():   # รหัสหุ้น HK แบบตัวเลข: ลองทั้งเติม/ตัด 0 นำหน้า
            for alt in (sym.lstrip("0") or sym, sym.zfill(4)):
                k = _mirror_key("HK", alt)
                if k not in keys:
                    keys.append(k)
    return keys


def finnomena_supported(symbol, is_dr=False):
    """เช็คก่อน sync ว่า symbol นี้ดึงจาก Finnomena ได้ไหม — ETF และตลาดนอก TH/US/HK ไม่ได้"""
    try:
        sym = symbol.upper().strip()
        if is_dr:
            e = next((s for s in load_dr_universe(_PROJECT_ROOT) if s["sym"] == sym), None)
            if not e or e.get("etf"):
                return False   # ETF ไม่มีงบการเงิน และส่วนใหญ่ไม่อยู่ใน Finnomena
        _finn_resolve(sym, is_dr=is_dr)
        return True
    except ValueError:
        return False


def _finn_map_rows(rows, sym, stock_name, stype, currency):
    """แปลงแถว summary ของ Finnomena เป็น payload โครงสร้างเดียวกับ yahoo_q
    คืน None ถ้าไม่มีค่ารายไตรมาสเลย (เช่น warrant/ตราสารที่ไม่มีงบ)

    หมายเหตุวันที่: Finnomena ให้ fiscal+quarter ไม่ให้วันสิ้นงวดจริง — สังเคราะห์เป็น
    Q1=31/03 Q2=30/06 Q3=30/09 Q4=31/12 ของปี fiscal (บริษัทปีบัญชีไม่ตรงปีปฏิทิน
    วันที่จะเลื่อนจากงบจริง 1-2 เดือน แต่ลำดับ/ระยะห่างระหว่างไตรมาสคงเส้นคงวา
    QoQ, YoY-Q, streak, การเร่งตัว จึงคำนวณถูกทั้งหมด)"""
    q_end = {1: "-03-31", 2: "-06-30", 3: "-09-30", 4: "-12-31"}

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    field_map = {
        "income": [("Total Revenue", "revenue"), ("Gross Profit", "gross_profit"),
                   ("Net Income", "net_profit"), ("Basic EPS", "earning_per_share"),
                   ("Selling General And Administration", "sga")],
        "balance": [("Total Assets", "asset"), ("Stockholders Equity", "equity"),
                    ("Total Debt", "total_debt"), ("Cash And Cash Equivalents", "cash")],
        "cashflow": [("Operating Cash Flow", "operating_activities"),
                     ("Investing Cash Flow", "investing_activities"),
                     ("Financing Cash Flow", "financing_activities"),
                     ("Depreciation And Amortization", "da")],
    }
    # อัตราส่วน/valuation รายไตรมาสสำเร็จรูปจาก Finnomena — เก็บแยก section
    # เพราะไม่ใช่รายการในงบ: valuation เป็นค่า ณ ราคาตลาดของงวดนั้นจริง
    # (close ต่างกันทุกงวด) จึงได้ PE/PBV band ย้อนหลังยาวไว้ทำ screener
    # ส่วน growth (revenue_qoq/yoy ฯลฯ) ไม่เก็บ — compute_quarterly_growth คำนวณเองได้
    extra_map = {
        "ratios": [("Gross Margin", "gpm"), ("Net Margin", "npm"),
                   ("ROA", "roa"), ("ROE", "roe"),
                   ("Debt To Equity", "debt_to_equity"),
                   ("SGA To Revenue", "sga_per_revenue"),
                   ("Cash Cycle", "cash_cycle")],
        "valuation": [("Close", "close"), ("Market Cap", "mkt_cap"),
                      ("PE", "price_earning_ratio"), ("PBV", "price_book_value"),
                      ("EV To EBITDA", "ev_per_ebit_da"),
                      ("Dividend Yield", "dividend_yield"),
                      ("Book Value Per Share", "book_value_per_share")],
    }
    payload = {"sym": sym, "yf": None, "name": stock_name, "type": stype,
               "currency": currency, "schema": FINN_SCHEMA,
               "period": "quarter", "source_note": "finnomena",
               "income": {}, "balance": {}, "cashflow": {},
               "ratios": {}, "valuation": {}}
    kept = 0
    for r in rows or []:
        qtr = r.get("quarter")
        if qtr not in (1, 2, 3, 4):
            continue   # quarter=9 คือสรุปทั้งปี — ข้าม (รายปีมี source yahoo/set อยู่แล้ว)
        dt = f"{r.get('fiscal')}{q_end[qtr]}"
        got = False
        for section, pairs in field_map.items():
            for ours, theirs in pairs:
                v = _f(r.get(theirs))
                if v is not None:
                    payload[section].setdefault(ours, {})[dt] = v
                    got = True
        # ratios/valuation ไม่นับเป็น 'มีงบ' — งวดที่งบยังไม่ออกก็มี close/PE ได้
        # (ตราสารที่ไม่มีงบเลยจะยังถูกบันทึกเป็น empty marker เหมือนเดิม)
        for section, pairs in extra_map.items():
            for ours, theirs in pairs:
                v = _f(r.get(theirs))
                if v is not None:
                    payload[section].setdefault(ours, {})[dt] = v
        kept += got
    return payload if kept else None


def record_search(base_dir, symbol):
    """จำว่าหุ้นตัวนี้ถูกค้นในหน้างบการเงิน (นับจำนวน + เวลาล่าสุด) — ใช้เลือก
    หุ้น 'ที่ค้นบ่อย' มาอัพเดทงบในโหมดเบา (update_financials.py)"""
    sym = (symbol or "").upper().strip()
    if not sym:
        return
    con = _connect(base_dir)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS search_log(
                         symbol TEXT PRIMARY KEY, count INTEGER, last_seen TEXT)""")
        con.execute("""INSERT INTO search_log(symbol, count, last_seen) VALUES(?,1,?)
                       ON CONFLICT(symbol) DO UPDATE SET count=count+1, last_seen=excluded.last_seen""",
                    (sym, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        con.commit()
    finally:
        con.close()


def get_recent_searches(base_dir, days=90, limit=300):
    """คืน list ของ symbol ที่ถูกค้นภายใน N วัน เรียงตามความถี่ (มากไปน้อย)"""
    if not db_exists(base_dir):
        return []
    con = _connect(base_dir)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS search_log(
                         symbol TEXT PRIMARY KEY, count INTEGER, last_seen TEXT)""")
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        rows = con.execute(
            "SELECT symbol FROM search_log WHERE last_seen >= ? ORDER BY count DESC, last_seen DESC LIMIT ?",
            (cutoff, limit)).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def refresh_mirror_stock(base_dir, symbol):
    """ดึงงบ Finnomena งวดใหม่ของหุ้น mirror US/HK รายตัว (นอกพอร์ต) มา merge —
    ใช้ตอน on-demand/หุ้นค้นบ่อย. คืน True ถ้าอัพเดทได้, False ถ้าไม่ใช่หุ้น mirror"""
    sym = (symbol or "").upper().strip()
    ids = _finn_load_ids()
    for mkey in _finn_mirror_keys(sym, is_dr=True):   # FINN:US:sym / FINN:HK:sym
        _, ex, name = mkey.split(":", 2)
        sid = ids.get((ex, name))
        if sid is None and ex == "HK":
            sid = ids.get((ex, name.lstrip("0") or name)) or ids.get((ex, name.zfill(4)))
        if not sid:
            continue
        rows = (_finn_get(f"/stock/summary/{sid}") or {}).get("data") or []
        payload = _finn_map_rows(rows, name, name, "mirror",
                                 {"US": "USD", "HK": "HKD"}.get(ex, "THB"))
        if payload is not None:
            upsert(base_dir, mkey, "finnomena_q", payload, is_dr=False)
            return True
    return False


def fetch_finnomena_quarterly(symbol, is_dr=False):
    """ดึงงบรายไตรมาสย้อนยาวจาก Finnomena — source 'finnomena_q'
    โครงสร้าง section -> field -> {วันสิ้นงวด: ค่า} เดียวกับ yahoo_q ทำให้
    merge สะสม + compute_quarterly_growth ใช้ร่วมกันได้ทันที (ดูหมายเหตุ
    เรื่องวันที่สังเคราะห์ใน _finn_map_rows)"""
    ex, name, stock_name, stype = _finn_resolve(symbol, is_dr=is_dr)
    sym = symbol.upper().strip()

    ids = _finn_load_ids()
    sid = ids.get((ex, name))
    if sid is None and ex == "HK":
        # รหัส HK ใน list มีทั้งแบบเติม 0 หน้า (0637) และไม่เติม (475) — ลองทั้งสองแบบ
        sid = ids.get((ex, name.lstrip("0") or name)) or ids.get((ex, name.zfill(4)))
    if sid is None:
        try:   # เผื่อ list ตกหล่น — ยิง quote ตรงเป็น fallback
            c = "" if ex == "TH" else f"?exchange={ex}"
            sid = ((_finn_get(f"/stock/quote/{name}{c}") or {}).get("data") or {}).get("ID")
        except Exception:
            sid = None
    if not sid:
        raise ValueError(f"ไม่พบ {sym} ใน Finnomena ({ex}:{name})")

    rows = (_finn_get(f"/stock/summary/{sid}") or {}).get("data") or []
    payload = _finn_map_rows(rows, sym, stock_name, stype,
                             {"US": "USD", "HK": "HKD"}.get(ex, "THB"))
    if payload is None:
        raise ValueError(f"ไม่มีงบรายไตรมาสสำหรับ {sym} ใน Finnomena")
    return payload


# รายการที่ "ไหล" (flow) — รวม 4 ไตรมาสเป็นยอดทั้งปี · ที่เหลือเป็น stock (ยอดคงเหลือ ณ สิ้นงวด)
_FINN_FLOW = {
    "income":   ["Total Revenue", "Gross Profit", "Net Income",
                 "Selling General And Administration", "Basic EPS"],
    "cashflow": ["Operating Cash Flow", "Investing Cash Flow",
                 "Financing Cash Flow", "Depreciation And Amortization"],
}
_FINN_STOCK = {
    "balance": ["Total Assets", "Stockholders Equity", "Total Debt",
                "Cash And Cash Equivalents"],
}


def build_annual_from_quarterly(q_payload):
    """รวมงบ 'รายไตรมาส' Finnomena (finnomena_q) เป็น 'งบรายปี' (finnomena_y)

    - รายได้/กำไร/กระแสเงินสด/EPS = รวม 4 ไตรมาสของปีบัญชี (เฉพาะปีที่ครบ 4 ไตรมาส)
    - งบดุล (สินทรัพย์/ทุน/หนี้/เงินสด) = ยอด ณ ไตรมาสสิ้นปี (ไม่รวม)
    - อัตราส่วน margin/ROE/ROA คำนวณใหม่จากยอดทั้งปี (แม่นกว่าเอา Q4 มาโชว์)
    - valuation (PE/PBV/close ฯลฯ) = ค่า ณ สิ้นปี (Q4)
    คีย์งวดเป็น YYYY-12-31 ให้รูปแบบเดียวกับงบรายปี Yahoo — render/analytics ใช้ต่อได้เลย
    พิสูจน์แล้วว่ายอดรวมตรงกับ Yahoo รายปี 0.00% (PoC 4 หุ้น)"""
    if not q_payload:
        return None

    def rows(section, field):
        return (q_payload.get(section, {}) or {}).get(field, {}) or {}

    # ปีที่ครบ 4 ไตรมาส (ดูจากเดือนสิ้นงวดของ Total Revenue/Net Income)
    months_by_year = {}
    for f in ("Total Revenue", "Net Income"):
        for d in rows("income", f):
            months_by_year.setdefault(d[:4], set()).add(d[5:7])
    complete = {y for y, mm in months_by_year.items() if len(mm) == 4}
    if not complete:
        return None

    out = {"sym": q_payload.get("sym"), "yf": q_payload.get("yf"),
           "name": q_payload.get("name"), "type": q_payload.get("type"),
           "currency": q_payload.get("currency", "—"),
           "schema": q_payload.get("schema"),
           "period": "year", "source_note": "finnomena_annual",
           "income": {}, "balance": {}, "cashflow": {},
           "ratios": {}, "valuation": {}}

    def year_key(y):
        return f"{y}-12-31"

    # flow: รวม 4 ไตรมาส (เฉพาะปีครบ และ field นั้นมีครบ 4 ไตรมาสในปีนั้น)
    for section, fields in _FINN_FLOW.items():
        for f in fields:
            by_year = {}
            for d, v in rows(section, f).items():
                by_year.setdefault(d[:4], []).append(v)
            res = {year_key(y): round(sum(vs), 4) for y, vs in by_year.items()
                   if y in complete and len(vs) == 4}
            if res:
                out[section][f] = res

    # stock: ยอด ณ ไตรมาสสิ้นปี (เดือนมากสุดของปีนั้น)
    for section, fields in _FINN_STOCK.items():
        for f in fields:
            by_year = {}
            for d, v in rows(section, f).items():
                if d[:4] in complete:
                    cur = by_year.get(d[:4])
                    if cur is None or d > cur[0]:
                        by_year[d[:4]] = (d, v)
            res = {year_key(y): dv[1] for y, dv in by_year.items()}
            if res:
                out[section][f] = res

    # margin/ROE/ROA คำนวณใหม่จากยอดทั้งปี
    inc, bal = out["income"], out["balance"]
    def ratio(num_sec, num_f, den_sec, den_f):
        num, den = num_sec.get(num_f, {}), den_sec.get(den_f, {})
        r = {}
        for yk, nv in num.items():
            dv = den.get(yk)
            if dv:
                r[yk] = round(nv / dv * 100, 4)
        return r
    for label, r in [("Gross Margin", ratio(inc, "Gross Profit", inc, "Total Revenue")),
                     ("Net Margin",   ratio(inc, "Net Income",  inc, "Total Revenue")),
                     ("SGA To Revenue", ratio(inc, "Selling General And Administration", inc, "Total Revenue")),
                     ("ROE", ratio(inc, "Net Income", bal, "Stockholders Equity")),
                     ("ROA", ratio(inc, "Net Income", bal, "Total Assets"))]:
        if r:
            out["ratios"][label] = r

    # valuation + ratio ที่คำนวณใหม่ไม่ได้ (D/E, Cash Cycle) = ค่า ณ สิ้นปี Q4
    for section, want in (("valuation", None),
                          ("ratios", ["Debt To Equity", "Cash Cycle"])):
        src = q_payload.get(section, {}) or {}
        for f, row in src.items():
            if want is not None and f not in want:
                continue
            by_year = {}
            for d, v in row.items():
                if d[:4] in complete:
                    cur = by_year.get(d[:4])
                    if cur is None or d > cur[0]:
                        by_year[d[:4]] = (d, v)
            res = {year_key(y): dv[1] for y, dv in by_year.items()}
            if res:
                out[section].setdefault(f, {}).update(res)

    if not out["income"]:
        return None
    return out


# ============================================================
# Finnomena full-market mirror — ทยอยโหลดงบไตรมาสทั้งตลาดเก็บถาวร
# เก็บใต้ namespace 'FINN:{ตลาด}:{ชื่อ}' แยกจากหุ้นไทย/DR ปกติ
# resume ได้เสมอ: ตัวที่มีใน DB แล้ว (รวมที่บันทึกว่า 'ไม่มีงบ') ถูกข้าม
# ============================================================

def _mirror_keep(ex, name):
    """กรองตราสารที่ไม่ใช่หุ้นสามัญออกก่อนยิง — ลด request เปล่าลงมหาศาล"""
    if any(ch in name for ch in (".", "$", " ")):
        return False
    if ex == "TH":
        if "-" in name:
            # ตัดเฉพาะ 'ส่วนต่อท้ายบอกชนิดตราสาร' หลังขีดกลาง: -R (NVDR) -F (foreign)
            # -U (unit) -W/-W1/-W2 (warrant) -P (preferred) — แต่คงหุ้นสามัญที่ชื่อมีขีด
            # กลางจริง เช่น Q-CON, SE-ED, TU-PF, B-WORK, M-CHAI ไว้ (ก่อนหน้าตัดทิ้งหมด)
            seg = name.rsplit("-", 1)[-1]
            if re.fullmatch(r"[RFUWP]\d*", seg):
                return False
        if re.fullmatch(r"[A-Z]{3,}\d{2}", name):
            return False                      # DR รายซีรีส์บนกระดานไทย (UBER06, AAPL80)
        return True
    if "-" in name:
        seg = name.rsplit("-", 1)[-1]
        return seg in ("A", "B", "C")         # share class (BRK-B) เก็บ / -WS -U -R -P ตัด
    if ex == "US" and len(name) == 5 and name[-1] in ("W", "R", "U"):
        return False                          # convention NASDAQ: อักษรตัวที่ 5 บอกชนิดตราสาร
    return True


def _mirror_key(ex, name):
    return f"FINN:{ex}:{name}"


def mirror_candidates(exchanges=("TH", "HK", "US")):
    """รายชื่อ (ex, name, security_id) ที่ผ่านตัวกรอง เรียง TH -> HK -> US"""
    ids = _finn_load_ids()
    order = {"TH": 0, "HK": 1, "US": 2}
    out = [(ex, name, sid) for (ex, name), sid in ids.items()
           if ex in exchanges and _mirror_keep(ex, name)]
    out.sort(key=lambda t: (order.get(t[0], 9), t[1]))
    return out


def mirror_finnomena(base_dir, exchanges=("TH", "HK", "US"), limit=None,
                     throttle=0.35, callback=None, force=False):
    """ทยอยโหลดงบไตรมาสทั้งตลาดจาก Finnomena เก็บลง DB — ปลอดภัยต่อการหยุดกลางทาง
    (Ctrl+C / เน็ตหลุด / เครื่องดับ) เพราะเขียนทีละตัวและรันใหม่จะต่อจากเดิมเอง
    ตัวที่ยิงแล้วพบว่า 'ไม่มีงบ' ก็บันทึก marker ไว้กันยิงซ้ำรอบหน้า
    หยุดเองถ้าล้มเหลวติดกัน 15 ตัว (สัญญาณโดนบล็อค/เน็ตล่ม)

    force=True (Mode F — refresh ทั้งตลาด): ยิงซ้ำทุกตัวที่ 'มีงบ' อยู่แล้ว เพื่อดึงงวด
    ใหม่มา merge (ข้ามเฉพาะตัวที่ทำเครื่องหมาย 'ไม่มีงบ' ไว้ — ไม่ใช่บริษัท) เหมาะรัน
    หลัง earnings season ไตรมาสละครั้ง"""
    init_db(base_dir)
    try:
        backup_db(base_dir)
    except Exception as e:
        print(f"[FinnMirror] backup ล้มเหลว (ไม่หยุด): {e}")

    cands = mirror_candidates(exchanges)
    con = _connect(base_dir)
    try:
        if force:
            # Mode F: ข้ามเฉพาะตัวที่รู้ว่า 'ไม่มีงบ' — ตัวที่มีงบยิงซ้ำหมดเพื่อดึงงวดใหม่
            have = {r[0] for r in con.execute(
                "SELECT symbol FROM financials WHERE source='finnomena_q' AND symbol LIKE 'FINN:%'"
                " AND payload LIKE '%\"empty\": true%'")}
        else:
            # ปกติ: ข้ามตัวที่เป็นโครง payload รุ่นปัจจุบันแล้ว หรือ 'ไม่มีงบ' — ตัวโครงเก่า
            # (ยังไม่มี ratios/valuation) จะถูกยิงซ้ำ merge เติม field ใหม่ (ประวัติไม่หาย)
            have = {r[0] for r in con.execute(
                "SELECT symbol FROM financials WHERE source='finnomena_q' AND symbol LIKE 'FINN:%'"
                " AND (payload LIKE ? OR payload LIKE '%\"empty\": true%')",
                (f'%"schema": {FINN_SCHEMA}%',))}
    finally:
        con.close()
    todo = [(ex, name, sid) for ex, name, sid in cands if _mirror_key(ex, name) not in have]
    skipped = len(cands) - len(todo)
    if limit:
        todo = todo[:limit]
    total = len(todo)
    print(f"[FinnMirror] candidate {len(cands)} | มีแล้ว (ข้าม) {skipped} | รอบนี้ {total}", flush=True)

    import random
    ok = empty = fail = 0
    consec_fail = 0
    t0 = time.time()
    cur = {"US": "USD", "HK": "HKD", "TH": "THB"}
    # พักเบรกสุ่ม 3-14 นาที ทุกๆ ~2500-4000 ตัว — เลียนแบบการใช้งานเป็นช่วงๆ ของคนจริง
    # ป้องกันโดนมองเป็น bot ยิงรัวต่อเนื่องหลายชั่วโมงแล้วโดนแบน
    next_break = random.randint(2500, 4000)
    try:
        for i, (ex, name, sid) in enumerate(todo):
            if i == next_break:
                rest = random.randint(180, 840)   # 3-14 นาที
                print(f"[FinnMirror] 💤 พักเบรก {rest // 60} นาที (ครบ {i} ตัว) "
                      f"— กันโดนแบน แล้วไปต่อเอง...", flush=True)
                time.sleep(rest)
                next_break = i + random.randint(2500, 4000)
            try:
                rows = (_finn_get(f"/stock/summary/{sid}") or {}).get("data") or []
                payload = _finn_map_rows(rows, name, name, "mirror", cur.get(ex, "—"))
                if payload is None:
                    # ไม่มีงบจริง — เก็บ marker กันยิงซ้ำ (upsert ตรง ไม่ผ่าน merge)
                    payload = {"sym": name, "empty": True, "period": "quarter",
                               "schema": FINN_SCHEMA, "source_note": "finnomena",
                               "income": {}, "balance": {}, "cashflow": {}}
                    empty += 1
                else:
                    ok += 1
                upsert(base_dir, _mirror_key(ex, name), "finnomena_q", payload, is_dr=False)
                consec_fail = 0
            except KeyboardInterrupt:
                raise
            except Exception as e:
                fail += 1
                consec_fail += 1
                print(f"[FinnMirror] {ex}:{name} ล้มเหลว: {str(e)[:80]}")
                if consec_fail >= 15:
                    print("[FinnMirror] ล้มเหลวติดกัน 15 ตัว — หยุดก่อน (อาจโดนบล็อคชั่วคราว) "
                          "รันใหม่ภายหลังจะต่อจากจุดนี้เอง", flush=True)
                    break
                time.sleep(3.0)   # พักยาวขึ้นหลังพลาด
            time.sleep(throttle * random.uniform(0.7, 1.6))   # สุ่มจังหวะ ไม่ให้ถี่คงที่แบบ bot
            done = i + 1
            if done % 50 == 0 or done == total:
                rate = done / max(time.time() - t0, 1)
                eta = (total - done) / max(rate, 0.1) / 60
                msg = (f"[FinnMirror] {done}/{total} ({ex}) | มีงบ {ok} ไม่มีงบ {empty} "
                       f"พลาด {fail} | ~{rate:.1f} ตัว/วิ เหลือ ~{eta:.0f} นาที")
                print(msg, flush=True)
                if callback:
                    callback(done, total, msg)
    except KeyboardInterrupt:
        print(f"[FinnMirror] หยุดโดยผู้ใช้ — เก็บไปแล้วรอบนี้ {ok + empty} ตัว รันใหม่จะต่อจากเดิม")

    # total/force เก็บเพิ่มไว้ให้ /api/data-health ตรวจจับสัญญาณ "Finnomena อาจเปลี่ยน API"
    # ได้ (force run ที่ ok=0 ทั้งที่ total เยอะ = ผิดปกติมาก ปกติต้องได้งวดใหม่บ้าง)
    _set_meta(base_dir, "finnomena_mirror_last",
              json.dumps({"at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                          "ok": ok, "empty": empty, "fail": fail,
                          "total": total, "force": force}, ensure_ascii=False))
    print(f"[FinnMirror] จบรอบ: มีงบ {ok} | ไม่มีงบ (บันทึก marker) {empty} | พลาด {fail}")
    return {"ok": ok, "empty": empty, "fail": fail, "total": total}


def _new_yahoo_session():
    """requests.Session() เดียวใช้ร่วมกันทุก thread ในหนึ่งรอบ batch sync — yfinance ต้องขอ
    'crumb' (token คู่กับ cookie) ก่อนยิง query จริงทุกครั้ง ถ้าไม่ reuse session แต่ละ
    thread/ticker จะขอ crumb ของตัวเอง ยิ่งยิงขนานเยอะยิ่งเพิ่มโอกาสโดน Yahoo มองว่า
    ผิดปกติแล้วเพิกถอน crumb ทั้ง IP (ต้นเหตุ HTTP 401 "Invalid Crumb" ที่เจอบ่อยตอน sync ก้อนใหญ่)

    ใช้ _TimeoutSession จาก sources.yahoo (ไม่ใช่ requests.Session() เปล่าๆ) — กัน socket
    ค้างถาวรเมื่อ Yahoo ไม่ตอบระหว่าง batch sync (ทำให้ ThreadPoolExecutor ไม่ยอมจบ,
    _state["running"] ค้าง True ตลอดไป ปุ่มงานหนักอื่นคืน 409 จนกว่าจะ restart server)"""
    from sources.yahoo import _TimeoutSession
    session = _TimeoutSession()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    return session


def _is_yahoo_throttle_err(e):
    s = str(e).lower()
    return "429" in s or "401" in s or "crumb" in s or "rate" in s or "too many" in s


class _YahooThrottle:
    """สถานะ throttle ร่วมข้าม thread ของ batch sync เดียว (คู่กับ session จาก
    _new_yahoo_session) — คนละก้อนต่อการเรียก sync_mirror_yahoo_index/sync_all/
    sync_dividends_batch แต่ละครั้ง ห้าม share ข้าม batch เพราะ cooldown ควรผูกกับ
    session/รอบนั้นๆ เท่านั้น"""

    def __init__(self, consec_fail_limit=15, cooldown_s=60):
        self._lock = threading.Lock()
        self._cooldown_until = 0.0
        self._consec_fail = 0
        self._consec_fail_limit = consec_fail_limit
        self._cooldown_s = cooldown_s

    def wait_if_cooling(self):
        with self._lock:
            wait_s = self._cooldown_until - time.time()
        if wait_s > 0:
            time.sleep(wait_s)

    def _extend_cooldown(self, seconds):
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, time.time() + seconds)

    def call_with_backoff(self, fetch_fn, attempts=3):
        """เรียก fetch_fn() พร้อม retry แบบ exponential backoff เฉพาะ error ที่เข้าข่าย
        โดน Yahoo throttle (401/429/crumb/rate) — error อื่น (เช่น delisted/ไม่มีข้อมูลจริง)
        โยนออกทันที ไม่ retry ให้เสียเวลาเปล่า"""
        self.wait_if_cooling()
        last_err = None
        for attempt in range(attempts):
            try:
                return fetch_fn()
            except Exception as e:
                last_err = e
                if not _is_yahoo_throttle_err(e):
                    raise
                backoff = (2 ** attempt) + random.uniform(1, 3)
                self._extend_cooldown(backoff)
                time.sleep(backoff)
        raise last_err

    def note_outcome(self, ok):
        """เรียกจาก completion loop (single-threaded ผ่าน as_completed) หลังรู้ผลแต่ละตัว —
        พลาดติดกันครบ limit ถือว่า Yahoo บล็อกทั้ง session แล้ว สั่งพักคิวทุก thread
        (เดิมแค่ print เตือนแล้วยิงต่อรัว ทำให้พังยกชุดทั้งที่รู้อยู่แล้วว่าโดน rate-limit)"""
        if ok:
            self._consec_fail = 0
            return
        self._consec_fail += 1
        if self._consec_fail >= self._consec_fail_limit:
            print(f"[YahooThrottle] ล้มเหลวติดกัน {self._consec_fail_limit} ตัว — น่าจะโดน Yahoo "
                  f"บล็อกทั้ง session แล้ว สั่งพักคิวทุก thread {self._cooldown_s}s ก่อนยิงต่อ")
            self._extend_cooldown(self._cooldown_s)
            self._consec_fail = 0


def sync_mirror_yahoo_index(base_dir, tickers_by_ex, workers=4, limit=None, callback=None):
    """ดึงงบ Yahoo annual ('yahoo' source) ให้หุ้น mirror US/HK — เดิมจำกัดแค่สมาชิกดัชนีหลัก
    (S&P500+Dow+NDX / HSI+HSCEI+HSTECH) ตอนนี้ผู้เรียกส่ง ticker ทั้ง mirror universe
    (~5,108 ตัว จาก mirror_candidates) เข้ามาได้แล้ว — ใช้ limit คุมจำนวนต่อรอบแทน
    เก็บใต้ namespace 'FINN:{ex}:{name}'
    เดียวกับ finnomena_q มิเรอร์ — **ไม่ใช้ namespace 'DR:'** เพราะพวกนี้ไม่ใช่ DR ที่มี NVDR
    ซื้อขายจริงในไทย (ใช้ is_dr=True แค่ตอนเรียก fetch_yahoo_full เพื่อให้ resolve yf ticker
    ถูกต้องตามตลาด — ดู docstring fetch_yahoo_full — แต่ upsert เก็บด้วย is_dr=False)

    tickers_by_ex: {"US": [ticker, ...], "HK": [ticker4digit, ...]}
    ข้าม ticker ที่มี source='yahoo' อยู่แล้วไม่ว่า namespace ไหน (กัน sync ซ้ำของ 104 ตัว
    ที่ overlap กับ DR portfolio ที่ sync ไปแล้ว) — resume ได้เสมอเหมือน mirror_finnomena

    limit: จำกัดจำนวนตัวที่ sync ต่อรอบ (สุ่มก่อนตัด — pattern เดียวกับ sync_dividends_batch)
    กัน mirror ทั้งก้อนที่ยังไม่เคย sync เลยทำให้รอบแรกช้ามาก — ตัวที่เหลือ resume รอบถัดไปได้เอง
    เพราะเช็คจาก 'have' (source='yahoo') เสมอ ไม่ fetch ซ้ำตัวที่ sync แล้ว

    หลบ rate-limit (Yahoo "Invalid Crumb" HTTP 401): session เดียวร่วมกันทุก thread (กัน
    crumb ขอใหม่ถี่ๆ ต่อ ticker) + retry แบบ exponential backoff เฉพาะ error ที่เข้าข่ายโดน
    throttle + circuit breaker พักคิวทั้ง batch ถ้าพลาดติดกันเยอะ — ดู _new_yahoo_session/
    _YahooThrottle ด้านบน (ใช้ pattern เดียวกับ sync_all/sync_dividends_batch)
    คืน {"ok": n, "fail": n, "total": n, "skipped": n}"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        have = {r[0] for r in con.execute("SELECT symbol FROM financials WHERE source='yahoo'")}
    finally:
        con.close()

    todo = []
    for ex, names in tickers_by_ex.items():
        for name in names:
            name = name.upper().strip()
            if _mirror_key(ex, name) in have or _dr_key(name) in have:
                continue
            todo.append((ex, name))
    skipped = sum(len(v) for v in tickers_by_ex.values()) - len(todo)
    if limit is not None and len(todo) > limit:
        random.shuffle(todo)   # กันตัวท้ายรายชื่อ (HK ต่อท้าย US เสมอ) ไม่เคยถูก sync สักที
        todo = todo[:limit]
    total = len(todo)
    ok = fail = 0
    gate = threading.Semaphore(3)
    session = _new_yahoo_session()
    throttle = _YahooThrottle()

    def _one(ex, name):
        with gate:
            try:
                def _fetch():
                    payload = fetch_yahoo_full(name, is_dr=True, market=ex, session=session)
                    upsert(base_dir, _mirror_key(ex, name), "yahoo", payload, is_dr=False)
                throttle.call_with_backoff(_fetch)
            finally:
                time.sleep(0.3)   # throttle เบาๆ ทุก request กัน Yahoo บล็อก IP ตอน sync รวด

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex_pool:
        futures = {ex_pool.submit(_one, ex, name): (ex, name) for ex, name in todo}
        for f in as_completed(futures):
            ex, name = futures[f]
            done += 1
            try:
                f.result()
                ok += 1
                throttle.note_outcome(True)
            except Exception as e:
                fail += 1
                print(f"[MirrorYahooIndex] {ex}:{name} ล้มเหลว: {str(e)[:80]}")
                throttle.note_outcome(False)
            if callback and (done % 10 == 0 or done == total):
                callback(done, total, f"Yahoo mirror index {done}/{total} | ok={ok} fail={fail}")

    print(f"[MirrorYahooIndex] จบ: ok={ok} fail={fail} total={total} skipped={skipped}")
    return {"ok": ok, "fail": fail, "total": total, "skipped": skipped}


def sync_dividends_batch(base_dir, symbols_by_market, workers=4, min_age_days=30, limit=None, callback=None):
    """ดึงประวัติปันผล (fetch_dividends) ให้หลายหุ้นพร้อมกันแบบ throttled — จุดผูก "Batch fetch
    ปันผล" ของงาน #5 เฟส B (เดิม fetch สดเฉพาะตอนเปิดหน้า "💵 ปันผล" ทีละตัวเท่านั้น ทำให้
    div_cagr_5y ใน factor_snapshot ว่างเปล่าเกือบทั้ง universe)

    symbols_by_market: {"TH": [...], "DR": [...], "US": [...], "HK": [...]} — ผู้เรียกเป็นคน
    ประกอบ universe (ดู _financials_universe/_dr_financials_universe/us_index_metrics/
    hk_index_metrics ใน app.py) ฟังก์ชันนี้ไม่รู้จัก universe เอง

    min_age_days: ข้าม (symbol, market) ที่ synced_at ใน meta table ใหม่กว่า N วัน (ใช้
    _dividends_meta_key เดียวกับ save_dividends/get_dividends — resume ได้เสมอเหมือน
    sync_mirror_yahoo_index/sync_all ไม่ใช่ fetch ซ้ำทุกตัวทุกรอบ Full Refresh)

    limit: จำกัดจำนวนตัวที่ fetch ต่อรอบ (ตัด todo หลังกรอง min_age_days แล้ว) — universe
    รวม TH+DR+US/HK ดัชนีหลัก ~2,300 ตัว รอบแรกที่ยังไม่มีใคร sync เลยจะช้ามากถ้าไม่จำกัด
    ตั้งไว้กันไม่ให้ Full Refresh ยืดยาวเกินไปรอบเดียว — ตัวที่เหลือ resume ต่อได้ในรอบถัดไป
    เพราะ meta table เก็บ progress ไว้แล้วเสมอ (ไม่ต้องเรียงตามลำดับเดิม)

    Throttle เบากว่า sync_mirror_yahoo_index (gate=2 ไม่ใช่ 3) เพราะ fetch_dividends เป็น
    endpoint yfinance คนละตัว (.dividends) ไม่เคยวัด rate-limit threshold มาก่อน — เผื่อไว้ก่อน
    คืน {"ok": n, "fail": n, "total": n, "skipped": n}"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        synced_map = {r[0][len("dividends_synced:"):]: r[1]
                      for r in con.execute("SELECT key, value FROM meta WHERE key LIKE 'dividends_synced:%'")}
    finally:
        con.close()

    cutoff = datetime.now() - timedelta(days=min_age_days)
    todo = []
    for market, syms in symbols_by_market.items():
        for sym in syms:
            sym = sym.upper().strip()
            ts = synced_map.get(f"{market}:{sym}")
            if ts:
                try:
                    if datetime.fromisoformat(ts) >= cutoff:
                        continue
                except ValueError:
                    pass
            todo.append((market, sym))
    skipped = sum(len(v) for v in symbols_by_market.values()) - len(todo)
    if limit is not None and len(todo) > limit:
        random.shuffle(todo)   # กันตัวท้ายรายชื่อ (เช่น HK ที่ต่อท้าย TH/DR/US เสมอ) ไม่เคยถูก sync สักที
        todo = todo[:limit]
    total = len(todo)
    ok = fail = 0
    gate = threading.Semaphore(2)
    session = _new_yahoo_session()
    throttle = _YahooThrottle()

    def _one(market, sym):
        with gate:
            try:
                def _fetch():
                    rows = fetch_dividends(sym, market=market, session=session)
                    save_dividends(base_dir, sym, market, rows)
                throttle.call_with_backoff(_fetch)
            finally:
                time.sleep(0.3)

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex_pool:
        futures = {ex_pool.submit(_one, market, sym): (market, sym) for market, sym in todo}
        for f in as_completed(futures):
            market, sym = futures[f]
            done += 1
            try:
                f.result()
                ok += 1
                throttle.note_outcome(True)
            except Exception as e:
                fail += 1
                print(f"[DividendsBatch] {market}:{sym} ล้มเหลว: {str(e)[:80]}")
                throttle.note_outcome(False)
            if callback and (done % 20 == 0 or done == total):
                callback(done, total, f"Batch fetch ปันผล {done}/{total} | ok={ok} fail={fail}")

    print(f"[DividendsBatch] จบ: ok={ok} fail={fail} total={total} skipped={skipped}")
    return {"ok": ok, "fail": fail, "total": total, "skipped": skipped}


# ============================================================
# Bulk sync
# ============================================================

def sync_all(base_dir, symbols, sources=("yahoo", "set"), workers=6, callback=None,
            is_dr=False, min_age_days=None, market=None):
    """ดึงงบการเงินเต็มของทุก symbol × ทุก source มา upsert เข้า DB
    คืน {"ok": n, "fail": n, "total": n, "skipped": n}

    min_age_days: ถ้าใส่ (เช่น 7) จะข้ามคู่ (symbol, source) ที่ synced_at ใน DB
    ใหม่กว่า N วันไปแล้ว — ไม่ยิง fetch ซ้ำของที่เพิ่งดึงไปไม่นาน (incremental sync)
    None = ดึงทุกคู่เสมอเหมือนพฤติกรรมเดิม (full sync)

    market: ส่งต่อให้ fetch_yahoo_full/fetch_yahoo_quarterly เดา yf ticker ตอน symbol
    ไม่อยู่ใน DR universe ที่ curate ไว้ (หุ้น mirror US/HK ทั่วไปนอกพอร์ต) — ไม่งั้นจะ
    พลาดไปดึงเป็นหุ้นไทย .BK (ดูคอมเมนต์ fetch_yahoo_full)

    Yahoo throttle: จาก bulk sync จริง (930 หุ้น) พบว่า ~60% ของหุ้นที่ล้มเหลว
    เป็นเพราะโดน rate-limit ชั่วคราว ไม่ใช่ไม่มีข้อมูลจริง (สุ่มทดสอบซ้ำ 25 ตัวที่
    เคยล้มเหลวผ่านหมด 25/25) — จึงจำกัดจำนวน Yahoo request พร้อมกันแยกจาก
    SET.or.th ด้วย semaphore + session ร่วม + retry แบบ exponential backoff + circuit
    breaker (ดู _new_yahoo_session/_YahooThrottle — pattern เดียวกับ sync_mirror_yahoo_index/
    sync_dividends_batch) (SET.or.th เจอปัญหานี้น้อยกว่ามาก เพราะ reuse cookie เดียวกันทุก request)"""
    init_db(base_dir)
    try:
        backup_db(base_dir)   # สำรอง DB แยกไว้อีกชุดก่อนเขียนชุดใหญ่ กันไฟล์เสียกลาง bulk sync
    except Exception as e:
        print(f"[FinancialsSync] backup ล้มเหลว (ไม่หยุด sync): {e}")
    symbols = [s.upper().strip().replace(".BK", "") for s in symbols]
    # ตัด symbol ที่ Finnomena ไม่รองรับออกจาก task ตั้งแต่ต้น (ETF / ตลาดนอก TH-US-HK)
    # จะได้ไม่นับเป็น fail ทั้งที่รู้อยู่แล้วว่าไม่มีข้อมูล
    tasks = [(sym, src) for sym in symbols for src in sources
             if not (src == "finnomena_q" and not finnomena_supported(sym, is_dr=is_dr))]

    skipped = 0
    if min_age_days is not None:
        synced_map = get_synced_map(base_dir, is_dr=is_dr)
        cutoff = datetime.now() - timedelta(days=min_age_days)
        fresh_tasks = []
        for sym, src in tasks:
            ts = synced_map.get((sym, src))
            if ts is not None and ts >= cutoff:
                skipped += 1
            else:
                fresh_tasks.append((sym, src))
        tasks = fresh_tasks

    total = len(tasks)
    done = ok = fail = 0

    set_ctx, set_hdr = (None, None)
    if "set" in sources:
        try:
            set_ctx, set_hdr = _bootstrap_headers()
        except Exception:
            set_ctx, set_hdr = None, None

    if any(src == "finnomena_q" for _, src in tasks):
        try:
            _finn_load_ids()   # โหลด security_id ทุกตลาดครั้งเดียวก่อนเริ่ม แทนที่จะให้ thread แรกแบก
        except Exception as e:
            print(f"[FinancialsSync] โหลดรายชื่อ Finnomena ไม่สำเร็จ (จะ fallback ยิง quote รายตัว): {e}")

    _yahoo_gate = threading.Semaphore(3)   # จำกัด Yahoo request พร้อมกันไม่เกิน 3 (แยกจาก SET.or.th)
    _finn_gate  = threading.Semaphore(2)   # Finnomena ไม่รู้เพดาน rate-limit — ยิงสุภาพไว้ก่อน
    _yahoo_session = _new_yahoo_session()
    _yahoo_throttle = _YahooThrottle()

    def _one(sym, src):
        if src in ("yahoo", "yahoo_q"):
            fetch = fetch_yahoo_full if src == "yahoo" else fetch_yahoo_quarterly
            with _yahoo_gate:
                try:
                    payload = _yahoo_throttle.call_with_backoff(
                        lambda: fetch(sym, is_dr=is_dr, market=market, session=_yahoo_session))
                finally:
                    time.sleep(0.3)           # throttle เบาๆ กัน Yahoo บล็อก IP ตอน sync รวด
        elif src == "finnomena_q":
            with _finn_gate:
                try:
                    payload = fetch_finnomena_quarterly(sym, is_dr=is_dr)
                except Exception:
                    time.sleep(1.0)
                    payload = fetch_finnomena_quarterly(sym, is_dr=is_dr)
                finally:
                    time.sleep(0.25)
        else:
            payload = fetch_set_full(sym, set_ctx, set_hdr)
            time.sleep(0.15)  # throttle เบาๆ กัน SET.or.th block IP
        upsert(base_dir, sym, src, payload, is_dr=is_dr)
        return sym, src

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_one, sym, src): (sym, src) for sym, src in tasks}
        for f in as_completed(futures):
            sym, src = futures[f]
            done += 1
            try:
                f.result()
                ok += 1
                if src in ("yahoo", "yahoo_q"):
                    _yahoo_throttle.note_outcome(True)
            except Exception as e:
                fail += 1
                print(f"[FinancialsSync] {sym} ({src}) failed: {e}")
                if src in ("yahoo", "yahoo_q"):
                    _yahoo_throttle.note_outcome(False)
            if callback:
                callback(done, total, f"งบการเงิน {done}/{total} ({sym} · {src})...")

    _set_meta(base_dir, "last_full_sync_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return {"ok": ok, "fail": fail, "total": total, "skipped": skipped}


# ============================================================
# วิเคราะห์ต่อยอด — Growth Score / PEG / FCF Yield / Cross-source DQ Check
# ============================================================

def extract_yearly_series(payload_yahoo):
    """ดึง revenue/net_income/gross_profit/total_debt/equity/ebit/interest_expense ต่อปีจาก
    payload Yahoo (เต็ม, สะสมหลายปีจาก merge-on-sync) คืน {year:int: {...}} เรียงปี เก่า→ใหม่
    (dict ปกติแต่ key เรียงแล้วเพื่อความสะดวก ผู้เรียกควร sorted() ซ้ำถ้าต้องการชัวร์)"""
    inc = payload_yahoo.get("income", {})
    bal = payload_yahoo.get("balance", {})
    rev_row  = inc.get("Total Revenue", {})
    ni_row   = inc.get("Net Income", {})
    gp_row   = inc.get("Gross Profit", {})
    debt_row = bal.get("Total Debt", {})
    eq_row   = bal.get("Stockholders Equity", {})
    ebit_row = inc.get("EBIT", {})
    int_row  = inc.get("Interest Expense", {})

    years = set()
    for row in (rev_row, ni_row, gp_row, debt_row, eq_row, ebit_row, int_row):
        for date_str in row:
            years.add(int(date_str[:4]))

    out = {}
    for y in years:
        # หาวันที่จริงของปีนี้จากแต่ละ row (อาจ fiscal year-end ไม่ตรงกันเป๊ะระหว่าง field แต่ปีเดียวกัน)
        def _val(row, _y=y):
            d = next((d for d in row if d.startswith(str(_y))), None)
            return row.get(d) if d else None
        out[y] = {
            "revenue":          _val(rev_row),
            "net_income":       _val(ni_row),
            "gross_profit":     _val(gp_row),
            "total_debt":       _val(debt_row),
            "equity":           _val(eq_row),
            "ebit":             _val(ebit_row),
            "interest_expense": _val(int_row),
        }
    return dict(sorted(out.items()))


MAX_RATIO_YEARS_BACK = 4   # เพดาน UI — ข้อมูลส่วนใหญ่ตอนนี้มีแค่ 4 ปี (สะสมเพิ่มเรื่อยๆ จาก merge-on-sync)


RATIO_KEYS = ("gross_margin", "roe", "net_margin", "de_ratio", "interest_coverage")


def compute_ratio_trends(payload_yahoo, max_years_back=MAX_RATIO_YEARS_BACK):
    """Gross Margin / ROE / Net Profit Margin / D-E Ratio / Interest Coverage — ทั้งค่า
    snapshot ปีล่าสุด และเทรนด์เทียบย้อนหลัง N ปี (N = 1..max_years_back จำกัดตามข้อมูลที่มี
    จริงของหุ้นตัวนั้น) — Gross Margin/ROE/Net Margin เป็นหน่วย % (จุดเปอร์เซ็นต์ตอนเทียบเทรนด์)
    ส่วน D-E Ratio/Interest Coverage เป็นหน่วยเท่า (ไม่ใช่ %)
    คืน {<key>: ค่าปีล่าสุด, <key>_trend: {N: ค่าที่เปลี่ยนจาก N ปีก่อนถึงปีล่าสุด},
         years_available, max_years_back: เพดานจริงของหุ้นตัวนี้}"""
    series = extract_yearly_series(payload_yahoo)
    years = sorted(series.keys())
    n = len(years)

    def _pct(num, den):
        if num is None or den is None or den == 0:
            return None
        return round(num / den * 100, 2)

    def _ratio(num, den):
        if num is None or den is None or den == 0:
            return None
        return round(num / den, 2)

    def _sane(v, lo, hi):
        # winsorize กันสมการหาร equity/asset ใกล้ศูนย์ (ปีขาดทุนหนักจนทุนแทบหมด) ระเบิดเป็น
        # ค่าพันเปอร์เซ็นต์ — bound เดียวกับที่ใช้ winsorize ratio ดิบฝั่ง mirror (ดู _sane ใน
        # factor_snapshot.py) เพื่อให้ TH/DR path ปลอดภัยเท่าฝั่ง mirror ที่กันไว้อยู่แล้ว
        return v if (v is not None and lo <= v <= hi) else None

    def _ratios_at(y):
        v = series[y]
        equity = v.get("equity")
        return {
            "gross_margin": _sane(_pct(v.get("gross_profit"), v.get("revenue")), -100, 100),
            "roe": _sane(_pct(v.get("net_income"), equity) if equity and equity > 0 else None, -300, 300),
            "net_margin": _sane(_pct(v.get("net_income"), v.get("revenue")), -200, 200),
            "de_ratio": _sane(_ratio(v.get("total_debt"), equity) if equity and equity > 0 else None, 0, 50),
            "interest_coverage": _ratio(v.get("ebit"), v.get("interest_expense")),
        }

    if n == 0:
        return {
            **{k: None for k in RATIO_KEYS},
            **{f"{k}_trend": {} for k in RATIO_KEYS},
            "years_available": 0, "max_years_back": 0,
        }

    latest_ratios = _ratios_at(years[-1])
    actual_max_back = min(max_years_back, n - 1)

    trends = {k: {} for k in RATIO_KEYS}
    for back in range(1, actual_max_back + 1):
        cmp_ratios = _ratios_at(years[-1 - back])
        for key in trends:
            lv, cv = latest_ratios[key], cmp_ratios[key]
            trends[key][back] = round(lv - cv, 2) if (lv is not None and cv is not None) else None

    return {
        **latest_ratios,
        **{f"{k}_trend": trends[k] for k in RATIO_KEYS},
        "years_available": n,
        "max_years_back": actual_max_back,
    }


def compute_growth_streaks(payload_yahoo):
    """นับจำนวนปีติดต่อกัน (นับจากปีล่าสุดย้อนหลัง) ที่รายได้/กำไรโตขึ้นทุกปีไม่มีสะดุด
    ใช้สำหรับ Screener กรอง 'หุ้นเติบโตต่อเนื่อง' ต่างจาก growth_score ที่เทียบแค่ปีแรก vs
    ปีล่าสุด (อาจดูดีทั้งที่มีปีที่ลดลงแทรกอยู่ระหว่างทาง) — คืน 0 ถ้าปีล่าสุดลดลงจากปีก่อนหน้า
    ทันที หรือมีข้อมูลไม่พอ (< 2 ปี)"""
    series = extract_yearly_series(payload_yahoo)
    years = sorted(series.keys())

    def _streak(field):
        vals = [(y, series[y][field]) for y in years if series[y].get(field) is not None]
        if len(vals) < 2:
            return 0
        streak = 0
        for i in range(len(vals) - 1, 0, -1):
            if vals[i][1] > vals[i - 1][1]:
                streak += 1
            else:
                break
        return streak

    return {
        "revenue_streak": _streak("revenue"),
        "profit_streak": _streak("net_income"),
        "years_available": len(years),
    }


def compute_growth_score(payload_yahoo):
    """คำนวณ growth score จาก payload Yahoo เต็ม — ใช้ปีแรกสุดกับปีล่าสุดสุดที่มี revenue
    คืน {growth_score, rev_cagr, profit_cagr, years_span} — field เป็น None ถ้าข้อมูลไม่พอ"""
    from core.metrics import calc_cagr, calc_growth_score as _calc_score

    series = extract_yearly_series(payload_yahoo)
    years_with_rev = [y for y, v in series.items() if v["revenue"] is not None]
    if len(years_with_rev) < 2:
        return {"growth_score": None, "rev_cagr": None, "profit_cagr": None, "years_span": None}

    y0, y1 = years_with_rev[0], years_with_rev[-1]
    span = y1 - y0
    s0, s1 = series[y0], series[y1]

    rev_cagr = calc_cagr(s0["revenue"], s1["revenue"], span)
    profit_cagr = calc_cagr(s0["net_income"], s1["net_income"], span)

    margin0 = (s0["net_income"] / s0["revenue"] * 100) if s0["revenue"] and s0["net_income"] is not None else None
    margin1 = (s1["net_income"] / s1["revenue"] * 100) if s1["revenue"] and s1["net_income"] is not None else None
    margin_trend = (margin1 - margin0) if margin0 is not None and margin1 is not None else None

    roe0 = (s0["net_income"] / s0["equity"] * 100) if s0["equity"] and s0["equity"] > 0 and s0["net_income"] is not None else None
    roe1 = (s1["net_income"] / s1["equity"] * 100) if s1["equity"] and s1["equity"] > 0 and s1["net_income"] is not None else None
    roe_trend = (roe1 - roe0) if roe0 is not None and roe1 is not None else None

    de0 = (s0["total_debt"] / s0["equity"]) if s0["equity"] and s0["equity"] > 0 and s0["total_debt"] is not None else None
    de1 = (s1["total_debt"] / s1["equity"]) if s1["equity"] and s1["equity"] > 0 and s1["total_debt"] is not None else None
    de_trend = (de1 - de0) if de0 is not None and de1 is not None else None

    score = _calc_score(rev_cagr, profit_cagr, margin_trend, roe_trend, de_trend)
    return {"growth_score": score, "rev_cagr": rev_cagr, "profit_cagr": profit_cagr, "years_span": span}


def compute_fcf_metrics(payload_yahoo, mkt_cap):
    """FCF ปีล่าสุด + FCF Yield (%) + Dividend Coverage จาก payload Yahoo เต็ม
    mkt_cap: หน่วยบาทเต็ม (ตรงกับหน่วยของ field การเงินใน Yahoo payload — ไม่ต้องแปลงหน่วย)"""
    cf = payload_yahoo.get("cashflow", {})
    fcf_row = cf.get("Free Cash Flow", {})
    if not fcf_row:
        return {"fcf": None, "fcf_yield": None, "dividend_coverage": None, "as_of": None}

    latest_date = max(fcf_row.keys())
    fcf = fcf_row.get(latest_date)

    div_row = cf.get("Cash Dividends Paid") or cf.get("Common Stock Dividend Paid") or {}
    dividends = div_row.get(latest_date)

    fcf_yield = (fcf / mkt_cap * 100) if fcf is not None and mkt_cap else None
    dividend_coverage = None
    if fcf is not None and dividends:
        dividend_coverage = fcf / abs(dividends)

    return {"fcf": fcf, "fcf_yield": fcf_yield, "dividend_coverage": dividend_coverage, "as_of": latest_date}


def _yahoo_row(payload_yahoo, section, *names):
    """คืน dict {date: value} ของ field แรกที่เจอใน section — Yahoo ตั้งชื่อ field ไม่คงที่"""
    sec = (payload_yahoo or {}).get(section, {})
    for n in names:
        row = sec.get(n)
        if row:
            return row
    return {}


def compute_cash_cycle(payload_yahoo):
    """Cash Conversion Cycle = DIO + DSO − DPO จากงบ Yahoo (รายปี งวดล่าสุดที่ครบ)
    คำนวณเองเพราะค่า cash_cycle สำเร็จรูปของ Finnomena เชื่อไม่ได้ (ทดสอบแล้วเพี้ยน)
      DIO = สินค้าคงคลัง / ต้นทุนขาย × 365   (ของค้างสต็อกกี่วัน)
      DSO = ลูกหนี้การค้า / รายได้ × 365       (เก็บเงินกี่วัน)
      DPO = เจ้าหนี้การค้า / ต้นทุนขาย × 365   (จ่ายเจ้าหนี้กี่วัน)
    คืน None ทุกค่าถ้า field ไม่ครบ (เช่นธนาคาร/ประกัน ที่ไม่มี COGS/inventory)"""
    rev  = _yahoo_row(payload_yahoo, "income", "Total Revenue", "Operating Revenue")
    cogs = _yahoo_row(payload_yahoo, "income", "Cost Of Revenue", "Reconciled Cost Of Revenue")
    inv  = _yahoo_row(payload_yahoo, "balance", "Inventory")
    ar   = _yahoo_row(payload_yahoo, "balance", "Accounts Receivable", "Receivables", "Gross Accounts Receivable")
    ap   = _yahoo_row(payload_yahoo, "balance", "Accounts Payable", "Payables", "Payables And Accrued Expenses")

    none = {"cash_cycle": None, "dio": None, "dso": None, "dpo": None, "cc_as_of": None}
    common = set(rev) & set(cogs) & set(inv) & set(ar) & set(ap)
    if not common:
        return none
    d = max(common)
    r, c = rev[d], cogs[d]
    if not r or not c or r <= 0 or c <= 0:
        return none
    dio = inv[d] / c * 365
    dso = ar[d] / r * 365
    dpo = ap[d] / c * 365
    return {"cash_cycle": round(dio + dso - dpo, 1), "dio": round(dio, 1),
            "dso": round(dso, 1), "dpo": round(dpo, 1), "cc_as_of": d[:10]}


def compute_balance_quality(payload_yahoo):
    """คุณภาพงบดุลจาก Yahoo (งวดปีล่าสุด) สำหรับ screener สาย VI + risk filter:
      net_cash        : เงินสด − หนี้รวม (บวก = เงินสดมากกว่าหนี้ = งบแกร่ง)
      net_cash_positive: True ถ้า net cash เป็นบวก
      de_ratio        : หนี้รวม / ส่วนผู้ถือหุ้น (เท่า)
      goodwill_ratio  : goodwill+สินทรัพย์ไม่มีตัวตน / สินทรัพย์รวม (%) — สูง = เสี่ยง write-off
      shares_chg_yoy  : %เปลี่ยนจำนวนหุ้นเทียบปีก่อน (ลบ = ซื้อคืน, บวกมาก = เพิ่มทุนเจือจาง)
      buyback         : True ถ้าจำนวนหุ้นลดลง YoY (ซื้อหุ้นคืนจริง)
      ocf_neg_years   : จำนวนปี (จากล่าสุดย้อนหลัง) ที่ OCF ติดลบติดต่อกัน"""
    # ตัวรวมเงินลงทุนระยะสั้นมาก่อน — บริษัทจำนวนมาก (โดยเฉพาะหุ้นเทค) พักเงินใน
    # ST investments ถ้าดูแค่ Cash เพียวๆ net cash จะติดลบทั้งที่จริงเป็นบวก
    cash = _yahoo_row(payload_yahoo, "balance", "Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents")
    debt = _yahoo_row(payload_yahoo, "balance", "Total Debt")
    eq   = _yahoo_row(payload_yahoo, "balance", "Stockholders Equity")
    gw   = _yahoo_row(payload_yahoo, "balance", "Goodwill And Other Intangible Assets", "Goodwill")
    ta   = _yahoo_row(payload_yahoo, "balance", "Total Assets")
    sh   = _yahoo_row(payload_yahoo, "balance", "Ordinary Shares Number", "Share Issued")
    ocf  = _yahoo_row(payload_yahoo, "cashflow", "Operating Cash Flow")

    out = {"net_cash": None, "net_cash_positive": None, "de_ratio": None,
           "goodwill_ratio": None, "shares_chg_yoy": None, "buyback": None, "ocf_neg_years": None,
           # จำนวนหุ้นล่าสุด (งวดปีล่าสุด) — ใช้คำนวณ mkt_cap = price × shares_out ตอนที่ไม่มี
           # mkt_cap สดจากแหล่งอื่น (เช่น us/hk_index_metrics.json ที่ไม่เก็บฟิลด์นี้เลย)
           "shares_out": (sh[max(sh)] if sh else None)}

    if cash and debt:
        d = max(set(cash) & set(debt), default=None)
        if d is not None:
            out["net_cash"] = cash[d] - debt[d]
            out["net_cash_positive"] = out["net_cash"] > 0
    if debt and eq:
        d = max(set(debt) & set(eq), default=None)
        if d is not None and eq[d] and eq[d] > 0:
            out["de_ratio"] = round(debt[d] / eq[d], 2)
    if gw and ta:
        d = max(set(gw) & set(ta), default=None)
        if d is not None and ta[d] and ta[d] > 0:
            out["goodwill_ratio"] = round(gw[d] / ta[d] * 100, 1)
    if sh and len(sh) >= 2:
        ds = sorted(sh)
        last, prev = sh[ds[-1]], sh[ds[-2]]
        if prev and prev > 0:
            out["shares_chg_yoy"] = round((last - prev) / prev * 100, 2)
            out["buyback"] = last < prev
    if ocf:
        ds = sorted(ocf)
        n = 0
        for d in reversed(ds):
            if ocf[d] is not None and ocf[d] < 0:
                n += 1
            else:
                break
        out["ocf_neg_years"] = n
    return out


FSCORE_CRITERIA_META = (
    ("F1", "ROA เป็นบวก"),
    ("F2", "กระแสเงินสดจากการดำเนินงานเป็นบวก"),
    ("F3", "ROA ปีนี้ดีกว่าปีก่อน"),
    ("F4", "CFO มากกว่ากำไรสุทธิ (คุณภาพกำไร)"),
    ("F5", "หนี้สินระยะยาวต่อสินทรัพย์ลดลง"),
    ("F6", "อัตราส่วนสภาพคล่องดีขึ้น"),
    ("F7", "ไม่เพิ่มทุน (จำนวนหุ้นไม่เพิ่มขึ้น)"),
    ("F8", "อัตรากำไรขั้นต้นดีขึ้น"),
    ("F9", "ประสิทธิภาพใช้สินทรัพย์ดีขึ้น"),
)


def compute_fscore(payload_yahoo):
    """Piotroski F-Score (0-9) จากงบ Yahoo รายปี — เทียบปีล่าสุดกับปีก่อนหน้าทันที
    (จัดตำแหน่งปีตามปฏิทินของ Total Assets ซึ่งมีครบทุกปีเสมอ)
    แต่ละข้อคำนวณอิสระ ข้อไหนขาด field ที่ต้องใช้ -> pass=None (ไม่นับทั้งตัวเศษ/ส่วน)
    คืน {f_score: จำนวนข้อที่ผ่าน, f_score_max: จำนวนข้อที่เช็คได้จริง (<=9),
         f_score_detail: [{code, label, pass}], f_score_as_of: ปีล่าสุดที่ใช้}"""
    ni  = _yahoo_row(payload_yahoo, "income", "Net Income")
    ta  = _yahoo_row(payload_yahoo, "balance", "Total Assets")
    ocf = _yahoo_row(payload_yahoo, "cashflow", "Operating Cash Flow")
    ltd = _yahoo_row(payload_yahoo, "balance", "Long Term Debt")
    if not ltd:
        ltd = _yahoo_row(payload_yahoo, "balance", "Total Debt")
    ca  = _yahoo_row(payload_yahoo, "balance", "Current Assets")
    cl  = _yahoo_row(payload_yahoo, "balance", "Current Liabilities")
    sh  = _yahoo_row(payload_yahoo, "balance", "Ordinary Shares Number", "Share Issued")
    gp  = _yahoo_row(payload_yahoo, "income", "Gross Profit")
    rev = _yahoo_row(payload_yahoo, "income", "Total Revenue", "Operating Revenue")

    ta_dates = sorted(ta.keys())
    if not ta_dates:
        detail = [{"code": c, "label": l, "pass": None} for c, l in FSCORE_CRITERIA_META]
        return {"f_score": 0, "f_score_max": 0, "f_score_detail": detail, "f_score_as_of": None}
    d1 = ta_dates[-1]
    d0 = ta_dates[-2] if len(ta_dates) >= 2 else None

    def _ratio(num_row, den_row, d):
        if d is None:
            return None
        num, den = num_row.get(d), den_row.get(d)
        if num is None or den is None or den == 0:
            return None
        return num / den

    roa1, roa0 = _ratio(ni, ta, d1), _ratio(ni, ta, d0)
    lev1, lev0 = _ratio(ltd, ta, d1), _ratio(ltd, ta, d0)
    cr1, cr0 = _ratio(ca, cl, d1), _ratio(ca, cl, d0)
    gm1, gm0 = _ratio(gp, rev, d1), _ratio(gp, rev, d0)
    at1, at0 = _ratio(rev, ta, d1), _ratio(rev, ta, d0)
    ni1, ocf1 = ni.get(d1), ocf.get(d1)
    sh1, sh0 = sh.get(d1), sh.get(d0) if d0 else None

    checks = (
        None if roa1 is None else roa1 > 0,
        None if ocf1 is None else ocf1 > 0,
        None if (roa1 is None or roa0 is None) else roa1 > roa0,
        None if (ocf1 is None or ni1 is None) else ocf1 > ni1,
        None if (lev1 is None or lev0 is None) else lev1 < lev0,
        None if (cr1 is None or cr0 is None) else cr1 > cr0,
        None if (sh1 is None or sh0 is None) else sh1 <= sh0 * 1.005,
        None if (gm1 is None or gm0 is None) else gm1 > gm0,
        None if (at1 is None or at0 is None) else at1 > at0,
    )
    detail = [{"code": c, "label": l, "pass": p} for (c, l), p in zip(FSCORE_CRITERIA_META, checks)]
    computed = [p for p in checks if p is not None]
    return {"f_score": sum(1 for p in computed if p), "f_score_max": len(computed),
            "f_score_detail": detail, "f_score_as_of": d1}


def compute_zscore(payload_yahoo, mkt_cap, variant="Z2"):
    """Altman Z-Score จากงบ Yahoo รายปี (ปีล่าสุด) — variant='Z' (ต้นฉบับ, ต้องมี mkt_cap)
    หรือ 'Z2' (Z'' emerging market, ไม่ง้อ mkt_cap) ค่า field ขาด -> คืน z_score=None
    โซน: Z  > 2.99 ปลอดภัย / 1.81-2.99 เทา / < 1.81 เสี่ยง
         Z'' > 2.6  ปลอดภัย / 1.1-2.6  เทา / < 1.1  เสี่ยง"""
    wc  = _yahoo_row(payload_yahoo, "balance", "Working Capital")
    ca  = _yahoo_row(payload_yahoo, "balance", "Current Assets")
    cl  = _yahoo_row(payload_yahoo, "balance", "Current Liabilities")
    re_ = _yahoo_row(payload_yahoo, "balance", "Retained Earnings")
    ebit = _yahoo_row(payload_yahoo, "income", "EBIT")
    ta  = _yahoo_row(payload_yahoo, "balance", "Total Assets")
    tl  = _yahoo_row(payload_yahoo, "balance", "Total Liabilities Net Minority Interest")
    eq  = _yahoo_row(payload_yahoo, "balance", "Stockholders Equity")
    rev = _yahoo_row(payload_yahoo, "income", "Total Revenue", "Operating Revenue")

    none = {"z_score": None, "z_variant": variant, "z_zone": None, "z_as_of": None}
    ta_dates = sorted(ta.keys())
    if not ta_dates:
        return none
    d = ta_dates[-1]
    ta_v = ta.get(d)
    if not ta_v or ta_v <= 0:
        return {**none, "z_as_of": d}

    wc_v = wc.get(d)
    if wc_v is None and d in ca and d in cl:
        wc_v = ca[d] - cl[d]
    re_v, ebit_v, tl_v, eq_v, rev_v = re_.get(d), ebit.get(d), tl.get(d), eq.get(d), rev.get(d)

    if variant == "Z":
        if None in (wc_v, re_v, ebit_v, rev_v) or not tl_v or not mkt_cap:
            return {**none, "z_as_of": d}
        z = (1.2 * wc_v / ta_v + 1.4 * re_v / ta_v + 3.3 * ebit_v / ta_v
             + 0.6 * mkt_cap / tl_v + 1.0 * rev_v / ta_v)
        zone = "safe" if z > 2.99 else ("grey" if z >= 1.81 else "distress")
    else:
        if None in (wc_v, re_v, ebit_v, eq_v) or not tl_v:
            return {**none, "z_as_of": d}
        z = (6.56 * wc_v / ta_v + 3.26 * re_v / ta_v + 6.72 * ebit_v / ta_v + 1.05 * eq_v / tl_v)
        zone = "safe" if z > 2.6 else ("grey" if z >= 1.1 else "distress")
    return {"z_score": round(z, 2), "z_variant": variant, "z_zone": zone, "z_as_of": d}


def compute_earnings_quality(payload_q):
    """คุณภาพกำไร: OCF สะสม 4 ไตรมาส / กำไรสุทธิสะสม 4 ไตรมาส (TTM)
    > 1 = เงินสดเข้าจริงมากกว่ากำไรทางบัญชี (ดี), < 0.8 = กำไรโตแต่เงินไม่เข้า (red flag)
    ใช้ payload รายไตรมาส (finnomena_q/yahoo_q) — คืน None ถ้าไตรมาส/ฐานไม่พอ
    (กำไร TTM ต้องเป็นบวก ไม่งั้นอัตราส่วนไม่มีความหมาย)"""
    inc = (payload_q or {}).get("income", {})
    cf = (payload_q or {}).get("cashflow", {})
    ni = inc.get("Net Income", {})
    ocf = cf.get("Operating Cash Flow", {})
    common = sorted(set(ni) & set(ocf))
    if len(common) < 4:
        return {"ocf_ni_ratio": None, "eq_as_of": None}
    last4 = common[-4:]
    ni_ttm = sum(ni[d] for d in last4)
    ocf_ttm = sum(ocf[d] for d in last4)
    if ni_ttm is None or ni_ttm <= 0:
        return {"ocf_ni_ratio": None, "eq_as_of": last4[-1][:10]}
    return {"ocf_ni_ratio": round(ocf_ttm / ni_ttm, 2), "eq_as_of": last4[-1][:10]}


def compute_quality_streaks(payload_finn_q, roe_min=15.0):
    """streak คุณภาพระยะยาวจาก Finnomena ratios รายไตรมาส (ย้อนได้ ~65 งวด):
      roe15_streak_q   : จำนวนไตรมาสติดกัน (จากงวดล่าสุดย้อนหลัง) ที่ ROE >= 15%
      nm_hold_streak_q : จำนวน 'ก้าว' ติดกันที่ Net Margin ไม่ลดลงจากงวดก่อน (>= เดิม)
    หัวใจของ Quality Compounder เต็มสูตร (ROE สูง 'ต่อเนื่อง' ไม่ใช่แค่งวดเดียว) —
    ใช้ได้ทั้งหุ้นไทย/DR (ผ่าน bridge) และ mirror US/HK เพราะไม่พึ่ง Yahoo
    จุดข้อมูลเพี้ยน (|ROE|>300, |NM|>200 — ค่าดิบ Finnomena เชื่อไม่ได้) ถือว่าตัด streak"""
    rat = (payload_finn_q or {}).get("ratios", {})

    def _series(name, lo, hi):
        row = rat.get(name) or {}
        return sorted((d, (v if lo <= v <= hi else None))
                      for d, v in row.items() if v is not None)

    roe = _series("ROE", -300, 300)
    nm = _series("Net Margin", -200, 200)

    roe_streak = 0
    for _, v in reversed(roe):
        if v is None or v < roe_min:
            break
        roe_streak += 1

    nm_streak = 0
    for i in range(len(nm) - 1, 0, -1):
        prev, cur = nm[i - 1][1], nm[i][1]
        if prev is None or cur is None or cur < prev:
            break
        nm_streak += 1

    return {"roe15_streak_q": roe_streak if roe else None,
            "nm_hold_streak_q": nm_streak if len(nm) >= 2 else None}


def compute_positive_streaks(payload_finn_q):
    """จำนวนไตรมาสติดกัน (จากงวดล่าสุดย้อนหลัง) ที่รายได้/กำไร/EBITDA/OCF เป็นบวก
    + เช็คว่างวดล่าสุด OCF > กำไรสุทธิไหม (คุณภาพกำไร ณ จุดเดียว ต่างจาก ocf_ni_ratio
    ที่เป็น TTM) — ใช้ Finnomena รายไตรมาสล้วน (ไม่พึ่ง Yahoo) ครอบคลุมหุ้นไทย/DR/mirror
    US-HK ทั้งหมดเหมือน compute_quality_streaks

    EBITDA ไม่มี field ตรงจาก Finnomena รายไตรมาส (มีแต่ EV/EBITDA ซึ่งเป็น ratio) —
    ประมาณจาก กำไรสุทธิ + ค่าเสื่อมราคา/ตัดจำหน่าย (ไม่รวมดอกเบี้ย+ภาษีเพิ่มกลับตาม
    นิยามเต็ม เพราะ Finnomena ไม่ให้ทั้งสองอย่างรายไตรมาส) — พอบอกเครื่องหมายบวก/ลบได้
    แม่นพอสมควร เพราะยิ่งเพิ่มดอกเบี้ย+ภาษีกลับเข้าไปยิ่งดันให้เป็นบวกมากขึ้น ไม่ใช่น้อยลง"""
    inc = (payload_finn_q or {}).get("income", {})
    cf = (payload_finn_q or {}).get("cashflow", {})

    def _pos_streak(row):
        vals = sorted((d, v) for d, v in (row or {}).items() if v is not None)
        if not vals:
            return None
        n = 0
        for _, v in reversed(vals):
            if v is None or v <= 0:
                break
            n += 1
        return n

    rev = inc.get("Total Revenue", {})
    ni = inc.get("Net Income", {})
    ocf = cf.get("Operating Cash Flow", {})
    da = cf.get("Depreciation And Amortization", {})

    ebitda = {d: ni[d] + da[d] for d in (set(ni) & set(da))
              if ni.get(d) is not None and da.get(d) is not None}

    common = sorted(set(ni) & set(ocf))
    ocf_gt_ni_latest = None
    if common:
        d = common[-1]
        if ni.get(d) is not None and ocf.get(d) is not None:
            ocf_gt_ni_latest = ocf[d] > ni[d]

    return {
        "rev_pos_streak_q": _pos_streak(rev),
        "profit_pos_streak_q": _pos_streak(ni),
        "ebitda_pos_streak_q": _pos_streak(ebitda) if ebitda else None,
        "ocf_pos_streak_q": _pos_streak(ocf),
        "ocf_gt_ni_latest": ocf_gt_ni_latest,
    }


def compute_seasonality(payload_q, field="Total Revenue", min_years=3):
    """หา 'ฤดูกาล' จากงบไตรมาสย้อนหลัง — seasonal index = ค่าไตรมาสนั้น ÷ เฉลี่ยทั้งปี
    เฉลี่ยข้ามปี (ใช้เฉพาะปีที่ครบ 4 ไตรมาส) : >1 = ไฮซีซั่น, <1 = โลว์ซีซั่น
    คืน {high_q, low_q, swing_pct (จุด% ระหว่างไฮ-โลว์), index:{1..4}, years} หรือ None
    ถ้าปีครบไม่ถึง min_years"""
    from collections import defaultdict
    row = (payload_q or {}).get("income", {}).get(field, {})
    by_year = defaultdict(dict)
    qmap = {"03": 1, "06": 2, "09": 3, "12": 4}
    for d, v in row.items():
        if v is None:
            continue
        q = qmap.get(d[5:7])
        if q:
            by_year[d[:4]][q] = v
    ratios = {1: [], 2: [], 3: [], 4: []}
    years = 0
    for qd in by_year.values():
        if len(qd) != 4:
            continue
        if any(v <= 0 for v in qd.values()):
            continue   # ต้องบวกทุกไตรมาส — กันปีที่รายได้เกือบศูนย์/ติดลบทำให้ ratio ระเบิด
        avg = sum(qd.values()) / 4
        years += 1
        for q in (1, 2, 3, 4):
            ratios[q].append(qd[q] / avg)
    if years < min_years:
        return None

    def _median(xs):
        xs = sorted(xs)
        n = len(xs)
        return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2

    # ใช้ median (ทนปีที่ข้อมูลเพี้ยนกว่าค่าเฉลี่ย)
    idx = {q: round(_median(ratios[q]) * 100, 1) for q in (1, 2, 3, 4)}
    high_q = max(idx, key=idx.get)
    low_q = min(idx, key=idx.get)
    swing = round(idx[high_q] - idx[low_q], 1)
    if swing > 150:
        return None   # สวิง >150 จุด% = ข้อมูลผิดปกติ (รายได้ไตรมาสเพี้ยน) ไม่รายงาน
    return {"high_q": high_q, "low_q": low_q, "swing_pct": swing, "index": idx, "years": years}


def _period_days(a, b):
    """จำนวนวันระหว่างสอง date string (YYYY-MM-DD...) — None ถ้า parse ไม่ได้"""
    from datetime import date as _date
    try:
        return (_date.fromisoformat(b[:10]) - _date.fromisoformat(a[:10])).days
    except Exception:
        return None


def _ttm_by_date(row):
    """แปลง series รายงวด {date: val} (flow เช่นรายได้/กำไร) เป็น {date: ยอดรวม 1 ปี (TTM)}

    ดูรอบรายงานจริง ไม่ใช่รวม 4 งวดติดกันตายตัว — หุ้น HK จำนวนหนึ่งรายงานครึ่งปี
    (มีค่าเฉพาะงวด 06/12) ถ้ารวม 4 งวดจะได้ยอด 2 ปี จึงหา k จากระยะห่างมัธยฐาน
    ระหว่างงวด (ราย Q ->4, ครึ่งปี ->2, รายปี ->1) และตัดจุดที่งวดในหน้าต่างขาดหาย
    (งวดแรกถึงงวดสุดท้ายใน k งวดห่างกันเกิน ~1 ปี) ทิ้ง"""
    rs = sorted((d, v) for d, v in (row or {}).items() if v is not None)
    if not rs:
        return {}
    rdates = [d for d, _ in rs]
    rvals = [v for _, v in rs]
    gaps = sorted(g for g in (_period_days(rdates[i - 1], rdates[i]) for i in range(1, len(rs)))
                  if g is not None and g > 0)
    if not gaps:
        return {}
    med_gap = gaps[len(gaps) // 2]
    k = 4 if med_gap <= 135 else (2 if med_gap <= 270 else 1)   # จำนวนงวดที่รวมเป็น 1 ปี
    out = {}
    for i in range(k - 1, len(rs)):
        span = _period_days(rdates[i - k + 1], rdates[i]) if k > 1 else 0
        if span is None or span > 340:
            continue
        out[rdates[i]] = sum(rvals[i - k + 1:i + 1])
    return out


def compute_ttm_growth(payload_q):
    """การเติบโตแบบ TTM (ยอดรวม 4 ไตรมาสล่าสุด เทียบ TTM ของ ~1 ปีก่อน, %):
      rev_ttm_yoy / profit_ttm_yoy — เรียบกว่า YoY รายไตรมาสเดี่ยว (เฉลี่ยทั้งปี
      ตัด noise งวดเดียว) ใช้ทำ PEG ให้หุ้น mirror US/HK ที่ไม่มี CAGR จาก Yahoo
    ฐานลบ/ศูนย์ -> None · cap ±3000% เหมือน compute_quarterly_growth"""
    _CAP = 3000.0
    out = {"rev_ttm_yoy": None, "profit_ttm_yoy": None}
    inc = (payload_q or {}).get("income", {})
    for field, key in (("Total Revenue", "rev_ttm_yoy"), ("Net Income", "profit_ttm_yoy")):
        ttm = _ttm_by_date(inc.get(field) or {})
        dates = sorted(ttm)
        if len(dates) < 2:
            continue
        last_d = dates[-1]
        base = None
        for d in dates[:-1]:
            gap = _period_days(d, last_d)
            if gap is not None and 330 <= gap <= 400:
                base = ttm[d]
        if base and base > 0:
            pct = round((ttm[last_d] - base) / base * 100, 2)
            out[key] = pct if abs(pct) <= _CAP else None
    return out


def compute_dividend_growth(payload_finn_q):
    """ปันผลโตต่อเนื่องกี่ปี — จาก DPS ≈ Dividend Yield × Close ต่องวด (Finnomena
    valuation มีทั้งคู่ย้อน ~16 ปี · yield เป็นฐาน TTM จึงเทียบปีต่อปีได้แม้ปีล่าสุด
    ยังไม่จบ) — ใช้จุดงวดท้ายสุดของแต่ละปีปฏิทิน แล้วนับปีติดกัน (ต้องเป็นปีถัดกันจริง)
    ที่ DPS สูงกว่าปีก่อน
    คืน {div_growth_streak_y, dps_last} — None ถ้าไม่มีข้อมูล/ปันผลหยุดจ่าย (stale
    เกิน ~2 ไตรมาสเทียบงวด Close ล่าสุด = ถือว่าไม่จ่ายแล้ว)"""
    val = (payload_finn_q or {}).get("valuation", {})
    dy = val.get("Dividend Yield", {}) or {}
    close = val.get("Close", {}) or {}
    none = {"div_growth_streak_y": None, "dps_last": None}
    pts = sorted((d, dy[d] * close[d] / 100.0) for d in (set(dy) & set(close))
                 if dy[d] and dy[d] > 0 and close[d] and close[d] > 0)
    if not pts:
        return none
    ref = max(close) if close else None
    gap = _period_days(pts[-1][0], ref) if ref else None
    if gap is not None and gap > 190:
        return none   # หยุดจ่ายแล้ว — ไม่นับ streak จากข้อมูลตกยุค
    by_year = {}
    for d, v in pts:
        y = int(d[:4])
        cur = by_year.get(y)
        if cur is None or d > cur[0]:
            by_year[y] = (d, v)
    years = sorted(by_year)
    streak = 0
    for i in range(len(years) - 1, 0, -1):
        y_cur, y_prev = years[i], years[i - 1]
        if y_cur - y_prev != 1:
            break   # ปีขาดช่วง (งดจ่ายกลางทาง) = ตัด streak
        if by_year[y_cur][1] > by_year[y_prev][1]:
            streak += 1
        else:
            break
    return {"div_growth_streak_y": streak, "dps_last": round(pts[-1][1], 4)}


def compute_ps(payload_finn_q, min_points=12):
    """P/S (Price to Sales) = Market Cap ÷ รายได้ TTM — คำนวณเองเพราะ
    Finnomena ไม่มี P/S ตรงๆ แต่มี mkt_cap + revenue รายไตรมาสย้อนยาว
    คืน {value ล่าสุด, percentile (ต่ำ=ถูกกว่าอดีต), median, n} เหมือน valuation percentile
    P/S ใช้ได้แม้หุ้นขาดทุน (ต่างจาก PE) — เทียบข้ามอุตสาหกรรมไม่ได้ ให้ดู percentile ตัวเอง
    (TTM ผ่าน _ttm_by_date — รองรับหุ้นรายงานครึ่งปี/รายปี และงวดขาดหาย)"""
    _days = _period_days
    mc = (payload_finn_q or {}).get("valuation", {}).get("Market Cap", {})
    rev = (payload_finn_q or {}).get("income", {}).get("Total Revenue", {})
    none = {"value": None, "percentile": None, "median": None, "mean": None, "n": 0}
    if not mc or not rev:
        return none
    ttm_by_date = _ttm_by_date(rev)
    if not ttm_by_date:
        return none
    ttm_dates = sorted(ttm_by_date)
    # P/S ผูกกับ 'วันที่ market cap' (สดกว่า/ถี่กว่าวันปิดงบ) = mktcap ÷ TTM rev ล่าสุด ณ ตอนนั้น
    # -> ค่าปัจจุบันใช้ราคาล่าสุดจริง สอดคล้องกับ PE/PBV (ไม่ใช้ราคาเก่า ณ วันปิดงบ)
    import bisect
    ps = []
    for d in sorted(mc):
        v = mc[d]
        if not v or v <= 0:
            continue
        idx = bisect.bisect_right(ttm_dates, d) - 1   # TTM ล่าสุดที่ ≤ วันของ market cap
        if idx < 0:
            continue
        gap = _days(ttm_dates[idx], d)
        if gap is None or gap > 400:
            continue   # รายได้หยุดอัพเดทนานเกินปี — ราคาสดหารรายได้ตกยุคจะหลอกตา
        ttm = ttm_by_date[ttm_dates[idx]]
        if ttm > 0:
            ps.append((d, v / ttm))
    if len(ps) < min_points:
        return {"value": (round(ps[-1][1], 2) if ps else None), "percentile": None,
                "median": None, "mean": None, "n": len(ps)}
    series = [v for _, v in ps]
    latest = ps[-1][1]
    pct = round(sum(1 for v in series if v < latest) / len(series) * 100, 1)
    srt = sorted(series)
    med = srt[len(srt) // 2] if len(srt) % 2 else (srt[len(srt) // 2 - 1] + srt[len(srt) // 2]) / 2
    avg = sum(series) / len(series)
    return {"value": round(latest, 2), "percentile": pct, "median": round(med, 2),
            "mean": round(avg, 2), "n": len(series)}


def _ttm_pe_series(payload_finn_q):
    """PE รายไตรมาสแบบ TTM (Close ÷ ผลรวม Basic EPS 4 ไตรมาสท้าย) — เหมือน
    _ttmPeSeries ฝั่ง static/dashboard.js ที่ใช้แก้บัค field 'PE' ดิบของ Finnomena
    (หารด้วย EPS ไตรมาสเดียวสำหรับงวดที่ไม่ใช่ Q4 ทำให้ PE พองผิดปกติ 50-655x
    เทียบ Q4 ปกติ 17-42x วัดจริงจาก BDMS) ใช้แทน val['PE'] ใน percentile ด้านล่าง
    เพื่อให้ Screener+/PEG ไม่ให้คะแนนหุ้นผิดจากบัคเดียวกัน"""
    inc = (payload_finn_q or {}).get("income", {})
    val = (payload_finn_q or {}).get("valuation", {})
    eps_row = inc.get("Basic EPS", {}) or {}
    close_row = val.get("Close", {}) or {}
    eps_items = sorted((d, v) for d, v in eps_row.items() if v is not None)
    if len(eps_items) < 4:
        return []
    out = []
    for i in range(3, len(eps_items)):
        ttm_eps = sum(v for _, v in eps_items[i - 3:i + 1])
        dt = eps_items[i][0]
        close = close_row.get(dt)
        if close is not None and ttm_eps > 0:
            out.append((dt, close / ttm_eps))
    return out


def compute_valuation_percentile(payload_finn_q, min_points=12):
    """เทียบ valuation งวดล่าสุด กับประวัติตัวเอง (จาก Finnomena valuation รายไตรมาส):
    คืนต่อ metric (pe/pbv/ev_ebitda/div_yield) = {value ล่าสุด, percentile (0-100),
    median, n} — percentile ต่ำ = ถูกกว่าค่ากลางในอดีตตัวเอง (สำหรับ PE/PBV/EV)
    ยกเว้น div_yield ที่ 'สูง = ถูก' (ให้ผู้เรียกตีความเอง)
    ต้องมีข้อมูล >= min_points งวดถึงจะคืนค่า (น้อยไปไม่มีนัยยะ)

    กันค่าตกยุค (stale): series เก็บเฉพาะจุดที่ค่าเป็นบวก — หุ้นที่พลิกขาดทุน (PE หาย)
    หรืองดปันผล (Dividend Yield หาย) จะเหลือจุดบวกสุดท้ายจากหลายปีก่อนค้างอยู่
    ถ้าเอามาโชว์เป็น 'ค่าปัจจุบัน' screener จะได้หุ้นผิด (เช่นกรอง div_yield >= 3
    เจอหุ้นที่เลิกจ่ายไปแล้ว) — จึงเทียบกับงวด Close ล่าสุด (Close มีทุกงวดเสมอ
    ตราบใดที่หุ้นยังซื้อขาย) ถ้าจุดบวกสุดท้ายเก่ากว่าเกิน ~2 ไตรมาส ให้ถือว่า
    metric นั้นไม่มีค่าปัจจุบัน (value/percentile = None)"""
    from datetime import date as _date
    val = (payload_finn_q or {}).get("valuation", {})
    close = val.get("Close", {}) or {}
    ref_date = max(close) if close else (
        max((d for row in val.values() for d in row), default=None))

    def _stale(last_d):
        if not ref_date or not last_d or last_d >= ref_date:
            return False
        try:
            return (_date.fromisoformat(ref_date[:10]) - _date.fromisoformat(last_d[:10])).days > 190
        except Exception:
            return False

    def _drop_spikes(pts, mult=20):
        """กรอง data point เดี่ยวที่เพี้ยนกะทันหัน (ห่างจาก median ทั้ง series เกิน mult เท่า)
        — field ดิบของ Finnomena เพี้ยนเป็นครั้งคราว (พบจริง: ADVANC EV/EBITDA นิ่ง 8-10x
        มา 65 ไตรมาส กระโดดเป็น ~1043x งวดเดียว) ใช้ threshold สัมพัทธ์กับประวัติตัวเอง ไม่ใช้
        absolute cap ตายตัว เพราะ PE/PBV/EV-EBITDA แต่ละหุ้น/เซคเตอร์มีช่วงปกติต่างกันมาก
        (หุ้นโตเร็วบางตัว เช่น DDOG EV/EBITDA ~350x ต่อเนื่องหลายไตรมาส คือของจริง ไม่ใช่ขยะ)"""
        if len(pts) < 5:
            return pts
        vs = sorted(v for _, v in pts)
        m = len(vs)
        med = vs[m // 2] if m % 2 else (vs[m // 2 - 1] + vs[m // 2]) / 2
        if med <= 0:
            return pts
        lo, hi = med / mult, med * mult
        return [(d, v) for d, v in pts if lo <= v <= hi]

    def _drop_stuck_tail(pts, min_repeat=4):
        """ตัดหางที่ค่าเดิมซ้ำกันเป๊ะติดกัน >= min_repeat งวด — เกิดจาก provider ไม่ได้คำนวณ
        ใหม่ (ค้างค่าล่าสุดที่เคยคำนวณได้ไว้) ไม่ใช่ธุรกิจนิ่งจริง เพราะ EV มาจาก mkt_cap ที่
        เปลี่ยนทุกวันซื้อขาย โอกาสค่า (float) ตรงกันเป๊ะหลายไตรมาสติดโดยบังเอิญแทบเป็นศูนย์
        (พบจริง: KASET EV/EBITDA ค่า 567.7013 ซ้ำกันเป๊ะ 7 ไตรมาสติด 2024Q3-2026Q1)
        ตัดทั้ง run ทิ้ง (ไม่ใช่แค่ตัวสุดท้าย) เพราะ median/percentile จะเพี้ยนถ้าเหลือค้างไว้"""
        if len(pts) < min_repeat:
            return pts
        tail_val = pts[-1][1]
        run = 1
        for i in range(len(pts) - 2, -1, -1):
            if pts[i][1] == tail_val:
                run += 1
            else:
                break
        return pts[: len(pts) - run] if run >= min_repeat else pts

    field_map = {"pe": "PE", "pbv": "PBV", "ev_ebitda": "EV To EBITDA", "div_yield": "Dividend Yield"}
    ttm_pe = None   # lazy: คำนวณครั้งเดียวถ้ามีการเรียกถึง short == "pe"
    out = {}
    for short, name in field_map.items():
        if short == "pe":
            # ใช้ TTM PE ที่คำนวณเองจาก Basic EPS แทน field 'PE' ดิบของ Finnomena
            # (หารด้วย EPS ไตรมาสเดียวสำหรับงวดที่ไม่ใช่ Q4 — ดู _ttm_pe_series)
            if ttm_pe is None:
                ttm_pe = _ttm_pe_series(payload_finn_q)
            pts = sorted((d, v) for d, v in ttm_pe if v is not None and v > 0)
            if not pts:   # ไม่มี Basic EPS พอคำนวณ TTM — fallback field ดิบ (ดีกว่าไม่มีเลย)
                series = val.get(name, {})
                pts = sorted((d, v) for d, v in series.items() if v is not None and v > 0)
        else:
            series = val.get(name, {})
            pts = sorted((d, v) for d, v in series.items() if v is not None and v > 0)
        if short != "div_yield":   # div_yield: median มักใกล้ 0 อยู่แล้ว filter สัมพัทธ์ไม่เสถียร
            pts = _drop_stuck_tail(pts)
            pts = _drop_spikes(pts)
        if pts and _stale(pts[-1][0]):
            out[short] = {"value": None, "percentile": None, "median": None, "mean": None,
                          "n": len(pts), "stale": True}
            continue
        if len(pts) < min_points:
            out[short] = {"value": (pts[-1][1] if pts else None), "percentile": None,
                          "median": None, "mean": None, "n": len(pts)}
            continue
        vals = [v for _, v in pts]
        latest = pts[-1][1]
        below = sum(1 for v in vals if v < latest)
        pct = round(below / len(vals) * 100, 1)
        srt = sorted(vals)
        m = srt[len(srt) // 2] if len(srt) % 2 else (srt[len(srt)//2 - 1] + srt[len(srt)//2]) / 2
        avg = sum(vals) / len(vals)
        out[short] = {"value": round(latest, 2), "percentile": pct,
                      "median": round(m, 2), "mean": round(avg, 2), "n": len(vals)}
    return out


def compare_sources(payload_yahoo, payload_set):
    """เทียบ Total Revenue/Net Income (Yahoo) กับ sales/netProfit (SET.or.th, ×1000 ปรับหน่วย)
    เฉพาะปีที่ SET เป็นงบเต็มปี (quarter=='Q9') เท่านั้น — flag ถ้าต่างกันเกิน 5%
    คืน {status: 'ok'|'mismatch'|'insufficient_data', mismatches: [...], checked_years: [...]}"""
    THRESHOLD_PCT = 5.0

    inc = (payload_yahoo or {}).get("income", {})
    yahoo_rev = {int(d[:4]): v for d, v in inc.get("Total Revenue", {}).items()}
    yahoo_ni  = {int(d[:4]): v for d, v in inc.get("Net Income", {}).items()}

    set_full_years = {
        e["year"]: e for e in (payload_set or {}).get("entries", [])
        if e.get("quarter") == "Q9"
    }

    common_years = sorted(set(yahoo_rev) & set(set_full_years))
    if not common_years:
        return {"status": "insufficient_data", "mismatches": [], "checked_years": []}

    mismatches = []
    for y in common_years:
        entry = set_full_years[y]
        checks = [
            ("Revenue", yahoo_rev.get(y), entry.get("sales")),
            ("Net Income", yahoo_ni.get(y), entry.get("netProfit")),
        ]
        for metric, yv, sv in checks:
            if yv is None or sv is None:
                continue
            sv_baht = sv * 1000
            denom = max(abs(yv), abs(sv_baht), 1)
            diff_pct = abs(yv - sv_baht) / denom * 100
            if diff_pct > THRESHOLD_PCT:
                mismatches.append({
                    "metric": metric, "year": y,
                    "yahoo": yv, "set": sv_baht, "diff_pct": round(diff_pct, 2),
                })

    return {
        "status": "mismatch" if mismatches else "ok",
        "mismatches": mismatches,
        "checked_years": common_years,
    }


_PRICE_DB_BY_MARKET = {"TH": "set_prices.db", "US": "us_prices.db", "HK": "hk_prices.db"}


def _price_db_ticker(mkt, raw_sym):
    if mkt == "TH":
        return raw_sym + ".BK"
    if mkt == "HK":
        return (raw_sym.zfill(4) if raw_sym.isdigit() and len(raw_sym) < 4 else raw_sym) + ".HK"
    return raw_sym   # US: bare ticker


def _nearest_real_close(base_dir, mkt, raw_sym, date_str, max_gap_days=10):
    """ราคาปิดจริงล่าสุดที่ <= date_str จาก {market}_prices.db (local) — คืน None ถ้าไม่มี DB/
    ไม่มีราคา/ห่างจากวันที่ขอเกิน max_gap_days (กันเอาราคาเก่ามาเทียบกับงวดที่ห่างเกินไป)"""
    db_name = _PRICE_DB_BY_MARKET.get(mkt)
    if not db_name:
        return None
    path = os.path.join(base_dir, db_name)
    if not os.path.exists(path):
        return None
    ticker = _price_db_ticker(mkt, raw_sym)
    con = sqlite3.connect(path)
    try:
        row = con.execute(
            "SELECT date, close FROM prices WHERE ticker=? AND date<=? ORDER BY date DESC LIMIT 1",
            (ticker, date_str)).fetchone()
    finally:
        con.close()
    if not row or row[1] is None or row[1] <= 0:
        return None
    gap = (datetime.fromisoformat(date_str[:10]) - datetime.fromisoformat(row[0][:10])).days
    if gap > max_gap_days:
        return None
    return row[1]


def _yahoo_bvps_series(base_dir, symbol, is_dr):
    """BVPS = Stockholders Equity ÷ Ordinary Shares Number จากงบ Yahoo (yahoo_q ก่อน ตกไป yahoo)
    ใช้เป็นแหล่งอิสระเทียบกับ 'Book Value Per Share' ดิบของ Finnomena"""
    for src in ("yahoo_q", "yahoo"):
        d = get(base_dir, symbol, src, is_dr=is_dr)
        if not d:
            continue
        bal = d.get("balance", {}) or {}
        eq = bal.get("Stockholders Equity") or bal.get("Common Stock Equity") or {}
        shares = bal.get("Ordinary Shares Number") or bal.get("Share Issued") or {}
        out = {}
        for dt, e in eq.items():
            s = shares.get(dt)
            if e is not None and s and s > 0:
                out[dt] = e / s
        if out:
            return out
    return {}


def check_valuation_quality(base_dir, symbol, payload, market=None, is_dr=False, recent_n=8):
    """เช็คความน่าเชื่อถือของ Close/BVPS ใน payload finnomena_q แบบเบาๆ (ไม่บล็อกการใช้งาน แค่
    ติดป้ายเตือน) — เทียบกับ 2 แหล่งอิสระที่มีอยู่แล้วในเครื่อง: ราคาปิดจริงจาก {set,us,hk}_prices.db
    และ BVPS ที่คำนวณเองจากงบ Yahoo (Stockholders Equity ÷ shares)

    เหตุผล: เจอจริงว่า Finnomena มีข้อมูลค้าง (ราคา/BVPS ไม่อัพเดทหลายไตรมาสติด) โดยเฉพาะหุ้นที่
    กำลังมีปัญหา (equity ติดลบ/ใกล้ล้ม เช่น NWR/TSR) — ราคาที่ Finnomena เก็บอาจต่างจากราคาที่เทรด
    จริงในตลาดหลายเท่าตัวโดยไม่มีสัญญาณเตือนใดๆ ทั้งที่ TH เรามีราคาจริงเก็บไว้ใน set_prices.db
    อยู่แล้ว (ไม่ต้องดึงใหม่) จึงเทียบได้ฟรี ไม่กระทบ performance มาก (query เบาๆ per ไตรมาส)

    เกณฑ์ threshold (15%/40%) กว้างพอเผื่อผลต่างปกติจากปัดเศษ/ฐานคำนวณต่างกัน (พิสูจน์จากเคส
    BGRIM ~5% ถือว่าปกติ ไม่ติดป้าย) แต่จับได้กรณีที่ต่างเป็นสิบ/ร้อยเปอร์เซ็นต์แบบ NWR/TSR/BGRIM
    บางไตรมาส (BVPS กระโดด 0.87->13x)"""
    import bisect
    warnings = []
    val = (payload or {}).get("valuation", {}) or {}
    close = val.get("Close", {}) or {}
    bvps = val.get("Book Value Per Share", {}) or {}
    if not close:
        return {"checked": False, "warnings": []}

    dates = sorted(close.keys())[-recent_n:]
    # ทั้ง yahoo/yahoo_q และ {market}_prices.db เก็บด้วยรหัสดิบ ไม่มี namespace "FINN:TH:/HK:/US:"
    # แบบ finnomena_q — ต้องตัด prefix ก่อนใช้ค้นหาข้าม source เสมอ
    raw_sym = symbol.split(":")[-1] if symbol.startswith("FINN:") else symbol

    # 1) ค่าค้าง (ซ้ำติดกัน >=3 ไตรมาส) — สัญญาณข้อมูลไม่อัพเดทตรงๆ ไม่ต้องพึ่งแหล่งอื่นเลย
    def _frozen_runs(series_map):
        runs, streak_val, streak_dates = [], None, []
        for d in dates:
            v = series_map.get(d)
            if v is None:
                if len(streak_dates) >= 3:
                    runs.append((streak_val, list(streak_dates)))
                streak_val, streak_dates = None, []
                continue
            if streak_val is not None and v == streak_val:
                streak_dates.append(d)
            else:
                if len(streak_dates) >= 3:
                    runs.append((streak_val, list(streak_dates)))
                streak_val, streak_dates = v, [d]
        if len(streak_dates) >= 3:
            runs.append((streak_val, list(streak_dates)))
        return runs

    for v, ds in _frozen_runs(close):
        warnings.append({"type": "stale_close",
                          "detail": f"ราคาปิดค้างที่ {v} ติดต่อกัน {len(ds)} ไตรมาส ({ds[0]}..{ds[-1]}) — อาจไม่ใช่ราคาตลาดจริง",
                          "dates": ds})
    for v, ds in _frozen_runs(bvps):
        warnings.append({"type": "stale_bvps",
                          "detail": f"BVPS ค้างที่ {v} ติดต่อกัน {len(ds)} ไตรมาส ({ds[0]}..{ds[-1]})",
                          "dates": ds})

    # 2) เทียบราคาจริงจาก {market}_prices.db — ข้าม DR (ราคา DR บนตลาดไทยไม่จำเป็นต้องเท่า
    # underlying เป๊ะ เทียบตรงๆ ไม่ได้)
    if not is_dr:
        mkt = (market or "TH").upper()
        mismatches = []
        for d in dates:
            c = close.get(d)
            if c is None or c <= 0:
                continue
            real = _nearest_real_close(base_dir, mkt, raw_sym, d)
            if real is None:
                continue
            diff = abs(c - real) / real
            if diff > 0.15:
                mismatches.append((d, c, real, round(diff * 100, 1)))
        if mismatches:
            worst = max(mismatches, key=lambda x: x[3])
            warnings.append({
                "type": "price_mismatch",
                "detail": f"ราคาปิดของ Finnomena ต่างจากราคาซื้อขายจริงเกิน 15% ใน {len(mismatches)} ไตรมาส "
                          f"(หนักสุด {worst[0]}: Finnomena {worst[1]} vs จริง {worst[2]} ต่าง {worst[3]}%)",
                "dates": [m[0] for m in mismatches],
            })

    # 3) เทียบ BVPS กับที่คำนวณเองจากงบ Yahoo
    ybvps = _yahoo_bvps_series(base_dir, raw_sym, is_dr)
    if ybvps:
        ydates = sorted(ybvps.keys())
        mismatches = []
        for d in dates:
            b = bvps.get(d)
            if b is None:
                continue
            idx = bisect.bisect_right(ydates, d) - 1
            if idx < 0:
                continue
            yv = ybvps[ydates[idx]]
            if not yv:
                continue
            diff = abs(b - yv) / abs(yv)
            if diff > 0.4:
                mismatches.append((d, b, round(yv, 3), round(diff * 100, 1)))
        if mismatches:
            worst = max(mismatches, key=lambda x: x[3])
            warnings.append({
                "type": "bvps_mismatch",
                "detail": f"BVPS ของ Finnomena ต่างจากที่คำนวณเองจากงบ Yahoo (equity÷shares) เกิน 40% ใน {len(mismatches)} ไตรมาส "
                          f"(หนักสุด {worst[0]}: Finnomena {worst[1]} vs Yahoo {worst[2]} ต่าง {worst[3]}%)",
                "dates": [m[0] for m in mismatches],
            })

    return {"checked": True, "warnings": warnings}
