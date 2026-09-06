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
from datetime import date, datetime, timedelta

import requests

from sources.set_api import _bootstrap_headers, _get_json
from sources.dr_universe import _DR_STATIC, load_dr_universe

# project root (โฟลเดอร์แม่ของ sources/) — ใช้หา dr_universe_auto.json ตอน resolve
# yf ticker ของ DR โดยไม่ต้องส่ง base_dir ผ่านทุก caller
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_FILE = "financials.db"

# sync_all() ที่แตะหุ้นตั้งแต่จำนวนนี้ขึ้นไปถือว่าเป็น batch sync จริง (ไม่ใช่ fallback
# sync หุ้นเดี่ยวๆ ตอนเปิดหน้า Tearsheet เจอข้อมูลขาด) — ถึงจะ trigger auto-rebuild
# factor_snapshot ต่อท้าย (ดู factor_snapshot.schedule_rebuild)
_AUTO_REBUILD_MIN_SYMBOLS = 5


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


def snapshot_stale_vs_sync(base_dir, snapshot_at_key, ref_key="last_full_sync_at"):
    """True ถ้า precomputed snapshot (factor_snapshot/mirror/dcf/pbv-pe) ถูกคำนวณ 'ก่อน'
    ข้อมูลต้นทางที่ sync ครั้งล่าสุด — ให้ฝั่ง UI ขึ้น badge เตือนว่าควรกด rebuild/คำนวณใหม่
    timestamp เก็บเป็น string "%Y-%m-%d %H:%M:%S" (fixed-width) เทียบ lexicographic ได้ตรงๆ
    ไม่มี ref = ยังไม่เคย sync → ไม่ถือว่า stale · มี ref แต่ไม่มี snapshot = stale"""
    ref = _get_meta(base_dir, ref_key)
    if not ref:
        return False
    snap = _get_meta(base_dir, snapshot_at_key)
    if not snap:
        return True
    return str(snap) < str(ref)


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


def _merge_set_qpl_payload(old, new):
    """ผสานงวด SET official (ตาราง P&L รายไตรมาส) ใหม่เข้ากับที่เก็บสะสมไว้ — periods endpoint
    ของ SET มีแค่ปีปัจจุบัน+ปีก่อนหน้าเท่านั้น (เช็คแล้ว 2026-08-12) พองวดเก่าเลื่อนหลุดออกจาก
    periods list ข้อมูลละเอียด (COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี) ของงวดนั้นดึงซ้ำไม่ได้อีกเลย
    ต้องเก็บสะสมถาวรทุกรอบ sync (หลักการเดียวกับ yahoo_q ที่ yfinance backfill ย้อนหลังไม่ได้)
    งวดที่เคย detail=True ห้ามถูกทับด้วยของใหม่ที่หยาบกว่า (detail=False) — เผื่อรอบ fetch ถัดไป
    period นั้นหลุดจาก periods list แล้วเหลือแค่ระดับ chart"""
    if old is None:
        return new
    merged_q = dict(old.get("quarters", {}))
    for key, row in new.get("quarters", {}).items():
        old_row = merged_q.get(key)
        if old_row and old_row.get("detail") and not row.get("detail"):
            continue
        merged_q[key] = row
    return {"quarters": merged_q}


def _merge_health_periods(old_periods, new_periods):
    """union periods list ของ set_health ตาม as_of_date — helper ของ _merge_set_health_payload"""
    merged = {p["as_of_date"]: p for p in (old_periods or []) if p.get("as_of_date")}
    for p in (new_periods or []):
        if p.get("as_of_date"):
            merged[p["as_of_date"]] = p
    return sorted(merged.values(), key=lambda p: p["as_of_date"])


def _merge_set_health_payload(old, new):
    """ผสาน SET Financial Health Check (source='set_health') สะสมข้ามรอบ sync — periods endpoint
    คืนแค่หน้าต่างจำกัด (3 ปีเต็ม + งวดครึ่งปีล่าสุด 2 งวด) พองวดเก่าเลื่อนหลุดออกจากหน้าต่างนี้
    ค่าที่เคยดึงมาแล้วจะหายถาวรถ้าไม่สะสมไว้ (เหตุผลเดียวกับ set_qpl/set_cashflow) — merge ที่
    ระดับ theme(name) > category(name) > account(code) > value(as_of_date) คงค่า as_of_date เก่า
    ที่ไม่มีในรอบใหม่ไว้ ให้ค่าใหม่ทับค่า as_of_date เดียวกัน (เผื่อ SET แก้เลขย้อนหลังตอนงบ
    ผ่านตรวจสอบ) — คงรูปทรง nested เดิมของ fetch_financial_health ไว้ทุกประการ (ไม่แปลง schema)
    เพื่อให้ route/frontend ที่ใช้อยู่แล้วอ่านต่อได้โดยไม่ต้องแก้"""
    if old is None:
        return new
    old_themes = {t["name"]: t for t in old.get("themes", [])}
    for new_theme in new.get("themes", []):
        old_theme = old_themes.get(new_theme["name"])
        if not old_theme:
            old_themes[new_theme["name"]] = new_theme
            continue
        old_cats = {c["name"]: c for c in old_theme.get("categories", [])}
        for new_cat in new_theme.get("categories", []):
            old_cat = old_cats.get(new_cat["name"])
            if not old_cat:
                old_cats[new_cat["name"]] = new_cat
                continue
            old_accs = {a["code"]: a for a in old_cat.get("accounts", [])}
            for new_acc in new_cat.get("accounts", []):
                old_acc = old_accs.get(new_acc["code"])
                if not old_acc:
                    old_accs[new_acc["code"]] = new_acc
                    continue
                merged_vals = {v["as_of_date"]: v for v in old_acc.get("values", []) if v.get("as_of_date")}
                for v in new_acc.get("values", []):
                    if v.get("as_of_date"):
                        merged_vals[v["as_of_date"]] = v
                old_acc["values"] = sorted(merged_vals.values(), key=lambda v: v["as_of_date"])
                old_acc["unit"], old_acc["change_unit"] = new_acc.get("unit"), new_acc.get("change_unit")
            old_cat["accounts"] = list(old_accs.values())
            old_cat["description"] = new_cat.get("description") or old_cat.get("description")
        old_theme["categories"] = list(old_cats.values())
    return {"symbol": new.get("symbol", old.get("symbol")),
            "periods": _merge_health_periods(old.get("periods"), new.get("periods")),
            "themes": list(old_themes.values())}


def _merge_factsheet_period_list(old_list, new_list):
    """union list ของ period-object (cash_cycle/financial_ratio/financial_growth — รูปร่าง
    เดียวกันทั้ง 3 ตัว) ตาม key (quarter,year) — endpoint คืนแค่หน้าต่างแคบ (4 งวด/ครั้ง) ต้อง
    สะสมข้าม sync เหมือน set_health (ดู _merge_set_health_payload) แต่ตื้นกว่า 1 ชั้น (ไม่มี
    theme/category ซ้อน แค่ period -> data array) union ชั้นในด้วย account_name เดียวกัน
    new_list เป็น None (sub-endpoint fetch พลาด/404) คืน old_list เดิมทันที กันลบข้อมูลสะสม"""
    if new_list is None:
        return old_list
    if old_list is None:
        return new_list
    old_by_key = {(p.get("quarter"), p.get("year")): p for p in old_list}
    for new_p in new_list:
        key = (new_p.get("quarter"), new_p.get("year"))
        old_p = old_by_key.get(key)
        if not old_p:
            old_by_key[key] = new_p
            continue
        merged_data = {d["account_name"]: d for d in old_p.get("data", []) if d.get("account_name")}
        for d in new_p.get("data", []):
            if d.get("account_name"):
                merged_data[d["account_name"]] = d
        old_p["data"] = list(merged_data.values())
        for f in ("as_of_date", "fs_type", "begin_date", "end_date", "is_restatement"):
            nv = new_p.get(f)
            if nv is not None:
                old_p[f] = nv   # อย่าทับด้วย None — รอบ fetch ถัดไปที่ SET คืน period นั้นมาไม่ครบ
                                # ทุก key (เช่นไม่มี is_restatement/begin_date) จะล้างค่าเดิมที่ดีอยู่
    return sorted(old_by_key.values(), key=lambda p: p.get("as_of_date") or "")


def _merge_factsheet_trading_stat(old_list, new_list):
    """union list แบบแบนของ trading_stat ตาม key 'period' (เช่น 'YTD'/'2025'/'2024') — ทั้ง
    record แทนที่กันตรงๆ เมื่อ key ชนกัน (ไม่มี data ซ้อนให้ union ชั้นในเหมือน
    _merge_factsheet_period_list) ดังนั้น 'YTD' ที่เป็นตัวเลขวิ่งเปลี่ยนทุกวันจะถูกค่าใหม่ทับ
    เสมอโดยอัตโนมัติ ส่วนปีเต็มที่ปิดไปแล้วแทบไม่เปลี่ยน แต่ก็ทับได้เช่นกันถ้า SET แก้ย้อนหลัง"""
    if new_list is None:
        return old_list
    if old_list is None:
        return new_list
    merged = {p.get("period"): p for p in old_list if p.get("period")}
    for p in new_list:
        if p.get("period"):
            merged[p["period"]] = p
    return list(merged.values())


def _merge_set_factsheet_payload(old, new):
    """ผสาน SET factsheet (source='set_factsheet') สะสมข้ามรอบ sync — รวม 5 sub-endpoint
    (cash_cycle/financial_ratio/financial_growth/trading_stat ใช้ merge สะสม เพราะแต่ละรอบ
    ได้แค่หน้าต่างแคบ, price_performance เป็น snapshot ปัจจุบันล้วนๆ ไม่มีประวัติ overwrite
    ทั้งก้อนเมื่อรอบนี้ได้ค่าจริง — ถ้ารอบนี้ None (fetch พลาด/body ว่าง, ดู
    fetch_factsheet_price_performance ที่คืน None เมื่อทุก field เป็น None) ใช้ของเก่าต่อ)
    key ไหน fetch พลาดรอบนี้ (None) ใช้ของเก่าต่อ ไม่ลบทิ้ง"""
    if old is None:
        return new
    return {
        "symbol": new.get("symbol", old.get("symbol")),
        "cash_cycle": _merge_factsheet_period_list(old.get("cash_cycle"), new.get("cash_cycle")),
        "financial_ratio": _merge_factsheet_period_list(old.get("financial_ratio"), new.get("financial_ratio")),
        "financial_growth": _merge_factsheet_period_list(old.get("financial_growth"), new.get("financial_growth")),
        "trading_stat": _merge_factsheet_trading_stat(old.get("trading_stat"), new.get("trading_stat")),
        "price_performance": new.get("price_performance") or old.get("price_performance"),
    }


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
    elif source in ("set_qpl", "set_cashflow", "set_balance"):   # โครง {"quarters": {...}} เดียวกัน
        merged = _merge_set_qpl_payload(old, payload)
    elif source == "set_health":
        merged = _merge_set_health_payload(old, payload)
    elif source == "set_factsheet":
        merged = _merge_set_factsheet_payload(old, payload)
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


def get(base_dir, symbol, source, is_dr=False, market=None, con=None):
    """con=None (ปกติ): เปิด/ปิด connection ของตัวเอง — ใช้ตอนเรียกเดี่ยวๆ

    con=<connection ที่เปิดไว้แล้ว>: reuse connection เดิม ไม่เปิด/ปิดใหม่ — ใช้ตอน caller
    วนเรียก get() หลายร้อย/พันครั้งติดกัน (เช่น _compute_fin_analytics_for ใน app.py ที่วน
    ทุกหุ้นในตลาด) เดิมเปิด-ปิด sqlite connection ใหม่ทุกครั้ง วัดจริงตอน /api/financials-analytics
    cache miss: 3,419 connection กิน connect+close+PRAGMA รวม ~5 วิ จาก 13 วิทั้งหมด"""
    if not db_exists(base_dir):
        return None
    key = _dr_key(symbol) if is_dr else symbol
    _owns_con = con is None
    if con is None:
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
                    try:
                        payload = json.loads(mrow[0])
                    except (TypeError, ValueError):
                        continue
                    if payload.get("empty"):
                        return None
                    row = mrow
                    break
    finally:
        if _owns_con:
            con.close()
    if not row:
        return None
    # payload เสีย (เขียนไฟล์ค้างกลางคัน/backup ตัดไม่สมบูรณ์) ไม่ควรทำให้ caller ทั้งหมด
    # ล้ม — /api/financials-analytics วนเรียก get() ทุกหุ้นในตลาด ถ้าโดนแถวเดียวพังไม่กัน
    # ไว้ตรงนี้ 500 ทั้ง endpoint (ดู get_names_bulk ที่กันแบบเดียวกันไว้แล้ว)
    try:
        data = json.loads(row[0])
    except (TypeError, ValueError):
        return None
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


def iter_source_payloads(base_dir, source, is_dr=False):
    """yield (symbol, payload_dict) ของทุกแถว source นี้ในตาราง financials — query ครั้งเดียว
    (ต่างจากวน get() รายตัวหลายร้อยครั้ง) ใช้ตอน caller ต้องสแกนเนื้อ payload ทั้งตลาด เช่น
    Data Health cross-check วงจรเงินสด SET vs Yahoo (ดู PLAN_set_api_expansion.txt งาน #5C)
    ข้าม namespace DR:/FINN: เมื่อ is_dr=False (คืนเฉพาะหุ้นไทยจริง เหมือน get_synced_symbols)
    payload ที่ parse ไม่ได้ถูกข้ามเงียบ ๆ (ไม่ raise — เป็นการสแกนเสริม ไม่ควรล้มทั้ง caller)"""
    if not db_exists(base_dir):
        return
    con = _connect(base_dir)
    try:
        rows = con.execute("SELECT symbol, payload FROM financials WHERE source=?", (source,)).fetchall()
    finally:
        con.close()
    for sym, payload in rows:
        if is_dr:
            if not sym.startswith("DR:"):
                continue
            sym = sym[3:]
        elif sym.startswith("DR:") or sym.startswith("FINN:"):
            continue
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            continue
        yield sym, data


def _target_period(source, today=None):
    """วันสิ้นงวดล่าสุดที่ 'ควรจะมีข้อมูลแล้ว' ของแหล่งนั้น ณ วันนี้ — ใช้เทียบกับ
    _payload_latest_period() ตัดสิน skip ใน sync_all(skip_up_to_date=True)

    ใช้ไตรมาสปฏิทินล่าสุดที่ปิดไปแล้ว (Q1=31มี.ค./Q2=30มิ.ย./Q3=30ก.ย./Q4=31ธ.ค.) เป็น
    target ของแหล่งรายไตรมาส (set/set_qpl/set_cashflow/set_balance/yahoo_q/finnomena_q) —
    ไม่ต้องรอ deadline ยื่นงบ 45/60 วัน เพราะ target แค่บอกว่า "ควรลองอีกไหม" ไม่ใช่
    "รับประกันว่ามีแน่" หุ้นที่ยังไม่ยื่นจะยังไม่ผ่าน check นี้เอง (latest_period ใน DB ไม่ถึง
    target) เลยถูก sync ซ้ำทุกครั้งจนกว่าจะยื่นจริง ไม่ต้องเดางวด/deadline ให้ผิดพลาดได้ —
    ต่างจาก 'yahoo' (รายปี) ที่ใช้แค่ 31 ธ.ค. และ 'set_health' (รายครึ่งปี — SET Financial
    Health Check ให้แค่จุดข้อมูลรายปี+ครึ่งปี ไม่มี Q1/Q3 เลย ใช้ target แบบไตรมาสทั่วไปจะ
    ไม่ match กับ as_of_date จริงช่วง เม.ย.-มิ.ย./ต.ค.-ธ.ค. ทำให้ skip_up_to_date ข้ามไม่ได้
    เลยครึ่งปี, code review 2026-08-26) ที่ใช้แค่ 30 มิ.ย./31 ธ.ค."""
    today = today or date.today()
    y = today.year
    if source == "yahoo":
        candidates = [date(y - 1, 12, 31), date(y, 12, 31)]
    elif source in ("set_health", "set_factsheet"):
        candidates = [date(y - 1, 12, 31), date(y, 6, 30), date(y, 12, 31)]
    else:
        candidates = [date(y - 1, 12, 31), date(y, 3, 31), date(y, 6, 30),
                      date(y, 9, 30), date(y, 12, 31)]
    return max(d for d in candidates if d <= today)


def _payload_latest_period(source, payload):
    """แกะวันสิ้นงวดล่าสุดที่ 'มีอยู่จริง' ใน payload ที่เก็บไว้ (ไม่ใช่ synced_at ที่บอกแค่
    เวลาที่เคยยิง fetch) — เทียบกับ _target_period() เพื่อตัดสิน skip แบบดูเนื้อหาจริง
    แทนเดาจากเวลา คืน None ถ้า parse ไม่ได้/ยังไม่มีข้อมูล (นับเป็น 'ยังไม่ครบ' เสมอ — ปลอดภัย
    กว่า ไม่ skip หุ้นที่ควรจะ sync ต่อผิด)"""
    if not payload:
        return None
    try:
        if source in ("set_qpl", "set_cashflow", "set_balance"):   # โครง quarters dict เดียวกัน
            qend = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
            dates = []
            for k in (payload.get("quarters") or {}):
                y, q = k.split("-")
                y, q = int(y), int(q)
                if q in qend:
                    m, d = qend[q]
                    dates.append(date(y, m, d))
            return max(dates) if dates else None
        if source == "set_health":
            dates = [date.fromisoformat(p["as_of_date"]) for p in (payload.get("periods") or [])
                     if p.get("as_of_date")]
            return max(dates) if dates else None
        if source == "set_factsheet":
            # cash_cycle เป็น None แน่นอนสำหรับหุ้นกลุ่มการเงิน/REIT (404 ที่ต้นทาง) — fallback
            # ไป financial_ratio ที่ครอบคลุมทุกหุ้นแทน กันหุ้นกลุ่มนี้ค้างสถานะ "ยังไม่มีข้อมูล"
            # ทั้งที่ sync สำเร็จแล้ว (skip_up_to_date จะไม่ยอม skip ให้ ยิงซ้ำทุกรอบไม่จบ)
            periods = payload.get("cash_cycle") or payload.get("financial_ratio") or []
            dates = [date.fromisoformat(p["as_of_date"]) for p in periods if p.get("as_of_date")]
            return max(dates) if dates else None
        if source == "set":
            dates = [date.fromisoformat(e["endDate"][:10])
                     for e in (payload.get("entries") or []) if e.get("endDate")]
            return max(dates) if dates else None
        if source in ("yahoo", "yahoo_q", "finnomena_q"):
            inc = payload.get("income") or {}
            # ยึดงวดล่าสุดจาก 'บรรทัดงบหลัก' เท่านั้น — Finnomena (และบางที Yahoo) เผยแพร่
            # Basic EPS ของไตรมาสล่วงหน้าก่อนบรรทัดงบจริง (Revenue/Net Income ยังว่าง) ถ้านับ
            # ทุก field จะได้งวดล่วงหน้าที่ยังไม่มีงบจริง ทำให้ skip_up_to_date (+ get_quarter_coverage)
            # ข้ามการ sync งบไตรมาสนั้นไปถาวรทั้งที่ยังไม่เข้า (เจอ ~445 หุ้นไทย, IVL Q2/2026)
            anchors = [inc.get(k) for k in ("Total Revenue", "Net Income")]
            dstrs = {d for m in anchors if isinstance(m, dict) for d in m}
            if not dstrs:   # payload เก่า/ทรงแปลกที่ไม่มี anchor — คงพฤติกรรมเดิม
                dstrs = {d for field_map in inc.values() if isinstance(field_map, dict)
                         for d in field_map}
            return date.fromisoformat(max(dstrs)[:10]) if dstrs else None
    except Exception:
        return None
    return None


def get_latest_period_map(base_dir, is_dr=False, sources=None):
    """คืน {(symbol, source): date|None} วันสิ้นงวดล่าสุดที่มีอยู่จริงในแต่ละคู่ (หุ้น, แหล่ง)
    ของ universe ฝั่งนั้น — ใช้ทำ incremental sync แบบดูเนื้อหาจริง (ดู sync_all
    skip_up_to_date) แทนเดาจากเวลา sync ล่าสุดแบบเดิม (get_synced_map ตัวเก่า)

    sources: จำกัดเฉพาะแหล่งที่สนใจ (กัน query/parse payload ของแหล่งอื่นที่ไม่เกี่ยวทิ้งโดยเปล่า
    ประโยชน์ — payload รวมของ 5 แหล่ง × ~930 หุ้นไทย ~86MB อ่านทีเดียวได้เร็วพอสำหรับปุ่มกดมือ)"""
    if not db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        if sources:
            qmarks = ",".join("?" * len(sources))
            rows = con.execute(
                f"SELECT symbol, source, payload FROM financials WHERE source IN ({qmarks})",
                list(sources)).fetchall()
        else:
            rows = con.execute("SELECT symbol, source, payload FROM financials").fetchall()
    finally:
        con.close()
    out = {}
    for sym, src, payload_raw in rows:
        if sym.startswith("FINN:"):
            continue   # mirror ทั้งตลาด — ไม่เกี่ยวกับ sync รายตัว
        is_dr_row = sym.startswith("DR:")
        if is_dr_row != is_dr:
            continue
        key = sym[3:] if is_dr_row else sym
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = None
        out[(key, src)] = _payload_latest_period(src, payload)
    return out


def get_latest_period_map_raw(base_dir, sources):
    """เหมือน get_latest_period_map แต่คืนคีย์เป็น symbol เต็มตามที่เก็บจริงใน DB (ไม่ตัด
    prefix 'DR:'/'FINN:{ex}:' ออก ไม่กรอง namespace ไหนทิ้งเลย) — ใช้เมื่อ caller ต้องเทียบ
    ข้าม namespace เอง เช่นเช็คสมาชิกดัชนีหลัก US/HK/JP ที่บางตัวอยู่ในพอร์ต DR ('DR:{sym}')
    บางตัวมีแค่ใน mirror ทั้งตลาด ('FINN:{ex}:{code}' จากปุ่ม 'Mirror ทั้งตลาด') แยกกันคนละที่"""
    if not db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        qmarks = ",".join("?" * len(sources))
        rows = con.execute(
            f"SELECT symbol, source, payload FROM financials WHERE source IN ({qmarks})",
            list(sources)).fetchall()
    finally:
        con.close()
    out = {}
    for sym, src, payload_raw in rows:
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = None
        out[(sym, src)] = _payload_latest_period(src, payload)
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


def get_quarter_coverage(base_dir, symbols, sources=("set", "set_qpl", "yahoo_q", "finnomena_q"),
                         is_dr=False, today=None):
    """เทียบ universe กับ 'ไตรมาสล่าสุดที่ควรจะมีข้อมูลแล้ว' (_target_period) ต่อ source —
    ต่างจาก get_coverage() ที่เช็คแค่ 'มีข้อมูลหรือยัง' (แม้จะเก่าแค่ไหนก็นับว่า covered)
    ตัวนี้เช็ค 'ข้อมูลที่มีเป็นงวดล่าสุดจริงหรือยัง' ใช้คู่กับปุ่ม 'ดึงเฉพาะที่ขาด/เก่า'
    (skip_up_to_date ใน sync_all ใช้ logic เดียวกันเป๊ะ — ดูคอมเมนต์ _target_period/
    _payload_latest_period ด้านบน)

    คืน {source: {"target": "YYYY-MM-DD", "fresh": n, "stale": n, "total": n,
                  "missing": [{"sym":..., "have": "YYYY-MM-DD"|None}, ...]}}
    symbol ที่ finnomena_q ไม่รองรับ (ETF/ตลาดนอก TH-US-HK) ถูกตัดออกจาก total ของ
    source นั้นไปเลย ไม่นับเป็น stale (เหมือน sync_all ตัดออกจาก task ตั้งแต่ต้น)"""
    symbols = sorted({s.upper().strip().replace(".BK", "") for s in symbols})
    latest_map = get_latest_period_map(base_dir, is_dr=is_dr, sources=sources)
    out = {}
    for src in sources:
        target = _target_period(src, today)
        syms = symbols
        if src == "finnomena_q":
            syms = [s for s in symbols if finnomena_supported(s, is_dr=is_dr)]
        missing = []
        for s in syms:
            have = latest_map.get((s, src))
            if have is None or have < target:
                missing.append({"sym": s, "have": have.isoformat() if have else None})
        out[src] = {
            "target": target.isoformat(),
            "total": len(syms),
            "fresh": len(syms) - len(missing),
            "stale": len(missing),
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
    mkt = (market or "TH").upper()
    yf_ticker, _is_etf = resolve_yf_ticker(_PROJECT_ROOT, sym, market=market)
    out = []
    if not yf_ticker:
        # resolve ไม่ได้ = ไม่ใช่ TH/SET, ไม่อยู่ DR universe, และไม่ใช่ US/HK/JP — เดา '.BK'
        # ได้เฉพาะตอนตั้งใจให้เป็นหุ้นไทยเท่านั้น ไม่งั้นจะไปดึง earnings หุ้นไทยผิดตัวมาเก็บ
        # ใต้ market ต่างประเทศ (เช่น DR: ที่รหัสไม่อยู่ใน DR universe)
        if mkt not in ("TH", "SET"):
            return out
        yf_ticker = sym + ".BK"

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
    # payload เสีย (เดียวกับเหตุผลใน get() ด้านบน) ไม่ควรทำ Tearsheet/เทียบเพื่อนของหุ้น
    # mirror นอกดัชนีหลักตัวนั้น 500 — ถือเหมือนไม่เคย cache เลย (caller เห็น None แล้วดึงสดใหม่)
    try:
        return json.loads(payload), stale
    except (TypeError, ValueError):
        return None, True


_QPL_Q_END_MD = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}   # ไตรมาสปฏิทิน -> วันสิ้นงวด


def _set_qpl_latest_extra(rev, ni, set_qpl_payload):
    """เติมไตรมาสล่าสุดจาก SET official (source 'set_qpl') เข้า rev/ni series ถ้าใหม่กว่า
    งวดล่าสุดที่มีอยู่แล้ว — Yahoo/Finnomena มักอัพเดตงบไตรมาสช้ากว่า SET.or.th เอง ~1 ไตรมาส
    (เจอกับ BA: yahoo_q/finnomena_q ค้าง Q1/69 ทำให้ QoQ เทียบ Q4/68→Q1/69 (+384%) แทนที่จะเป็น
    Q1/69→Q2/69 ตัวจริง (-84%) ซึ่งมีอยู่แล้วใน set_qpl ตอนคำนวณ) ไม่แตะงวดเก่าที่ซ้อนทับกัน
    (ให้ Yahoo/Finnomena เป็นหลักอยู่เหมือนเดิม แค่เติมงวดที่ยังไม่มี)"""
    quarters = (set_qpl_payload or {}).get("quarters") or {}
    if not quarters:
        return rev, ni
    last_date = rev[-1][0] if rev else (ni[-1][0] if ni else None)
    extra_rev, extra_ni = [], []
    for key, row in quarters.items():
        try:
            y_s, q_s = key.split("-")
            y, q = int(y_s), int(q_s)
        except (ValueError, AttributeError):
            continue
        md = _QPL_Q_END_MD.get(q)
        if not md:
            continue
        d = f"{y:04d}-{md[0]:02d}-{md[1]:02d}"
        if last_date and d <= last_date:
            continue
        if row.get("revenue") is not None:
            extra_rev.append((d, row["revenue"]))
        if row.get("net_profit") is not None:
            extra_ni.append((d, row["net_profit"]))
    if extra_rev:
        rev = sorted(rev + extra_rev)
    if extra_ni:
        ni = sorted(ni + extra_ni)
    return rev, ni


def compute_quarterly_growth(payload_yahoo_q, set_qpl_payload=None):
    """การเติบโตรายไตรมาสจากงบ quarterly (source 'yahoo_q'/'finnomena_q'):
    - rev_qoq / profit_qoq  : ไตรมาสล่าสุด vs ไตรมาสก่อนหน้า (%)
    - rev_yoy_q / profit_yoy_q: ไตรมาสล่าสุด vs ไตรมาสเดียวกันของปีก่อน (%)
      (ตัวหลังกัน seasonality — ธุรกิจจำนวนมากมี high/low season ทำให้ QoQ เพี้ยนตามฤดู)
    ฐานติดลบ/ศูนย์ -> None (เปอร์เซ็นต์ไม่มีความหมาย เช่นพลิกจากขาดทุน) — เคสนี้เช็คได้จาก
    profit_turnaround_qoq/profit_turnaround_yoy (bool) แทน ว่าเป็น "พลิกกำไรจริง" หรือแค่ "ไม่มีข้อมูล"

    set_qpl_payload: payload source 'set_qpl' (ถ้ามี) — เติมงวดล่าสุดที่ SET.or.th มีแล้วแต่
    Yahoo/Finnomena ยังไม่อัพเดต ดู _set_qpl_latest_extra"""
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
        """คืน (pct หรือ None, turnaround bool) — turnaround = ไตรมาสก่อนขาดทุน/เท่ากับศูนย์แต่
        ไตรมาสนี้กำไรเป็นบวก (ใช้กับ ni เท่านั้นตอนเรียก ไม่มีความหมายกับ rev ที่ไม่ติดลบอยู่แล้ว)"""
        if len(vals) < 2:
            return None, False
        prev, last = vals[-2][1], vals[-1][1]
        turn = prev is not None and prev <= 0 and last is not None and last > 0
        if not prev or prev <= 0 or last is None:
            return None, turn
        return _cap(round((last - prev) / prev * 100, 2)), turn

    def yoy(vals):
        """หาไตรมาสที่ห่างจากงวดล่าสุด ~1 ปี (330-400 วัน) — ทนกรณีไตรมาสขาดหาย
        คืน (pct หรือ None, turnaround bool) เหมือน qoq ด้านบน"""
        if len(vals) < 2:
            return None, False
        from datetime import date
        try:
            ld = date.fromisoformat(vals[-1][0][:10])
        except Exception:
            return None, False
        base = None
        for d, v in vals[:-1]:
            try:
                gap = (ld - date.fromisoformat(d[:10])).days
            except Exception:
                continue
            if 330 <= gap <= 400:
                base = v
        last = vals[-1][1]
        turn = base is not None and base <= 0 and last is not None and last > 0
        if not base or base <= 0 or last is None:
            return None, turn
        return _cap(round((last - base) / base * 100, 2)), turn

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
    rev, ni = _set_qpl_latest_extra(rev, ni, set_qpl_payload)

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

    rev_qoq_v, _ = qoq(rev)
    profit_qoq_v, profit_qoq_turn = qoq(ni)
    rev_yoy_v, _ = yoy(rev)
    profit_yoy_v, profit_yoy_turn = yoy(ni)

    return {
        "rev_qoq": rev_qoq_v, "profit_qoq": profit_qoq_v,
        "rev_yoy_q": rev_yoy_v, "profit_yoy_q": profit_yoy_v,
        # หุ้นพลิกกำไรจริง (งวดเทียบขาดทุน/เท่ากับศูนย์ แต่งวดนี้กำไรบวก) — profit_yoy_q/profit_qoq
        # เป็น None ในเคสนี้เสมอ (% ไม่มีความหมายเมื่อฐานติดลบ) ต้องใช้ flag นี้แทนถ้าอยากหาหุ้น
        # พลิกกำไรจริงๆ ไม่ใช่แค่ "โตจากฐานบวกเล็กๆ" — ดู Screener+ preset "🔄 Turnaround"
        "profit_turnaround_qoq": profit_qoq_turn,
        "profit_turnaround_yoy": profit_yoy_turn,
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


_QTR_MONTH = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}
# เดือนสิ้นงวด -> ไตรมาสปฏิทิน (bucket ทุกเดือนเข้าไตรมาสปฏิทินที่ใกล้ที่สุด) — เดิมมีแค่ {3,6,9,12}
# ตามสมมติฐานบริษัทไทยส่วนใหญ่ FYE ธ.ค. (ไตรมาสปิดตรงเดือนสุดท้ายของไตรมาสปฏิทินพอดี) แต่หุ้น DR
# ต่างประเทศจำนวนมากปีบัญชีไม่ตรงปฏิทิน (NVDA/WMT FYE ม.ค. ไตรมาสปิด เม.ย./ก.ค./ต.ค./ม.ค. — ไม่มีเดือน
# ไหนอยู่ใน {3,6,9,12} เลย) ทำให้ _year_quarter_from_date คืน q=None ทุกแถว แล้วถูก skip ทิ้งทั้งชั้น
# Yahoo (เจอจริง 2026-08-19: NVDA/WMT ทุกไตรมาส financial_cost/pretax_profit/tax_expense เป็น None
# หมด ทั้งที่ยิง yfinance ตรงๆ มีข้อมูลจริงครบ — เพราะ Yahoo เป็นแหล่งเดียวที่มี 3 field นี้ Finnomena
# ไม่มี พอชั้น Yahoo โดน skip เลยไม่มีที่มาให้ field พวกนี้เลย)
#
# ⚠ การขยายเป็นครบ 12 เดือนเพียงอย่างเดียว "ไม่ปลอดภัย" กับหุ้นไทยเหมือนที่คอมเมนต์เดิมอ้าง — เดือน
# 3/6/9/12 แม็ปตรงเป๊ะก็จริง แต่ถ้าแหล่งข้อมูลหนึ่งรายงานวันสิ้นงวดคลาดเคลื่อนไปแค่ 1-2 วันข้ามเข้าเดือน
# ถัดไป (เช่น '2026-04-01' แทนที่จะเป็น '2026-03-31' — เจอได้จริงจาก timezone/rounding ของแต่ละแหล่ง)
# เดิม (ก่อนขยาย) เดือน 4 จะ map เป็น q=None แล้วถูก skip ทิ้งอย่างปลอดภัย แต่หลังขยายจะถูก bucket เป็น
# Q2 ทันที ปนกับข้อมูล Q2 จริงที่มาจากอีกแหล่งในคีย์ (year,q) เดียวกัน — กลับไปเป็นบั๊กชนกันข้ามไตรมาส
# แบบ KTIS เดิม แต่คราวนี้เกิดกับหุ้น FYE ธ.ค. ปกติทั่วไปได้ด้วย ไม่ใช่แค่เคส DR ปีบัญชีเพี้ยนที่ตั้งใจแก้
#
# ทางแก้: เดือนที่ "เพิ่งข้ามไตรมาสปฏิทินหมาดๆ" (1 เดือนถัดจาก 3/6/9/12) ถ้าวันที่ตกอยู่ในช่วงต้นเดือน
# (≤ _QTR_SNAP_BACK_DAYS วัน) ให้ถอยกลับไปนับเป็นไตรมาสก่อนหน้าแทน — เพราะปีบัญชีที่เพี้ยนจริงอย่าง
# NVDA/WMT วันสิ้นงวดมักอยู่ปลายเดือน (~วันที่ 25-28) ไม่เคยตกช่วงต้นเดือน จึงไม่ชนกับเคสนี้เลย ในขณะที่
# "รายงานไตรมาสปฏิทินปกติที่วันที่คลาดเคลื่อนนิดเดียว" มักตกต้นเดือนเสมอ — แยกสองเคสออกจากกันได้ด้วย
# วันที่ โดยไม่เสีย coverage ของ NVDA/WMT ที่เพิ่งแก้ไป
_QTR_SNAP_BACK = {4: 3, 7: 6, 10: 9, 1: 12}   # เดือน "เพิ่งข้าม" -> เดือนปิดไตรมาสปฏิทินก่อนหน้า (1->12 ข้ามปี)
_QTR_SNAP_BACK_DAYS = 10


def _year_quarter_from_date(date_str):
    """(year, quarter 1-4) จากวันที่สิ้นงวด 'YYYY-MM-DD' หรือ (None, None) ถ้า parse ไม่ได้/ไม่ใช่
    เดือนปิดไตรมาสปฏิทิน (ผ่าน _QTR_MONTH ด้านบน) — helper กลางที่ใช้ร่วมกันทุกจุดที่ map วันที่
    สิ้นงวดจริงเป็นไตรมาสปฏิทิน กันแก้ตรรกะ parse/mapping ที่จุดหนึ่งแล้วลืมอีกจุด (ต้นเหตุบั๊ก
    quarter-key ชนกันของ KTIS ที่เคยเจอมาก่อน — ดูคอมเมนต์ใน fetch_set_qpl_chart_series) —
    ถอยเดือนกลับ (_QTR_SNAP_BACK) ถ้าวันที่ตกต้นเดือนหมาดๆ ดูคอมเมนต์ตรง _QTR_MONTH ด้านบน"""
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
    except (TypeError, ValueError, IndexError):
        return None, None
    snap_m = _QTR_SNAP_BACK.get(m)
    if snap_m is not None and d <= _QTR_SNAP_BACK_DAYS:
        if snap_m == 12:
            y -= 1
        m = snap_m
    return y, _QTR_MONTH.get(m)


_QPL_FIELDS = ("revenue", "cogs", "gross_profit", "selling_exp", "admin_exp", "sga_total",
               "total_expenses", "operating_profit", "financial_cost", "pretax_profit",
               "tax_expense", "net_profit")


def _qpl_merge_layer(combined, layer_rows, source_name):
    """เขียน layer_rows ({(year_ad,q): row}) ทับ combined ทีละ field เฉพาะ field ที่ layer นี้
    มีค่า (ไม่ใช่ None) — ทำให้ layer ที่มาทีหลัง (แม่น/ละเอียดกว่า) อัพเกรดทีละช่องได้โดยไม่ล้าง
    ของเก่าที่ layer ก่อนหน้าเคยมีแต่ layer นี้ไม่มี (เช่น SET-chart หยาบมีแค่ revenue/cogs/
    net_profit — ไม่ควรไปล้าง selling_exp/financial_cost ที่ Yahoo เคยเติมไว้ก่อนหน้า)

    dest['source'] อัพเดตเฉพาะตอนที่ layer นี้เขียน net_profit จริง (ไม่ใช่แค่ field อื่นใน
    _QPL_FIELDS ตัวใดตัวหนึ่ง และไม่ใช่ทุกครั้งที่มี key ตรงกันใน layer_rows) — กันเคส SET-chart
    มี revenue/cogs ของไตรมาสนั้นแต่ netProfit เป็น None (company-highlight ขาดช่วง) แล้ว
    'source' ถูกเปลี่ยนเป็น 'set' ทั้งที่ net_profit ยังเป็นค่าเดิมจาก Finnomena/Yahoo อยู่ — ผูกกับ
    net_profit ตรงๆ เพราะเป็น field เดียวที่ 'source' มีไว้สื่อที่มา (2026-09-05: เดิมผูกกับ
    'ตัวใดก็ได้ใน 12 field' ซึ่งยัง set source='set' อยู่ดีในเคสนี้เพราะ revenue/cogs ทำให้
    touched=True ไปก่อนถึงคิว net_profit — ไม่ได้แก้ตามที่ docstring อ้างจริง)"""
    for key, row in layer_rows.items():
        dest = combined.setdefault(key, {"year_ad": key[0], "q": key[1], "detail": False})
        for f in _QPL_FIELDS:
            v = row.get(f)
            if v is not None:
                dest[f] = v
        if row.get("selling_exp") is not None or row.get("admin_exp") is not None:
            dest["detail"] = True
        if row.get("net_profit") is not None:
            dest["source"] = source_name


def compute_qpl_report(payload_finn_q, payload_yahoo_q, set_series=None):
    """ตาราง 'งบกำไรขาดทุนรายไตรมาส' สไตล์ broker research (รายได้/ต้นทุนขาย/กำไรขั้นต้น/
    SG&A แยกขาย-บริหาร/กำไรดำเนินงาน/ต้นทุนการเงิน/กำไรก่อนภาษี/ภาษี/กำไรสุทธิ + %YoY) —
    ผสาน 3 แหล่งแบบ field-level (เขียนทับทีละช่อง ไม่ใช่ทั้งแถว) เรียงจากหยาบ->ละเอียด/แม่นสุด
    เพราะไม่มีแหล่งเดียวให้ทั้งประวัติยาว+รายละเอียดครบ+ความแม่นระดับทางการพร้อมกัน:

    1. Finnomena (finnomena_q): ยาว ~16 ปีแต่มีแค่ รายได้/กำไรขั้นต้น/SG&A รวม/กำไรสุทธิ ->
       คำนวณต้นทุนขาย/กำไรดำเนินงานเองจากอัตลักษณ์ทางบัญชี (Revenue-GrossProfit, GrossProfit-SGA)
       ไม่มีต้นทุนทางการเงิน/กำไรก่อนภาษี/ภาษี (Finnomena ไม่ได้ให้ตัวเลขนี้มาเลย)
    2. Yahoo (yahoo_q): ครบทุกบรรทัดตรงตัว แต่สะสมได้แค่ไม่กี่ไตรมาสล่าสุดต่อรอบ sync (yfinance
       ไม่มีทาง backfill ไตรมาสเก่าที่ไม่เคยดึงไว้)
    3. SET official (set_series, ดู fetch_set_qpl_series): ผสาน 2 ชั้นย่อยมาแล้ว — chart 5 ปี
       หยาบ (revenue/cogs/gross_profit/net_profit จาก grossProfitMargin — ไม่ derive sga เพราะ
       totalExpense ของ SET ปนค่าใช้จ่ายอื่นมาด้วย) + detail งวดล่าสุด/อนุพันธ์จากงวดสะสม (COGS/
       SG&A แยกขาย-บริหาร/ต้นทุนการเงิน/ภาษี ตรงจากบัญชีที่บริษัทยื่นจริง) — เป็นชั้นที่แม่นสุด
       (ทางการ, เช็คแล้วตรงกับ Yahoo เป๊ะในช่อง COGS/selling/admin กับ 5 หุ้นสุ่ม 2026-08-12)
       เขียนทับ Yahoo/Finnomena เฉพาะ field ที่มีจริง ไม่ล้าง field อื่นที่ชั้นก่อนหน้ามี

    คืน {"quarters": [...]} เรียงเก่า->ใหม่ ทุก entry มี "source" (ชื่อ layer ล่าสุดที่แตะแถวนี้)
    + "detail": bool (True = มีแยกค่าใช้จ่ายขาย/บริหาร จาก Yahoo หรือ SET detail)"""

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _pct_change(cur, prev):
        if cur is None or prev is None or prev == 0:
            return None
        return round((cur - prev) / abs(prev) * 100, 2)

    def _safe_pct(num, den):
        if num is None or den is None or den <= 0:
            return None
        return round(num / den * 100, 2)

    combined = {}   # (year_ad, q) -> row dict

    # ── ชั้น 1: Finnomena — คลุมทุกไตรมาสที่มีประวัติ (คำนวณต้นทุน/กำไรดำเนินงานเองจาก
    # อัตลักษณ์ทางบัญชี: COGS = Revenue-GrossProfit, Operating = GrossProfit-SG&A) ──
    finn_layer = {}
    if payload_finn_q:
        inc = payload_finn_q.get("income", {})
        rev_m = inc.get("Total Revenue", {})
        gp_m  = inc.get("Gross Profit", {})
        ni_m  = inc.get("Net Income", {})
        sga_m = inc.get("Selling General And Administration", {})
        for d in set(rev_m) | set(gp_m) | set(ni_m) | set(sga_m):
            y, q = _year_quarter_from_date(d)
            if not q:
                continue
            r, g, n, s = _f(rev_m.get(d)), _f(gp_m.get(d)), _f(ni_m.get(d)), _f(sga_m.get(d))
            cogs = (r - g) if (r is not None and g is not None) else None
            op   = (g - s) if (g is not None and s is not None) else None
            total_exp = (cogs + s) if (cogs is not None and s is not None) else None
            finn_layer[(y, q)] = {
                "revenue": r, "cogs": cogs, "gross_profit": g,
                "sga_total": s, "total_expenses": total_exp, "operating_profit": op,
                "net_profit": n,
            }
    _qpl_merge_layer(combined, finn_layer, "finnomena")

    # ── ชั้น 2: Yahoo — ครบทุกบรรทัดตรงตัว ใช้ field จริงตรงๆ ไม่ derive (เผื่อบริษัทมีรายการ
    # นอกเหนือ COGS+SG&A เช่น other income ซึ่งทำให้ผลรวมบรรทัดอาจไม่ reconcile เป๊ะ 100%) ──
    yahoo_layer = {}
    if payload_yahoo_q:
        inc = payload_yahoo_q.get("income", {})
        def g(field, d):
            return _f(inc.get(field, {}).get(d))
        rev_m = inc.get("Total Revenue", {})
        for d in rev_m:
            y, q = _year_quarter_from_date(d)
            if not q:
                continue
            r     = g("Total Revenue", d)
            cogs  = g("Cost Of Revenue", d)
            gp_   = g("Gross Profit", d)
            if gp_ is None and r is not None and cogs is not None:
                gp_ = r - cogs
            sell  = g("Selling And Marketing Expense", d)
            admin = g("General And Administrative Expense", d)
            sga_combo = g("Selling General And Administration", d)
            if sga_combo is not None:
                sga_total = sga_combo
            elif sell is not None or admin is not None:
                sga_total = (sell or 0) + (admin or 0)
            else:
                sga_total = None
            op = g("Operating Income", d)
            if op is None and gp_ is not None and sga_total is not None:
                op = gp_ - sga_total
            total_exp = (cogs + sga_total) if (cogs is not None and sga_total is not None) else None
            yahoo_layer[(y, q)] = {
                "revenue": r, "cogs": cogs, "gross_profit": gp_,
                "selling_exp": sell, "admin_exp": admin, "sga_total": sga_total,
                "total_expenses": total_exp, "operating_profit": op,
                "financial_cost": g("Interest Expense", d), "pretax_profit": g("Pretax Income", d),
                "tax_expense": g("Tax Provision", d), "net_profit": g("Net Income", d),
            }
    _qpl_merge_layer(combined, yahoo_layer, "yahoo")

    # ── ชั้น 3: SET official — แม่นสุด (ทางการ) เขียนทับเฉพาะ field ที่มีจริง (ดู docstring) ──
    if set_series:
        _qpl_merge_layer(combined, set_series, "set")

    out = []
    for key in sorted(combined.keys()):
        row = combined[key]
        prior = combined.get((key[0] - 1, key[1]))   # ไตรมาสเดียวกันปีก่อน (YoY)
        rev, pretax = row.get("revenue"), row.get("pretax_profit")
        row["year_be"] = row["year_ad"] + 543
        row["revenue_yoy"] = _pct_change(rev, prior.get("revenue") if prior else None)
        row["gpm"] = _safe_pct(row.get("gross_profit"), rev)
        row["selling_pct"] = _safe_pct(row.get("selling_exp"), rev)
        row["admin_pct"] = _safe_pct(row.get("admin_exp"), rev)
        row["sga_pct"] = _safe_pct(row.get("sga_total"), rev)
        row["pretax_yoy"] = _pct_change(pretax, prior.get("pretax_profit") if prior else None) if pretax is not None else None
        row["tax_pct"] = _safe_pct(row.get("tax_expense"), pretax)
        row["npm"] = _safe_pct(row.get("net_profit"), rev)
        out.append(row)
    return {"quarters": out}


# field งบดุล/กระแสเงินสดที่ผสานได้ — เฉพาะ Finnomena+Yahoo เพราะสอง payload นี้ใช้ชื่อ raw
# field เดียวกันโดยตั้งใจ (ดู _finn_map_rows field_map ด้านล่าง) SET.or.th ไม่มีรายไตรมาสจริง
# (company-highlight เป็นรายปี/ยอดสะสมบางส่วนของปีปัจจุบันเท่านั้น) จึงไม่เอามาผสานที่นี่
_BSCF_RAW_MAP = {
    "balance": (("Total Assets", "total_assets"), ("Stockholders Equity", "total_equity"),
                ("Total Debt", "total_debt"), ("Cash And Cash Equivalents", "cash"),
                ("Current Assets", "current_assets"), ("Current Liabilities", "current_liabilities"),
                ("Inventory", "inventory"),
                ("Accounts Receivable", "accounts_receivable"),
                ("Receivables", "accounts_receivable"),
                ("Gross Accounts Receivable", "accounts_receivable")),
    "cashflow": (("Operating Cash Flow", "cfo"), ("Investing Cash Flow", "cfi"),
                 ("Financing Cash Flow", "cff"), ("Depreciation And Amortization", "da"),
                 ("Capital Expenditure", "capex")),
}
_BSCF_FIELDS = ("total_assets", "total_equity", "total_debt", "cash",
                "current_assets", "current_liabilities", "inventory", "accounts_receivable",
                "cfo", "cfi", "cff", "da", "capex")


def _bscf_layer_from_payload(payload):
    """{(year_ad,q): {field: value}} จาก payload ทรง finnomena_q/yahoo_q (โครงเดียวกัน
    section->field->{วันที่สิ้นงวด: ค่า}) ใช้ได้กับทั้งสองแหล่งเพราะชื่อ raw field ตรงกัน
    (ดู _finn_map_rows field_map บรรทัด ~1616)"""
    out = {}
    if not payload:
        return out
    for section, pairs in _BSCF_RAW_MAP.items():
        sec = payload.get(section, {}) or {}
        for raw, ours in pairs:
            for d, v in sec.get(raw, {}).items():
                y, q = _year_quarter_from_date(d)
                if not q:
                    continue
                fv = None
                try:
                    fv = float(v) if v is not None else None
                except (TypeError, ValueError):
                    fv = None
                if fv is not None:
                    out.setdefault((y, q), {})[ours] = fv
    return out


def _merge_bscf_layer(combined, layer_rows, source_name):
    """เหมือน _qpl_merge_layer แต่สำหรับ _BSCF_FIELDS — เก็บชื่อแหล่งไว้ที่ 'bscf_source' แยก
    จาก 'source' ของ P&L (ไม่ reuse _qpl_merge_layer ตรงๆ เพราะมันจะไปทับ 'source' ของ P&L
    layer ที่ผสานไว้ก่อนหน้า เช่นแถวที่ P&L มาจาก SET แต่งบดุลมาจาก Yahoo จะโดนเขียนทับ
    label 'source' เป็น 'yahoo' ทั้งที่ P&L ยังเป็นของ SET อยู่)

    bscf_source อัพเดตเฉพาะตอนที่ layer นี้เขียน field จริงอย่างน้อย 1 ช่องใน _BSCF_FIELDS
    (2026-09-05: เดิมเขียนทุกครั้งที่ key ตรงกันใน layer_rows แม้ทุก field จะเป็น None หมด —
    บั๊กเดียวกับที่เพิ่งแก้ใน _qpl_merge_layer แต่ยังไม่เคยแก้คู่กัน)"""
    for key, row in layer_rows.items():
        dest = combined.setdefault(key, {"year_ad": key[0], "q": key[1], "detail": False})
        touched = False
        for f in _BSCF_FIELDS:
            v = row.get(f)
            if v is not None:
                dest[f] = v
                touched = True
        if touched:
            dest["bscf_source"] = source_name


def compute_full_report(payload_finn_q, payload_yahoo_q, set_series=None):
    """ตารางงบผสานทุกแหล่งสำหรับแท็บ 'งบรวมทุกแหล่ง' — P&L ผสานครบ 3 แหล่งเหมือน
    compute_qpl_report ทุกประการ (เรียกใช้ตรงๆ) ส่วนงบดุล/กระแสเงินสดผสานได้แค่ Finnomena+Yahoo
    (ดูเหตุผลที่ _BSCF_RAW_MAP) เรียง Finnomena ก่อน (ประวัติยาวกว่า) แล้ว Yahoo ทับ (สดกว่า)
    เหมือนลำดับชั้นของ P&L คืน {"quarters": [...]} เรียงเก่า->ใหม่ แถวเดียวกันมีทั้ง field P&L
    และงบดุล/กระแสเงินสด"""
    pl = compute_qpl_report(payload_finn_q, payload_yahoo_q, set_series=set_series)
    combined = {}
    for row in pl["quarters"]:
        combined[(row["year_ad"], row["q"])] = dict(row)

    _merge_bscf_layer(combined, _bscf_layer_from_payload(payload_finn_q), "finnomena")
    _merge_bscf_layer(combined, _bscf_layer_from_payload(payload_yahoo_q), "yahoo")

    # _merge_bscf_layer อาจสร้างแถวใหม่สำหรับไตรมาสที่มีแต่งบดุล/กระแสเงินสดแต่ไม่มี P&L จากแหล่ง
    # ไหนเลย (เจอกับ PTTGC 2011Q3/Q4 — Finnomena มีงบดุลแต่ pl ไม่มีแถวนั้น) แถวพวกนี้จะไม่มี
    # "year_be" (ใส่เฉพาะใน compute_qpl_report) ทำให้ frontend group by year_be พังตอน grouping
    # (Number(undefined)=NaN -> byYear[NaN] อ่าน [0] จาก undefined) เติมให้ครบตรงนี้
    out = [combined[k] for k in sorted(combined.keys())]
    for row in out:
        row.setdefault("year_be", row["year_ad"] + 543)
    return {"quarters": out}


_BSCF_FLOW_FIELDS = ("cfo", "cfi", "cff", "da")
_BSCF_STOCK_FIELDS = ("total_assets", "total_equity", "total_debt", "cash")
_QPL_FLOW_FIELDS = _QPL_FIELDS   # ทุก field ของ P&L เป็น flow (สะสมได้) ทั้งหมด
# field ที่ SET company-highlight มีความหมายตรงกับของเราเป๊ะ ใช้ override รายปีได้ — ไม่รวม
# total_debt (SET มีแค่ totalLiability = หนี้สินรวมทั้งหมด ต่างความหมายจาก total_debt ของ
# Finnomena/Yahoo ที่หมายถึงเฉพาะหนี้มีภาระดอกเบี้ย) และ cash/da (SET ไม่มี field ตรงๆ)
_SET_ANNUAL_OVERRIDE_FIELDS = ("total_assets", "total_equity", "cfo", "cfi", "cff")


def set_bscf_annual_layer(set_payload):
    """{year_ad: {total_assets, total_equity, cfo, cfi, cff}} จาก SET.or.th company-highlight
    (payload ทรง fetch_set_full: {"sym","entries":[...]}) เฉพาะปีที่ปิดงบเต็มปีแล้ว
    (quarter=='Q9' — ปีปัจจุบันที่ยังไม่ปิดงบเป็นยอดสะสมบางส่วน ไม่ใช่ตัวเลขเต็มปี ข้ามไปกัน
    rollup_full_report_annual เอาไปแทนที่ผิด) ใช้เป็น override ชั้นสุดท้าย (ทางการ/แม่นสุด) ทับ
    ผลรวมจาก Finnomena+Yahoo ใน rollup_full_report_annual — เฉพาะหุ้นไทย (DR ไม่มีข้อมูลนี้)
    คูณ 1000 ทุกค่าเงิน (หน่วย 'พันบาท' เหมือน company-highlight ทุก field อื่น ดู _set_qpl_amt_map)"""
    out = {}
    for e in (set_payload or {}).get("entries", []):
        if e.get("quarter") != "Q9":
            continue
        y = e.get("year")
        if y is None:
            continue
        def g(k):
            v = e.get(k)
            return v * 1000 if v is not None else None
        out[int(y)] = {
            "total_assets": g("totalAsset"), "total_equity": g("equity"),
            "cfo": g("netOperating"), "cfi": g("netInvesting"), "cff": g("netFinancing"),
        }
    return out


def rollup_full_report_annual(quarters, set_annual=None):
    """รวมผลลัพธ์รายไตรมาสของ compute_full_report เป็นรายปี (ไม่ไปดึงงบรายปีแยกจากแหล่งอื่น
    ป้องกันปัญหา field ไม่ตรงกันข้ามแหล่ง + ปีปัจจุบันของ SET annual เป็นยอดสะสมบางส่วน) —
    field 'flow' (P&L + cfo/cfi/cff/da) รวมทุกไตรมาสที่มีของปีนั้น, field 'stock' (งบดุล) เอา
    ค่า ณ ไตรมาสล่าสุดที่มีของปีนั้น (แนวทางเดียวกับ build_annual_from_quarterly) 'complete'=True
    เฉพาะปีที่มีครบ 4 ไตรมาส (เช็คจาก revenue) — ปีไม่ครบยังคืนผลรวมเท่าที่มีแต่ flag ไว้ให้
    frontend เตือนผู้ใช้ (เหมือนเครื่องหมาย * ของคอลัมน์ SET ปีที่ยังไม่ปิดงบ)

    set_annual: ผลลัพธ์จาก set_bscf_annual_layer() (เฉพาะหุ้นไทย) — ถ้ามีของปีไหน override
    total_assets/total_equity/cfo/cfi/cff ของปีนั้นทับผลรวมจาก Finnomena+Yahoo ทันที (SET
    เป็นแหล่งทางการ แม่นกว่า)"""
    def _pct_change(cur, prev):
        if cur is None or prev is None or prev == 0:
            return None
        return round((cur - prev) / abs(prev) * 100, 2)

    def _safe_pct(num, den):
        if num is None or den is None or den <= 0:
            return None
        return round(num / den * 100, 2)

    by_year = {}
    for row in quarters:
        by_year.setdefault(row["year_ad"], []).append(row)

    years = {}
    for y, rows in by_year.items():
        rows = sorted(rows, key=lambda r: r["q"])
        rev_quarters = {r["q"] for r in rows if r.get("revenue") is not None}
        yr = {"year_ad": y, "year_be": y + 543,
              "complete": bool(rev_quarters) and len(rev_quarters) == 4}
        for f in _QPL_FLOW_FIELDS + _BSCF_FLOW_FIELDS:
            f_quarters = {r["q"] for r in rows if r.get(f) is not None}
            # รวมเป็นยอดรายปีเฉพาะเมื่อ field นี้ครอบไตรมาสเดียวกับ revenue เป๊ะ — บรรทัดละเอียด
            # (cogs/selling_exp/admin_exp/financial_cost/pretax_profit/tax_expense) มีเฉพาะไตรมาส
            # Yahoo/SET ล่าสุด ~6-8 งวด ถ้ารวมมั่วจะได้ผลรวม 2 ไตรมาสไปโชว์ในแถวที่ revenue เป็น
            # ยอด 4 ไตรมาสเต็ม แล้ว pretax_yoy/tax_pct ที่ derive ต่อก็ผิดตาม
            if f_quarters and f_quarters == rev_quarters:
                yr[f] = round(sum(r[f] for r in rows if r.get(f) is not None), 4)
            else:
                yr[f] = None
        for f in _BSCF_STOCK_FIELDS:
            latest = next((r[f] for r in reversed(rows) if r.get(f) is not None), None)
            yr[f] = latest
        sa = (set_annual or {}).get(y)
        if sa:
            for f in _SET_ANNUAL_OVERRIDE_FIELDS:
                if sa.get(f) is not None:
                    yr[f] = sa[f]
        years[y] = yr

    out = []
    for y in sorted(years.keys()):
        yr = years[y]
        prior = years.get(y - 1)
        rev, pretax = yr.get("revenue"), yr.get("pretax_profit")
        yr["revenue_yoy"] = _pct_change(rev, prior.get("revenue") if prior else None)
        yr["gpm"] = _safe_pct(yr.get("gross_profit"), rev)
        yr["selling_pct"] = _safe_pct(yr.get("selling_exp"), rev)
        yr["admin_pct"] = _safe_pct(yr.get("admin_exp"), rev)
        yr["sga_pct"] = _safe_pct(yr.get("sga_total"), rev)
        yr["pretax_yoy"] = _pct_change(pretax, prior.get("pretax_profit") if prior else None) if pretax is not None else None
        yr["tax_pct"] = _safe_pct(yr.get("tax_expense"), pretax)
        yr["npm"] = _safe_pct(yr.get("net_profit"), rev)
        out.append(yr)
    return {"years": out}


# ชื่อบัญชีเดียวกันที่ SET สะกดไม่คงที่ข้ามงวด/บริษัท (ยืนยันแล้วว่าความหมายเดียวกันเป๊ะ ต่างจาก
# คู่ fallback อื่นใน _set_qpl_row_from_amt ที่เป็นบัญชีคนละตัวกันจริง) — normalize ให้เป็นชื่อ
# เดียวกันตั้งแต่ตอนสร้าง amt map เพื่อไม่ให้ sub() (ลบงวดสะสม) เห็นเป็นคนละ key แล้วได้ None
# ทั้งที่มีข้อมูลจริงทั้งสองงวด (เช่น 9M สะกดแบบหนึ่ง 6M สะกดอีกแบบในปีเดียวกัน)
_SET_ACCOUNT_ALIASES = {
    "กำไร (ขาดทุน) ก่อนต้นทุนทางการเงิน และ/หรือ ภาษีเงินได้":
        "กำไร (ขาดทุน) ก่อนต้นทุนทางการเงิน และภาษีเงินได้",
}


def _set_qpl_amt_map(payload):
    """{accountName: amount} — คูณ 1000 ทุกค่า เพราะ financialstatement ของ SET รายงานเป็น
    'พันบาท' (เช็คแล้ว: รวมรายได้ PTT ตรงกับ company-highlight เป๊ะโดยไม่ต้องหารเพิ่ม) ต่างจาก
    Yahoo/Finnomena ที่เก็บเป็นหน่วยบาทดิบ — ถ้าไม่ปรับหน่วยตรงนี้ field-level merge ใน
    compute_qpl_report จะได้แถวที่ปนหน่วยกัน (revenue จาก SET เป็นพันบาท แต่ selling_exp จาก
    Yahoo เป็นบาทดิบ ในไตรมาสเดียวกัน) พังทั้งตาราง — ชื่อบัญชี normalize ผ่าน
    _SET_ACCOUNT_ALIASES ก่อนเก็บ key เผื่องวดที่ใช้คนละคำสะกดสำหรับบัญชีเดียวกัน

    เก็บซ้ำอีกชุดคีย์ตาม accountCode (prefix "#" กันชนกับชื่อบัญชี) เพราะบัญชีบางตัว
    (เช่น 439700 "กำไร(ขาดทุน)อื่น" ที่ get_qpl_growth_screener ใช้แยกกำไรปกติ/กำไรพิเศษ)
    ไม่มี alias ให้เชื่อว่าสะกดคงที่ทุกบริษัท/ทุกงวดเหมือนที่เช็คไว้กับบัญชีหลักในฟังก์ชันด้านบน
    — accountCode เป็น taxonomy มาตรฐานของ SET เอง เชื่อถือได้กว่า (ยืนยันแล้ว 2026-09-04:
    439700 = ผลรวม 439710 FX + 439720 อนุพันธ์ FVTPL + 439770 hedge ตรงกันทุกบริษัทที่สุ่มเช็ค)
    ไม่ผ่าน sub() (ลบงวดสะสม) ต่างจากชื่อบัญชีตรงไหน — เป็น key เดียวกันในดิกต์เดียวกัน
    ลบงวดสะสมได้เหมือนกันเพราะ sub() วนทุก key แบบ set(a)|set(b) อยู่แล้ว"""
    out = {}
    for a in (payload or {}).get("accounts", []):
        name = _SET_ACCOUNT_ALIASES.get(a.get("accountName"), a.get("accountName"))
        amt = a["amount"] * 1000 if a.get("amount") is not None else None
        out[name] = amt
        code = a.get("accountCode")
        if code:
            out["#" + code] = amt
    return out


def _set_qpl_row_from_amt(amt):
    """แปลง {accountName: amount} (จาก fetch_financial_statement หรือผลลบงวดสะสม) เป็น
    raw-row เดียวกับ layer อื่นใน compute_qpl_report — จับคู่ด้วยชื่อบัญชีเพราะเช็คแล้วชื่อ
    เหมือนกันข้ามบริษัทที่สุ่มทดสอบ (5 ตัว 2026-08-12) ทนกว่า accountCode เผื่อบางบริษัทจัด
    schema เลขบัญชีต่าง — คืน None ถ้าไม่มีรายได้เลย (บัญชีธนาคาร/ประกันใช้ผังบัญชีคนละแบบ
    ไม่มีรายการ 'รายได้จากการขายและให้บริการ')

    กำไรสุทธิ (net_profit) ใช้ 'กำไร (ขาดทุน) สุทธิ สำหรับงวด' รวมส่วนได้เสียที่ไม่มีอำนาจควบคุม
    (NCI) — เดิมเคยใช้ 'ส่วนผู้ถือหุ้นบริษัทใหญ่' (ไม่รวม NCI) แต่พบว่าทำให้ time series ที่ผสาน
    งวดนี้ (~3-4 ไตรมาสล่าสุดที่ financial_statement ดึงถึง) เข้ากับ Finnomena/Yahoo/SET
    chart-layer (fetch_set_qpl_chart_series — netProfit ก็รวม NCI เหมือนกัน) สลับนิยามกลางทาง
    ตรงรอยต่อพอดี ทำให้ YoY/QoQ/NPM กระโดดหลอกในหุ้นที่มี NCI เยอะ (SIAM ต่างกันถึง +74%, เช็ค
    2026-08-12) — แก้โดยให้ทุก layer ใช้นิยามรวม NCI ตรงกันหมด (2026-09-05) ตัวเลขส่วนผู้ถือหุ้น
    บริษัทใหญ่ยังเก็บแยกไว้ใน 'net_profit_parent' เผื่อมีผู้ใช้ต้องการภายหลัง (ยังไม่มี caller
    ใช้ ณ ตอนนี้) ส่วน 'กำไรก่อนต้นทุนทางการเงินและภาษี' เป็นบรรทัดที่ SET ให้แทน 'กำไรจากการ
    ดำเนินงาน' ของเรา (ไม่มี EBT แยกต่างหากในผังบัญชีไทย — กำไรก่อนภาษีต้อง derive เอง = บรรทัดนี้
    ลบต้นทุนทางการเงิน)"""
    def g(*names):
        for n in names:
            v = amt.get(n)
            if v is not None:
                return v
        return None
    # 'รายได้จากการดำเนินธุรกิจ' มาก่อน — เช็ค 5 หุ้นสุ่มแล้วพบว่านี่คือตัวที่ตรงกับ Yahoo
    # Total Revenue เป๊ะเสมอ (PTT/IRPC/CH: เท่ากับ 'รายได้จากการขายและให้บริการ' พอดีเพราะไม่มี
    # รายได้ธุรกิจย่อยอื่น แต่ LPH ต่างกันจริง 619,024 vs 512,530 — 'รายได้จากการขายและให้บริการ'
    # แคบกว่า ไม่รวมรายได้ธุรกิจย่อยอื่น เช่น รายได้ค่าเช่า ทำให้ต่ำกว่า Yahoo ~20% ถ้าใช้ผิดตัว)
    revenue = g("รายได้จากการดำเนินธุรกิจ", "รายได้จากการขายและให้บริการ")
    if revenue is None:
        return None
    cogs = g("ต้นทุน")
    selling = g("ค่าใช้จ่ายในการขาย")
    admin = g("ค่าใช้จ่ายในการบริหาร")
    sga_combo = g("ค่าใช้จ่ายในการขายและบริหาร")
    if sga_combo is not None:
        sga_total = sga_combo
    elif selling is not None or admin is not None:
        sga_total = (selling or 0) + (admin or 0)
    else:
        sga_total = None
    op = g("กำไร (ขาดทุน) ก่อนต้นทุนทางการเงิน และภาษีเงินได้",
           "กำไร (ขาดทุน) ก่อนต้นทุนทางการเงิน และ/หรือ ภาษีเงินได้")
    fin_cost = g("ต้นทุนทางการเงิน")
    # บัญชี 'ต้นทุนทางการเงิน' หายไปจากงบ มักแปลว่างวดนั้นไม่มีหนี้ที่มีดอกเบี้ย (=0) ไม่ใช่ข้อมูล
    # ขาด — ถ้าข้อมูลขาดจริง op เองก็จะเป็น None ด้วยอยู่แล้ว (ต้องมี 2 บัญชีมาลบกันถึงได้ op)
    # ไม่ปล่อย pretax เป็น None ทั้งที่มี op จริง เพราะทำให้ 'กำไรก่อนภาษี'/'% TAX' ว่างเปล่าโดยไม่จำเป็น
    pretax = (op - (fin_cost if fin_cost is not None else 0)) if op is not None else None
    # ลำดับ fallback สลับจากเดิม — 'สำหรับงวด' (รวม NCI) เป็นหลักตอนนี้ ดูเหตุผลใน docstring ด้านบน
    net_profit = g("กำไร (ขาดทุน) สุทธิ สำหรับงวด", "การแบ่งปันกำไร (ขาดทุน) สุทธิ : ผู้ถือหุ้นบริษัทใหญ่")
    net_profit_parent = g("การแบ่งปันกำไร (ขาดทุน) สุทธิ : ผู้ถือหุ้นบริษัทใหญ่")
    # บัญชี 439700 "กำไร(ขาดทุน)อื่น" — ผลรวมของ 439710 อัตราแลกเปลี่ยน/439720 อนุพันธ์ FVTPL/
    # 439770 การบัญชีป้องกันความเสี่ยง (ยืนยันแล้ว parent=sum(children) 2026-09-04, สุ่ม 24 ตัว
    # ไม่มี mismatch) ใช้เป็นตัวตั้งต้นแยก "กำไรพิเศษ" ออกจากกำไรสุทธิใน get_qpl_growth_screener —
    # ไม่รวม 431100 ส่วนแบ่งกำไรบริษัทร่วม (เป็นกำไรที่เกิดประจำ ไม่ใช่รายการพิเศษ) อยู่เหนือบรรทัด
    # EBIT (409992) ในงบ SET คือรวมอยู่ใน operating_profit/net_profit ข้างบนแล้ว — ไม่ได้แยกคำนวณใหม่
    # ที่นี่ ใช้ code-key (ดู _set_qpl_amt_map) เพราะชื่อบัญชีนี้ไม่ได้ยืนยันว่าสะกดคงที่ทุกบริษัท
    # เหมือนบัญชีหลักด้านบน — None ถ้างวด/บริษัทนั้นไม่มีบรรทัดนี้ (ผังบัญชีธนาคาร/ประกัน หรือ
    # SET ไม่แยกให้ในงวดนั้น — caller แยกสองเคสนี้เองจาก sector ถ้าต้องการ)
    other_gl = amt.get("#439700")
    return {
        "revenue": revenue, "cogs": cogs,
        "gross_profit": (revenue - cogs) if cogs is not None else None,
        "selling_exp": selling, "admin_exp": admin, "sga_total": sga_total,
        "total_expenses": (cogs + sga_total) if (cogs is not None and sga_total is not None) else None,
        "operating_profit": op, "financial_cost": fin_cost, "pretax_profit": pretax,
        "tax_expense": g("ภาษีเงินได้"), "net_profit": net_profit,
        "net_profit_parent": net_profit_parent, "other_gl": other_gl,
        "detail": selling is not None or admin is not None,
    }


def fetch_set_qpl_chart_series(symbol, ctx=None, hdr=None):
    """{(year_ad,q): raw-row} หยาบ 5 ปีจาก company-highlight/financial-data-chart (ดู
    sources/set_api.py::fetch_financial_data_chart) — revenue/cogs/gross_profit derive จาก
    grossProfitMargin% ไม่ derive sga_total/operating_profit เพราะ totalExpense ของ SET ปน
    'ค่าใช้จ่ายอื่น' มาด้วย (เช็คกับ PTT: totalExpense = COGS+SGA+ค่าใช้จ่ายอื่น ไม่ใช่แค่ COGS+SGA
    2026-08-12) — derive ตรงๆ จะได้ sga เพี้ยน ปล่อยให้ Yahoo/Finnomena เติมช่องนั้นแทน

    คูณ 1000 ทุกค่าเงิน — endpoint นี้รายงานเป็น 'พันบาท' เหมือน company-highlight ต่างจาก
    Yahoo/Finnomena ที่เป็นบาทดิบ (ดูคอมเมนต์เดียวกันใน _set_qpl_amt_map)

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)"""
    from sources.set_api import fetch_financial_data_chart
    rows = fetch_financial_data_chart(symbol, ctx=ctx, hdr=hdr)
    out = {}
    for r in rows:
        # คีย์จาก 'เดือนที่สิ้นงวดจริง' (endDate) ไม่ใช่ label 'quarter'/'year' ที่ SET ให้มา —
        # label เป็นเลขไตรมาสบัญชีของบริษัทเอง สำหรับหุ้นปีบัญชีไม่ตรงปฏิทิน (เช่น KTIS ต.ค.-ก.ย.,
        # KSL พ.ย.-ต.ค.) "Q1" ของบริษัทอาจจบเดือน ธ.ค./ม.ค. ไม่ใช่ มี.ค. — ถ้าใช้ label ตรงๆ จะได้
        # key ชนกับไตรมาสปฏิทินจริงของ Finnomena/Yahoo (ซึ่งคีย์จากเดือนสิ้นงวดจริงอยู่แล้วผ่าน
        # _QTR_MONTH) ทำให้ field-level merge ปนข้อมูลคนละช่วงเวลาเข้าด้วยกัน (เจอกับ KTIS จริง
        # 2026-08-18: SET label 'Q1_2022' จบ 31 ธ.ค. 2021 แต่จะถูกคีย์เป็น (2022,1) ชนกับไตรมาส
        # ปฏิทิน ม.ค.-มี.ค. 2022 จริงของ Finnomena/Yahoo)
        end_date = r.get("endDate") or r.get("asOfDate")
        year, q = _year_quarter_from_date(end_date)
        if not q:
            continue
        # ธนาคาร/ประกัน/เงินทุนฯ ไม่มีแนวคิด 'ยอดขาย' แบบธุรกิจทั่วไป — SET เลยเว้น 'sales' เป็น None
        # เสมอ (เช็คสด KBANK/BLA/TISCO 2026-08-18) แต่ 'totalRevenue' (รายได้รวม) ยังมีให้ใช้แทนได้
        # ผิดจากธุรกิจปกติที่ทั้งสอง field ต่างกันจริง (เช่น PTT sales 830B vs totalRevenue 837B
        # — totalRevenue ปนรายได้อื่นที่ไม่ใช่ธุรกิจหลัก) จึงจำกัด fallback ไว้เฉพาะตอน sales ขาดจริง
        sales = r.get("sales")
        if sales is None:
            sales = r.get("totalRevenue")
        gpm = r.get("grossProfitMargin")
        gross_profit = (sales * gpm / 100) if (sales is not None and gpm is not None) else None
        cogs = (sales - gross_profit) if (sales is not None and gross_profit is not None) else None
        net_profit = r.get("netProfit")
        out[(year, q)] = {
            "revenue": sales * 1000 if sales is not None else None,
            "cogs": cogs * 1000 if cogs is not None else None,
            "gross_profit": gross_profit * 1000 if gross_profit is not None else None,
            "net_profit": net_profit * 1000 if net_profit is not None else None,
            "detail": False,
        }
    return out


def _fetch_set_cumulative_series(symbol, account_type, row_builder, ctx=None, hdr=None):
    """โครงกลาง (periods loop + ลบงวดสะสม Q3=9M-6M/Q4=YE-9M/Q2=6M-Q1 + คีย์จาก endDate จริงกัน
    mislabel หุ้นปีบัญชีไม่ตรงปฏิทิน) ที่ fetch_set_qpl_detail_series/fetch_set_cashflow_series
    ใช้ร่วมกันเป๊ะ (เคยเป็นโค้ด copy-paste แยก 2 ชุด รวมเป็นที่เดียวหลัง code review 2026-08-26
    กันแก้บั๊กที่เดียวแล้วลืมอีกชุด — เจอบั๊กจริงจากโครงนี้มาแล้ว 1 ครั้ง 2026-08-18: หุ้นปีบัญชี
    ไม่ตรงปฏิทินถูก mislabel เพราะใช้ period 'year'/'type' แทน endDate จริง) ใช้กับ account_type
    ที่รายงานเป็นยอดสะสม (income_statement/cash_flow) เท่านั้น — balance_sheet เป็นยอดคงเหลือ
    ไม่ต้องลบงวดสะสม ใช้ fetch_set_balance_series แยกต่างหาก

    row_builder(amt_dict) -> raw-row หรือ None — แปลง {accountName: amount} เป็นรูปที่ layer
    เหนือขึ้นไปต้องการ (ต่างกันตาม account_type)

    ได้ประมาณ 3-4 ไตรมาสล่าสุดเท่านั้น — periods endpoint มีแค่ปีปัจจุบัน+ปีก่อนหน้า (เช็คแล้ว
    2026-08-12) ไม่ย้อนลึกหลายปี ไตรมาสเก่ากว่านั้นต้องพึ่ง Yahoo/Finnomena/chart-series แทน

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)"""
    from sources.set_api import (fetch_financial_statement_periods, fetch_financial_statement,
                                  _bootstrap_headers)
    if ctx is None or hdr is None:
        ctx, hdr = _bootstrap_headers()
    periods = fetch_financial_statement_periods(symbol, ctx=ctx, hdr=hdr)
    if not periods:
        return {}

    parsed = {}   # period_code -> {"type","year","amt","end_year","end_q"}
    for p in periods:
        try:
            ptype, year_s = p.rsplit("_", 1)
            year = int(year_s)
        except ValueError:
            continue
        try:
            d = fetch_financial_statement(symbol, account_type, period=p, ctx=ctx, hdr=hdr)
        except Exception:
            continue
        end_year, end_q = _year_quarter_from_date((d or {}).get("endDate"))
        parsed[p] = {"type": ptype, "year": year, "amt": _set_qpl_amt_map(d),
                     "end_year": end_year, "end_q": end_q}

    def sub(a, b):
        keys = set(a) | set(b)
        return {k: (a.get(k) - b.get(k)) if (a.get(k) is not None and b.get(k) is not None) else None
                for k in keys}

    def real_key(info):
        # คีย์จาก endDate จริงของงวด ไม่ใช่ 'year' (label ปีบัญชีบริษัท) + ตำแหน่ง Q1-Q4 ที่
        # derive จาก type — เหตุผลเดียวกับ fetch_set_qpl_chart_series ด้านบน (ดูคอมเมนต์ที่นั่น)
        # หุ้นปีบัญชีไม่ตรงปฏิทินที่ไตรมาสจบเดือนนอก มี.ค./มิ.ย./ก.ย./ธ.ค. จะไม่มี key ให้เลย —
        # เจตนา ปล่อยให้ Yahoo/Finnomena ไม่มีเหมือนกัน (ไม่ใช่ mislabel เข้าไปช่องผิด)
        return (info["end_year"], info["end_q"]) if (info["end_year"] and info["end_q"]) else None

    amt_by_qtr = {}   # (year, q) -> amt map เดี่ยว
    by_year = {}
    for info in parsed.values():
        if info["type"] == "Q1":
            key = real_key(info)
            if key:
                amt_by_qtr[key] = info["amt"]
        by_year.setdefault(info["year"], {})[info["type"]] = info
    for by_t in by_year.values():
        if "9M" in by_t and "6M" in by_t:
            key = real_key(by_t["9M"])
            if key:
                amt_by_qtr[key] = sub(by_t["9M"]["amt"], by_t["6M"]["amt"])
        if "YE" in by_t and "9M" in by_t:
            key = real_key(by_t["YE"])
            if key:
                amt_by_qtr[key] = sub(by_t["YE"]["amt"], by_t["9M"]["amt"])
        if "6M" in by_t and "Q1" in by_t:
            key = real_key(by_t["6M"])
            if key:
                amt_by_qtr[key] = sub(by_t["6M"]["amt"], by_t["Q1"]["amt"])

    out = {}
    for key, amt in amt_by_qtr.items():
        row = row_builder(amt)
        if row:
            out[key] = row
    return out


def fetch_set_qpl_detail_series(symbol, ctx=None, hdr=None):
    """{(year_ad,q): raw-row} ละเอียด (COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี) จากงบที่บริษัทยื่นจริง
    (ดู sources/set_api.py::fetch_financial_statement) — งวดล่าสุดใช้ตรงๆ ส่วนงวดสะสม (6M/9M/YE)
    ลบกันเองเป็นไตรมาสเดี่ยว (ดู _fetch_set_cumulative_series สำหรับโครงเต็ม)

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)"""
    return _fetch_set_cumulative_series(symbol, "income_statement", _set_qpl_row_from_amt,
                                         ctx=ctx, hdr=hdr)


def _set_cashflow_row_from_amt(amt):
    """แปลง {accountName: amount} (จาก fetch_financial_statement account_type='cash_flow' หรือ
    ผลลบงวดสะสม) เป็น raw-row — field หลักคือ OCF (เงินสดจากการดำเนินงาน) ใช้เทียบกำไรสุทธิดู
    คุณภาพกำไรว่ามีเงินสดหนุนจริงไหม (กำไรบัญชีสูงแต่ OCF ต่ำ/ติดลบ = สัญญาณเตือน เช่น ลูกหนี้
    ค้างเยอะ/บันทึกรายได้เร็วเกินจริง) คืน None ถ้าไม่มี OCF เลย (กันแถวว่างเปล่าเข้า DB)"""
    def g(*names):
        for n in names:
            v = amt.get(n)
            if v is not None:
                return v
        return None
    ocf = g("เงินสดสุทธิได้มาจาก (ใช้ไปใน) กิจกรรมดำเนินงาน")
    if ocf is None:
        return None
    return {
        "ocf": ocf,
        "cfi": g("เงินสดสุทธิได้มาจาก (ใช้ไปใน) กิจกรรมลงทุน"),
        "cff": g("เงินสดสุทธิได้มาจาก (ใช้ไปใน) กิจกรรมจัดหาเงิน"),
        "capex": g("เงินสดจ่ายจากการซื้อสินทรัพย์ถาวร"),
    }


def fetch_set_cashflow_series(symbol, ctx=None, hdr=None):
    """{(year_ad,q): raw-row} กระแสเงินสดรายไตรมาสเดี่ยว จากงบที่บริษัทยื่นจริง — ใช้โครงกลาง
    เดียวกับ fetch_set_qpl_detail_series (ดู _fetch_set_cumulative_series) เปลี่ยนแค่
    account_type='cash_flow' และ row-builder — **ไม่มี chart-layer เสริม** (company-highlight
    ไม่มีข้อมูลกระแสเงินสด) จึงได้แค่ ~3-4 ไตรมาสล่าสุดเท่านั้น

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)"""
    return _fetch_set_cumulative_series(symbol, "cash_flow", _set_cashflow_row_from_amt,
                                         ctx=ctx, hdr=hdr)


def _set_balance_row_from_amt(amt):
    """แปลง {accountName: amount} (จาก fetch_financial_statement account_type='balance_sheet')
    เป็น raw-row — งบดุลเป็นยอดคงเหลือ ณ วันสิ้นงวด (ไม่ใช่ยอดสะสมแบบงบกำไรขาดทุน) ไม่ต้องลบ
    งวดสะสม ใช้ตรงๆ ได้ทุก period ในลิสต์ คืน None ถ้าไม่มีสินทรัพย์รวมเลย"""
    def g(*names):
        for n in names:
            v = amt.get(n)
            if v is not None:
                return v
        return None
    total_assets = g("รวมสินทรัพย์")
    if total_assets is None:
        return None
    return {
        "cash": g("เงินสดและรายการเทียบเท่าเงินสด"),
        "receivables": g("ลูกหนี้การค้าและลูกหนี้หมุนเวียนอื่น - สุทธิ"),
        "inventory": g("สินค้าคงเหลือ - สุทธิ"),
        "current_assets": g("รวมสินทรัพย์หมุนเวียน"),
        "total_assets": total_assets,
        "payables": g("เจ้าหนี้การค้าและเจ้าหนี้หมุนเวียนอื่น"),
        "current_liabilities": g("รวมหนี้สินหมุนเวียน"),
        "total_liabilities": g("รวมหนี้สิน"),
        "total_equity_parent": g("รวมส่วนของผู้ถือหุ้นของบริษัทใหญ่"),
        "total_equity": g("รวมส่วนของผู้ถือหุ้น"),
    }


def fetch_set_balance_series(symbol, ctx=None, hdr=None):
    """{(year_ad,q): raw-row} งบดุลรายไตรมาส จากงบที่บริษัทยื่นจริง — ต่างจาก
    fetch_set_cashflow_series/fetch_set_qpl_detail_series ตรงที่**ไม่ต้องลบงวดสะสม** (งบดุลเป็น
    ยอดคงเหลือ ณ วันสิ้นงวด ไม่ใช่ยอดสะสมแบบ P&L) ทุก period ในลิสต์ (Q1/6M/9M/YE) จึงเป็นจุด
    ข้อมูลจริงใช้ได้ทันที ไม่ต้องรอคู่ subtract — ได้จุดข้อมูลมากกว่า cash_flow ต่อรอบ sync
    คีย์จาก endDate จริงเหมือนกัน (กัน mislabel หุ้นปีบัญชีไม่ตรงปฏิทิน) — โครงต่างจากอีก 2 ตัว
    พอที่จะไม่คุ้มรวมเป็น helper เดียวกัน (ไม่มีขั้นตอนลบงวดสะสม) แต่ใช้ _set_qpl_amt_map ร่วม
    (normalize ชื่อบัญชีผ่าน _SET_ACCOUNT_ALIASES) เหมือนกันแทนการ build amt dict เองแบบเดิม

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)"""
    from sources.set_api import (fetch_financial_statement_periods, fetch_financial_statement,
                                  _bootstrap_headers)
    if ctx is None or hdr is None:
        ctx, hdr = _bootstrap_headers()
    periods = fetch_financial_statement_periods(symbol, ctx=ctx, hdr=hdr)
    if not periods:
        return {}

    out = {}
    for p in periods:
        try:
            d = fetch_financial_statement(symbol, "balance_sheet", period=p, ctx=ctx, hdr=hdr)
        except Exception:
            continue
        end_year, end_q = _year_quarter_from_date((d or {}).get("endDate"))
        if not end_q:
            continue
        row = _set_balance_row_from_amt(_set_qpl_amt_map(d))
        if row:
            out[(end_year, end_q)] = row
    return out


def fetch_set_qpl_series(symbol, ctx=None, hdr=None):
    """รวม 2 ชั้นจาก SET official สำหรับ compute_qpl_report: chart (5 ปี หยาบ) + detail
    (ล่าสุด+อนุพันธ์ ละเอียด ทับ chart ทั้งแถวสำหรับงวดที่มีของละเอียดกว่า) เงียบถ้าพังทั้งคู่
    (caller ยังมี Yahoo/Finnomena เป็นฐานอยู่แล้ว ไม่ใช่จุดเดียวที่พังแล้วทั้งตารางหาย)

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลาย symbol ตอน bulk sync (ดู sync_all)

    ถ้าทั้ง chart+detail raise (SET.or.th ล่ม/บล็อก IP/cookie หมดอายุ) — re-raise ให้ caller
    รู้ว่า fetch พังจริง ไม่ใช่ 'หุ้นนี้ไม่มีงบ QPL' (ก่อนหน้านี้คืน {} เงียบ ทำให้ sync_all นับ
    เป็น ok ทั้งที่ยิงไม่ผ่านสักครั้ง — สรุปผล sync เขียวหลอกตอน SET.or.th ล่มทั้งรอบ)
    live path (_sync_set_series) ห่อ try/except คืนของสะสมเดิมจาก DB อยู่แล้ว ไม่กระทบ"""
    out = {}
    errs = 0
    for _fn in (fetch_set_qpl_chart_series, fetch_set_qpl_detail_series):
        try:
            out.update(_fn(symbol, ctx=ctx, hdr=hdr))
        except Exception:
            errs += 1
    if errs == 2 and not out:
        raise RuntimeError(f"SET QPL fetch ล้มเหลวทั้ง chart+detail สำหรับ {symbol}")
    return out


def _set_qpl_payload_to_dict(payload):
    """{"quarters": {"YYYY-Q": row}} (รูปแบบเก็บใน DB, key เป็น string เพราะ JSON ไม่รองรับ
    tuple key) -> {(year_ad,q): row} (รูปแบบที่ compute_qpl_report ใช้)"""
    out = {}
    for key_str, row in (payload or {}).get("quarters", {}).items():
        y_s, q_s = key_str.split("-")
        out[(int(y_s), int(q_s))] = row
    return out


def _get_set_series(base_dir, symbol, source):
    """อ่านข้อมูลรายไตรมาสรูปแบบ {"quarters": {...}} (source='set_qpl'/'set_cashflow'/
    'set_balance' — โครง payload เหมือนกันเป๊ะ) ที่สะสมไว้ใน DB ตรงๆ ไม่ยิง SET.or.th สดเลย —
    คืน None ถ้ายังไม่เคย sync หุ้นตัวนี้เลย (caller fallback ไป _sync_set_series ต่อ) — รวมจาก
    get_set_qpl_series/get_set_cashflow_series/get_set_balance_series เดิมที่เป็น
    byte-identical template ต่างกันแค่ชื่อ source (code review 2026-08-26)"""
    payload = get(base_dir, symbol, source)
    return _set_qpl_payload_to_dict(payload) if payload else None


def _sync_set_series(base_dir, symbol, source, fetch_fn):
    """ดึง SET official สดรอบใหม่ผ่าน fetch_fn(symbol) + ผสานกับที่เก็บสะสมไว้ใน DB (source,
    ดู _merge_set_qpl_payload) แล้วเขียนกลับ — คืน {(year_ad,q): row} — รวมจาก
    sync_set_qpl_series/sync_set_cashflow_series/sync_set_balance_series เดิม (code review
    2026-08-26) 'ยิงสดเสมอ' ต่างจาก _get_set_series — ใช้เป็น fallback ตอนยังไม่เคย sync หุ้น
    ตัวนี้เลย (DB ว่าง) หรือตอนผู้ใช้ขอ refresh เอง (?refresh=1) ไม่ใช่ path ปกติที่ทุกการเปิด
    หน้าจะยิง เพราะ SET periods endpoint มีแค่ปีปัจจุบัน+ปีก่อนหน้า พองวดเลื่อนหลุดจาก periods
    list แล้วจะดึงซ้ำไม่ได้อีกเลย — sync ครั้งไหนที่ทำได้ก็เก็บสะสมไว้ถาวร ถ้า fetch สดพังหมด
    (เน็ตสะดุด/SET.or.th ล่ม) ยังคืนของสะสมเดิมจาก DB แทนที่จะให้ตารางว่างเปล่า"""
    try:
        fresh = fetch_fn(symbol)
    except Exception:
        fresh = {}
    if fresh:
        fresh_payload = {"quarters": {f"{y}-{q}": row for (y, q), row in fresh.items()}}
        upsert(base_dir, symbol, source, fresh_payload, is_dr=False)
    payload = get(base_dir, symbol, source)
    return _set_qpl_payload_to_dict(payload) if payload else {}


def get_set_qpl_series(base_dir, symbol):
    """อ่าน SET official (source='set_qpl') ที่สะสมไว้ใน DB ตรงๆ — ใช้เป็นค่าเริ่มต้นของ
    endpoint (เหมือน pattern เดียวกับ Yahoo/Finnomena ที่อ่าน DB ก่อนเสมอ ไม่ sync สดทุกครั้ง
    ที่มีคนเปิดหน้า) — ดู _get_set_series สำหรับ pattern เต็ม"""
    return _get_set_series(base_dir, symbol, "set_qpl")


def sync_set_qpl_series(base_dir, symbol):
    """ดึง SET official สดรอบใหม่ + ผสาน + เขียนกลับ พร้อมใช้กับ compute_qpl_report — ดู
    _sync_set_series สำหรับ pattern เต็ม"""
    return _sync_set_series(base_dir, symbol, "set_qpl", fetch_set_qpl_series)


def get_set_cashflow_series(base_dir, symbol):
    """อ่านกระแสเงินสดรายไตรมาส (source='set_cashflow') ที่สะสมไว้ใน DB ตรงๆ — ดู
    get_set_qpl_series/_get_set_series"""
    return _get_set_series(base_dir, symbol, "set_cashflow")


def sync_set_cashflow_series(base_dir, symbol):
    """ยิงสด + ผสาน + เขียนกลับ — ดู sync_set_qpl_series/_sync_set_series (fetch_set_cashflow_series
    ไม่มี chart-layer สำรอง ถ้าพังจะคืนของสะสมเดิมจาก DB แทน)"""
    return _sync_set_series(base_dir, symbol, "set_cashflow", fetch_set_cashflow_series)


def get_set_balance_series(base_dir, symbol):
    """อ่านงบดุลรายไตรมาส (source='set_balance') ที่สะสมไว้ใน DB ตรงๆ — ดู
    get_set_qpl_series/_get_set_series"""
    return _get_set_series(base_dir, symbol, "set_balance")


def sync_set_balance_series(base_dir, symbol):
    """ยิงสด + ผสาน + เขียนกลับ — ดู sync_set_qpl_series/_sync_set_series"""
    return _sync_set_series(base_dir, symbol, "set_balance", fetch_set_balance_series)


def get_set_health(base_dir, symbol):
    """อ่าน SET Financial Health Check (source='set_health') ที่สะสมไว้ใน DB ตรงๆ ไม่ยิงสด —
    คืน None ถ้ายังไม่เคย sync หุ้นตัวนี้เลย คืนโครงเดียวกับ fetch_financial_health เป๊ะ (นำไป
    ให้ frontend ใช้ต่อได้ทันทีไม่ต้องแปลง)"""
    return get(base_dir, symbol, "set_health")


def sync_set_health(base_dir, symbol):
    """ยิงสด SET Financial Health Check รอบใหม่ + ผสานกับที่เก็บสะสมไว้ (ดู
    _merge_set_health_payload) แล้วเขียนกลับ — **ปล่อย exception ของการ fetch ให้ caller
    (route) จัดการเอง** ต่างจาก sync_set_qpl_series/sync_set_cashflow_series ที่ silent fallback
    เพราะ 404 ของ endpoint นี้มีความหมายจริง (กลุ่มการเงิน/REIT ไม่มีข้อมูลแน่นอน ไม่ใช่แค่ fetch
    พลาดชั่วคราว) route ต้องแยกข้อความ 2 แบบนี้ให้ผู้ใช้เห็น"""
    from sources.set_api import fetch_financial_health
    fresh = fetch_financial_health(symbol)
    if fresh and fresh.get("themes"):
        upsert(base_dir, symbol, "set_health", fresh, is_dr=False)
    return get(base_dir, symbol, "set_health")


_FACTSHEET_KEYS = ("cash_cycle", "financial_ratio", "financial_growth",
                   "trading_stat", "price_performance")


def get_set_factsheet(base_dir, symbol):
    """อ่าน SET factsheet (source='set_factsheet' — วงจรเงินสด/อัตราส่วนการเงิน/อัตราการเติบโต/
    สถิติซื้อขาย/ผลตอบแทนเทียบ sector-market) ที่สะสมไว้ใน DB ตรงๆ ไม่ยิงสด — คืน None ถ้ายังไม่
    เคย sync หุ้นตัวนี้เลย คืนโครงเดียวกับ fetch_financial_factsheet เป๊ะ"""
    return get(base_dir, symbol, "set_factsheet")


def sync_set_factsheet(base_dir, symbol):
    """ยิงสด SET factsheet รอบใหม่ (fetch_financial_factsheet — ห่อ try/except แยกรายตัวต่อ
    sub-endpoint อยู่แล้วในชั้น fetch) + ผสานกับที่เก็บสะสมไว้ (ดู _merge_set_factsheet_payload)
    แล้วเขียนกลับ — guard 'ว่าง' ตรวจทุก key พร้อมกัน (any) ไม่ใช่ key เดียว เพราะ cash_cycle
    เป็น None ได้ปกติสำหรับหุ้นกลุ่มการเงิน/REIT โดยอีก 4 key ยังมีค่าอยู่"""
    from sources.set_api import fetch_financial_factsheet
    fresh = fetch_financial_factsheet(symbol)
    if fresh and any(fresh.get(k) for k in _FACTSHEET_KEYS):
        upsert(base_dir, symbol, "set_factsheet", fresh, is_dr=False)
    return get(base_dir, symbol, "set_factsheet")


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
        loaded = {}
        for ex in ("TH", "US", "HK"):
            d = _finn_get(f"/stock/list?exchange={ex}&limit=100000", timeout=90)
            for row in d.get("data") or []:
                nm = (row.get("name") or "").upper()
                if nm and row.get("security_id"):
                    loaded[(ex, nm)] = row["security_id"]
        # เขียนลง _finn_ids ครั้งเดียวหลังโหลดครบทั้ง 3 ตลาด — ถ้าตลาดใดตลาดหนึ่ง raise
        # กลางทาง (เช่น timeout ลิสต์ US ที่ใหญ่สุด) _finn_ids ยังว่าง รอบหน้า retry ใหม่ทั้งชุด
        # (เดิม assign ทีละตลาด → พังหลังโหลด TH เสร็จ = ทั้งโปรเซสได้ map เฉพาะ TH ค้างถาวร
        # US/HK ทุกตัวเลยตก fallback ยิง quote รายตัวช้าๆ)
        _finn_ids.update(loaded)
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
        fiscal = r.get("fiscal")
        if not fiscal:
            # แถวมี quarter ถูกต้องแต่ fiscal หาย — ถ้าไม่ข้าม จะได้คีย์วันที่ปลอม "None-03-31"
            # ที่ merge สะสมเข้า payload ถาวร (upsert ไม่เคยลบคีย์เก่า) แล้วโผล่เป็นปีผีใน
            # ตาราง/กราฟ/CSV export ภายหลัง
            continue
        dt = f"{fiscal}{q_end[qtr]}"
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
        # ชื่อบริษัท: /stock/summary ไม่ให้ชื่อเต็ม (name = ticker ล้วน) — เก็บชื่อเดิมที่ mirror
        # bulk sync เคยลงไว้ ไม่งั้น merge (ใช้ metadata ล่าสุดเสมอ) จะทับด้วย ticker เปล่า แล้ว
        # _load_mirror_qpl_all ทิ้ง (เงื่อนไข nm != bare) → growth screener โชว์ ticker แทนชื่อ
        old = _get_raw_payload(base_dir, mkey, "finnomena_q") or {}
        disp_name = old.get("name") or name
        payload = _finn_map_rows(rows, name, disp_name, "mirror",
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
    # รหัสจีน A-share (เซินเจิ้น/เซี่ยงไฮ้) ปนมาในลิสต์ Finnomena ฝั่ง HK — HKEX ใช้รหัสจริงแค่
    # 4-5 หลัก ต่อ .HK ให้รหัส 6 หลักไม่มีทางเจอข้อมูลบน Yahoo แน่นอน (ดู fetch_yahoo_full)
    if ex == "HK" and len(name) == 6 and name.isdigit():
        return False
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
                if not rows:
                    # response ว่าง (HTTP 200 แต่ data:[]) เกิดได้ทั้งจาก "หุ้นนี้ไม่มีงบจริง"
                    # และ "API ตอบว่างชั่วคราว" (rate-limit/edge-case) แยกไม่ออกจากตัว response
                    # เดียว — ลอง retry สั้นๆ ก่อนสรุปว่า "ไม่มีงบ" เพราะ marker empty:true
                    # จะทำให้ประวัติ Finnomena สะสมจริงที่มีอยู่แล้ว (ถ้ามี) ถูกซ่อนถาวรจาก
                    # ทุกจุดที่ใช้ (get() คืน None เมื่อ empty:true) และ non-force run รอบหน้า
                    # จะข้ามตัวนี้ไปเลย ไม่มีวัน retry เอง (Finnomena ไม่มี API ให้ backfill
                    # ย้อนหลังได้ถ้าข้อมูลจริงหายไปจาก DB)
                    time.sleep(2.0)
                    rows = (_finn_get(f"/stock/summary/{sid}") or {}).get("data") or []
                payload = _finn_map_rows(rows, name, name, "mirror", cur.get(ex, "—"))
                if payload is None:
                    # ไม่มีงบจริง (ยืนยันแล้ว 2 ครั้ง) — เก็บ marker กันยิงซ้ำ (upsert ตรง ไม่ผ่าน merge)
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
    if ok > 0:
        _set_meta(base_dir, "mirror_source_synced_at",
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
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
    return _TimeoutSession()


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


def sync_mirror_yahoo_index(base_dir, tickers_by_ex, workers=4, limit=None, callback=None,
                            recheck_empty_days=90):
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

    ticker ที่ Yahoo ไม่มีข้อมูลงบการเงินจริง (ไม่ใช่โดน throttle — ดู _one) จะถูกจำเป็น marker
    'empty' กัน sync ซ้ำทุก Full Refresh (ก่อนหน้านี้ไม่มี marker เลยยิงซ้ำ ticker กลุ่มนี้ทุกรอบ
    ไม่มีวันหาย เจอ mirror universe ที่มี preferred/ticker เก่าปนอยู่เยอะจนพาลไป trip circuit
    breaker รัว) — recheck_empty_days: ตัว marker หมดอายุแล้วจะกลับมาลองใหม่ (เผื่อ Yahoo เพิ่ม
    ข้อมูลย้อนหลัง/ปรับ mapping ทีหลัง) ค่าเริ่มต้น 90 วัน ~1 ไตรมาส

    หลบ rate-limit (Yahoo "Invalid Crumb" HTTP 401): session เดียวร่วมกันทุก thread (กัน
    crumb ขอใหม่ถี่ๆ ต่อ ticker) + retry แบบ exponential backoff เฉพาะ error ที่เข้าข่ายโดน
    throttle + circuit breaker พักคิวทั้ง batch ถ้าพลาดติดกันเยอะ — ดู _new_yahoo_session/
    _YahooThrottle ด้านบน (ใช้ pattern เดียวกับ sync_all/sync_dividends_batch) — circuit breaker
    นับเฉพาะ error ที่เข้าข่าย throttle จริงเท่านั้น (_is_yahoo_throttle_err) ไม่ปนกับ 'ไม่มีข้อมูล
    จริง' ไม่งั้น mirror universe ที่ fail ธรรมชาติเยอะจะ trip breaker ทั้งที่ไม่ได้โดนบล็อกจริง
    คืน {"ok": n, "fail": n, "total": n, "skipped": n}"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        rows = con.execute(
            "SELECT symbol, payload, synced_at FROM financials WHERE source='yahoo'").fetchall()
    finally:
        con.close()
    empty_cutoff = datetime.now() - timedelta(days=recheck_empty_days)
    have = set()
    for symbol, payload, synced_at in rows:
        if '"empty": true' in payload:
            try:
                if datetime.strptime(synced_at, "%Y-%m-%d %H:%M:%S") < empty_cutoff:
                    continue   # marker หมดอายุ — ปล่อยให้กลับเข้า todo ลองใหม่
            except (TypeError, ValueError):
                continue
        have.add(symbol)

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
            except ValueError as e:
                if "ไม่พบข้อมูลงบการเงินสำหรับ" in str(e):
                    upsert(base_dir, _mirror_key(ex, name), "yahoo",
                           {"sym": name, "empty": True, "income": {}, "balance": {}, "cashflow": {}},
                           is_dr=False)
                raise
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
                if _is_yahoo_throttle_err(e):
                    throttle.note_outcome(False)
            if callback and (done % 10 == 0 or done == total):
                callback(done, total, f"Yahoo mirror index {done}/{total} | ok={ok} fail={fail}")

    print(f"[MirrorYahooIndex] จบ: ok={ok} fail={fail} total={total} skipped={skipped}")
    if ok > 0:
        # marker ให้ mirror_snapshot_meta เทียบ staleness — ข้อมูล mirror ต้นทางเพิ่งขยับ
        # ถ้าหลังจากนี้ยังไม่ได้ build_mirror_snapshot() ให้ UI ขึ้น badge เตือน
        _set_meta(base_dir, "mirror_source_synced_at",
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return {"ok": ok, "fail": fail, "total": total, "skipped": skipped}


def load_mirror_yahoo_have(base_dir):
    """คืน {symbol: payload_raw} ของทุกแถว source='yahoo' ในตาราง financials — ใช้แยกจาก
    get_mirror_index_coverage เพื่อให้ caller ที่ต้องเรียกหลายรอบ (เช่น /api/data-health ที่เช็ค
    ทั้งก้อน US/HK/JP รวม แล้วแยกย่อย SP500/Dow/Nasdaq100 อีก 3 รอบ) ยิง SELECT ทีเดียวแล้ว reuse
    ผลลัพธ์ แทนที่จะ query ทั้งตาราง financials ซ้ำทุกรอบ (วัดจริง: 4 เรียก ~0.9 วิ)"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        rows = con.execute(
            "SELECT symbol, payload FROM financials WHERE source='yahoo'").fetchall()
    finally:
        con.close()
    return {sym: payload for sym, payload in rows}


def mirror_index_coverage_from_have(have, tickers_by_ex, stale_days=365):
    """ส่วนคำนวณล้วนของ get_mirror_index_coverage — รับ `have` (ผลจาก load_mirror_yahoo_have)
    มาจาก caller แทนที่จะ query DB เอง ดู docstring get_mirror_index_coverage สำหรับ logic
    namespace FINN:/DR: และความหมายของค่าที่คืน"""
    today = date.today()
    missing, stale, fresh = [], [], 0
    for ex, names in tickers_by_ex.items():
        for name in names:
            name = name.upper().strip()
            payload_raw = have.get(_mirror_key(ex, name)) or have.get(_dr_key(name))
            if payload_raw is None:
                missing.append((ex, name))
                continue
            try:
                payload = json.loads(payload_raw)
            except Exception:
                payload = None
            latest = _payload_latest_period("yahoo", payload)
            if latest is None:
                missing.append((ex, name))
                continue
            age_days = (today - latest).days
            if age_days > stale_days:
                stale.append((ex, name, latest, age_days))
            else:
                fresh += 1
    stale.sort(key=lambda x: -x[3])
    total = sum(len(v) for v in tickers_by_ex.values())
    return {"total": total, "missing": missing, "stale": stale, "fresh": fresh}


def get_mirror_index_coverage(base_dir, tickers_by_ex, stale_days=365):
    """เช็ค coverage ของ mirror งบดัชนีหลัก US/HK/JP (source 'yahoo', namespace 'FINN:{ex}:{name}')
    ใช้ทำ item หน้า Data Health — แยก "ไม่เคย sync จริง" ออกจาก "เก่าเกิน stale_days วัน"

    สำคัญ: ต้องเช็คทั้ง namespace 'FINN:' และ 'DR:' เหมือน logic จริงใน sync_mirror_yahoo_index
    (ที่ข้าม ticker ที่มี source='yahoo' อยู่แล้วไม่ว่า namespace ไหน เพราะบางตัวซ้ำกับพอร์ต DR/NVDR
    ที่ sync ไปแล้ว) — เช็คแค่ 'FINN:' อย่างเดียวจะได้ false positive 'ไม่เคย sync' ทั้งที่จริงมี
    ข้อมูลถูกต้องอยู่แล้วใต้ 'DR:' (เจอเคสนี้จริง 104 ตัวตอนไล่เช็คมือ 2026-08-15 ก่อนจะทำฟังก์ชันนี้)

    คืน {"total": n, "missing": [(ex,name)], "stale": [(ex,name,latest_date,age_days)],
    "fresh": n} — 'missing' คือของจริงที่ไม่เคย sync เลยไม่ว่า namespace ไหน

    เรียกซ้ำหลายรอบ (เช่นแยกตามดัชนีย่อย) ให้ใช้ load_mirror_yahoo_have() +
    mirror_index_coverage_from_have() แทน — ตัวนี้ query DB ใหม่ทุกครั้งที่เรียก"""
    have = load_mirror_yahoo_have(base_dir)
    return mirror_index_coverage_from_have(have, tickers_by_ex, stale_days=stale_days)


def _load_set_qpl_all(base_dir, allowed_symbols, source="set_qpl"):
    """Bulk-load source='set_qpl' (ค่าเริ่มต้น) ทั้งตาราง (1 query เดียว — เหมือน pattern
    get_mirror_index_coverage ด้านบน) แปลงเป็น {symbol: {(year_ad,q): row}} เฉพาะ symbol ที่อยู่ใน
    allowed_symbols (caller กรอง universe เอง เช่นเฉพาะ SET main board) — ใช้ร่วมกันโดย
    get_sector_qpl_compare (snapshot ไตรมาสเดียว) และ get_market_trend (ย้อนหลังหลายไตรมาส)

    source รับ 'set_cashflow' ได้ด้วย — payload โครงเดียวกัน ({"quarters": {"YYYY-Q": row}})
    เพราะ upsert() merge ทั้งคู่ผ่าน _merge_set_qpl_payload อยู่แล้ว (ดู get_qpl_growth_screener
    ที่โหลดทั้งสอง source มา compose เป็นแถวเดียวต่อไตรมาส)"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        rows = con.execute(
            "SELECT symbol, payload FROM financials WHERE source=?", (source,)).fetchall()
    finally:
        con.close()

    parsed = {}   # symbol -> {(year_ad,q): row}
    for sym, payload_raw in rows:
        if sym not in allowed_symbols:
            continue
        try:
            payload = json.loads(payload_raw)
        except Exception:
            continue
        quarters = {}
        for key_str, row in (payload or {}).get("quarters", {}).items():
            try:
                y_s, q_s = key_str.split("-")
                quarters[(int(y_s), int(q_s))] = row
            except (ValueError, AttributeError):
                continue
        if quarters:
            parsed[sym] = quarters
    return parsed


def _mirror_income_layer(payload):
    """{(year_ad,q): {revenue, gross_profit, net_profit}} จาก payload ทรง finnomena_q/yahoo_q
    (section->field->{วันสิ้นงวด: ค่า}) — ยกตรรกะสกัด income field มาจาก compute_qpl_report
    (ชั้น Finnomena/Yahoo) แต่เอาแค่ 3 บรรทัดที่ get_qpl_growth_screener ใช้จริง (รายได้/
    กำไรขั้นต้น/กำไรสุทธิ) bucket วันสิ้นงวด -> ไตรมาสปฏิทินผ่าน _year_quarter_from_date เดิม
    (จัดการ FYE ไม่ตรงปฏิทิน NVDA/WMT ไว้แล้ว)"""
    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    out = {}
    if not payload:
        return out
    inc = payload.get("income", {}) or {}
    rev_m = inc.get("Total Revenue") or inc.get("Operating Revenue") or {}
    gp_m = inc.get("Gross Profit") or {}
    cogs_m = inc.get("Cost Of Revenue") or {}
    ni_m = inc.get("Net Income") or inc.get("Net Income Common Stockholders") or {}
    for d in set(rev_m) | set(gp_m) | set(ni_m):
        y, q = _year_quarter_from_date(d)
        if not q:
            continue
        r, g, n = _f(rev_m.get(d)), _f(gp_m.get(d)), _f(ni_m.get(d))
        if g is None:
            c = _f(cogs_m.get(d))
            if r is not None and c is not None:
                g = r - c
        row = {}
        if r is not None:
            row["revenue"] = r
        if g is not None:
            row["gross_profit"] = g
        if n is not None:
            row["net_profit"] = n
        if row:
            out[(y, q)] = row
    return out


def _load_mirror_qpl_all(base_dir, like_pattern, allowed_bare=None):
    """คู่ขนานกับ _load_set_qpl_all แต่สำหรับหุ้นต่างประเทศ (DR / mirror US-HK-JP) ที่ SET.or.th
    ไม่มีข้อมูล — อ่าน source 'finnomena_q' (ฐาน, ลึก ~16 ปี) + 'yahoo_q' (ทับ, สดกว่า แต่ตื้น)
    ทีละ query (เหมือน _load_set_qpl_all — ไม่เปิด connection ต่อ ticker) แล้วผสาน field-level
    ต่อไตรมาสเป็น shape เดียวกับ _load_set_qpl_all ({รหัสดิบ: {(year_ad,q): row}}) เพื่อป้อนเข้า
    get_qpl_growth_screener ตัวเดิมโดยไม่ต้องแก้ logic คำนวณ

    like_pattern (เช็ค namespace จริง 2026-08-30):
      'DR:%'      -> DR portfolio (finnomena_q + yahoo_q) และ Nikkei 225 (yahoo_q ล้วน —
                    _run_jp_index_sync เก็บใต้ 'DR:{numeric}' ไม่ใช่ 'FINN:JP:') caller
                    กรองด้วย allowed_bare ให้เหลือเฉพาะชุดที่ต้องการ
      'FINN:US:%' / 'FINN:HK:%' -> mirror US/HK (finnomena_q ล้วน — ไม่มี yahoo_q ในสอง namespace นี้)
    ('FINN:JP:%' มีแค่ yahoo รายปี ไม่มีรายไตรมาส — อย่าใช้)
    allowed_bare: set ของรหัสดิบที่อนุญาต (เช่น หุ้นที่ผ่าน quality filter จาก
    factor_snapshot.get_mirror_symbols) — None = เอาทุกตัวที่ query เจอ

    คืน dict {"parsed", "cashflow_parsed", "mktcap_by_symbol", "name_by_symbol"} —
    - parsed: {รหัสดิบ: {(year_ad,q): {revenue, gross_profit, net_profit}}}
    - cashflow_parsed: {รหัสดิบ: {(year_ad,q): {ocf}}} (จาก _bscf_layer_from_payload คีย์ 'cfo'
      remap เป็น 'ocf' ให้ตรงกับที่ get_qpl_growth_screener คาดหวัง)
    - mktcap_by_symbol: {รหัสดิบ: market_cap} งวดล่าสุดของ valuation 'Market Cap' (Finnomena
      เท่านั้น — แหล่งเดียวกับ build_mirror_snapshot) JP ไม่มี -> ไม่มีคีย์
    - name_by_symbol: {รหัสดิบ: ชื่อบริษัท} จาก payload 'name' (yahoo_q ก่อน finnomena_q)
    """
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        finn_rows = con.execute(
            "SELECT symbol, payload FROM financials WHERE source='finnomena_q' AND symbol LIKE ?",
            (like_pattern,)).fetchall()
        yq_rows = con.execute(
            "SELECT symbol, payload FROM financials WHERE source='yahoo_q' AND symbol LIKE ?",
            (like_pattern,)).fetchall()
    finally:
        con.close()

    def _load(rows):
        out = {}
        for sym, raw in rows:
            bare = sym.split(":")[-1]   # 'DR:NVDA' / 'FINN:US:AAPL' / 'FINN:HK:0700' -> ตัวสุดท้าย
            if allowed_bare is not None and bare not in allowed_bare:
                continue
            try:
                out[bare] = json.loads(raw)
            except (TypeError, ValueError):
                continue
        return out

    finn_by = _load(finn_rows)
    yq_by = _load(yq_rows)

    parsed, cashflow_parsed, mktcap_by_symbol, name_by_symbol = {}, {}, {}, {}
    for bare in set(finn_by) | set(yq_by):
        fp, yp = finn_by.get(bare), yq_by.get(bare)

        # P&L — Finnomena เป็นฐาน แล้ว Yahoo ทับทีละ field (สดกว่า) เหมือนลำดับชั้น compute_qpl_report
        merged = {}
        for key, row in _mirror_income_layer(fp).items():
            merged[key] = dict(row)
        for key, row in _mirror_income_layer(yp).items():
            merged.setdefault(key, {}).update(row)
        if merged:
            parsed[bare] = merged

        # OCF ต่อไตรมาส — _bscf_layer_from_payload คืนคีย์ 'cfo' (Operating Cash Flow) remap เป็น 'ocf'
        cf = {}
        for pl in (fp, yp):   # Yahoo ทับ Finnomena (คีย์ (year,q) เดียวกันเพราะ derive จาก endDate เหมือนกัน)
            for key, row in _bscf_layer_from_payload(pl).items():
                if row.get("cfo") is not None:
                    cf[key] = {"ocf": row["cfo"]}
        if cf:
            cashflow_parsed[bare] = cf

        # market cap งวดล่าสุด (Finnomena valuation) — สำหรับ P/S · เอาวันล่าสุดที่ค่าไม่ใช่ None
        # (บางตัวมีคีย์วันใหม่แต่ค่า null — ถ้าใช้ max(keys) ตรงๆ จะได้ None แล้ว float() ระเบิด)
        mc_row = (fp or {}).get("valuation", {}).get("Market Cap", {}) or {}
        mc_vals = [(d, v) for d, v in mc_row.items() if v is not None]
        if mc_vals:
            try:
                mktcap_by_symbol[bare] = float(max(mc_vals)[1])
            except (TypeError, ValueError):
                pass

        # ชื่อบริษัท — yahoo_q ก่อน (สดสุด) แล้ว finnomena_q
        for pl in (yp, fp):
            nm = (pl or {}).get("name")
            if nm and nm != bare:
                name_by_symbol[bare] = nm
                break

    return {"parsed": parsed, "cashflow_parsed": cashflow_parsed,
            "mktcap_by_symbol": mktcap_by_symbol, "name_by_symbol": name_by_symbol}


def _target_quarter(parsed):
    """quarter เป้าหมาย = ค่าที่พบบ่อยที่สุดของ "quarter ล่าสุดที่แต่ละหุ้นมีข้อมูล" (mode) — หุ้นที่ยัง
    ไม่รายงานงวดนั้นถูกข้ามในการรวมต่อ (จำนวนหุ้นต่อจุดจึงไม่เท่ากันเป็นปกติ) คืน None ถ้า parsed ว่าง"""
    from collections import Counter
    if not parsed:
        return None
    latest_per_sym = {sym: max(qs) for sym, qs in parsed.items()}
    counts = Counter(latest_per_sym.values())
    return max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _prev_quarter(y, q):
    return (y - 1, 4) if q == 1 else (y, q - 1)


_QTR_END_MD = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}   # เดือน/วันสิ้นไตรมาสปฏิทิน


def _QTR_END_DATE(y, q):
    """วันสิ้นไตรมาสปฏิทินของ (year_ad, q) — เหมือน mapping ใน _payload_latest_period ด้านบน
    ใช้กรองคีย์ไตรมาส 'อนาคต' (วันสิ้นงวดยังไม่ถึงวันนี้) ออกจาก available_quarters ใน
    get_qpl_growth_screener"""
    m, d = _QTR_END_MD[q]
    return date(y, m, d)


_GROWTH_PCT_MIN_BASE_RATIO = 0.005   # ฐานเทียบ (prior) ต้องมีขนาด >= 0.5% ของ scale_ref (รายได้
                                      # ไตรมาสปัจจุบันของหุ้นตัวเดียวกัน) ถึงจะเชื่อ % ได้ — currency-
                                      # agnostic เพราะเทียบภายในหุ้นเดียวกันเอง ไม่ต้องรู้สกุลเงิน/
                                      # แปลง FX (สำคัญมากสำหรับ universe='dr' ที่มิเรอร์ได้ทั้งหุ้น
                                      # US/HK/JP ปนกันในตารางเดียว — เกณฑ์สกุลเงินตายตัวใช้ไม่ได้)
                                      # ยืนยันด้วยข้อมูลจริง 2026-09-06: SBLK(US) profit_prior/revenue
                                      # =0.011%, 8107(HK)/JOBY(DR) revenue_prior/revenue=0.008%/0.039%
                                      # ล้วนต่ำกว่า threshold มาก ส่วน AMATA(TH, swing จริง)=3.86% สูง
                                      # กว่ามาก — แยกออกจากกันได้ชัดเจน ดู CALC_RISK_AUDIT ข้อ [9]


def _stock_pct_change(cur_val, prior_val, scale_ref=None):
    """% เปลี่ยนแปลง QoQ/YoY ต่อหุ้น — prior_val <= 0 (ไม่ใช่แค่ ==0) กันหุ้นพลิกกำไร/ขาดทุนหนักขึ้น
    หารด้วยฐานติดลบพลิกเครื่องหมายผลลัพธ์ กลายเป็น % หลอก (เช่น พลิกจากขาดทุนเป็นกำไรจะโชว์ % ติดลบ
    มหาศาล ทั้งที่จริงคือข่าวดีที่สุด) กรณีนี้ต้องดู net_profit ตรงๆ ไม่ใช่ % — caller เช็คฐานดิบ
    (revenue_prior/profit_prior ฯลฯ ที่ผลลัพธ์แนบมาด้วยเสมอ) แทนถ้าอยากหาหุ้นพลิกกำไรจริง

    scale_ref (ถ้าใส่): ค่าอ้างอิงสเกลของหุ้นตัวเดียวกัน สกุลเงินเดียวกัน (รายได้ไตรมาสปัจจุบัน) —
    prior_val ที่มีขนาด < scale_ref × _GROWTH_PCT_MIN_BASE_RATIO ถือว่าฐานจิ๋วเกินกว่า % จะมี
    ความหมาย (เช่น กำไร 39,000→144,949,000 ของบริษัทรายได้หลักร้อยล้าน ไม่ใช่ 'โต 371,564%' จริง
    แค่ noise รอบจุดศูนย์) คืน None แทนเชื่อ % นั้น — ก่อนหน้านี้มีแค่ mask ฝั่ง frontend
    (GROWTH_SCR_MIN_BASE บาทตายตัว) ซึ่งใช้ได้เฉพาะ universe='th' เท่านั้น ทำให้ DR/US/HK/JP
    ไม่มี guard นี้เลย (ดู CALC_RISK_AUDIT_2026-09-05.txt ข้อ [9])"""
    if cur_val is None or prior_val is None or prior_val <= 0:
        return None
    if scale_ref and abs(prior_val) < abs(scale_ref) * _GROWTH_PCT_MIN_BASE_RATIO:
        return None
    return round((cur_val / prior_val - 1) * 100, 1)


_QPL_STREAK_MAX = 60   # กันเดินย้อนไม่มีที่สิ้นสุดถ้าข้อมูลสะสมในอนาคตยาวขึ้นเรื่อยๆ (60 ไตรมาส = 15 ปี เกินพอ)
_GROWTH_SCR_ETR_DEFAULT = 0.20   # อัตราภาษีสมมติเมื่อคำนวณเองไม่ได้ (ไม่มี tax_expense/pretax_profit)
_GROWTH_SCR_ETR_MAX = 0.35       # กันอัตราภาษีที่คำนวณได้ (tax_expense/pretax_profit) หลุดช่วงสมเหตุสมผล
                                  # (งวดเดียวมีรายการปรับภาษีย้อนหลัง/deferred tax ทำให้ ETR ดิบเพี้ยนได้)


def _qpl_growth_streak(qs, target, field):
    """นับไตรมาสติดต่อกัน (QoQ, เดินย้อนจาก target ผ่าน _prev_quarter) ที่ field (revenue/
    net_profit) มากกว่าไตรมาสก่อนหน้าติดกันไปเรื่อยๆ — ใช้แยกหุ้น 'โตต่อเนื่องจริง' จาก 'โตแค่
    ไตรมาสเดียวแบบสุ่ม/ฤดูกาล' คืน 0 ถ้าไตรมาส target เองก็ไม่ได้โตกว่าไตรมาสก่อนแล้ว (ฐาน
    None/เทียบไม่ได้ก็นับว่าไม่โต ไม่ error)

    ต้อง cur_v > 0 ด้วย (ไม่ใช่แค่ cur_v > prev_v) — กันเคสขาดทุนหดตัวลงเรื่อยๆ (เช่น -400 -> -300
    -> -200 -> -100 ทุกไตรมาสยังขาดทุนอยู่) แต่ผ่านเงื่อนไข 'มากกว่าไตรมาสก่อน' ได้ ทั้งที่ไม่ใช่
    'โตต่อเนื่องจริง' ตามความหมายที่ badge สื่อ (เทียบแนวคิดกับ prior_val<=0 guard ใน
    _stock_pct_change ด้านบน — ที่นี่เช็คฝั่ง cur_v แทนเพราะ streak สนใจว่าตัวเลข ณ จุดนั้นเป็น
    บวกจริงไหม ไม่ใช่แค่ % เปลี่ยนเทียบได้ไหม)"""
    streak = 0
    y, q = target
    while streak < _QPL_STREAK_MAX:
        cur_row = qs.get((y, q))
        prev_y, prev_q = _prev_quarter(y, q)
        prev_row = qs.get((prev_y, prev_q))
        if not cur_row or not prev_row:
            break
        cur_v, prev_v = cur_row.get(field), prev_row.get(field)
        if cur_v is None or prev_v is None or cur_v <= 0 or not (cur_v > prev_v):
            break
        streak += 1
        y, q = prev_y, prev_q
    return streak


def _qpl_stock_growth_rows(parsed, target):
    """รายได้/กำไรสุทธิรายไตรมาส (source 'set_qpl') ต่อหุ้นแบบแบน (ไม่จัดกลุ่ม sector) เทียบไตรมาส
    ก่อนหน้า (QoQ) และไตรมาสเดียวกันปีก่อน (YoY) ของ target ที่ caller กำหนด — ใช้ร่วมกันโดย
    get_sector_qpl_compare (group ตาม sector ต่อ) และ get_qpl_growth_screener (คืนแบนตรงๆ ให้
    sort/filter ทั้งตลาดได้ทีละไตรมาสที่เลือก)

    คืน list ของ {symbol, revenue, net_profit, revenue_prior, profit_prior, revenue_prior_qoq,
    profit_prior_qoq, revenue_yoy, profit_yoy, revenue_qoq, profit_qoq, gpm, npm, npm_change_yoy,
    npm_change_qoq, revenue_streak, profit_streak, revenue_ttm, profit_ttm, other_gl, tax_expense,
    pretax_profit} เฉพาะหุ้นที่มีข้อมูล งวด target แล้ว (revenue หรือ net_profit อย่างน้อยหนึ่งตัว)
    — ไม่แนบ sector มาด้วย (caller แนบเอง เพราะบางจุดจัดกลุ่ม บางจุดไม่จัดกลุ่ม)

    other_gl/tax_expense/pretax_profit: ดิบจากงวด target เฉยๆ (ไม่ derive) ให้
    get_qpl_growth_screener คำนวณกำไรปกติ/กำไรพิเศษต่อ (ดู PLAN_core_special_profit.txt) —
    None ทั้งคู่ถ้างวด/หุ้นนั้นไม่มี (ผังบัญชีกลุ่มการเงิน หรือ SET ไม่แยกบรรทัด 439700 งวดนั้น)"""
    prior_key = (target[0] - 1, target[1])   # YoY เทียบไตรมาสเดียวกันปีก่อน ไม่ใช่ _prev_quarter (QoQ)
    prior_qoq_key = _prev_quarter(*target)   # QoQ เทียบไตรมาสก่อนหน้าติดกัน (ต่างจาก prior_key)

    def _ttm_sum(qs, field):
        total, y, q = 0, target[0], target[1]
        for _ in range(4):
            row = qs.get((y, q))
            if not row or row.get(field) is None:
                return None
            total += row[field]
            y, q = _prev_quarter(y, q)
        return total

    def _npm(row):
        rev_, net_ = (row or {}).get("revenue"), (row or {}).get("net_profit")
        return round(net_ / rev_ * 100, 1) if net_ is not None and rev_ else None

    rows = []
    for sym, qs in parsed.items():
        cur = qs.get(target)
        if not cur:
            continue
        rev, net = cur.get("revenue"), cur.get("net_profit")
        if rev is None and net is None:
            continue
        prev = qs.get(prior_key) or {}
        prev_qoq = qs.get(prior_qoq_key) or {}
        gross = cur.get("gross_profit")
        npm, npm_prior, npm_prior_qoq = _npm(cur), _npm(prev), _npm(prev_qoq)
        rows.append({
            "symbol": sym, "revenue": rev, "net_profit": net,
            "revenue_prior": prev.get("revenue"), "profit_prior": prev.get("net_profit"),
            "revenue_prior_qoq": prev_qoq.get("revenue"), "profit_prior_qoq": prev_qoq.get("net_profit"),
            "revenue_yoy": _stock_pct_change(rev, prev.get("revenue"), scale_ref=rev),
            "profit_yoy": _stock_pct_change(net, prev.get("net_profit"), scale_ref=rev),
            "revenue_qoq": _stock_pct_change(rev, prev_qoq.get("revenue"), scale_ref=rev),
            "profit_qoq": _stock_pct_change(net, prev_qoq.get("net_profit"), scale_ref=rev),
            "gpm": round(gross / rev * 100, 1) if gross is not None and rev else None,
            "npm": npm,
            # มาร์จิ้นเปลี่ยนกี่ 'จุด' (percentage point ไม่ใช่ % เปลี่ยน) เทียบ YoY/QoQ — บอกว่า
            # การเติบโตของรายได้/กำไรมาพร้อมมาร์จิ้นขยาย (คุณภาพดี) หรือมาร์จิ้นหด (อาจตัดราคาแข่ง)
            "npm_change_yoy": round(npm - npm_prior, 1) if (npm is not None and npm_prior is not None) else None,
            "npm_change_qoq": round(npm - npm_prior_qoq, 1) if (npm is not None and npm_prior_qoq is not None) else None,
            "revenue_streak": _qpl_growth_streak(qs, target, "revenue"),
            "profit_streak": _qpl_growth_streak(qs, target, "net_profit"),
            "revenue_ttm": _ttm_sum(qs, "revenue"),
            "profit_ttm": _ttm_sum(qs, "net_profit"),
            "other_gl": cur.get("other_gl"),
            "tax_expense": cur.get("tax_expense"),
            "pretax_profit": cur.get("pretax_profit"),
        })
    return rows


def _qpl_core_special(other_gl, net_profit, tax_expense, pretax_profit):
    """แยกกำไรสุทธิ (SET) เป็นกำไรปกติ/กำไรพิเศษ จากบัญชี 439700 "กำไร(ขาดทุน)อื่น" — ดู
    PLAN_core_special_profit.txt สำหรับที่มา/ข้อจำกัดเต็ม (สรุป: จับ FX+อนุพันธ์+hedge ~90%
    ของรายการไม่เกิดประจำ ไม่รวมกำไรขายสินทรัพย์/ด้อยค่าที่บางบริษัทลงบัญชีอื่น)

    other_gl = None -> คืน (None, None, None) ทั้งชุด (ผังบัญชีกลุ่มการเงินไม่มีบรรทัดนี้ หรือ
    SET ไม่แยกให้งวดนั้น — caller แยกสองเคสนี้เองจาก sector ถ้าต้องการ ดู _GROWTH_SCR_FIN_SECTORS
    ฝั่ง frontend)

    อัตราภาษี (ETR) = tax_expense/pretax_profit ของงวดเดียวกัน (ไม่ใช่ tax_rate มาตรฐาน 20% เพราะ
    BOI/สิทธิประโยชน์ภาษีทำให้ ETR จริงต่างกันมากข้ามบริษัท) clamp [0, _GROWTH_SCR_ETR_MAX] กัน
    รายการปรับภาษีย้อนหลังทำให้ ETR ดิบเพี้ยน (ติดลบ/เกิน 100%) — คำนวณไม่ได้ (ไม่มี tax_expense/
    pretax_profit หรือ pretax_profit<=0) ใช้ _GROWTH_SCR_ETR_DEFAULT แทน

    คืน (special_after_tax, core_profit, special_share_pct) — special_share_pct เป็น None
    เพิ่มเติมถ้า net_profit เป็น None/0 (หารไม่ได้) โดยไม่ต้อง mask ฐานจิ๋วที่นี่ (ฝั่ง frontend
    _growthScrMaskedRows คุมด้วย GROWTH_SCR_MIN_BASE เหมือนคอลัมน์อื่นอยู่แล้ว)"""
    if other_gl is None or net_profit is None:
        return None, None, None
    etr = _GROWTH_SCR_ETR_DEFAULT
    if tax_expense is not None and pretax_profit is not None and pretax_profit > 0:
        etr = min(_GROWTH_SCR_ETR_MAX, max(0.0, tax_expense / pretax_profit))
    special_after_tax = other_gl * (1 - etr)
    core_profit = net_profit - special_after_tax
    special_share_pct = round(abs(special_after_tax) / abs(net_profit) * 100, 1) if net_profit else None
    return round(special_after_tax), round(core_profit), special_share_pct


def get_qpl_growth_screener(parsed, sector_by_symbol, target_quarter=None, name_by_symbol=None,
                             cashflow_parsed=None, mktcap_by_symbol=None):
    """ตารางเติบโต QoQ/YoY รายหุ้นทั้งตลาดแบบแบน (ไม่จัดกลุ่ม sector ต่างจาก get_sector_qpl_compare)
    เลือกไตรมาสย้อนหลังได้ผ่าน target_quarter ("YYYY-Q" string) — ไม่ผูกกับไตรมาสล่าสุดตายตัวเหมือน
    get_sector_qpl_compare เพื่อให้กดดูไตรมาสอื่นได้ (เมนู "หุ้นโตแรงรายไตรมาส")

    caller ต้องโหลด parsed เอง (financials_store._load_set_qpl_all) แล้วส่งเข้ามา แทนที่จะให้ฟังก์ชัน
    นี้โหลดเอง — เพื่อให้ route แคชผลโหลด sqlite+JSON parse (แพงกว่า) แยกจากการเลือกไตรมาส (ถูกมาก
    แค่ loop ในหน่วยความจำ) ทำให้สลับไตรมาสไปมาในหน้าเว็บไม่ต้องอ่าน DB ใหม่ทุกครั้ง

    target_quarter ที่ parse ไม่ได้ หรือไม่มีอยู่จริงในข้อมูล -> fallback ไปไตรมาสล่าสุด (เหมือนไม่ใส่)

    cashflow_parsed: ผล _load_set_qpl_all(base_dir, allowed, source='set_cashflow') (คีย์ (year,q)
    เดียวกับ set_qpl พอดี เพราะทั้งคู่ derive จาก endDate จริงแบบเดียวกัน) ใช้แนบ ocf/ocf_ni_pct
    (กำไรมีเงินสดหนุนจริงไหม) — ไม่ใส่ = ไม่มี field นี้ในผลลัพธ์ (เผื่อ caller ที่ไม่สนใจคุณภาพกำไร
    ไม่ต้องโหลด set_cashflow แบบเปล่าประโยชน์) ตั้งชื่อ ocf_ni_pct ไม่ใช่ ocf_ni_ratio เพื่อไม่ชนกับ
    field ocf_ni_ratio ของ compute_earnings_quality (อัตราส่วนดิบจาก TTM เช่น 1.2 คนละหน่วย/
    คนละช่วงเวลากับตัวนี้ที่เป็น % จากไตรมาสเดียว เช่น 120.0)

    mktcap_by_symbol: {symbol: mkt_cap} จาก set_data.json (ราคาสด ณ วันที่ sync ล่าสุด — ไม่ใช่
    ราคา ณ วันปิดงวด target_quarter) ใช้แนบ "ps" = mkt_cap ÷ revenue_ttm (คนละที่มากับ compute_ps
    ที่ผูก market cap รายวันกับ TTM ตามช่วงเวลาจริง — ตัวนี้ง่ายกว่าเพราะไม่มี market cap ย้อนหลัง
    ให้ใช้ในหน้านี้ จึงเป็น "P/S ราคาปัจจุบัน" เทียบ TTM ของไตรมาสที่เลือกดูเสมอ แม้เลื่อนไปดูไตรมาส
    เก่า — ไม่ใส่ = ไม่มี field นี้ในผลลัพธ์

    คืน {"quarter": "YYYY-Q" หรือ None, "available_quarters": [...ทั้งหมดที่มีข้อมูลจริง เรียง
    ใหม่->เก่า...], "stocks": [...]} โครง stock เหมือน _qpl_stock_growth_rows + "sector" ต่อแถว
    (ไม่มี sector ใน sector_by_symbol -> "อื่นๆ") + "ps" ถ้าส่ง mktcap_by_symbol มา

    ทุกแถวมี special_after_tax/core_profit/special_share_pct เสมอ (None ถ้าคำนวณไม่ได้ — ดู
    _qpl_core_special) — แยกกำไรปกติ/กำไรพิเศษจากบัญชี 439700 ของ SET (เมนู "หุ้นโตแรงรายไตรมาส"
    เท่านั้น ไม่ใช่ get_sector_qpl_compare ซึ่งกรอง field ก่อน export เอง) ดู PLAN_core_special_profit.txt

    กรอง key ไตรมาสที่ "วันสิ้นงวดยังไม่ถึงวันนี้" ทิ้งจาก available_quarters เสมอ — พบจริงว่าหุ้น
    ปีบัญชีไม่ตรงปฏิทิน (BTS/VGI/STANLY/TIF1 ฯลฯ ดู qpl-quarterly-report-view memory) บางตัวยังมี
    key ค้างจากก่อนแก้บั๊ก mislabel (คีย์จาก fiscal label เก่าแทน endDate จริง) โผล่เป็นไตรมาส
    "อนาคต" เช่น 2027-1 ทั้งที่วันนี้ยังไม่ถึง — ข้อมูลเก่านี้จะถูกทับด้วย key ที่ถูกต้องเองรอบ sync
    ถัดไป (ไม่ต้องแก้ DB มือ) แต่ dropdown เลือกไตรมาสไม่ควรโชว์ตัวเลือกที่เป็นไปไม่ได้ระหว่างนั้น"""
    if not parsed:
        return {"quarter": None, "available_quarters": [], "stocks": []}

    today = date.today()
    all_keys = {q for qs in parsed.values() for q in qs}
    available = sorted((q for q in all_keys if _QTR_END_DATE(*q) <= today), reverse=True)

    target = None
    if target_quarter:
        try:
            y_s, q_s = target_quarter.split("-")
            cand = (int(y_s), int(q_s))
            if cand in available:
                target = cand
        except (ValueError, AttributeError):
            pass
    if target is None:
        target = _target_quarter(parsed)
        # _target_quarter คือ mode ของ "งวดล่าสุดต่อหุ้น" ดิบ ๆ — อาจเป็น key 'อนาคต' ที่ mislabel
        # (เช่น 2027-1 จากหุ้นปีบัญชีไม่ตรงปฏิทิน ดู docstring) ซึ่งถูกกรองออกจาก available ไปแล้ว
        # ทำให้ quarter ที่คืนไม่มีใน available_quarters -> ปุ่ม ◀▶/dropdown ฝั่ง frontend ตายทั้งแผง
        if available and target not in available:
            target = available[0]

    rows = _qpl_stock_growth_rows(parsed, target)
    has_ocf = False
    for row in rows:
        row["sector"] = sector_by_symbol.get(row["symbol"], "อื่นๆ")
        row["name"] = (name_by_symbol or {}).get(row["symbol"])
        # กำไรปกติ/กำไรพิเศษ — pop ฟิลด์ดิบที่ _qpl_stock_growth_rows แนบมาออก ไม่ export ดิบ
        # (caller/frontend ใช้ special_after_tax/core_profit/special_share_pct ที่คำนวณแล้วพอ)
        other_gl = row.pop("other_gl", None)
        tax_expense = row.pop("tax_expense", None)
        pretax_profit = row.pop("pretax_profit", None)
        row["special_after_tax"], row["core_profit"], row["special_share_pct"] = _qpl_core_special(
            other_gl, row["net_profit"], tax_expense, pretax_profit)
        if cashflow_parsed is not None:
            ocf_row = cashflow_parsed.get(row["symbol"], {}).get(target)
            ocf = ocf_row.get("ocf") if ocf_row else None
            net = row["net_profit"]
            row["ocf"] = ocf
            # % มีความหมายเฉพาะกำไรสุทธิเป็นบวกและไม่จิ๋วเกินไป (หารด้วยฐาน<=0 ได้ค่าหลอกเหมือน
            # _stock_pct_change, หารด้วยฐานจิ๋วได้ % บวมเกินจริงแม้เครื่องหมายถูก) — เทียบสัดส่วนกับ
            # รายได้ไตรมาสปัจจุบันของหุ้นตัวเอง (_GROWTH_PCT_MIN_BASE_RATIO) แทน GROWTH_SCR_MIN_BASE
            # บาทตายตัวเดิม (ใช้ได้แค่ TH) — currency-agnostic ใช้ได้ทุก market เหมือน _stock_pct_change
            # (2026-09-06: เดิมเช็ค net>=50,000,000 ตรงๆ ทุก market ทำให้หุ้น US ต้องกำไร >=$50M ถึงจะ
            # เห็นค่านี้ ทั้งที่เกณฑ์ตั้งใจจริงคือ ~$1.4M — ซ่อนข้อมูลถูกต้องของบริษัทขนาดกลาง-เล็กไปเยอะ)
            rev = row.get("revenue")
            row["ocf_ni_pct"] = round(ocf / net * 100, 1) if (
                ocf is not None and net is not None and rev
                and net >= rev * _GROWTH_PCT_MIN_BASE_RATIO) else None
            if ocf is not None:
                has_ocf = True
        if mktcap_by_symbol is not None:
            mc = mktcap_by_symbol.get(row["symbol"])
            rev_ttm = row.get("revenue_ttm")
            row["ps"] = round(mc / rev_ttm, 2) if (mc and rev_ttm and rev_ttm > 0) else None

    return {
        "quarter": f"{target[0]}-{target[1]}",
        "available_quarters": [f"{y}-{q}" for y, q in available],
        "stocks": rows,
        # ไตรมาสที่เลือกไม่มีข้อมูล set_cashflow เข้าเลย (นอกช่วงที่ SET sync ไว้) ต่างจาก "มีข้อมูล
        # แต่หุ้นนี้ไม่มี" — frontend ใช้เตือนผู้ใช้แยกจากคอลัมน์ OCF/NI ว่างเพราะไม่มีข้อมูลจริง
        "ocf_coverage": has_ocf if cashflow_parsed is not None else None,
    }


def get_sector_qpl_compare(base_dir, sector_by_symbol):
    """รวมรายได้/กำไรสุทธิรายไตรมาส (source='set_qpl', official SET เท่านั้น — sync ครบทุกหุ้น
    ไทยแล้วผ่าน update_financials.py) กลุ่มตาม Sector (SET) ที่ caller ส่งมา (data/set_data.json
    field 'sector' — ฟังก์ชันนี้ไม่รู้จัก set_data.json เอง เพื่อแยกชั้นเดียวกับ
    get_mirror_index_coverage ด้านบนที่รับ tickers_by_ex มาจาก caller เหมือนกัน)

    quarter เป้าหมาย: ดู _target_quarter — YoY คำนวณจากผลรวมกลุ่มปีนี้เทียบผลรวมกลุ่มปีก่อน โดยใช้
    เฉพาะหุ้นที่มีข้อมูลครบทั้งสองงวด (ไม่ผสมหุ้นที่ไม่มี prior เข้าไปในผลรวม ไม่งั้น YoY จะเพี้ยนเพราะ
    ตัวเศษ/ตัวส่วนมาจากชุดหุ้นคนละชุดกัน)

    'total'/'reported'/'missing_symbols' คือ coverage งวดล่าสุด (นับจากหุ้นทั้งหมดที่ classify
    ลง sector นั้นใน sector_by_symbol ไม่ใช่แค่หุ้นที่เคย sync set_qpl) — ธนาคาร/ประกัน/เงินทุนฯ
    (sector การเงิน) จะติด 'ขาดงบ' ค้างถาวรเป็นปกติ ไม่ใช่ล่าช้าจริง เพราะ set_qpl parse จาก
    บัญชี 'รายได้จากการขายและให้บริการ' ซึ่งผังบัญชีธนาคาร/ประกันไม่มีรายการนี้ (ดู
    _set_qpl_row_from_amt) — caller ควรตัดกลุ่มนี้ออกก่อนถ้าจะใช้ตัวเลข coverage จริงจัง

    คืน {"quarter": "YYYY-Q" หรือ None, "sectors": [{sector, count, total, reported,
    missing_symbols, revenue, revenue_yoy, revenue_qoq, profit, profit_yoy, profit_qoq, npm,
    profit_stocks, loss_stocks, profit_share_pct, stocks: [{symbol, revenue, net_profit,
    revenue_yoy, profit_yoy, revenue_qoq, profit_qoq, gpm, npm, revenue_ttm, profit_ttm,
    profit_prior, profit_prior_qoq}, ...]}, ...]}

    'stocks' คือรายตัวในกลุ่ม (เฉพาะที่รายงานงวด target แล้ว) เรียงลำดับเอง (revenue มาก->น้อย)
    ฝั่ง caller/frontend — TTM รวม 4 ไตรมาสล่าสุดนับจาก target ต้องมีครบทั้ง 4 งวดไม่งั้นคืน None
    (กันผลรวมเพี้ยนจากหุ้นที่เพิ่งเข้าตลาด/ขาดงบเก่า)

    profit_yoy/profit_qoq (ระดับ sector และรายหุ้น) เป็น None เมื่อฐานเทียบ (งวดก่อน) ติดลบหรือศูนย์
    — หารด้วยฐานติดลบพลิกเครื่องหมายผลลัพธ์ กลายเป็น % หลอก (หุ้นพลิกกำไรจริงจะโชว์ % ติดลบมหาศาล)
    caller ที่อยากหาหุ้นพลิกกำไรให้เทียบ net_profit > 0 กับ profit_prior/profit_prior_qoq <= 0 เอง
    (ดู field ดิบสองตัวนี้ในแต่ละ stock) แทนการอ่านจาก profit_yoy/qoq ตรงๆ"""
    parsed = _load_set_qpl_all(base_dir, sector_by_symbol)
    if not parsed:
        return {"quarter": None, "sectors": []}

    target = _target_quarter(parsed)

    sector_all_symbols = {}   # sector -> set ของหุ้นทั้งหมดที่ classify ลงกลุ่มนี้ (ไม่ว่าจะมีงบหรือไม่)
    for sym, sector in sector_by_symbol.items():
        sector_all_symbols.setdefault(sector, set()).add(sym)

    buckets = {}   # sector -> list of per-stock dict
    for row in _qpl_stock_growth_rows(parsed, target):
        buckets.setdefault(sector_by_symbol[row["symbol"]], []).append(row)

    total_profit_all = sum(r["net_profit"] for items in buckets.values() for r in items
                            if r["net_profit"] is not None)

    def _yoy(cur_key, prior_key_):
        pairs = [(r[cur_key], r[prior_key_]) for r in items
                 if r[cur_key] is not None and r[prior_key_] is not None]
        if not pairs:
            return None
        cur_sum, prior_sum = sum(p[0] for p in pairs), sum(p[1] for p in pairs)
        # prior_sum <= 0 กันเคส sector ทั้งกลุ่มพลิกจากขาดทุนรวมเป็นกำไรรวม (ฐานติดลบ) เจอ % หลอก
        # เหมือน _stock_yoy ด้านบน
        return round((cur_sum / prior_sum - 1) * 100, 1) if prior_sum > 0 else None

    sectors_out = []
    for sector in sector_all_symbols:   # ครบทุก sector แม้ยังไม่มีหุ้นไหนรายงานเลย (count=0)
        items = buckets.get(sector, [])
        rev_vals = [r["revenue"] for r in items if r["revenue"] is not None]
        rev_sum = sum(rev_vals) if rev_vals else None
        profit_vals = [r["net_profit"] for r in items if r["net_profit"] is not None]
        profit_sum = sum(profit_vals) if profit_vals else None
        all_syms = sector_all_symbols.get(sector, set())
        reported_syms = {r["symbol"] for r in items}
        missing_syms = sorted(all_syms - reported_syms)
        sectors_out.append({
            "sector": sector,
            "count": len(items),
            "total": len(all_syms),
            "reported": len(reported_syms),
            "missing_symbols": missing_syms,
            "revenue": rev_sum,
            "revenue_yoy": _yoy("revenue", "revenue_prior"),
            "revenue_qoq": _yoy("revenue", "revenue_prior_qoq"),
            "profit": profit_sum,
            "profit_yoy": _yoy("net_profit", "profit_prior"),
            "profit_qoq": _yoy("net_profit", "profit_prior_qoq"),
            "npm": round(profit_sum / rev_sum * 100, 1) if rev_sum and profit_sum is not None else None,
            "profit_stocks": sum(1 for r in items if r["net_profit"] is not None and r["net_profit"] > 0),
            "loss_stocks": sum(1 for r in items if r["net_profit"] is not None and r["net_profit"] < 0),
            # total_profit_all > 0 กันเคสตลาดรวมขาดทุนสุทธิ (ฐานติดลบ) พลิกเครื่องหมาย
            # profit_share_pct ทั้งกระดาน เหมือน _yoy ด้านบน
            "profit_share_pct": round(profit_sum / total_profit_all * 100, 1)
                if total_profit_all > 0 and profit_sum is not None else None,
            # profit_prior/profit_prior_qoq (กำไรงวดก่อนแบบดิบ) ใส่มาด้วยแม้ profit_yoy/qoq จะ
            # เป็น None ตอนฐานติดลบ — ให้ caller เช็คหุ้นพลิกกำไร (prior<=0 แต่ net_profit>0) ได้เอง
            # โดยไม่ต้องพึ่ง % ที่ไม่มีความหมายในเคสนี้
            "stocks": [{k: r[k] for k in (
                "symbol", "revenue", "net_profit", "revenue_yoy", "profit_yoy",
                "revenue_qoq", "profit_qoq", "gpm", "npm", "revenue_ttm", "profit_ttm",
                "profit_prior", "profit_prior_qoq")} for r in items],
        })

    return {"quarter": f"{target[0]}-{target[1]}", "sectors": sectors_out}


def get_market_trend(base_dir, symbols, excluded_symbols, n_quarters=20):
    """แนวโน้มตลาดย้อนหลังสูงสุด n_quarters ไตรมาส — ทั้งตลาด ไม่แยก sector (ต่างจาก
    get_sector_qpl_compare ที่ snapshot ไตรมาสเดียวแต่แยกกลุ่ม) รายได้/กำไร/NPM ตัด
    excluded_symbols ออก (หุ้นการเงิน+REIT ให้ caller กำหนดเอง — นิยาม 'รายได้' ธุรกิจกลุ่มนี้ไม่
    เทียบเท่าบริษัททั่วไป) ส่วน ROE/Cash Quality/Breadth ใช้ symbols ทั้งหมด (equity/CFO เทียบกัน
    ข้ามอุตสาหกรรมได้ปกติ)

    revenue/profit/NPM เป็นรายไตรมาสเดี่ยวจาก set_qpl ตรงๆ ส่วน ROE/Cash Quality เป็น TTM (กำไร/CFO
    สะสม 4 ไตรมาสล่าสุด) — equity มาจาก finnomena_q (balance sheet ไม่มีใน set_qpl) CFO ก็มาจาก
    finnomena_q ผ่าน _ttm_by_date (เช็คข้อมูลจริงแล้วว่า Finnomena เก็บ Operating Cash Flow เป็นค่า
    รายไตรมาสเดี่ยว ไม่ใช่สะสม YTD — สุ่มดู PTT/AOT/KBANK/CPALL/SCC 2026-08-17 — รวม 4 งวดตรงๆ
    ปลอดภัย)

    คืน {"quarters": [{quarter, revenue, revenue_yoy, revenue_qoq, revenue_coverage, profit,
    profit_yoy, profit_qoq, profit_coverage, npm,
    npm_median, roe_aggregate, roe_median, roe_coverage, cfo_np_aggregate, cfo_np_median,
    cfo_np_coverage, pct_revenue_growing, pct_profit_growing, pct_cfo_positive, flipped_profit,
    flipped_loss}, ...]} เรียงเก่า -> ใหม่"""
    import statistics

    qpl = _load_set_qpl_all(base_dir, symbols)
    if not qpl:
        return {"quarters": []}
    target = _target_quarter(qpl)

    seq = []   # เก่า -> ใหม่ จบที่ target
    y, q = target
    for _ in range(n_quarters):
        seq.append((y, q))
        y, q = _prev_quarter(y, q)
    seq.reverse()

    init_db(base_dir)
    con = _connect(base_dir)
    try:
        # symbol NOT LIKE '%:%' — ตัด mirror ('FINN:US:', 'DR:') ออกที่ชั้น SQL ก่อน json.loads
        # (หุ้น SET เป็น ticker เปล่าไม่มี ':' เสมอ) เดิมโหลดทั้งตาราง ~33k แถวแล้ว parse ทิ้ง ~32k
        fq_rows = con.execute(
            "SELECT symbol, payload FROM financials "
            "WHERE source='finnomena_q' AND symbol NOT LIKE '%:%'").fetchall()
    finally:
        con.close()

    def _bucket_by_quarter(date_val_dict):
        out = {}
        for d, v in (date_val_dict or {}).items():
            if v is None:
                continue
            yy, qq = _year_quarter_from_date(d)
            if qq:
                out[(yy, qq)] = v
        return out

    equity_by_sym, cfo_ttm_by_sym = {}, {}
    for sym, payload_raw in fq_rows:
        if sym not in symbols:
            continue
        try:
            payload = json.loads(payload_raw)
        except Exception:
            continue
        eq_by_q = _bucket_by_quarter((payload or {}).get("balance", {}).get("Stockholders Equity"))
        if eq_by_q:
            equity_by_sym[sym] = eq_by_q
        cfo_ttm = _ttm_by_date((payload or {}).get("cashflow", {}).get("Operating Cash Flow") or {})
        cfo_by_q = _bucket_by_quarter(cfo_ttm)
        if cfo_by_q:
            cfo_ttm_by_sym[sym] = cfo_by_q

    def _ttm_profit(sym, y0, q0):
        qs = qpl.get(sym)
        if not qs:
            return None
        total, yy, qq = 0, y0, q0
        for _ in range(4):
            row = qs.get((yy, qq))
            if not row or row.get("net_profit") is None:
                return None
            total += row["net_profit"]
            yy, qq = _prev_quarter(yy, qq)
        return total

    def _sum_yoy(pairs):
        m = [(c, p) for c, p in pairs if c is not None and p is not None]
        if not m:
            return None
        cs, ps = sum(x[0] for x in m), sum(x[1] for x in m)
        # ps > 0 ไม่ใช่แค่ != 0 — ฐานเทียบรวมติดลบ (ทั้งตลาด/scope ขาดทุนรวม) หารแล้วพลิก
        # เครื่องหมาย กลายเป็น % หลอก (เหมือน _yoy ใน get_sector_qpl_compare + docstring ระบุไว้)
        return round((cs / ps - 1) * 100, 1) if ps > 0 else None

    quarters_out = []
    for (y, q) in seq:
        prior_qoq = _prev_quarter(y, q)
        prior_yoy = (y - 1, q)

        # ── รายได้/กำไร/NPM รายไตรมาสเดี่ยว (ตัดหุ้นการเงิน+REIT) ──
        rev_pairs, profit_pairs, npm_vals = [], [], []
        rev_qoq_pairs, profit_qoq_pairs = [], []
        for sym, qs in qpl.items():
            if sym in excluded_symbols:
                continue
            row = qs.get((y, q))
            if not row:
                continue
            rev, net = row.get("revenue"), row.get("net_profit")
            prev_row = qs.get(prior_yoy) or {}
            prev_qoq_fin_row = qs.get(prior_qoq) or {}
            rev_pairs.append((rev, prev_row.get("revenue")))
            profit_pairs.append((net, prev_row.get("net_profit")))
            rev_qoq_pairs.append((rev, prev_qoq_fin_row.get("revenue")))
            profit_qoq_pairs.append((net, prev_qoq_fin_row.get("net_profit")))
            if rev and net is not None and rev != 0:
                npm_vals.append(net / rev * 100)

        rev_sum = sum(c for c, _ in rev_pairs if c is not None)
        rev_n = sum(1 for c, _ in rev_pairs if c is not None)
        profit_sum = sum(c for c, _ in profit_pairs if c is not None)
        profit_n = sum(1 for c, _ in profit_pairs if c is not None)

        # ── ROE / Cash Quality TTM (ทั้งตลาด ไม่ตัดการเงิน/REIT) ──
        roe_vals, roe_pairs, cfo_np_vals, cfo_np_pairs = [], [], [], []
        for sym in symbols:
            pttm = _ttm_profit(sym, y, q)
            if pttm is None:
                continue
            eq = equity_by_sym.get(sym, {}).get((y, q))
            if eq and eq > 0:
                roe_vals.append(pttm / eq * 100)
                roe_pairs.append((pttm, eq))
            cfo = cfo_ttm_by_sym.get(sym, {}).get((y, q))
            if cfo is not None and pttm != 0:
                cfo_np_vals.append(cfo / pttm)
                cfo_np_pairs.append((cfo, pttm))

        roe_aggregate = (round(sum(p for p, _ in roe_pairs) / sum(e for _, e in roe_pairs) * 100, 1)
                          if roe_pairs else None)
        cfo_np_denom = sum(p for _, p in cfo_np_pairs)
        # pttm มีเครื่องหมายได้ทั้งบวก/ลบ (ต่างจาก eq ของ roe_aggregate ที่การันตี >0 เสมอ) —
        # ผลรวมทั้งกลุ่มเป็นศูนย์ได้ (กัน ZeroDivisionError) และ 'ติดลบ' ได้ด้วยเมื่อ scope ขาดทุน
        # รวม (โดยเฉพาะ /api/sector-trend ที่เหลือหุ้นน้อยตัว) — หาร CFO ด้วยกำไรรวมติดลบพลิก
        # เครื่องหมาย ได้ ratio หลอก (เช่น CFO บวกแต่ได้ -1.8x) ต้อง > 0 เท่านั้น
        cfo_np_aggregate = (round(sum(c for c, _ in cfo_np_pairs) / cfo_np_denom, 2)
                             if cfo_np_pairs and cfo_np_denom > 0 else None)

        # ── Breadth (ทั้งตลาด) — %โต YoY เทียบไตรมาสเดียวกันปีก่อน, พลิกกำไร/ขาดทุนเทียบไตรมาส
        # ก่อนหน้าติดกัน (QoQ) เพราะ 'พลิก' หมายถึงเปลี่ยนสถานะจากงวดล่าสุดที่รายงาน ไม่ใช่ปีก่อน ──
        grow_rev = grow_profit = cfo_pos = flip_profit = flip_loss = 0
        n_grow_rev = n_grow_profit = n_cfo = 0
        for sym, qs in qpl.items():
            row = qs.get((y, q))
            if not row:
                continue
            prev_yoy_row = qs.get(prior_yoy) or {}
            prev_qoq_row = qs.get(prior_qoq) or {}
            rev, net = row.get("revenue"), row.get("net_profit")
            if rev is not None and prev_yoy_row.get("revenue") is not None:
                n_grow_rev += 1
                if rev > prev_yoy_row["revenue"]:
                    grow_rev += 1
            if net is not None and prev_yoy_row.get("net_profit") is not None:
                n_grow_profit += 1
                if net > prev_yoy_row["net_profit"]:
                    grow_profit += 1
            if net is not None and prev_qoq_row.get("net_profit") is not None:
                if prev_qoq_row["net_profit"] <= 0 and net > 0:
                    flip_profit += 1
                elif prev_qoq_row["net_profit"] > 0 and net <= 0:
                    flip_loss += 1
            cfo = cfo_ttm_by_sym.get(sym, {}).get((y, q))
            if cfo is not None:
                n_cfo += 1
                if cfo > 0:
                    cfo_pos += 1

        quarters_out.append({
            "quarter": f"{y}-{q}",
            "revenue": rev_sum, "revenue_yoy": _sum_yoy(rev_pairs), "revenue_coverage": rev_n,
            "revenue_qoq": _sum_yoy(rev_qoq_pairs),
            "profit": profit_sum, "profit_yoy": _sum_yoy(profit_pairs),
            "profit_qoq": _sum_yoy(profit_qoq_pairs), "profit_coverage": profit_n,
            "npm": round(profit_sum / rev_sum * 100, 1) if rev_sum else None,
            "npm_median": round(statistics.median(npm_vals), 1) if npm_vals else None,
            "roe_aggregate": roe_aggregate,
            "roe_median": round(statistics.median(roe_vals), 1) if roe_vals else None,
            "roe_coverage": len(roe_vals),
            "cfo_np_aggregate": cfo_np_aggregate,
            "cfo_np_median": round(statistics.median(cfo_np_vals), 2) if cfo_np_vals else None,
            "cfo_np_coverage": len(cfo_np_vals),
            "pct_revenue_growing": round(grow_rev / n_grow_rev * 100, 1) if n_grow_rev else None,
            "pct_profit_growing": round(grow_profit / n_grow_profit * 100, 1) if n_grow_profit else None,
            "pct_cfo_positive": round(cfo_pos / n_cfo * 100, 1) if n_cfo else None,
            "flipped_profit": flip_profit,
            "flipped_loss": flip_loss,
        })

    return {"quarters": quarters_out}


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

def _set_health_excluded_symbols(base_dir):
    """หุ้นไทยกลุ่มที่ SET Financial Health Check (source='set_health') ไม่ครอบคลุมแน่ (คืน
    404 เสมอ) — สถาบันการเงิน (ธนาคาร/เงินทุน/ประกัน, factor_snapshot._financial_sector_symbols
    ตัวเดียวกับที่กัน Z-Score/market-trend) + Property Fund & REITs (งบดุลกลุ่มนี้ตีความไม่ได้
    ด้วยสูตรบริษัททั่วไปเหมือนกัน) — ใช้กรอง task ออกจาก sync_all ตั้งแต่ต้น กันนับเป็น 'fail'
    ทั้งที่รู้อยู่แล้วว่าไม่มีข้อมูลจริง (pattern เดียวกับ finnomena_supported ด้านล่าง,
    code review 2026-08-26) เงียบถ้าอ่าน set_data.json ไม่ได้ (คืนแค่กลุ่มสถาบันการเงิน)"""
    from sources import factor_snapshot
    excluded = set(factor_snapshot._financial_sector_symbols(base_dir))
    try:
        with open(os.path.join(base_dir, "set_data.json"), encoding="utf-8") as f:
            for s in json.load(f).get("stocks", []):
                if s.get("sector") == "Property Fund & REITs":
                    excluded.add(s["symbol"])
    except Exception:
        pass
    return excluded


def sync_all(base_dir, symbols, sources=("yahoo", "set"), workers=6, callback=None,
            is_dr=False, skip_up_to_date=False, market=None, auto_rebuild=True):
    """ดึงงบการเงินเต็มของทุก symbol × ทุก source มา upsert เข้า DB
    คืน {"ok": n, "fail": n, "total": n, "skipped": n}

    skip_up_to_date=True: ข้ามคู่ (symbol, source) ที่ "มีข้อมูลงวดล่าสุดที่ควรจะมีอยู่แล้ว"
    (เทียบ _payload_latest_period ของ payload จริงใน DB กับ _target_period ของวันนี้) —
    ต่างจาก logic เดิมที่เดาจาก "sync ไปแล้วกี่วัน" (เช่น หุ้นที่ sync เมื่อวานแต่ยังไม่มี Q2
    เพราะ SET ยังไม่ขึ้นข้อมูล จะถูก skip ทิ้งผิดๆ ทั้งที่ยังไม่มีของจริง) — แบบใหม่นี้ยิงซ้ำทุกครั้ง
    ที่กดจนกว่าจะได้งวดล่าสุดจริง แล้วจะหยุด skip เองพอมีครบ ไม่ต้องเดาจำนวนวัน
    False (ปกติ) = ดึงทุกคู่เสมอเหมือนพฤติกรรมเดิม (full sync — ใช้กับ scheduled task รายไตรมาส)

    auto_rebuild=True (ปกติ): หลัง sync สำเร็จ ตั้ง timer debounce 300 วิ rebuild factor_snapshot
    ให้เอง — caller ที่เรียก factor_snapshot.build_snapshot() ต่อเองแบบ synchronous ทันที
    (เช่น _run_financials_sync / _run_financials_update_all ใน app.py) ควรส่ง False กัน rebuild
    ซ้ำอีกรอบตอน timer ครบ (เปลืองเปล่า — DELETE+INSERT ทั้งตารางหลักพันตัว)

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
    # ตัด symbol ที่ Finnomena ไม่รองรับ + หุ้นกลุ่มที่ set_health ไม่ครอบคลุมแน่ (สถาบันการเงิน/
    # REIT) ออกจาก task ตั้งแต่ต้น จะได้ไม่นับเป็น fail ทั้งที่รู้อยู่แล้วว่าไม่มีข้อมูล
    _sh_excl = (_set_health_excluded_symbols(base_dir)
                if (not is_dr) and "set_health" in sources else set())
    tasks = [(sym, src) for sym in symbols for src in sources
             if not (src == "finnomena_q" and not finnomena_supported(sym, is_dr=is_dr))
             and not (src == "set_health" and sym in _sh_excl)]

    skipped = 0
    if skip_up_to_date:
        latest_map = get_latest_period_map(base_dir, is_dr=is_dr, sources=sources)
        fresh_tasks = []
        for sym, src in tasks:
            have = latest_map.get((sym, src))
            if have is not None and have >= _target_period(src):
                skipped += 1
            else:
                fresh_tasks.append((sym, src))
        tasks = fresh_tasks

    total = len(tasks)
    done = ok = fail = 0

    set_ctx, set_hdr = (None, None)
    _SET_EXTRA_SRCS = ("set_cashflow", "set_balance", "set_health")
    if "set" in sources or "set_qpl" in sources or any(s in sources for s in _SET_EXTRA_SRCS):
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
    _set_qpl_gate = threading.Semaphore(2)   # set_qpl ยิงหลาย request/หุ้น (chart+periods+detail สูงสุด 6) หนักกว่า 'set' เฉยๆ
    _set_extra_gate = threading.Semaphore(2)   # set_cashflow/set_balance (periods loop เหมือน set_qpl detail) + set_health (1 call) + set_factsheet (5 call sequential ใน gate เดียว) ใช้ gate เดียวกัน กันยิง SET.or.th ถี่เกินตอน sync พร้อมกันหลาย source
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
        elif src == "set_qpl":
            with _set_qpl_gate:
                try:
                    raw = fetch_set_qpl_series(sym, ctx=set_ctx, hdr=set_hdr)
                    payload = {"quarters": {f"{y}-{q}": row for (y, q), row in raw.items()}}
                finally:
                    time.sleep(0.3)  # throttle มากกว่า 'set' เพราะยิงหลาย request/หุ้น
        elif src == "set_cashflow":
            with _set_extra_gate:
                try:
                    raw = fetch_set_cashflow_series(sym, ctx=set_ctx, hdr=set_hdr)
                    payload = {"quarters": {f"{y}-{q}": row for (y, q), row in raw.items()}}
                finally:
                    time.sleep(0.3)
        elif src == "set_balance":
            with _set_extra_gate:
                try:
                    raw = fetch_set_balance_series(sym, ctx=set_ctx, hdr=set_hdr)
                    payload = {"quarters": {f"{y}-{q}": row for (y, q), row in raw.items()}}
                finally:
                    time.sleep(0.3)
        elif src == "set_health":
            with _set_extra_gate:
                try:
                    from sources.set_api import fetch_financial_health
                    payload = fetch_financial_health(sym, ctx=set_ctx, hdr=set_hdr)
                finally:
                    time.sleep(0.2)  # เบากว่า cashflow/balance เพราะยิงแค่ 1 request/หุ้น
        elif src == "set_factsheet":
            with _set_extra_gate:
                try:
                    from sources.set_api import fetch_financial_factsheet
                    payload = fetch_financial_factsheet(sym, ctx=set_ctx, hdr=set_hdr)
                finally:
                    time.sleep(0.2)  # 5 sub-call sequential ในฟังก์ชันเดียว ใช้ gate คลุมทั้งก้อนครั้งเดียว
            # ทั้ง 5 sub-endpoint คืน None หมด = fetch พังจริง (SET.or.th ล่ม/cookie หมด) ไม่ใช่
            # 'หุ้นนี้ไม่มี factsheet' — raise ให้ sync_all นับเป็น fail (เดิม fetch_financial_factsheet
            # กลืน exception รายตัวหมด → payload None-ล้วน → _empty → ข้าม upsert แต่ยังนับ ok หลอก)
            if not any(payload.get(k) is not None for k in _FACTSHEET_KEYS):
                raise RuntimeError(f"factsheet ทั้ง 5 sub-endpoint ล้มเหลวสำหรับ {sym}")
        else:
            payload = fetch_set_full(sym, set_ctx, set_hdr)
            time.sleep(0.15)  # throttle เบาๆ กัน SET.or.th block IP
        # set_cashflow/set_balance/set_health: ถ้า fetch สำเร็จ (ไม่ raise/ไม่ 404) แต่ได้ผลว่างเปล่า
        # ชั่วคราว (เช่น หุ้น IPO ใหม่ที่ยังไม่มีงวดพอให้ SET Financial Health คืนมา — ได้ 200 พร้อม
        # periods/themes ว่าง ไม่ใช่ 404) ห้าม upsert แถวเปล่าทับ — coverage จะเข้าใจผิดว่า sync
        # ผ่านแล้ว (get_coverage เช็คแค่ "แถวมีอยู่" ไม่เช็คเนื้อหา) ทั้งที่ไม่มีข้อมูลจริง และ
        # skip_up_to_date จะไม่ retry ให้อีกเลย — เหมือน guard ที่ sync_set_cashflow_series/
        # sync_set_balance_series/sync_set_health มีอยู่แล้ว (`if fresh: upsert(...)`) แค่ยังไม่เคยมีใน
        # bulk sync path นี้ (code review 2026-08-26, ขยายครอบ set_health ด้วยหลัง code review ต่อ) ·
        # set_qpl ใช้โครง {"quarters": {...}} เดียวกับ set_cashflow/set_balance (fetch_set_qpl_series
        # ก็ silent-fail คืน {} เหมือนกัน — ดู comment ใน fetch_set_qpl_series) แต่ตกหล่นจาก guard นี้
        # มาตลอด เพิ่มเข้าด้วย (code review 2026-09-02)
        _empty = ((src in ("set_qpl", "set_cashflow", "set_balance") and not payload.get("quarters"))
                  or (src == "set_health" and not payload.get("themes"))
                  or (src == "set_factsheet" and not any(payload.get(k) for k in _FACTSHEET_KEYS)))
        if not _empty:
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
                if src in ("yahoo", "yahoo_q") and _is_yahoo_throttle_err(e):
                    _yahoo_throttle.note_outcome(False)
            if callback:
                callback(done, total, f"งบการเงิน {done}/{total} ({sym} · {src})...")

    _set_meta(base_dir, "last_full_sync_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if auto_rebuild and ok > 0 and len(symbols) >= _AUTO_REBUILD_MIN_SYMBOLS:
        try:
            from sources import factor_snapshot   # lazy import กัน circular import (factor_snapshot import โมดูลนี้อยู่แล้ว)
            factor_snapshot.schedule_rebuild(base_dir)
        except Exception as e:
            print(f"[FinancialsSync] เรียก auto-rebuild factor_snapshot ไม่สำเร็จ (ไม่กระทบ sync): {e}")
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


_CC_MAX_DAYS = 730  # เพดาน DSO/DPO (2 ปี) — ดู docstring ด้านล่างสำหรับที่มา


def compute_cash_cycle(payload_yahoo):
    """Cash Conversion Cycle = DIO + DSO − DPO จากงบ Yahoo (รายปี งวดล่าสุดที่ครบ)
    คำนวณเองเพราะค่า cash_cycle สำเร็จรูปของ Finnomena เชื่อไม่ได้ (ทดสอบแล้วเพี้ยน)
      DIO = สินค้าคงคลัง / ต้นทุนขาย × 365   (ของค้างสต็อกกี่วัน)
      DSO = ลูกหนี้การค้า / รายได้ × 365       (เก็บเงินกี่วัน)
      DPO = เจ้าหนี้การค้า / ต้นทุนขาย × 365   (จ่ายเจ้าหนี้กี่วัน)
    คืน None ทุกค่าถ้า field ไม่ครบ (เช่นธนาคาร/ประกัน ที่ไม่มี COGS/inventory)

    2 guard เพิ่ม (CALC_RISK_AUDIT_ROUND3 [1], ยืนยันด้วยข้อมูลจริงจาก financials.db แล้วว่า
    เกิดขึ้นจริง ~11% ของหุ้นไทยที่คำนวณได้):
    1. COGS งวดล่าสุดจิ๋วผิดปกติเทียบกับสเกลปกติของบริษัทเอง (เช่น BTC ปี 2024 cogs=14M ทั้งที่
       ปีอื่น 81-276M, QDC ธุรกิจหดตัวจนเกือบเป็น 0) — เดิม guard แค่ r<=0/c<=0 ปล่อยผ่านตัวเลข
       บวกจิ๋วเข้าสูตรตรงๆ ทำให้ DIO/DPO ระเบิดหลักหมื่นวัน เทียบกับ median ของปีอื่นที่มีอยู่ใน
       payload เดียวกัน (pattern เดียวกับ scale_ref ของ growth screener near-zero-base fix)
       — ไม่กระทบ COGS ที่เล็กแต่คงเส้นคงวาทุกปี (เช่นธุรกิจ high-margin จริง)
    2. DSO/DPO เกิน _CC_MAX_DAYS (2 ปี) → ถือว่า AR/AP ที่ดึงมาไม่ใช่ลูกหนี้/เจ้าหนี้การค้าจริง
       ของธุรกิจปกติ (พบจริงในบริษัทกลุ่มการเงิน/โฮลดิ้ง/ลีสซิ่งที่ field ชื่อ "Accounts
       Receivable/Payable" ของ Yahoo คือยอดคงค้างทางการเงินคนละความหมาย เช่น JP:8253 DSO=2681
       วัน, HK:0227 DPO=6211 วัน) — ตั้งใจ **ไม่แคป DIO** เพราะ DIO หลักพันวันเป็นค่าจริงที่ถูกต้อง
       สำหรับธุรกิจ land-bank/อสังหาริมทรัพย์ (ยืนยันจริงจากหุ้นไทย SENA/ORI/RML/PROUD/MJD/TITLE
       ฯลฯ ที่ inventory คือที่ดิน/โครงการระหว่างพัฒนาถือหลายปีเป็นปกติทางธุรกิจ ไม่ใช่บั๊ก)"""
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

    other_cogs = sorted(cogs[k] for k in common if k != d and cogs.get(k) and cogs[k] > 0)
    if other_cogs:
        scale_ref = other_cogs[len(other_cogs) // 2]
        if c < scale_ref * 0.2:
            return none

    dio = inv[d] / c * 365
    dso = ar[d] / r * 365
    dpo = ap[d] / c * 365
    if dso > _CC_MAX_DAYS or dpo > _CC_MAX_DAYS:
        return none
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


def compute_dcf_forecast_inputs(payload_yahoo):
    """วัตถุดิบสำหรับ DCF Model แบบเต็ม (Revenue→EBIT→NOPLAT→D&A→CapEx→NWC→FCFF) จากงบ Yahoo
    annual ปีล่าสุดที่มี Revenue+EBIT ครบ — เป็นแค่ 'ค่าเริ่มต้น' ให้ผู้ใช้ปรับเอง ไม่ใช่การพยากรณ์
    (ทุก % คำนวณจากปีล่าสุดปีเดียว ไม่ได้เฉลี่ยหลายปี) รวม total_debt/cost_of_debt_pretax
    (ดอกเบี้ยจ่าย÷หนี้รวม) สำหรับ Capital Structure/Cost of Debt ของ WACC เพราะมีในงบจริง —
    ส่วน Rf/Beta/Equity Risk Premium ไม่คำนวณที่นี่ (ไม่มีในงบเลย ไม่มี beta/risk-free rate
    เก็บไว้ในระบบ) ฝั่ง UI ให้ผู้ใช้กรอกเองทั้งหมด (ลิงก์ไปหน้า factsheet ของ SET.or.th ที่มีค่า
    Beta หุ้นรายตัวให้ดูประกอบ)

    nwc_pct_revenue = **ระดับ** Working Capital (Current Assets − Current Liabilities) หารรายได้
    ปีล่าสุด — ไม่ใช่ 'การเปลี่ยนแปลง' ของ NWC เหมือนเดิม (เทียบกับหน้า reference ภายนอกที่ผู้ใช้
    เอามาเทียบ 2026-08-21: PTT ตรงเป๊ะ 12.75% vs 12.7% ด้วยนิยามนี้ ตรงข้ามกับของเดิมที่ใช้
    'Change In Working Capital' จากงบกระแสเงินสดซึ่งห่างจากอ้างอิงมาก ~10-12pp ทุกตัวที่ทดสอบ)
    ฝั่งคำนวณ FCFF (compute_dcf_for_symbol/_tsDcfModelRecalc) ต้องคูณกับ 'ส่วนต่างรายได้ปีต่อปี'
    ไม่ใช่รายได้เต็มปี ถึงจะได้ผลกระทบต่อกระแสเงินสดที่ถูกต้อง (มาตรฐาน DCF: ΔNWC ดอลลาร์ = NWC ratio
    × ΔRevenue, รายได้โตยิ่งใช้เงินทุนหมุนเวียนเพิ่มยิ่งกินกระแสเงินสด — ต่างจากเดิมที่คูณรายได้เต็มปี
    ซึ่งถูกต้องเฉพาะตอนใช้นิยาม 'การเปลี่ยนแปลง' แบบเดิมเท่านั้น) ถ้า Yahoo ไม่มีทั้ง Working Capital
    และ Current Assets/Current Liabilities คืน None ไปเลย (ไม่ fallback ไปใช้ ΔNWC จากกระแสเงินสด
    เหมือนเดิม — นั่นเป็น 'อัตราการไหล' คนละมิติกับ 'ระดับ' ป้อนเข้าสูตรเดียวกันไม่ได้ ผสมแล้วเครื่องหมาย
    พลิกแบบเงียบๆ ดู CALC_RISK_AUDIT_2026-09-05.txt ข้อ [3]) — ฝั่งเรียกใช้ (compute_dcf_for_symbol/
    _tsDcfModelRecalc) เจอ None แล้ว default เป็น 0% เอง (ไม่ตั้งสมมติฐานผลกระทบ NWC เลย ปลอดภัยกว่า
    เดาเครื่องหมายผิด)
    คืน None ถ้าไม่มี Revenue/EBIT ปีล่าสุดให้คำนวณ"""
    inc = payload_yahoo.get("income", {})
    bal = payload_yahoo.get("balance", {})
    cf = payload_yahoo.get("cashflow", {})

    rev_row = inc.get("Total Revenue", {})
    ebit_row = inc.get("EBIT", {}) or inc.get("Operating Income", {})
    tax_row = inc.get("Tax Provision", {})
    pretax_row = inc.get("Pretax Income", {})
    da_row = inc.get("Reconciled Depreciation", {})
    capex_row = cf.get("Capital Expenditure", {})
    wc_row = bal.get("Working Capital", {})
    ca_row = bal.get("Current Assets", {})
    cl_row = bal.get("Current Liabilities", {})
    debt_row = bal.get("Total Debt", {})
    int_row = inc.get("Interest Expense", {})

    if not rev_row or not ebit_row:
        return None
    latest_date = max(set(rev_row) & set(ebit_row), default=None)
    if latest_date is None:
        return None
    revenue = rev_row.get(latest_date)
    ebit = ebit_row.get(latest_date)
    if not revenue or revenue <= 0:
        return None

    # เดิมใช้ next((d for d in tax_row if d in pretax_row)) ซึ่งหยิบ 'คีย์แรกตามลำดับ dict'
    # ไม่ใช่ปีล่าสุด (dict ที่ merge จากหลายรอบ sync ไม่รับประกันเรียงตามวันที่) — ทำให้บางหุ้น
    # (เช่น PTT) ได้ tax rate ปีเก่าสุดที่ชนเพดาน 40% แทนปีล่าสุดจริง พบจากเทียบ reference
    # ภายนอก 2026-08-21 (PTT ห่าง 9pp ทั้งที่ปีล่าสุดจริงห่างแค่ 1.85pp) แก้เป็นไล่ย้อนหาปีล่าสุด
    # ที่ยังมีกำไร (pretax > 0) แทน — ปีขาดทุนหารอัตราภาษีไม่ได้ (นับจริง 2026-08-21: หุ้นไทยที่ไม่ใช่
    # กลุ่มการเงิน 26.1% ขาดทุนในปีล่าสุด เดิม fallback ไป 20% เหมาทันทีทั้งที่ 72.8% ของกลุ่มนี้มีปี
    # กำไรอยู่ในประวัติให้ดึงมาใช้ได้จริง) — ทดลองเทียบ 'ปีล่าสุดที่กำไร' กับ 'ค่าเฉลี่ยหลายปีที่กำไร'
    # แล้ว ส่วนใหญ่ต่างกันแค่ median 2.6pp แต่เคสที่ต่างเยอะ (เช่น หมดสิทธิ BOI อัตราภาษีกระโดดถาวร)
    # ปีล่าสุดสะท้อนสถานะปัจจุบันแม่นกว่าค่าเฉลี่ยที่เอายุคเก่ามาผสม จึงเลือกปีล่าสุดที่กำไร ไม่ใช่ค่าเฉลี่ย
    tax_rate = None
    if tax_row and pretax_row:
        for td in sorted(set(tax_row) & set(pretax_row), reverse=True):
            pretax, tax = pretax_row.get(td), tax_row.get(td)
            if pretax and pretax > 0 and tax is not None:
                tax_rate = round(max(0.0, min(tax / pretax, 0.4)) * 100, 2)
                break
    if tax_rate is None:
        tax_rate = 20.0   # อัตราภาษีนิติบุคคลไทยมาตรฐาน — fallback เมื่อไม่เคยมีปีกำไรในข้อมูลที่มี

    def _pct_of_revenue(row, use_abs=False):
        if latest_date not in row or row[latest_date] is None:
            return None
        v = row[latest_date]
        return round((abs(v) if use_abs else v) / revenue * 100, 2)

    # NWC % Revenue: ระดับ Working Capital ปีล่าสุด ÷ Revenue — ลำดับ fallback: field
    # 'Working Capital' ตรงๆ จาก Yahoo -> คำนวณเอง (Current Assets − Current Liabilities) ->
    # ไม่มีข้อมูล balance sheet เลย -> None (ห้าม fallback ไปใช้ ΔNWC จากงบกระแสเงินสด — เป็นคนละ
    # นิยาม 'อัตราการไหล' ไม่ใช่ 'ระดับ' ป้อนสูตร nwc_impact = -(nwc_pct × ΔRevenue) ไม่ได้ ดู
    # docstring ฟังก์ชันนี้ + CALC_RISK_AUDIT_2026-09-05.txt ข้อ [3])
    if latest_date in wc_row and wc_row[latest_date] is not None:
        nwc_pct_revenue = round(wc_row[latest_date] / revenue * 100, 2)
    elif latest_date in ca_row and latest_date in cl_row and ca_row[latest_date] is not None and cl_row[latest_date] is not None:
        nwc_pct_revenue = round((ca_row[latest_date] - cl_row[latest_date]) / revenue * 100, 2)
    else:
        nwc_pct_revenue = None

    rev_history = [{"year": int(d[:4]), "revenue": v} for d, v in sorted(rev_row.items()) if v is not None]

    # หนี้สินรวมงวดล่าสุด (สำหรับ Capital Structure ของ WACC) + ดอกเบี้ยจ่ายเฉลี่ยต่อหนี้
    # (ประมาณ Cost of Debt ก่อนภาษี จากงบจริง — ต่างจาก Rf/Beta/ERP ที่ไม่มีในงบเลย)
    # total_debt ติดลบ = ข้อมูล Yahoo เพี้ยน (พบจริงกับหุ้นกลุ่มการเงินบางตัว) ไม่ใช่โครงสร้างทุนจริง
    # ทิ้งเป็น None แทนปล่อยให้ debt_val ติดลบไปกวน E/V-D/V weight ของ WACC ที่ dcf_screener.py
    total_debt = debt_row.get(latest_date) if latest_date in debt_row else None
    if total_debt is not None and total_debt < 0:
        total_debt = None
    interest_expense = int_row.get(latest_date) if latest_date in int_row else None
    cost_of_debt_pretax = None
    if total_debt and total_debt > 0 and interest_expense is not None:
        cost_of_debt_pretax = round(abs(interest_expense) / total_debt * 100, 2)
        # เพดานกันเคส total_debt เล็กผิดปกติเทียบดอกเบี้ยจ่าย (พบจริงหลายพันจุดข้อมูล กลุ่มธนาคาร/
        # โบรก/ประกัน ที่ 'หนี้' ตามนิยาม Yahoo แทบไม่รวมเงินฝาก/ภาระหลักของธุรกิจ) ให้ผลลัพธ์พุ่งเกิน
        # จริงหลายร้อย-หลายพัน% — ไม่สมเหตุสมผลเป็น cost of debt จึงทิ้งกลับไปใช้ DEFAULT_COST_OF_DEBT
        # _PRETAX_PCT (dcf_screener.py) แทน เหมือนตอนไม่มีข้อมูลเลย
        if cost_of_debt_pretax > 30.0:
            cost_of_debt_pretax = None

    return {
        "as_of": latest_date[:10] if isinstance(latest_date, str) else latest_date,
        "revenue": revenue, "ebit": ebit,
        "ebit_margin": round(ebit / revenue * 100, 2),
        "tax_rate": tax_rate,
        # ไม่ใช้ abs() (ต่างจาก capex_pct_revenue ด้านล่าง) — Reconciled Depreciation ปกติ Yahoo
        # รายงานเป็นบวกอยู่แล้ว แต่บางปี/บางหุ้น (พบจริง 66 จุดข้อมูล 31 สัญลักษณ์ เช่น ประกัน/
        # พลังงาน/กระดาษ) รายงานติดลบจริง (รายการปรับปรุงบัญชี/reversal) — บังคับ abs() จะกลบเครื่องหมาย
        # ทำให้ FCFF บวก D&A ผิดทางในปีนั้น ปล่อยเครื่องหมายจริงไว้ให้สูตร noplat+da-capex+nwc_impact
        # จัดการเอง (ค่าติดลบ = ลด FCFF ถูกต้องแล้ว) ดู CALC_RISK_AUDIT_2026-09-05.txt ข้อ [3]
        "da_pct_revenue": _pct_of_revenue(da_row, use_abs=False),
        "capex_pct_revenue": _pct_of_revenue(capex_row, use_abs=True),
        "nwc_pct_revenue": nwc_pct_revenue,
        "total_debt": total_debt,
        "cost_of_debt_pretax": cost_of_debt_pretax,
        "rev_history": rev_history,
    }


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
    ref_date = max(close) if close else _date.today().isoformat()

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
        pts = _drop_stuck_tail(pts)  # provider ค้างค่าซ้ำ — เกิดได้กับทุก metric รวม div_yield
        if short != "div_yield":   # div_yield: median มักใกล้ 0 อยู่แล้ว filter สัมพัทธ์ไม่เสถียร
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


# ─────────────────────────────────────────────────────────────────────────────
# bake งบกำไรขาดทุนรายไตรมาส (SET.or.th official — source 'set_qpl') สำหรับ
# เวอร์ชันมือถือ/iPad → data/financials_quarterly.json
#
# 'set_qpl' สะสมถาวรอยู่แล้ว (_merge_set_qpl_payload) ทำให้บริษัทส่วนใหญ่มี ≥8 ไตรมาส
# แม้ SET.or.th จะ serve สดแค่ ~2 ปี — ตรงนี้แค่ดึงออกมา bake ไม่มี Finnomena/Yahoo ปน
# (Finnomena ห้ามขึ้น GitHub ตาม CLAUDE.md · SET.or.th public เหมือน set_daily_valuation)
# ─────────────────────────────────────────────────────────────────────────────
def bake_quarterly_pl(base_dir, symbols, max_q=16):
    """คืน {"as_of": "YYYY-MM-DD", "set": {sym: {...}}} สำหรับ bake ไฟล์ static

    ต่อหุ้น: {"q": ["2024Q1", ...], "revenue": [...], "gross_profit": [...],
             "op_profit": [...], "net_profit": [...]}  — เอา max_q ไตรมาสล่าสุด
    ค่าเป็นบาทเต็มหน่วย · null ถ้า field ขาด · ข้ามหุ้นที่ < 4 ไตรมาส
    ถ้าไม่มี financials.db (เช่นบน CI) คืน {"set": {}} ให้ตัว bake ใช้ guard คงไฟล์เดิม
    """
    from datetime import datetime, timezone, timedelta
    as_of = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")
    if not db_exists(base_dir):
        return {"as_of": as_of, "set": {}}

    parsed = _load_set_qpl_all(base_dir, set(symbols), source="set_qpl")
    FIELDS = [("revenue", "revenue"), ("gross_profit", "gross_profit"),
              ("op_profit", "operating_profit"), ("net_profit", "net_profit")]
    out = {}
    for sym, quarters in parsed.items():
        keys = sorted(quarters.keys())[-max_q:]
        if len(keys) < 4:
            continue
        # เก็บเป็น "ล้านบาท" (จำนวนเต็ม) ให้ไฟล์เล็กลง — มือถือคูณ 1e6 กลับก่อน format
        rec = {"q": [f"{y}Q{q}" for (y, q) in keys], "unit": "M฿"}
        for out_key, src_key in FIELDS:
            vals = []
            for k in keys:
                v = quarters[k].get(src_key)
                try:
                    vals.append(round(float(v) / 1e6) if v is not None else None)
                except (TypeError, ValueError):
                    vals.append(None)
            rec[out_key] = vals
        # ข้ามถ้าไม่มีรายได้เลยสักไตรมาส (payload เปล่า)
        if any(v is not None for v in rec["revenue"]):
            out[sym] = rec
    return {"as_of": as_of, "set": out}
