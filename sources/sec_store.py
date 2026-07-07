# -*- coding: utf-8 -*-
"""sources/sec_store.py — เก็บสะสม SEC filings (แบบ 59 / แบบ 246-2) ใน SQLite ถาวร

เหมือน core/store.py กับราคาหุ้น: sync ครั้งแรกดึงย้อนหลังเต็ม (ช้า, ใช้ pagination)
ครั้งต่อไป sync แค่ "หน้าต่างทับซ้อน" ไม่กี่วันจาก SEC (เร็วมาก) แล้ว upsert เข้า DB
ข้อมูลสะสมไปเรื่อยๆ ไม่ถูกลบ ไม่จำกัดแค่ 180 วันเหมือนตอน query สดจาก SEC ทุกครั้ง

ทำไมต้อง "หน้าต่างทับซ้อน" ตอน sync (ไม่ใช่แค่ดึงตั้งแต่วันล่าสุดที่มี +1):
SEC กรองผลค้นหาตาม "วันที่ยื่นแบบรายงาน" ไม่ใช่ "วันที่ซื้อขายจริง" (trade_date)
ผู้บริหารมีสิทธิ์ยื่นช้ากว่าวันเทรดจริงได้หลายวัน ทำให้รายการที่ trade_date เก่า
โผล่มาทีหลังได้ — SYNC_OVERLAP_DAYS จึงต้องกว้างพอจะจับรายการยื่นช้าเหล่านี้ทัน
"""
import os
import sqlite3
from datetime import datetime, timedelta

DB_FILE = "sec_filings.db"
SYNC_OVERLAP_DAYS = 21     # ย้อนดึงซ้ำทุกรอบ กันรายการยื่นช้าตกหล่น
INITIAL_BACKFILL_DAYS = 180


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
            CREATE TABLE IF NOT EXISTS insider_trades(
              symbol TEXT, company TEXT, name TEXT, relation TEXT, sec_type TEXT,
              trade_date TEXT, qty INTEGER, price REAL, method TEXT, action TEXT,
              PRIMARY KEY(symbol, name, relation, sec_type, trade_date, qty, price, method)
            );
            CREATE TABLE IF NOT EXISTS major_changes(
              symbol TEXT, holder TEXT, method TEXT, sec_type TEXT,
              pct_before REAL, pct_change REAL, pct_after REAL,
              trade_date TEXT, action TEXT,
              PRIMARY KEY(symbol, holder, method, sec_type, trade_date, pct_before, pct_after)
            );
            CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(trade_date);
            CREATE INDEX IF NOT EXISTS idx_major_date   ON major_changes(trade_date);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        con.commit()
    finally:
        con.close()


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


def upsert_insider_trades(base_dir, records):
    if not records:
        return
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        con.executemany(
            """INSERT OR REPLACE INTO insider_trades
               (symbol, company, name, relation, sec_type, trade_date, qty, price, method, action)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [(r["symbol"], r["company"], r["name"], r["relation"], r["sec_type"],
              r["trade_date"], r["qty"], r["price"], r["method"], r["action"])
             for r in records if r.get("trade_date")]
        )
        con.commit()
    finally:
        con.close()


def upsert_major_changes(base_dir, records):
    if not records:
        return
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        con.executemany(
            """INSERT OR REPLACE INTO major_changes
               (symbol, holder, method, sec_type, pct_before, pct_change, pct_after, trade_date, action)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            [(r["symbol"], r["holder"], r["method"], r["sec_type"],
              r["pct_before"], r["pct_change"], r["pct_after"], r["trade_date"], r["action"])
             for r in records if r.get("trade_date")]
        )
        con.commit()
    finally:
        con.close()


def query_insider_trades(base_dir, days):
    if not db_exists(base_dir):
        return []
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    con = _connect(base_dir)
    try:
        rows = con.execute(
            """SELECT symbol, company, name, relation, sec_type, trade_date, qty, price, method, action
               FROM insider_trades WHERE trade_date >= ? ORDER BY trade_date DESC""",
            (cutoff,)).fetchall()
    finally:
        con.close()
    cols = ["symbol", "company", "name", "relation", "sec_type", "trade_date", "qty", "price", "method", "action"]
    return [dict(zip(cols, row)) for row in rows]


def query_major_changes(base_dir, days):
    if not db_exists(base_dir):
        return []
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    con = _connect(base_dir)
    try:
        rows = con.execute(
            """SELECT symbol, holder, method, sec_type, pct_before, pct_change, pct_after, trade_date, action
               FROM major_changes WHERE trade_date >= ? ORDER BY trade_date DESC""",
            (cutoff,)).fetchall()
    finally:
        con.close()
    cols = ["symbol", "holder", "method", "sec_type", "pct_before", "pct_change", "pct_after", "trade_date", "action"]
    return [dict(zip(cols, row)) for row in rows]


def _fetch_and_parse_insider(date_from, date_to):
    """คืน list[dict] ของแบบ 59 ในช่วง date_from..date_to (ใช้ sec_fetch_chunked)"""
    import re as _re
    from sources.sec import sec_fetch_chunked, _thai_date

    UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
    URL = "https://market.sec.or.th/public/idisc/th/r59"

    def _build_payload(d_from, d_to, vs, vsg, ev):
        return {
            "__VIEWSTATE": vs, "__VIEWSTATEGENERATOR": vsg, "__EVENTVALIDATION": ev,
            "ctl00$CPH$rblDateType": "T",
            "ctl00$CPH$BSDateFrom": _thai_date(d_from),
            "ctl00$CPH$BSDateTo":   _thai_date(d_to),
            "ctl00$CPH$btSearch":   "Search",
            "ctl00$CPH$ddlCompany": "",
        }
    df = sec_fetch_chunked(URL, UA, date_from, date_to, _build_payload)

    records = []
    for _, row in df.iterrows():
        vals = row.tolist()
        if len(vals) < 8:
            continue
        company = str(vals[0])
        sym = _extract_symbol(company)
        if not sym:
            continue
        method = str(vals[7]) if len(vals) > 7 else ""
        action = "buy" if "ซื้อ" in method else "sell" if "ขาย" in method else "other"
        try:    qty = int(str(vals[5]).replace(",", ""))
        except: qty = 0
        try:
            price = float(str(vals[6]).replace(",", ""))
            if price != price: price = None
        except: price = None

        raw_date = str(vals[4]) if len(vals) > 4 else ""
        trade_date = ""
        m = _re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_date)
        if m:
            dd, mm, yy = m.groups()
            trade_date = f"{int(yy) - 543}-{mm.zfill(2)}-{dd.zfill(2)}"

        records.append({
            "symbol": sym, "company": company, "name": str(vals[1]),
            "relation": str(vals[2]), "sec_type": str(vals[3]),
            "trade_date": trade_date, "qty": qty, "price": price,
            "method": method, "action": action,
        })
    return records


def _fetch_and_parse_major(date_from, date_to):
    """คืน list[dict] ของแบบ 246-2 ในช่วง date_from..date_to (ใช้ sec_fetch_chunked)"""
    import re as _re
    from sources.sec import sec_fetch_chunked, _thai_date

    UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
    URL = "https://market.sec.or.th/public/idisc/th/r246"

    def _build_payload(d_from, d_to, vs, vsg, ev):
        return {
            "__VIEWSTATE": vs, "__VIEWSTATEGENERATOR": vsg, "__EVENTVALIDATION": ev,
            "ctl00$CPH$BsCompany": "", "ctl00$CPH$BsCompany_t": "", "ctl00$CPH$BsCompany_v": "",
            "ctl00$CPH$txtSearchPerson": "",
            "ctl00$CPH$rblDateType": "T",
            "ctl00$CPH$BSDateFrom": _thai_date(d_from),
            "ctl00$CPH$BSDateTo":   _thai_date(d_to),
            "ctl00$CPH$btSearch":   "Search",
        }
    df = sec_fetch_chunked(URL, UA, date_from, date_to, _build_payload)

    records = []
    for _, row in df.iterrows():
        vals = row.tolist()
        if len(vals) < 5:
            continue
        sym = str(vals[0]).strip().upper()
        if not sym or sym == "NAN" or sym == "หลักทรัพย์":
            continue
        sym_clean = sym.split()[0].split("(")[0].strip()

        holder = str(vals[1])
        method = str(vals[2])
        action = "buy" if "ได้มา" in method else "sell" if "จำหน่าย" in method else "other"

        try:    pct_before = float(str(vals[4]).replace(",", ""))
        except: pct_before = None
        try:    pct_change = float(str(vals[5]).replace(",", ""))
        except: pct_change = None
        try:    pct_after  = float(str(vals[6]).replace(",", ""))
        except: pct_after  = None

        raw_date = str(vals[7]) if len(vals) > 7 else ""
        trade_date = ""
        m = _re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_date)
        if m:
            dd, mm, yy = m.groups()
            trade_date = f"{int(yy) - 543}-{mm.zfill(2)}-{dd.zfill(2)}"

        records.append({
            "symbol": sym_clean, "holder": holder, "method": method,
            "sec_type": str(vals[3]) if len(vals) > 3 else "",
            "pct_before": pct_before, "pct_change": pct_change, "pct_after": pct_after,
            "trade_date": trade_date, "action": action,
        })
    return records


def _extract_symbol(company_str):
    from sources.sec import _extract_symbol as _ex
    return _ex(company_str)


def sync_insider_trades(base_dir):
    """ดึงข้อมูลใหม่จาก SEC มา upsert เข้า DB สะสม คืนจำนวน record ที่ดึงมารอบนี้"""
    init_db(base_dir)
    last_synced = _get_meta(base_dir, "insider_last_synced_at")
    date_to = datetime.now()
    if last_synced:
        date_from = date_to - timedelta(days=SYNC_OVERLAP_DAYS)
    else:
        date_from = date_to - timedelta(days=INITIAL_BACKFILL_DAYS)
    records = _fetch_and_parse_insider(date_from, date_to)
    upsert_insider_trades(base_dir, records)
    _set_meta(base_dir, "insider_last_synced_at", date_to.strftime("%Y-%m-%d %H:%M:%S"))
    return len(records)


def sync_major_changes(base_dir):
    init_db(base_dir)
    last_synced = _get_meta(base_dir, "major_last_synced_at")
    date_to = datetime.now()
    if last_synced:
        date_from = date_to - timedelta(days=SYNC_OVERLAP_DAYS)
    else:
        date_from = date_to - timedelta(days=INITIAL_BACKFILL_DAYS)
    records = _fetch_and_parse_major(date_from, date_to)
    upsert_major_changes(base_dir, records)
    _set_meta(base_dir, "major_last_synced_at", date_to.strftime("%Y-%m-%d %H:%M:%S"))
    return len(records)
