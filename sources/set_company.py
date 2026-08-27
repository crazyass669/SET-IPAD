# -*- coding: utf-8 -*-
"""sources/set_company.py — ข้อมูล "บริษัท" จาก SET.or.th ที่ไม่ใช่งบการเงิน: CG/ESG
rating, ผู้ถือหุ้นใหญ่/free float, โควตาต่างชาติ (ดู PLAN_set_api_expansion.txt
งาน #0/#1/#2/#3) เก็บใน financials.db ตาราง set_company_master +
set_major_shareholders — ใช้ DB เดียวกับ factor_snapshot เพราะ Screener+ ต้อง
join กัน ไม่ใช่ DB แยก (financials.db เป็น local-only อยู่แล้ว)

pattern เดียวกับ sources/factor_snapshot.py: _connect() reuse
financials_store._db_path, init_table() เรียกจากฟังก์ชันเขียนเองแบบ idempotent
(CREATE TABLE IF NOT EXISTS + _ensure_columns) ไม่ต้องมี central bootstrap

overlay ตอน query เข้า /api/factor-screener (เหมือน pe_live/mkt_cap ที่ overlay
จาก set_data.json อยู่แล้ว) ไม่ยัดรวมกับ factor_snapshot.factors เพราะตารางนั้น
DELETE+INSERT ทับทั้งก้อนทุกครั้งที่ build_snapshot() — ข้อมูลชุดนี้จะหายทุกรอบ
ถ้าฝังรวม

set_company_master มี timestamp sync แยก 3 คอลัมน์ (updated_at=ESG,
shareholder_synced_at, profile_synced_at) เพราะ 3 ชุดข้อมูล sync คนละจังหวะ
กัน (ESG ปีละครั้ง, ผู้ถือหุ้นใหญ่ราย 7 วัน, โควตาต่างชาติรายวัน) ถ้าใช้คอลัมน์
เดียวรวมกัน sync ชุดหนึ่งจะไปทับ timestamp ของอีกชุดที่ยังไม่ได้ sync ซ้ำ ทำให้
Data Health เข้าใจผิดว่าข้อมูลสดกว่าที่เป็นจริง
"""
import sqlite3
from datetime import datetime, timezone

from sources import financials_store as fs

TABLE = "set_company_master"
SHAREHOLDER_TABLE = "set_major_shareholders"


def _connect(base_dir):
    con = sqlite3.connect(fs._db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def _ensure_columns(con, table, col_defs):
    """เพิ่มคอลัมน์ใหม่เข้าตารางที่มีอยู่แล้วแบบ idempotent (SQLite ไม่มี
    'ADD COLUMN IF NOT EXISTS') — เช็คจาก PRAGMA table_info ก่อน ข้ามคอลัมน์ที่มี
    อยู่แล้ว ใช้ตอนเพิ่ม field ชุดใหม่ (เช่น ผู้ถือหุ้นใหญ่/โควตาต่างชาติ) เข้า
    set_company_master ที่ผู้ใช้อาจมีตารางเดิมจาก sync_esg รอบก่อนอยู่แล้ว
    col_defs: dict {column_name: "SQL type"} เช่น {"free_float": "REAL"}"""
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, sql_type in col_defs.items():
        if col not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {sql_type}")


def init_table(base_dir):
    con = _connect(base_dir)
    try:
        con.executescript(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}(
              symbol TEXT PRIMARY KEY,
              cg_score INTEGER, esg_rating TEXT, esg_rank INTEGER, esg_as_of TEXT,
              setesg_index INTEGER, djsi_index INTEGER,
              esgbook_score REAL, refinitiv_score REAL, morningstar_risk REAL,
              updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS {SHAREHOLDER_TABLE}(
              symbol TEXT, seq INTEGER, holder_name TEXT, shares INTEGER,
              pct REAL, is_nvdr INTEGER, book_close_date TEXT,
              PRIMARY KEY(symbol, seq, book_close_date)
            );
        """)
        _ensure_columns(con, TABLE, {
            "free_float": "REAL", "total_shareholder": "INTEGER",
            "book_close_date": "TEXT", "nvdr_pct_holding": "REAL",
            "shareholder_synced_at": "TEXT",
            "foreign_room": "REAL", "foreign_limit": "REAL",
            "foreign_available": "INTEGER", "foreign_as_of": "TEXT",
            "listed_share": "INTEGER", "par": "REAL", "first_trade_date": "TEXT",
            "profile_synced_at": "TEXT",
        })
        con.commit()
    finally:
        con.close()


def _th_symbols(base_dir):
    """รายชื่อหุ้นสามัญไทยทั้งหมด (SET+mai) จากไฟล์ listedCompanies ในเครื่อง/
    ดาวน์โหลดใหม่ถ้าเก่าเกิน (set_data_fetcher.load_set_symbols — แหล่งเดียวกับที่
    /api/set-universe-check-updates ใช้เทียบ universe) คืน list ของ "PTT"
    (bare symbol ไม่มี .BK)"""
    from set_data_fetcher import load_set_symbols
    return [s["symbol"] for s in load_set_symbols(base_dir)]


# ============================================================
# CG/ESG rating — งาน #3 (bulk 1 call ได้ทั้งตลาด)
# ============================================================

# AAA ดีสุด -> BBB (ต่ำสุดที่ SET แจก) — ใช้เทียบ "ขั้นต่ำ" ใน Screener+ (filter
# esg_rank แบบ gte) เพราะ SETESG rating เป็นสเกลอันดับ ไม่ใช่ตัวเลข เทียบตรงๆ ไม่ได้
_ESG_RANK = {"AAA": 4, "AA": 3, "A": 2, "BBB": 1}


def fetch_esg_list(ctx=None, hdr=None):
    """ดึง CG/ESG rating ทั้งตลาดใน call เดียวจาก /api/set/esg/list — คืน raw list
    ของ dict ตรงจาก SET (~358 บริษัท, coverage ~38% ของหุ้นสามัญทั้งหมด 931 ตัว —
    เช็คแล้ว 2026-08-23 บริษัทที่ไม่ได้เข้าร่วมประเมินจะไม่มีข้อมูล ไม่ใช่ว่าคะแนนแย่)
    raise ถ้า SET ไม่คืน list หรือ list ว่าง (รูปแบบ endpoint เปลี่ยน/ถูกบล็อค)"""
    from sources.set_api import _bootstrap_headers, _get_json
    if ctx is None or hdr is None:
        ctx, hdr = _bootstrap_headers()
    d = _get_json(ctx, hdr, "/api/set/esg/list?lang=th", timeout=20)
    if not isinstance(d, list) or not d:
        raise ValueError("SET API esg/list ไม่คืนข้อมูล (รูปแบบอาจเปลี่ยน)")
    return d


def sync_esg(base_dir, callback=None):
    """ดึง CG/ESG ทั้งตลาด (1 call) แล้ว upsert เข้า set_company_master — เขียนทับ
    เฉพาะคอลัมน์ ESG (ไม่แตะคอลัมน์ผู้ถือหุ้นใหญ่/โควตาต่างชาติที่ sync ชุดอื่นเติม)
    ไม่ raise ถ้า symbol ไหนในตารางเดิมไม่อยู่ใน esg/list รอบนี้ (แค่ไม่อัพเดท ไม่ลบทิ้ง
    — บริษัทอาจหลุด/เข้าร่วมประเมินใหม่ปีถัดไป) คืนจำนวนบริษัทที่ sync สำเร็จ"""
    init_table(base_dir)
    if callback:
        callback(0, 1, "ดึง CG/ESG rating ทั้งตลาด (SET API)...")
    rows = fetch_esg_list()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = _connect(base_dir)
    try:
        for r in rows:
            sym = r.get("symbol")
            if not sym:
                continue
            rating = r.get("setesgRating") or {}
            tp = {t.get("name"): t for t in (r.get("thirdPartyScores") or []) if t.get("name")}
            rating_code = rating.get("rating") or None
            con.execute(f"""
                INSERT INTO {TABLE}(symbol, cg_score, esg_rating, esg_rank, esg_as_of,
                    setesg_index, djsi_index, esgbook_score, refinitiv_score,
                    morningstar_risk, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    cg_score=excluded.cg_score, esg_rating=excluded.esg_rating,
                    esg_rank=excluded.esg_rank, esg_as_of=excluded.esg_as_of,
                    setesg_index=excluded.setesg_index, djsi_index=excluded.djsi_index,
                    esgbook_score=excluded.esgbook_score,
                    refinitiv_score=excluded.refinitiv_score,
                    morningstar_risk=excluded.morningstar_risk, updated_at=excluded.updated_at
            """, (
                sym, r.get("cgScore"), rating_code, _ESG_RANK.get(rating_code),
                (rating.get("asOf") or "")[:10],
                int(bool(r.get("setesgIndex"))), int(bool(r.get("djsiIndex"))),
                (tp.get("ESGBOOK") or {}).get("totalScore"),
                (tp.get("REFINITIV") or {}).get("totalScore"),
                (tp.get("MORNINGSTAR") or {}).get("riskScore"),
                now,
            ))
        con.commit()
    finally:
        con.close()
    if callback:
        callback(1, 1, f"CG/ESG rating: {len(rows)} บริษัท")
    return len(rows)


# ============================================================
# ผู้ถือหุ้นใหญ่ + free float — งาน #1 (พารัลเลลทั้งกระดาน ~8 วิ)
# ============================================================

def sync_shareholders(base_dir, callback=None):
    """ดึงผู้ถือหุ้นใหญ่+free float ทั้งกระดานจาก SET API (พารัลเลล ~8 วิ/931 ตัว —
    ดู sources/set_api.py::fetch_shareholders_batch) แล้ว upsert เข้า
    set_company_master (free_float/total_shareholder/book_close_date/
    nvdr_pct_holding) + set_major_shareholders (10 อันดับต่อหุ้น เก็บ
    book_close_date ใน PK ด้วย — sync งวดใหม่ไม่ทับของเก่า ได้ประวัติสะสมข้ามงวดฟรี)
    tickers มาจาก _th_symbols (หุ้นสามัญไทยทั้งหมดที่ SET list ไว้)
    คืน (จำนวนหุ้นที่ sync สำเร็จ, จำนวนหุ้นทั้งหมดที่พยายาม) raise ถ้าสำเร็จ <50%"""
    from sources.set_api import fetch_shareholders_batch
    init_table(base_dir)
    tickers = _th_symbols(base_dir)
    data = fetch_shareholders_batch(tickers, callback=callback)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = _connect(base_dir)
    try:
        for sym, d in data.items():
            con.execute(f"""
                INSERT INTO {TABLE}(symbol, free_float, total_shareholder,
                    book_close_date, nvdr_pct_holding, shareholder_synced_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    free_float=excluded.free_float,
                    total_shareholder=excluded.total_shareholder,
                    book_close_date=excluded.book_close_date,
                    nvdr_pct_holding=excluded.nvdr_pct_holding,
                    shareholder_synced_at=excluded.shareholder_synced_at
            """, (sym, d["free_float"], d["total_shareholder"], d["book_close_date"],
                  d["nvdr_pct_holding"], now))
            bcd = d["book_close_date"] or ""
            for m in d["major_shareholders"]:
                con.execute(f"""
                    INSERT INTO {SHAREHOLDER_TABLE}(symbol, seq, holder_name, shares,
                        pct, is_nvdr, book_close_date)
                    VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT(symbol, seq, book_close_date) DO UPDATE SET
                        holder_name=excluded.holder_name, shares=excluded.shares,
                        pct=excluded.pct, is_nvdr=excluded.is_nvdr
                """, (sym, m["seq"], m["name"], m["shares"], m["pct"],
                      int(bool(m["is_nvdr"])), bcd))
        con.commit()
    finally:
        con.close()
    return len(data), len(tickers)


# ============================================================
# โควตาต่างชาติ — งาน #2 (พารัลเลลทั้งกระดาน ~7 วิ)
# ============================================================

def sync_profiles(base_dir, callback=None):
    """ดึงโควตาต่างชาติทั้งกระดานจาก SET API (พารัลเลล ~7 วิ/931 ตัว — ดู
    sources/set_api.py::fetch_profiles_batch) แล้ว upsert เข้า set_company_master
    (foreign_room/foreign_limit/foreign_available/foreign_as_of/listed_share/par/
    first_trade_date) คืน (จำนวนสำเร็จ, จำนวนทั้งหมดที่พยายาม) raise ถ้าสำเร็จ <50%"""
    from sources.set_api import fetch_profiles_batch
    init_table(base_dir)
    tickers = _th_symbols(base_dir)
    data = fetch_profiles_batch(tickers, callback=callback)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = _connect(base_dir)
    try:
        for sym, d in data.items():
            con.execute(f"""
                INSERT INTO {TABLE}(symbol, foreign_room, foreign_limit,
                    foreign_available, foreign_as_of, listed_share, par,
                    first_trade_date, profile_synced_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    foreign_room=excluded.foreign_room,
                    foreign_limit=excluded.foreign_limit,
                    foreign_available=excluded.foreign_available,
                    foreign_as_of=excluded.foreign_as_of,
                    listed_share=excluded.listed_share, par=excluded.par,
                    first_trade_date=excluded.first_trade_date,
                    profile_synced_at=excluded.profile_synced_at
            """, (sym, d["foreign_room"], d["foreign_limit"], d["foreign_available"],
                  d["foreign_as_of"], d["listed_share"], d["par"],
                  d["first_trade_date"], now))
        con.commit()
    finally:
        con.close()
    return len(data), len(tickers)


# ============================================================
# อ่านกลับ — overlay เข้า Screener+ / Tearsheet / Insider / Valuation
# ============================================================

def _room_used_pct(room, limit):
    """% ที่ต่างชาติถือไปแล้วเทียบเพดาน (100 - room/limit*100) — None ถ้าไม่มี
    เพดานกำหนด (limit=0/None) หรือไม่มีข้อมูล room"""
    if room is None or not limit:
        return None
    return round(100 - (room / limit * 100), 2)


def get_company_map(base_dir):
    """คืน {symbol: {...ทุก field ของ set_company_master รวม foreign_room_used_pct
    ที่คำนวณให้แล้ว}} ทั้งตาราง — ใช้ overlay เข้า /api/factor-screener คืน {} เฉยๆ
    ถ้ายังไม่เคย sync เลย ไม่ raise เพราะเป็นข้อมูลเสริม ไม่ควรทำให้ Screener+ พังทั้งหน้า"""
    if not fs.db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        cur = con.execute(f"""SELECT symbol, cg_score, esg_rating, esg_rank, esg_as_of,
            setesg_index, djsi_index, esgbook_score, refinitiv_score, morningstar_risk,
            free_float, total_shareholder, book_close_date, nvdr_pct_holding,
            foreign_room, foreign_limit, foreign_available, foreign_as_of
            FROM {TABLE}""")
        out = {}
        for row in cur.fetchall():
            (sym, cg, esg_r, esg_rk, esg_as, seti, djsi, esgb, refi, morn,
             ff, tot_sh, bcd, nvdr_pct, f_room, f_limit, f_avail, f_asof) = row
            out[sym] = {
                "cg_score": cg, "esg_rating": esg_r, "esg_rank": esg_rk, "esg_as_of": esg_as,
                "setesg_index": bool(seti), "djsi_index": bool(djsi),
                "esgbook_score": esgb, "refinitiv_score": refi, "morningstar_risk": morn,
                "free_float": ff, "total_shareholder": tot_sh, "book_close_date": bcd,
                "nvdr_pct_holding": nvdr_pct,
                "foreign_room": f_room, "foreign_limit": f_limit,
                "foreign_available": f_avail, "foreign_as_of": f_asof,
                "foreign_room_used_pct": _room_used_pct(f_room, f_limit),
            }
        return out
    except sqlite3.OperationalError:
        return {}   # ตารางยังไม่ถูกสร้าง (ยังไม่เคย sync)
    finally:
        con.close()


def get_ownership(base_dir, symbol):
    """ข้อมูลผู้ถือหุ้น+โควตาต่างชาติของหุ้น 1 ตัวจาก DB (อ่านเร็ว ไม่ยิง SET สด) —
    ใช้กับ Tearsheet/Insider คืน None ถ้ายังไม่เคย sync หรือไม่พบ symbol นี้เลย"""
    if not fs.db_exists(base_dir):
        return None
    con = _connect(base_dir)
    try:
        row = con.execute(f"""SELECT free_float, total_shareholder, book_close_date,
            nvdr_pct_holding, foreign_room, foreign_limit, foreign_available,
            foreign_as_of FROM {TABLE} WHERE symbol=?""", (symbol,)).fetchone()
        if not row:
            return None
        free_float, total_sh, bcd, nvdr_pct, f_room, f_limit, f_avail, f_asof = row
        majors = []
        if bcd:
            cur = con.execute(f"""SELECT seq, holder_name, shares, pct, is_nvdr
                FROM {SHAREHOLDER_TABLE} WHERE symbol=? AND book_close_date=?
                ORDER BY seq""", (symbol, bcd))
            majors = [{"seq": r[0], "name": r[1], "shares": r[2], "pct": r[3],
                       "is_nvdr": bool(r[4])} for r in cur.fetchall()]
        return {
            "free_float": free_float, "total_shareholder": total_sh,
            "book_close_date": bcd, "nvdr_pct_holding": nvdr_pct,
            "foreign_room": f_room, "foreign_limit": f_limit,
            "foreign_available": f_avail, "foreign_as_of": f_asof,
            "foreign_room_used_pct": _room_used_pct(f_room, f_limit),
            "major_shareholders": majors,
        }
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def get_esg(base_dir, symbol):
    """CG/ESG rating ของหุ้น 1 ตัวจาก DB (อ่านเร็ว ไม่ยิง SET สด) — ใช้กับ badge หัว Tearsheet
    (ดู PLAN_set_api_expansion.txt งาน #3B) คืน None ถ้ายังไม่เคย sync ESG หรือหุ้นตัวนี้ไม่มี
    เรตติ้ง (SET ครอบแค่ ~38% ของกระดาน — ไม่มีค่า ≠ คะแนนแย่ ผู้เรียกต้องไม่โชว์ badge เลย)"""
    if not fs.db_exists(base_dir):
        return None
    con = _connect(base_dir)
    try:
        row = con.execute(f"""SELECT cg_score, esg_rating, esg_rank, esg_as_of,
            setesg_index, djsi_index, esgbook_score, refinitiv_score, morningstar_risk
            FROM {TABLE} WHERE symbol=?""", (symbol,)).fetchone()
        if not row:
            return None
        cg, esg_r, esg_rk, esg_as, seti, djsi, esgb, refi, morn = row
        # ไม่มี rating เลยและไม่อยู่ดัชนีไหน = หุ้นไม่ได้เข้าร่วมประเมิน — คืน None ให้ frontend
        # ซ่อน badge (ห้ามโชว์ "—" จะดูเหมือนคะแนนแย่ — ดู PLAN งาน #3B)
        if esg_r is None and cg is None and not seti and not djsi:
            return None
        return {
            "cg_score": cg, "esg_rating": esg_r, "esg_rank": esg_rk, "esg_as_of": esg_as,
            "setesg_index": bool(seti), "djsi_index": bool(djsi),
            "esgbook_score": esgb, "refinitiv_score": refi, "morningstar_risk": morn,
        }
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def get_foreign_room_ranking(base_dir):
    """คืน list ของหุ้นที่มี foreign_room เรียงตาม "% ที่ใช้ไปแล้ว" มากสุดก่อน — ใช้กับ
    ปุ่ม "เช็คห้องต่างชาติใกล้เต็ม" ในหน้า Valuation (ดู PLAN_set_api_expansion.txt
    งาน #2B) คืน [] ถ้ายังไม่เคย sync"""
    m = get_company_map(base_dir)
    rows = [{"symbol": sym, "foreign_room": v["foreign_room"],
             "foreign_limit": v["foreign_limit"], "foreign_available": v["foreign_available"],
             "foreign_as_of": v["foreign_as_of"], "used_pct": v["foreign_room_used_pct"]}
            for sym, v in m.items() if v["foreign_room"] is not None]
    rows.sort(key=lambda r: r["used_pct"] if r["used_pct"] is not None else -1, reverse=True)
    return rows


def get_meta(base_dir):
    """คืน dict สรุปความครอบคลุม+ความสดของข้อมูลบริษัททั้ง 3 ชุด (ESG/ผู้ถือหุ้นใหญ่/
    โควตาต่างชาติ) ใช้โชว์ในหน้า Screener+/Data Health — คืนค่า 0/None ทุกช่องถ้ายังไม่
    เคย sync เลย ไม่ raise"""
    empty = {"esg_count": 0, "esg_updated_at": None,
              "shareholder_count": 0, "shareholder_updated_at": None,
              "foreign_count": 0, "foreign_updated_at": None}
    if not fs.db_exists(base_dir):
        return empty
    con = _connect(base_dir)
    try:
        def _one(where_col, ts_col):
            row = con.execute(f"SELECT COUNT(*), MAX({ts_col}) FROM {TABLE} "
                               f"WHERE {where_col} IS NOT NULL").fetchone()
            return (row[0] or 0), row[1]
        esg_n, esg_ts = _one("esg_rating", "updated_at")
        sh_n, sh_ts = _one("free_float", "shareholder_synced_at")
        fr_n, fr_ts = _one("foreign_room", "profile_synced_at")
        return {"esg_count": esg_n, "esg_updated_at": esg_ts,
                "shareholder_count": sh_n, "shareholder_updated_at": sh_ts,
                "foreign_count": fr_n, "foreign_updated_at": fr_ts}
    except sqlite3.OperationalError:
        return empty
    finally:
        con.close()
