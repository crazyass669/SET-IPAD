# -*- coding: utf-8 -*-
"""core/store.py — persistence layer: atomic write, history I/O, write guards

Historical prices เก็บใน SQLite (set_prices.db) เป็นหลัก:
  - get_series(ticker)   point query ~6ms (แทนการ parse JSON 98MB)
  - upsert_bars(map)     เขียนเฉพาะแท่งใหม่ใน transaction เดียว
  - iter_all_series()    generator สำหรับ recompute pipeline
ทุก connection เปิด per-call ใน thread ของผู้เรียก (WAL: 1 writer + N readers)

ช่วง dual-write: ยังเขียน set_history.json ควบคู่ (DUAL_WRITE_JSON=True)
เพื่อถอยกลับได้ — เมื่อใช้งานนิ่งแล้วตั้งเป็น False เพื่อเลิกจ่าย cost 98MB
"""
import json
import os
import sqlite3
import threading
from datetime import datetime

OUT_FILE     = "set_data.json"
HISTORY_FILE = "set_history.json"
DB_FILE      = "set_prices.db"

# เขียน set_history.json ควบคู่กับ SQLite (ปิดแล้ว 2026-07-16 — DB นิ่ง,
# มีสำเนาสำรองใน backups/ · ถอยกลับ: ตั้ง True แล้ว Full Refresh หนึ่งรอบ)
DUAL_WRITE_JSON = False


# ============================================================
# 1a. SQLite price store
# ============================================================

def _db_path(base_dir):
    return os.path.join(base_dir, DB_FILE)


def db_exists(base_dir):
    return os.path.exists(_db_path(base_dir))


def _connect(base_dir):
    """เปิด connection ใหม่ (per-call, ใน thread ผู้เรียก) — ห้าม share ข้าม thread"""
    con = sqlite3.connect(_db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_db(base_dir):
    con = _connect(base_dir)
    try:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS prices(
              ticker TEXT NOT NULL, date TEXT NOT NULL,
              close REAL NOT NULL, volume INTEGER NOT NULL,
              PRIMARY KEY(ticker, date)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        # migration: เพิ่ม OHLC + adj_close (nullable) ให้ตารางเดิม — แถวเก่าจะเป็น
        # NULL จนกว่าจะรัน Full Refresh เติมให้ครบ (close/volume ยังใช้งานได้ปกติ)
        cols = {r[1] for r in con.execute("PRAGMA table_info(prices)")}
        for c in ("open", "high", "low", "adj_close"):
            if c not in cols:
                con.execute(f"ALTER TABLE prices ADD COLUMN {c} REAL")
        con.commit()
    finally:
        con.close()


def get_meta(base_dir, key, default=None):
    if not db_exists(base_dir):
        return default
    con = _connect(base_dir)
    try:
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default
    finally:
        con.close()


def get_series(base_dir, ticker):
    """คืน {'dates','closes','volumes'} ของหุ้นตัวเดียว (point query)
    fallback อ่านจาก set_history.json ถ้ายังไม่ได้ migrate"""
    if db_exists(base_dir):
        con = _connect(base_dir)
        try:
            rows = con.execute(
                "SELECT date, close, volume FROM prices WHERE ticker=? ORDER BY date",
                (ticker,)).fetchall()
        finally:
            con.close()
        if not rows:
            return None
        d, c, v = zip(*rows)
        return {"dates": list(d), "closes": list(c), "volumes": list(v)}
    hist = load_history(base_dir)
    return (hist or {}).get("stocks", {}).get(ticker)


def get_ohlc_series(base_dir, ticker):
    """คืน {'dates','opens','highs','lows','closes','adj_closes','volumes'} ของหุ้นตัวเดียว
    สำหรับ analytics ที่ต้องใช้ OHLC/Adj Close (seasonality/drawdown/candlestick/ATR)
    — คอลัมน์ open/high/low/adj_close อาจเป็น None สำหรับแท่งเก่าที่ยังไม่ได้ Full Refresh
    คืน None ถ้าไม่มี DB หรือไม่พบหุ้น"""
    if not db_exists(base_dir):
        return None
    con = _connect(base_dir)
    try:
        rows = con.execute(
            "SELECT date, open, high, low, close, adj_close, volume "
            "FROM prices WHERE ticker=? ORDER BY date", (ticker,)).fetchall()
    finally:
        con.close()
    if not rows:
        return None
    d, o, h, l, c, a, v = zip(*rows)
    return {"dates": list(d), "opens": list(o), "highs": list(h), "lows": list(l),
            "closes": list(c), "adj_closes": list(a), "volumes": list(v)}


def get_last_dates(base_dir):
    """คืน {ticker: วันที่แท่งสุดท้าย} — ใช้หา gap ตอน Quick Update"""
    if db_exists(base_dir):
        con = _connect(base_dir)
        try:
            rows = con.execute(
                "SELECT ticker, MAX(date) FROM prices GROUP BY ticker").fetchall()
        finally:
            con.close()
        return dict(rows)
    hist = load_history(base_dir) or {"stocks": {}}
    return {t: d["dates"][-1] for t, d in hist["stocks"].items() if d.get("dates")}


def iter_all_series(base_dir):
    """generator: yield (ticker, {'dates','closes','volumes','highs','lows','adj_closes'}) เรียงตาม ticker
    highs/lows/adj_closes เพิ่มมาแบบ additive (ไว้คำนวณ ATR จริง + seasonality) — consumer เก่าที่อ่านแค่
    dates/closes/volumes ไม่กระทบ · ผู้เรียกต้อง consume จนจบ (connection ปิดเมื่อ generator จบ)
    ใช้ query เดียวจบทั้งกระดาน แทนที่จะเปิด connection ใหม่ต่อหุ้น (ดู annotate_seasonality
    ที่เคยเรียก get_ohlc_series ทีละตัว 900+ ครั้ง — ซ้ำงาน scan นี้ทั้งหมด)"""
    if db_exists(base_dir):
        con = _connect(base_dir)
        try:
            cur = con.execute(
                "SELECT ticker, date, close, volume, high, low, adj_close FROM prices ORDER BY ticker, date")
            cur_t, d, c, v, h, lo, aj = None, [], [], [], [], [], []
            for t, dt, cl, vol, hi, low, adj in cur:
                if t != cur_t:
                    if cur_t is not None:
                        yield cur_t, {"dates": d, "closes": c, "volumes": v, "highs": h, "lows": lo, "adj_closes": aj}
                    cur_t, d, c, v, h, lo, aj = t, [], [], [], [], [], []
                d.append(dt); c.append(cl); v.append(vol); h.append(hi); lo.append(low); aj.append(adj)
            if cur_t is not None:
                yield cur_t, {"dates": d, "closes": c, "volumes": v, "highs": h, "lows": lo, "adj_closes": aj}
        finally:
            con.close()
        return
    hist = load_history(base_dir) or {"stocks": {}}
    for t, data in hist["stocks"].items():
        yield t, data


def iter_recent_series(base_dir, warmup_rows):
    """generator: yield (ticker, {'dates','closes','volumes','highs','lows'}) เหมือน
    iter_all_series แต่ดึงเฉพาะ warmup_rows แถวล่าสุดต่อหุ้น — ใช้กับ breadth/
    market-internals ที่ต้องการแค่ช่วงล่าสุด ไม่ใช่ full history

    query ทีละ ticker ด้วย ORDER BY date DESC LIMIT (ใช้ clustered index บน
    (ticker,date) ตรงๆ) เร็วกว่า SELECT ทั้งตาราง(prices โต 4.4M+ แถว) แล้วมา slice
    ฝั่ง Python มาก — วัดจริง ~2 วิ เทียบกับ ~35 วิ ของ full-scan"""
    if not db_exists(base_dir):
        hist = load_history(base_dir) or {"stocks": {}}
        for t, data in hist["stocks"].items():
            yield t, data
        return
    con = _connect(base_dir)
    try:
        tickers = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM prices ORDER BY ticker")]
        for t in tickers:
            rows = con.execute(
                "SELECT date, close, volume, high, low FROM prices "
                "WHERE ticker=? ORDER BY date DESC LIMIT ?", (t, warmup_rows)).fetchall()
            if not rows:
                continue
            rows.reverse()
            d, c, v, h, lo = zip(*rows)
            yield t, {"dates": list(d), "closes": list(c), "volumes": list(v),
                      "highs": list(h), "lows": list(lo)}
    finally:
        con.close()


def get_closes_on_date(base_dir, date):
    """คืน {ticker: close} ของทุกหุ้นในวันที่ระบุ — ใช้เช็ค sanity ราคา 'prior' จาก
    SET API fast path (Quick Update) เทียบกับราคาปิดที่เก็บไว้จริงของวันเดียวกัน
    (ดู services/refresh.py::run_quick_update)"""
    if not db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        rows = con.execute("SELECT ticker, close FROM prices WHERE date=?", (date,)).fetchall()
    finally:
        con.close()
    return dict(rows)


def _r4(x):
    """float ปัด 4 ตำแหน่ง หรือ None ถ้าเป็น NaN/None (สำหรับคอลัมน์ OHLC/adj ที่ nullable)"""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if f != f else round(f, 4)   # f != f => NaN


_UPSERT_CHUNK_ROWS = 20000  # commit ทุกๆ N แถว แทน transaction เดียวทั้งก้อน (ดูเหตุผลด้านล่าง)


def _safe_vol(x):
    """int ปัดเศษ หรือ 0 ถ้าเป็น NaN/None/แปลงไม่ได้ (ต่างจาก _r4 ตรงคอลัมน์ volume
    เป็น NOT NULL — ใช้ float(x) เช็คก่อนเสมอ กัน None==None ที่หลุดผ่านเช็ค NaN แบบ
    x==x เดิม (Python None==None เป็น True) แล้วไปพัง int(None) กลาง batch)"""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return 0
    return 0 if f != f else int(f)


def upsert_bars(base_dir, all_data_map, chunk_rows=_UPSERT_CHUNK_ROWS, replace_tickers=None):
    """เขียนแท่งราคาลง SQLite (INSERT OR REPLACE) แบ่ง commit เป็นช่วงละ chunk_rows แถว
    แทน transaction เดียวทั้งก้อน — Full Refresh ~900 ตัว x 10 ปี ได้หลักล้านแถว ถ้าถือ
    write lock ยาวเดียวจบ writer อื่น (busy_timeout=5000ms) จะเจอ "database is locked"
    ทันที และถ้า process ตาย/ไฟดับกลางทาง (หรือกด Restart) จะ rollback ทิ้งทั้งก้อนเสีย
    เวลาทำใหม่ 30-60 นาที — แบ่ง commit เป็นช่วงทำให้ lock hold time สั้นลงมาก และ resume
    ได้จากจุดที่ commit ไปแล้ว (ticker+date เป็น PRIMARY KEY — แท่งที่ commit แล้วจะไม่ถูก
    มองเป็น gap อีกในรอบ Quick Update ถัดไป แม้รอบนี้โดนขัดจังหวะกลางคัน)

    all_data_map: {ticker -> {'close','volume'[,'open','high','low','adj_close']: pd.Series}}
    ทุก series ควร align index เดียวกับ close (ดู _extract_ohlcav ใน sources/yahoo.py)
    — open/high/low/adj_close เป็น optional; ถ้าไม่มีจะเขียน NULL (แถวเก่าก่อนเพิ่ม OHLC)

    replace_tickers: ticker ที่ต้อง "แทนที่ทั้ง series" (ลบของเก่าทิ้งก่อน insert ใหม่)
    ลบในทรานแซกชันเดียวกับ chunk insert แรกของก้อนนี้ (commit พร้อมกัน) แทนที่จะ commit
    การลบแยกต่างหากก่อนแล้วค่อยเรียก upsert_bars ทีหลัง — กันกรณี insert ล้มเหลวกลางทาง
    (ข้อมูลเสีย/exception) แล้วราคาของ ticker นั้นหายถาวรเพราะลบไปแล้วแต่ไม่มีของใหม่มาแทน"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        if replace_tickers:
            con.executemany("DELETE FROM prices WHERE ticker=?",
                             [(t,) for t in replace_tickers])

        def rows():
            for ticker, data in all_data_map.items():
                close = data["close"]; vol = data["volume"]
                op = data.get("open"); hi = data.get("high")
                lo = data.get("low"); adj = data.get("adj_close")
                idx = close.index
                for i in range(len(idx)):
                    c = _r4(close.iloc[i])
                    if c is None:
                        continue  # close ไม่ valid (None/NaN) — ข้ามแท่งนี้ ห้ามเขียนทับคอลัมน์ NOT NULL
                    ds = idx[i].strftime("%Y-%m-%d")
                    yield (ticker, ds,
                           _r4(op.iloc[i]) if op is not None else None,
                           _r4(hi.iloc[i]) if hi is not None else None,
                           _r4(lo.iloc[i]) if lo is not None else None,
                           c,
                           _r4(adj.iloc[i]) if adj is not None else None,
                           _safe_vol(vol.iloc[i]) if vol is not None else 0)

        sql = ("INSERT OR REPLACE INTO prices"
               "(ticker,date,open,high,low,close,adj_close,volume) VALUES (?,?,?,?,?,?,?,?)")
        buf = []
        for row in rows():
            buf.append(row)
            if len(buf) >= chunk_rows:
                con.executemany(sql, buf)
                con.commit()
                buf.clear()
        if buf:
            con.executemany(sql, buf)
            con.commit()
        con.execute("INSERT OR REPLACE INTO meta VALUES ('updated_at', ?)",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def upsert_history_dict(base_dir, history):
    """เขียน history dict ธรรมดา ({stocks:{ticker:{dates,closes,volumes}}}) ลง DB
    ใช้โดย migrate script และ import_eod_archive.py"""
    init_db(base_dir)
    con = _connect(base_dir)
    try:
        def rows():
            for ticker, v in history.get("stocks", {}).items():
                for dt, cl, vol in zip(v["dates"], v["closes"], v["volumes"]):
                    yield (ticker, dt, float(cl), int(vol))
        con.executemany(
            "INSERT OR REPLACE INTO prices(ticker,date,close,volume) VALUES (?,?,?,?)",
            rows())
        con.execute("INSERT OR REPLACE INTO meta VALUES ('updated_at', ?)",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
        con.commit()
    finally:
        con.close()


def get_closes_map(base_dir, ticker, dates):
    """คืน {date: close} ของ ticker เฉพาะวันที่ระบุ — ใช้เทียบแท่ง overlap
    ตอนตรวจ corporate action (split detector)"""
    if not dates:
        return {}
    if db_exists(base_dir):
        con = _connect(base_dir)
        try:
            q = ",".join("?" * len(dates))
            rows = con.execute(
                f"SELECT date, close FROM prices WHERE ticker=? AND date IN ({q})",
                (ticker, *dates)).fetchall()
        finally:
            con.close()
        return dict(rows)
    s = get_series(base_dir, ticker)
    if not s:
        return {}
    m = dict(zip(s["dates"], s["closes"]))
    return {d: m[d] for d in dates if d in m}


def delete_ticker_bars(base_dir, ticker):
    """ลบราคาทั้งหมดของหุ้นตัวเดียว — ใช้ก่อน replace ด้วยข้อมูล refetch เต็ม
    (กรณี Yahoo ปรับราคาย้อนหลังหลัง corporate action)"""
    if not db_exists(base_dir):
        return
    con = _connect(base_dir)
    try:
        con.execute("DELETE FROM prices WHERE ticker=?", (ticker,))
        con.commit()
    finally:
        con.close()


# ============================================================
# 1b. History helpers — load / merge / save set_history.json
# ============================================================

def _atomic_write_json(path, obj):
    """เขียน JSON แบบ atomic: เขียนลง .tmp ก่อนแล้ว os.replace ทับ
    ป้องกันไฟล์เสียหายถ้า process ตาย/ไฟดับกลางคัน

    ชื่อไฟล์ .tmp ผูกกับ pid+thread id (ไม่ใช่ path+".tmp" คงที่) — กัน 2 thread เขียนไฟล์
    เดียวกันพร้อมกัน (เช่น sync 2 อุปกรณ์ยิง POST /api/watchlist ชนกัน บน server ที่รัน
    threaded=True) แล้วได้ bytes สลับกันมั่วก่อน os.replace ซึ่งจะได้ JSON เสีย/อ่านไม่ขึ้น
    แทนที่จะเป็นแค่ last-writer-wins ตามที่ตั้งใจ"""
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _check_stock_count(base_dir, new_count, min_ratio=0.8):
    """กันการเขียน set_data.json ทับด้วย universe ที่หดผิดปกติ
    (เช่น Yahoo ล่มทำให้ดึงได้ไม่กี่ตัว) — raise เพื่อให้ caller restore backup"""
    old_total = 0
    try:
        with open(os.path.join(base_dir, OUT_FILE), encoding="utf-8") as f:
            old_total = json.load(f).get("total", 0) or 0
    except Exception:
        return  # ไม่มีไฟล์เดิม/อ่านไม่ได้ — เขียนได้เลย
    if old_total > 0 and new_count < old_total * min_ratio:
        raise ValueError(
            f"ดึงข้อมูลได้แค่ {new_count}/{old_total} หุ้น (<{int(min_ratio*100)}%) — "
            f"อาจเกิดจากแหล่งข้อมูลล่ม จึงไม่บันทึกทับข้อมูลเดิม"
        )


def load_history(base_dir):
    path = os.path.join(base_dir, HISTORY_FILE)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _merge_history(existing, new_dates, new_closes, new_volumes):
    """Merge new bars into existing, upsert by date (overwrite if exists), keep sorted."""
    if not existing or not existing.get("dates"):
        return new_dates, new_closes, new_volumes
    data_map = {d: (c, v) for d, c, v in zip(existing["dates"], existing["closes"], existing["volumes"])}
    for d, c, v in zip(new_dates, new_closes, new_volumes):
        data_map[d] = (c, v)  # overwrite ถ้ามีอยู่แล้ว
    triples = sorted((d, c, v) for d, (c, v) in data_map.items())
    if not triples:
        return [], [], []
    dates, closes, volumes = zip(*triples)
    return list(dates), list(closes), list(volumes)


def save_history(all_data_map, base_dir, existing_hist=None, replace_tickers=None):
    """
    all_data_map: {ticker -> {close: pd.Series, volume: pd.Series}}
    เขียนลง SQLite (หลัก) + merge/เขียน set_history.json (เมื่อ DUAL_WRITE_JSON)
    replace_tickers: หุ้นที่ต้อง "แทนที่ทั้ง series" แทนการ merge
      (กรณี refetch เต็มหลังตรวจพบ corporate action — ข้อมูลเก่าฐานราคาผิดแล้ว)
    Returns merged history dict (JSON path) หรือ None เมื่อปิด dual-write
    """
    replace_tickers = set(replace_tickers or ())

    # SQLite คือ source of truth — upsert เฉพาะแท่งที่ได้มา ลบ replace_tickers ของเก่า
    # ในทรานแซกชันเดียวกับ insert (ดู upsert_bars) กัน "ลบสำเร็จแต่ insert ล้มเหลว"
    # ทำให้ราคาหุ้นตัวนั้นหายถาวร (เดิมลบแยก commit ก่อนเรียกฟังก์ชันนี้)
    upsert_bars(base_dir, all_data_map, replace_tickers=replace_tickers)

    if not DUAL_WRITE_JSON:
        return None

    stocks_hist = {}
    if existing_hist:
        stocks_hist = dict(existing_hist.get("stocks", {}))

    for ticker, data in all_data_map.items():
        close  = data["close"]
        volume = data["volume"]
        new_dates   = [d.strftime("%Y-%m-%d") for d in close.index]
        new_closes  = [round(float(c), 4) for c in close]
        new_volumes = [int(v) for v in volume]
        # replace ticker: ทิ้งของเก่าทั้ง series (ฐานราคาผิดหลัง CA) ไม่ merge
        existing = None if ticker in replace_tickers else stocks_hist.get(ticker)
        merged_d, merged_c, merged_v = _merge_history(
            existing, new_dates, new_closes, new_volumes
        )
        stocks_hist[ticker] = {
            "dates":   merged_d,
            "closes":  merged_c,
            "volumes": merged_v,
        }

    new_hist = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stocks": stocks_hist,
    }
    path = os.path.join(base_dir, HISTORY_FILE)
    _atomic_write_json(path, new_hist)
    return new_hist


