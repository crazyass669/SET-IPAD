# -*- coding: utf-8 -*-
"""core/store_factory.py — โรงงานสร้าง price store แบบ OHLC ต่อไฟล์ DB เดียว ใช้ร่วมกัน
ระหว่าง core/us_store.py และ core/hk_store.py (เดิม 2 ไฟล์นี้เหมือนกันทุกบรรทัดยกเว้น
DB_FILE — เก็บ logic ไว้ที่เดียว แก้บั๊ก/เพิ่ม feature ครั้งเดียวใช้ได้ทั้งสองตลาด รวมถึง
ตลาดที่ 4 ในอนาคตด้วย make_store(db_file) ใหม่)

คืน object ที่มี method ชื่อ/signature เดิมทุกตัว (func(base_dir, ...) ไม่มี self) เพื่อให้
เรียกจาก us_store.func(...)/hk_store.func(...) ได้เหมือนเดิมทุกจุดในโค้ดเดิม"""
import os
import sqlite3
from datetime import datetime
from types import SimpleNamespace


def _r4(x):
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if f != f else round(f, 4)


def make_store(db_file):
    def _db_path(base_dir):
        return os.path.join(base_dir, db_file)

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
                CREATE TABLE IF NOT EXISTS prices(
                  ticker TEXT NOT NULL, date TEXT NOT NULL,
                  open REAL, high REAL, low REAL,
                  close REAL NOT NULL, adj_close REAL, volume INTEGER NOT NULL,
                  PRIMARY KEY(ticker, date)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
            """)
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

    def get_last_dates(base_dir):
        """คืน {ticker: วันที่แท่งสุดท้าย} — ใช้หา gap ตอน Quick Update"""
        if not db_exists(base_dir):
            return {}
        con = _connect(base_dir)
        try:
            rows = con.execute(
                "SELECT ticker, MAX(date) FROM prices GROUP BY ticker").fetchall()
        finally:
            con.close()
        return dict(rows)

    def _safe_vol(x):
        """int ปัดเศษ หรือ 0 ถ้าเป็น NaN/None/แปลงไม่ได้ — ดูเหตุผลเต็มใน core/store.py::_safe_vol
        (x==x เดิมจับ NaN ได้แต่ None==None ก็ True เหมือนกัน หลุดไปพัง int(None))"""
        try:
            f = float(x)
        except (TypeError, ValueError):
            return 0
        return 0 if f != f else int(f)

    def upsert_bars(base_dir, all_data_map, chunk_rows=20000, replace_tickers=None):
        """all_data_map: {ticker -> {'close','volume'[,'open','high','low','adj_close']: pd.Series}}
        ทุก series align index เดียวกับ close (ดู _extract_ohlcav ใน sources/yahoo.py)

        commit เป็นช่วงละ chunk_rows แถวแทน transaction เดียวทั้งก้อน — ลด lock hold time
        กัน writer อื่นเจอ "database is locked" (busy_timeout=5000ms) และกันเสีย full
        refresh ทั้งรอบถ้าโดนขัดจังหวะกลางคัน (ดูเหตุผลเต็มใน core/store.py::upsert_bars)

        replace_tickers: ticker ที่ต้อง "แทนที่ทั้ง series" — ลบในทรานแซกชันเดียวกับ
        chunk insert แรก (commit พร้อมกัน) กันราคาหายถาวรถ้า insert ล้มเหลวกลางทาง"""
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

    def get_closes_map(base_dir, ticker, dates, con=None):
        """คืน {date: close} ของ ticker เฉพาะวันที่ระบุ — ใช้เทียบแท่ง overlap
        ตอนตรวจ corporate action (split detector) เหมือน core.store.get_closes_map

        con: reuse connection ที่เปิดค้างไว้ได้ (detect_ca_mismatch loop) — ไม่ส่งมาก็เปิด/ปิดเอง"""
        if not dates or not db_exists(base_dir):
            return {}
        _con = con or _connect(base_dir)
        try:
            q = ",".join("?" * len(dates))
            rows = _con.execute(
                f"SELECT date, close FROM prices WHERE ticker=? AND date IN ({q})",
                (ticker, *dates)).fetchall()
        finally:
            if con is None:
                _con.close()
        return dict(rows)

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

    def get_ohlc_series(base_dir, ticker):
        """คืน {'dates','opens','highs','lows','closes','adj_closes','volumes'} ของหุ้นตัวเดียว
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

    def iter_all_series(base_dir):
        """generator: yield (ticker, {'dates','closes','volumes','highs','lows','opens'}) เรียงตาม ticker
        รูปแบบเดียวกับ core.store.iter_all_series — ใช้กับ services/breadth.py + _etf_do_rebuild
        (app.py) เปิด connection เดียวอ่านทุก ticker แทนเปิดทีละ connection ต่อตัว 'opens' เพิ่มมา
        แบบ additive (consumer เก่าที่อ่านแค่ dates/closes/volumes/highs/lows ไม่กระทบ)"""
        if not db_exists(base_dir):
            return
        con = _connect(base_dir)
        try:
            cur = con.execute(
                "SELECT ticker, date, close, volume, high, low, open FROM prices ORDER BY ticker, date")
            cur_t, d, c, v, h, lo, op = None, [], [], [], [], [], []
            for t, dt, cl, vol, hi, low, o in cur:
                if t != cur_t:
                    if cur_t is not None:
                        yield cur_t, {"dates": d, "closes": c, "volumes": v, "highs": h, "lows": lo, "opens": op}
                    cur_t, d, c, v, h, lo, op = t, [], [], [], [], [], []
                d.append(dt); c.append(cl); v.append(vol); h.append(hi); lo.append(low); op.append(o)
            if cur_t is not None:
                yield cur_t, {"dates": d, "closes": c, "volumes": v, "highs": h, "lows": lo, "opens": op}
        finally:
            con.close()

    def iter_recent_series(base_dir, warmup_rows):
        """เหมือน iter_all_series แต่ดึงเฉพาะ warmup_rows แถวล่าสุดต่อหุ้น (ดู core.store คำอธิบายเหตุผล)"""
        if not db_exists(base_dir):
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

    def get_all_tickers(base_dir):
        if not db_exists(base_dir):
            return []
        con = _connect(base_dir)
        try:
            return [r[0] for r in con.execute("SELECT DISTINCT ticker FROM prices ORDER BY ticker")]
        finally:
            con.close()

    return SimpleNamespace(
        DB_FILE=db_file, _connect=_connect,
        db_exists=db_exists, init_db=init_db, get_meta=get_meta,
        get_last_dates=get_last_dates, upsert_bars=upsert_bars,
        get_closes_map=get_closes_map, delete_ticker_bars=delete_ticker_bars,
        get_ohlc_series=get_ohlc_series, iter_all_series=iter_all_series,
        iter_recent_series=iter_recent_series, get_all_tickers=get_all_tickers,
    )
