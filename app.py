"""
SET Dashboard — Flask Web Server
รัน: python app.py
หรือดับเบิ้ลคลิก start.bat
"""

import json
import os
import random
import shutil
import string
import subprocess
import threading
import time
import traceback
import sys
import socket

# log ภาษาไทยต้องไม่ทำ thread ตาย ไม่ว่า stdout จะเป็น console/ไฟล์/cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# atomic write ใช้จาก core.store (Phase 3 — เลิก def ซ้ำในไฟล์นี้)
from core.store import _atomic_write_json

# Band cache — เก็บผล mrlikestock.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_band_cache: dict = {}
_BAND_CACHE_TTL = 6 * 3600

# DR cache — เก็บราคา underlying foreign stocks ไว้ 4 ชั่วโมง
_dr_cache: dict = {}
_DR_CACHE_TTL = 4 * 3600

# Financials cache — งบการเงิน cache 24 ชั่วโมง (ข้อมูลไม่เปลี่ยนบ่อย)
_fin_cache: dict = {}
_FIN_CACHE_TTL = 24 * 3600

# Indices cache — ดัชนีราคากลุ่ม SET/MAI cache 4 ชั่วโมง
_indices_cache: dict = {}
_INDICES_CACHE_TTL = 4 * 3600

from flask import Flask, jsonify, send_file, Response, request

# สูตรคำนวณกลาง — ห้าม copy สูตรมาวางในไฟล์นี้ ให้ import จาก core.metrics เท่านั้น
from core.metrics import calc_rs_raw

# HTTP clients / static universe — แยกไว้ที่ sources/ (Phase 2 refactor)
from sources.tradingview import INDEX_INFO, _yf_to_tv, _fetch_tv_bars
from sources.dr_universe import _DR_STATIC
from sources.sec import _sec_viewstate, _sec_post, _thai_date, _extract_symbol


BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_FILE    = os.path.join(BASE_DIR, "set_data.json")
BACKUP_FILE  = os.path.join(BASE_DIR, "set_data_backup.json")
HTML_FILE    = os.path.join(BASE_DIR, "set_dashboard.html")
HISTORY_FILE = os.path.join(BASE_DIR, "set_history.json")
DR_CACHE_FILE = os.path.join(BASE_DIR, "dr_cache.json")

# ── Logging: rotating file (5MB × 3) รับทั้ง log แอปและ werkzeug ──
import logging
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
_log_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "dashboard.log"),
    maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
_log_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

_dr_refresh_state = {"running": False, "error": None, "done": False}

def _load_dr_cache_from_file():
    """โหลด DR cache จากไฟล์ตอน server เริ่มทำงาน"""
    if not os.path.exists(DR_CACHE_FILE):
        return
    try:
        with open(DR_CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        file_ts = os.path.getmtime(DR_CACHE_FILE)
        _dr_cache["result"] = data
        _dr_cache["ts"] = file_ts
        print(f"[DR] Loaded cache: {len(data.get('stocks', []))} stocks from dr_cache.json")
    except Exception as e:
        print(f"[DR] Failed to load cache: {e}")

def _save_dr_cache_to_file(result):
    try:
        _atomic_write_json(DR_CACHE_FILE, result)
        print(f"[DR] Saved cache: {len(result.get('stocks', []))} stocks -> dr_cache.json")
    except Exception as e:
        print(f"[DR] Failed to save cache: {e}")

# History: อ่านจาก SQLite ผ่าน core.store (point query ~6ms) — ไม่มี in-memory
# cache 434MB อีกต่อไป และไม่ต้องมี mtime invalidation (query ตรงทุกครั้ง)
from core import store as price_store

app = Flask(__name__)


@app.after_request
def _static_no_cache(resp):
    # /static (dashboard.js/css) ให้ browser revalidate ทุกครั้ง — werkzeug ใส่
    # ETag/Last-Modified ให้อยู่แล้ว จึงได้ 304 เมื่อไฟล์ไม่เปลี่ยน
    if request.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-cache"
    return resp


# โหลด DR cache จากไฟล์ตอน import (ใช้ได้ทั้ง __main__ และ WSGI)
_load_dr_cache_from_file()

# ============================================================
# Refresh state — shared between threads
# ============================================================

_state = {
    "running": False,
    "done": False,
    "error": None,
    "current": 0,
    "total": 0,
    "message": "กำลังเริ่ม...",
}
_lock = threading.Lock()


def _update(**kw):
    with _lock:
        _state.update(kw)


def _snapshot():
    with _lock:
        return dict(_state)


# ============================================================
# Routes
# ============================================================

@app.route("/")
def index():
    resp = send_file(HTML_FILE)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


# Data cache — validate + gzip ครั้งเดียวต่อการอัปเดตไฟล์ เก็บใน memory (mtime-keyed)
_data_gz_cache = {"path": None, "mtime": None, "raw": None, "gz": None}
_data_gz_lock  = threading.Lock()


def _resolve_data_bytes():
    """คืน (raw_bytes, gz_bytes, etag) ของ set_data.json ที่ validate แล้ว
    ถ้าไฟล์หลักเสียหาย (JSON พัง) → fallback ไป set_data_backup.json อัตโนมัติ
    คืน (None, None, None) ถ้าไม่มีไฟล์ใช้ได้เลย"""
    import gzip as _gzip
    for path in (DATA_FILE, BACKUP_FILE):
        if not os.path.exists(path):
            continue
        mtime = os.path.getmtime(path)
        with _data_gz_lock:
            if _data_gz_cache["path"] == path and _data_gz_cache["mtime"] == mtime:
                return _data_gz_cache["raw"], _data_gz_cache["gz"], f'"{path == BACKUP_FILE}-{mtime}"'
            try:
                with open(path, "rb") as f:
                    raw = f.read()
                json.loads(raw)  # validate ครั้งเดียวต่อ mtime
            except Exception:
                print(f"[Data] ไฟล์เสียหาย: {os.path.basename(path)} — "
                      f"{'ลองใช้ backup แทน' if path == DATA_FILE else 'backup ก็เสียหาย'}")
                continue
            _data_gz_cache.update(path=path, mtime=mtime, raw=raw,
                                  gz=_gzip.compress(raw, compresslevel=6))
            if path == BACKUP_FILE:
                print("[Data] เสิร์ฟจาก set_data_backup.json (ไฟล์หลักเสียหาย)")
            return _data_gz_cache["raw"], _data_gz_cache["gz"], f'"{path == BACKUP_FILE}-{mtime}"'
    return None, None, None


@app.route("/api/data")
def get_data():
    raw, gz, etag = _resolve_data_bytes()
    if raw is None:
        if os.path.exists(DATA_FILE) or os.path.exists(BACKUP_FILE):
            return jsonify({"error": "ไฟล์ข้อมูลเสียหายทั้งไฟล์หลักและ backup — กรุณากด Refresh ใหม่"}), 500
        return jsonify({"error": "ยังไม่มีข้อมูล กด Refresh เพื่อดึงข้อมูลครั้งแรก"}), 404

    # ข้อมูลไม่เปลี่ยน → 304 ไม่ต้องส่ง payload
    if request.headers.get("If-None-Match") == etag:
        return Response(status=304, headers={"ETag": etag})

    common = {"ETag": etag, "Cache-Control": "no-cache"}

    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        return Response(gz, mimetype="application/json",
                        headers={**common, "Content-Encoding": "gzip",
                                 "Vary": "Accept-Encoding"})
    return Response(raw, mimetype="application/json", headers=common)


@app.route("/api/refresh", methods=["POST"])
def start_refresh():
    period = "max"
    if request.is_json:
        p = request.json.get("period", "max")
        if p in {"1y", "2y", "5y", "10y", "max"}:
            period = p
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message=f"กำลังเริ่ม... ({period})")

    threading.Thread(target=_run_refresh, args=(period,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/progress")
def progress_stream():
    """SSE endpoint — ส่ง progress ทุก 0.5 วิ"""
    def generate():
        while True:
            snap = _snapshot()
            yield f"data: {json.dumps(snap, ensure_ascii=False)}\n\n"
            if snap["done"] or snap["error"]:
                break
            time.sleep(0.5)
    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            # ห้ามใส่ "Connection" — เป็น hop-by-hop header, waitress (PEP 3333)
            # จะ raise AssertionError ทำ SSE 500 ทั้ง endpoint
        },
    )


@app.route("/api/history/<symbol>")
def get_history(symbol):
    """ส่ง full price history จาก SQLite point query (สำหรับ 5Y/Max chart)"""
    ticker = symbol.upper().strip() + ".BK"
    data = price_store.get_series(BASE_DIR, ticker)
    if not data:
        return jsonify({"error": f"ไม่พบข้อมูล {symbol} — กรุณา Full Refresh ก่อน"}), 404
    return jsonify(data)


@app.route("/api/quick-update", methods=["POST"])
def start_quick_update():
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่ม Quick Update...")
    threading.Thread(target=_run_quick, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/band/<symbol>")
def get_band(symbol):
    """ดึง PE Band / PBV Band จาก mrlikestock.com สำหรับหุ้นที่ระบุ — cache 6 ชั่วโมง"""
    import requests as req, re as _re
    from datetime import datetime as _dt

    def _parse_section(html):
        m = _re.search(
            r'Last (?:PE|PBV) = ([\d.]+)\s*\]\s*\((-?[\d.]+)\)\s*\((-?[\d.]+)\)'
            r'.*?AVG = ([\d.]+)\s*\]\s*\((-?[\d.]+)\)\s*\((-?[\d.]+)\)',
            html, _re.DOTALL
        )
        if not m:
            return None
        cur, m2, m1, avg, p1, p2 = [float(x) for x in m.groups()]
        rows_m = _re.search(r'data\.addRows\(\[(.*?)\]\);', html, _re.DOTALL)
        history = []
        if rows_m:
            for r in _re.finditer(
                r"\['([^']+)',\s*(-?[\d.]+),\s*-?[\d.]+,\s*-?[\d.]+,\s*-?[\d.]+,\s*-?[\d.]+,\s*-?[\d.]+\]",
                rows_m.group(1)
            ):
                history.append({"month": r.group(1), "val": float(r.group(2))})
        return {"current": cur, "m2sd": m2, "m1sd": m1, "avg": avg, "p1sd": p1, "p2sd": p2,
                "history": history}

    sym = symbol.upper().strip()

    # ตรวจ cache
    cached = _band_cache.get(sym)
    if cached and (time.time() - cached["ts"] < _BAND_CACHE_TTL):
        result = dict(cached["data"])
        result["cached_at"] = cached["fetched_at"]
        return jsonify(result)

    try:
        r = req.post(
            "https://www.mrlikestock.com/web/np_chart/np_chart.php",
            data={"quote": sym},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=20,
        )
        html = r.text
        pe_html  = _re.search(r'<h2>[^<]*PE Band[^<]*</h2>(.*?)(?=<h2>|$)', html, _re.DOTALL)
        pbv_html = _re.search(r'<h2>[^<]*PBV Band[^<]*</h2>(.*?)(?=<h2>|$)', html, _re.DOTALL)
        result = {"symbol": sym}
        if pe_html:  result["pe"]  = _parse_section(pe_html.group(1))
        if pbv_html: result["pbv"] = _parse_section(pbv_html.group(1))
        if not result.get("pe") and not result.get("pbv"):
            return jsonify({"error": f"ไม่พบข้อมูล Band สำหรับ {sym}"}), 404
        fetched_at = _dt.now().strftime("%H:%M น.")
        _band_cache[sym] = {"ts": time.time(), "fetched_at": fetched_at, "data": result}
        result["cached_at"] = None
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/dr")
def get_dr_data():
    """ดึงราคา underlying foreign stocks ของ DR/DRx ทั้งหมด — cache 4 ชั่วโมง"""
    import yfinance as yf
    import pandas as pd
    from datetime import datetime as _dt

    now = time.time()
    if _dr_cache.get("ts") and (now - _dr_cache["ts"] < _DR_CACHE_TTL):
        return jsonify(_dr_cache["result"])

    yf_tickers = list({s["yf"] for s in _DR_STATIC})

    try:
        raw = yf.download(
            yf_tickers,
            period="max",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    is_multi = len(yf_tickers) > 1

    def _series(yticker, field):
        try:
            if is_multi:
                s = raw[yticker][field]
            else:
                s = raw[field]
            return s.dropna()
        except (KeyError, TypeError):
            return pd.Series(dtype=float)

    def _dr_ret(close, days):
        if len(close) < days + 1:
            return None
        p = float(close.iloc[-(days + 1)])
        n = float(close.iloc[-1])
        return round((n - p) / p * 100, 2) if p else None

    results = []
    for stock in _DR_STATIC:
        yticker = stock["yf"]
        try:
            close = _series(yticker, "Close")
            if len(close) < 2:
                continue

            price = float(close.iloc[-1])
            prev  = float(close.iloc[-2])
            chg   = (price - prev) / prev * 100

            close100 = [round(float(x), 4) for x in close.tail(100).tolist()]

            # เก็บ full price history สำหรับ chart popup (date + price)
            dates_all  = [str(d)[:10] for d in close.index.tolist()]
            closes_all = [round(float(x), 6) for x in close.tolist()]

            open_s = _series(yticker, "Open")
            high_s = _series(yticker, "High")
            low_s  = _series(yticker, "Low")
            vol_s  = _series(yticker, "Volume")

            n = min(30, len(close))
            ohlc30 = []
            for i in range(-n, 0):
                try:
                    o = float(open_s.iloc[i]) if len(open_s) >= abs(i) else price
                    h = float(high_s.iloc[i]) if len(high_s) >= abs(i) else price
                    l = float(low_s.iloc[i])  if len(low_s)  >= abs(i) else price
                    c = float(close.iloc[i])
                    v = float(vol_s.iloc[i])  if len(vol_s)  >= abs(i) else 0
                    ohlc30.append([round(o,4), round(h,4), round(l,4), round(c,4), int(v)])
                except Exception:
                    pass

            ret_1w = _dr_ret(close, 5)
            ret_1m = _dr_ret(close, 21)
            ret_3m = _dr_ret(close, 63)
            ret_6m = _dr_ret(close, 126)
            ret_1y = _dr_ret(close, 250)
            ret_3y = _dr_ret(close, 756)
            ret_5y = _dr_ret(close, 1260)

            # 52W High/Low
            close_52w = close.iloc[-252:] if len(close) >= 252 else close
            high_52w = round(float(close_52w.max()), 4)
            low_52w  = round(float(close_52w.min()), 4)

            # ATH
            ath     = round(float(close.max()), 4)
            ath_pct = round((price - ath) / ath * 100, 2) if ath else None

            # YTD%
            try:
                import datetime as _datetime
                cur_year   = _datetime.datetime.now().year
                close_ytd  = close[close.index >= pd.Timestamp(f"{cur_year}-01-01")]
                if len(close_ytd) > 0:
                    first_ytd = float(close_ytd.iloc[0])
                    ret_ytd   = round((price - first_ytd) / first_ytd * 100, 2) if first_ytd else None
                else:
                    ret_ytd = None
            except Exception:
                ret_ytd = None

            rs_raw = calc_rs_raw(ret_1m, ret_3m, ret_6m, ret_1y)

            # Market cap via fast_info (best-effort)
            mkt_cap = None
            try:
                fi = yf.Ticker(yticker).fast_info
                mkt_cap = getattr(fi, "market_cap", None)
                if mkt_cap: mkt_cap = float(mkt_cap)
            except Exception:
                pass

            results.append({
                "sym":      stock["sym"],
                "name":     stock["name"],
                "region":   stock["region"],
                "ind":      stock["ind"],
                "yf":       stock["yf"],
                "price":    round(price, 2),
                "chg":      round(chg, 2),
                "ret_1w":   ret_1w,
                "ret_1m":   ret_1m,
                "ret_3m":   ret_3m,
                "ret_6m":   ret_6m,
                "ret_1y":   ret_1y,
                "ret_3y":   ret_3y,
                "ret_5y":   ret_5y,
                "ret_ytd":  ret_ytd,
                "high_52w": high_52w,
                "low_52w":  low_52w,
                "ath":      ath,
                "ath_pct":  ath_pct,
                "rs_raw":   round(rs_raw, 4) if rs_raw is not None else None,
                "rs_score": None,
                "mkt_cap":  mkt_cap,
                "drs":      stock["drs"],
                "close100": close100,
                "ohlc30":   ohlc30,
                "dates":    dates_all,
                "closes":   closes_all,
            })
        except Exception as e:
            print(f"[DR] {stock['sym']}: {e}")

    # RS rank within DR universe
    valid_rs = [r for r in results if r.get("rs_raw") is not None]
    valid_rs.sort(key=lambda x: x["rs_raw"])
    n_rs = len(valid_rs)
    for i, r in enumerate(valid_rs):
        r["rs_score"] = int(round(i / n_rs * 99)) if n_rs > 0 else None

    result = {"stocks": results, "ts": _dt.now().isoformat()}
    _dr_cache.update(result=result, ts=time.time())
    _save_dr_cache_to_file(result)
    return jsonify(result)


@app.route("/api/dr-quick-update", methods=["POST"])
def dr_quick_update():
    """อัปเดตราคาล่าสุด DR โดย download แค่ 5 วัน — เร็วมาก"""
    import yfinance as yf
    import pandas as pd
    from datetime import datetime as _dt

    if _dr_refresh_state["running"]:
        return jsonify({"status": "running"})

    cached = _dr_cache.get("result")
    if not cached or not cached.get("stocks"):
        return jsonify({"error": "ยังไม่มี DR cache — กรุณาโหลดหน้า DR ก่อน"}), 400

    def _do_quick():
        _dr_refresh_state.update(running=True, error=None, done=False)
        try:
            yf_tickers = list({s["yf"] for s in _DR_STATIC})

            # คำนวณ gap จาก last date ที่บันทึกไว้ในแต่ละ DR stock
            cached_stocks = (cached or {}).get("stocks", [])
            last_dates_dr = [s["dates"][-1] for s in cached_stocks if s.get("dates")]
            if last_dates_dr:
                from datetime import date as _date, timedelta as _td
                min_last_dr = min(last_dates_dr)
                start_dr = (pd.to_datetime(min_last_dr) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                dl_kwargs = {"start": start_dr}
                print(f"[DR quick] gap fetch from {start_dr}")
            else:
                dl_kwargs = {"period": "30d"}
                print("[DR quick] no history, fallback to 30d")

            raw = yf.download(yf_tickers, auto_adjust=True,
                              progress=False, group_by="ticker", threads=True, **dl_kwargs)
            is_multi = len(yf_tickers) > 1

            def _series(yticker, field):
                try:
                    return (raw[yticker][field] if is_multi else raw[field]).dropna()
                except (KeyError, TypeError):
                    return pd.Series(dtype=float)

            # Build lookup จาก sym → stock entry เพื่ออัปเดต
            stock_map = {s["sym"]: s for s in cached["stocks"]}
            for st in _DR_STATIC:
                sym, yticker = st["sym"], st["yf"]
                try:
                    close = _series(yticker, "Close")
                    if len(close) < 2:
                        continue
                    price = float(close.iloc[-1])
                    prev  = float(close.iloc[-2])
                    chg   = round((price - prev) / prev * 100, 2) if prev else 0

                    open_s = _series(yticker, "Open")
                    high_s = _series(yticker, "High")
                    low_s  = _series(yticker, "Low")
                    vol_s  = _series(yticker, "Volume")

                    entry = stock_map.get(sym)
                    if entry:
                        entry["price"] = round(price, 2)
                        entry["chg"]   = chg
                        new_closes_raw = [round(float(c), 4) for c in close.tolist()]
                        new_dates_raw  = [str(d)[:10] for d in close.index.tolist()]
                        # อัปเดต close100
                        old100 = entry.get("close100", [])
                        entry["close100"] = (old100 + new_closes_raw)[-100:]
                        # อัปเดต full history
                        old_dates  = entry.get("dates", [])
                        old_closes = entry.get("closes", [])
                        for dt, cl in zip(new_dates_raw, new_closes_raw):
                            if not old_dates or dt > old_dates[-1]:
                                old_dates.append(dt)
                                old_closes.append(cl)
                        entry["dates"]  = old_dates
                        entry["closes"] = old_closes
                        # recalculate return metrics from updated full history
                        def _ret_q(arr, n):
                            if len(arr) < n + 1:
                                return None
                            p = arr[-(n+1)]
                            return round((arr[-1] - p) / p * 100, 2) if p else None
                        entry["ret_1w"] = _ret_q(old_closes, 5)
                        entry["ret_1m"] = _ret_q(old_closes, 21)
                        entry["ret_3m"] = _ret_q(old_closes, 63)
                        entry["ret_6m"] = _ret_q(old_closes, 126)
                        entry["ret_1y"] = _ret_q(old_closes, 250)
                        # rebuild ohlc30 with volume from latest 5d data
                        try:
                            n = min(30, len(close))
                            ohlc30 = []
                            for i in range(-n, 0):
                                o = float(open_s.iloc[i]) if len(open_s) >= abs(i) else price
                                h = float(high_s.iloc[i]) if len(high_s) >= abs(i) else price
                                l = float(low_s.iloc[i])  if len(low_s)  >= abs(i) else price
                                c2 = float(close.iloc[i])
                                v  = float(vol_s.iloc[i]) if len(vol_s) >= abs(i) else 0
                                ohlc30.append([round(o,4), round(h,4), round(l,4), round(c2,4), int(v)])
                            if ohlc30:
                                # merge: keep old 30d base, replace tail with fresh data
                                old_ohlc = entry.get("ohlc30", [])
                                keep = max(0, 30 - len(ohlc30))
                                entry["ohlc30"] = old_ohlc[:keep] + ohlc30
                                entry["ohlc30"] = entry["ohlc30"][-30:]
                        except Exception:
                            pass
                        # recalculate 52W, ATH, YTD from updated full history
                        try:
                            import datetime as _datetime
                            closes_arr = old_closes
                            high_52w = round(max(closes_arr[-252:]), 4) if closes_arr else entry.get("high_52w")
                            low_52w  = round(min(closes_arr[-252:]), 4) if closes_arr else entry.get("low_52w")
                            ath_val  = round(max(closes_arr), 4) if closes_arr else entry.get("ath")
                            entry["high_52w"] = high_52w
                            entry["low_52w"]  = low_52w
                            entry["ath"]      = ath_val
                            entry["ath_pct"]  = round((price - ath_val) / ath_val * 100, 2) if ath_val else None
                            cur_year = _datetime.datetime.now().year
                            ytd_idx  = next((i for i, d in enumerate(old_dates) if d >= f"{cur_year}-01-01"), None)
                            if ytd_idx is not None:
                                first_ytd = old_closes[ytd_idx]
                                entry["ret_ytd"] = round((price - first_ytd) / first_ytd * 100, 2) if first_ytd else None
                        except Exception:
                            pass
                except Exception as e:
                    print(f"[DR quick] {sym}: {e}")

            cached["ts"] = _dt.now().isoformat()
            _dr_cache.update(result=cached, ts=time.time())
            _save_dr_cache_to_file(cached)
            _dr_refresh_state["done"] = True
        except Exception as e:
            _dr_refresh_state["error"] = str(e)
            print(f"[DR quick] ERROR: {e}")
        finally:
            _dr_refresh_state["running"] = False

    threading.Thread(target=_do_quick, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/dr-quick-status")
def dr_quick_status():
    return jsonify(_dr_refresh_state)


@app.route("/api/dr-full-refresh", methods=["POST"])
def dr_full_refresh():
    """ล้าง DR cache ให้ /api/dr ดึงข้อมูลใหม่ทั้งหมด (2Y)"""
    _dr_cache.clear()
    return jsonify({"status": "cleared"})


@app.route("/api/dr-history/<symbol>")
def get_dr_history(symbol):
    """ดึง price history สำหรับ DR stock — เสิร์ฟจาก cache ก่อน ไม่ต้อง fetch yfinance ซ้ำ"""
    import yfinance as yf
    sym = symbol.upper().strip()
    dr_entry = next((s for s in _DR_STATIC if s["sym"] == sym), None)
    if not dr_entry:
        return jsonify({"error": f"ไม่พบ DR stock: {sym}"}), 404

    # ลองเสิร์ฟจาก DR cache ก่อน (มี dates + closes จาก full fetch)
    cached = _dr_cache.get("result")
    if cached:
        for s in cached.get("stocks", []):
            if s.get("sym") == sym and s.get("dates") and s.get("closes"):
                return jsonify({"sym": sym, "yf": dr_entry["yf"],
                                "dates": s["dates"], "closes": s["closes"]})

    # fallback: fetch จาก yfinance โดยตรง
    yf_ticker = dr_entry["yf"]
    try:
        t = yf.Ticker(yf_ticker)
        hist = t.history(period="max")
        if hist.empty:
            return jsonify({"error": "ไม่พบข้อมูลราคา"}), 404
        dates  = [str(d)[:10] for d in hist.index]
        closes = [round(float(c), 6) for c in hist["Close"]]
        return jsonify({"sym": sym, "yf": yf_ticker, "dates": dates, "closes": closes})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/financials/<symbol>")
def get_financials(symbol):
    """ดึงงบการเงินรายปี (Income / Balance / CashFlow) — cache 24h"""
    import yfinance as yf
    import pandas as pd
    import math
    import traceback

    sym = symbol.upper().strip()

    cached = _fin_cache.get(sym)
    if cached and (time.time() - cached["ts"] < _FIN_CACHE_TTL):
        return jsonify(cached["data"])

    # หา yfinance ticker: ค้นใน DR static ก่อน ไม่เจอ → ใช้ .BK
    dr_entry = next((s for s in _DR_STATIC if s["sym"] == sym), None)
    if dr_entry:
        yf_ticker, stock_type, stock_name = dr_entry["yf"], "dr", dr_entry["name"]
    else:
        yf_ticker, stock_type, stock_name = sym + ".BK", "set", sym

    def _df_to_dict(df):
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
                    val   = df.loc[idx, col]
                    fval  = float(val)
                    if not math.isnan(fval) and not math.isinf(fval):
                        row[label] = fval
                except Exception:
                    pass
            if row:
                out[str(idx)] = row
        return out

    try:
        t = yf.Ticker(yf_ticker)

        # รองรับทั้ง yfinance เก่า (t.financials) และใหม่ (t.income_stmt)
        income = {}
        for attr in ("income_stmt", "financials"):
            try:
                income = _df_to_dict(getattr(t, attr, None))
                if income:
                    break
            except Exception:
                pass

        balance = {}
        try:
            balance = _df_to_dict(t.balance_sheet)
        except Exception:
            pass

        cashflow = {}
        try:
            cashflow = _df_to_dict(t.cashflow)
        except Exception:
            pass

        if not income and not balance and not cashflow:
            return jsonify({"error": f"ไม่พบข้อมูลงบการเงินสำหรับ {sym} ({yf_ticker})"}), 404

        # TTM — คำนวณจาก quarterly data
        def _calc_ttm(q_df, flow=True):
            """flow=True: sum 4Q (income/cashflow), flow=False: latest Q (balance sheet)"""
            result = {}
            if q_df is None:
                return result
            try:
                if q_df.empty:
                    return result
            except Exception:
                return result
            try:
                cols = sorted(q_df.columns, key=str, reverse=True)[:4]
            except Exception:
                return result
            # Gap detection: ถ้า quarters ไม่ติดกัน (>105 วัน) แสดงว่าข้อมูลขาด → ไม่คำนวณ TTM
            if flow and len(cols) == 4:
                try:
                    import pandas as pd
                    dates = [pd.Timestamp(c) for c in cols]
                    if any((dates[i] - dates[i+1]).days > 105 for i in range(3)):
                        return result
                except Exception:
                    pass
            for idx in q_df.index:
                try:
                    if flow:
                        vals = []
                        for col in cols:
                            try:
                                v = float(q_df.loc[idx, col])
                                if not math.isnan(v) and not math.isinf(v):
                                    vals.append(v)
                            except Exception:
                                pass
                        if len(vals) == 4:
                            result[str(idx)] = sum(vals)
                    else:
                        col = cols[0] if cols else None
                        if col is not None:
                            v = float(q_df.loc[idx, col])
                            if not math.isnan(v) and not math.isinf(v):
                                result[str(idx)] = v
                except Exception:
                    pass
            return result

        q_inc_df = None
        ttm_income = {}
        try:
            for attr in ("quarterly_income_stmt", "quarterly_financials"):
                q_inc_df = getattr(t, attr, None)
                ttm_income = _calc_ttm(q_inc_df, flow=True)
                if ttm_income:
                    break
        except Exception:
            pass

        ttm_balance = {}
        try:
            ttm_balance = _calc_ttm(t.quarterly_balance_sheet, flow=False)
        except Exception:
            pass

        ttm_cashflow = {}
        try:
            ttm_cashflow = _calc_ttm(t.quarterly_cashflow, flow=True)
        except Exception:
            pass

        # Validate TTM: ถ้า quarterly revenue มี quarter ติดลบ หรือ TTM ห่างจาก annual มากเกินไป → ล้าง TTM
        rev_keys = ['Total Revenue', 'Revenue', 'Revenues', 'Net Revenue']
        ttm_rev = next((ttm_income.get(k) for k in rev_keys if ttm_income.get(k) is not None), None)
        ann_rev = None
        for k in rev_keys:
            row = income.get(k)
            if row:
                vals = [v for v in row.values() if v is not None]
                if vals:
                    ann_rev = max(vals, key=abs)
                    break
        ttm_bad = False
        # เช็ค individual quarter revenue ต้องไม่ติดลบ
        if q_inc_df is not None and not q_inc_df.empty:
            try:
                q_cols = sorted(q_inc_df.columns, key=str, reverse=True)[:4]
                for k in rev_keys:
                    if k in q_inc_df.index:
                        for col in q_cols:
                            try:
                                v = float(q_inc_df.loc[k, col])
                                if not math.isnan(v) and v < 0:
                                    ttm_bad = True
                            except Exception:
                                pass
                        break
            except Exception:
                pass
        if ttm_rev is not None and ann_rev and ann_rev != 0:
            ratio = abs(ttm_rev) / abs(ann_rev)
            if ttm_rev < 0 or ratio < 0.05 or ratio > 20:
                ttm_bad = True
        if ttm_bad:
            ttm_income, ttm_balance, ttm_cashflow = {}, {}, {}

        # ดึง currency + ชื่อบริษัท (fast_info เร็วกว่า .info)
        currency, full_name = "—", stock_name
        try:
            fi       = t.fast_info
            currency = getattr(fi, "currency", None) or "—"
        except Exception:
            pass
        try:
            info      = t.info
            full_name = info.get("longName") or info.get("shortName") or stock_name
            if currency == "—":
                currency = info.get("financialCurrency") or info.get("currency") or "—"
        except Exception:
            pass

        data = {
            "sym": sym, "yf": yf_ticker, "name": full_name,
            "type": stock_type, "currency": currency,
            "income": income, "balance": balance, "cashflow": cashflow,
            "ttm_income": ttm_income, "ttm_balance": ttm_balance, "ttm_cashflow": ttm_cashflow,
        }
        _fin_cache[sym] = {"ts": time.time(), "data": data}
        return jsonify(data)

    except Exception as e:
        print(f"[Financials ERROR] {sym}: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


INDICES_FILE = "indices_cache.json"

def _compute_idx_rs(result: dict):
    """คำนวณ rs_set + rs_history สำหรับดัชนีทุกตัว เทียบกับ universe หุ้น SET"""
    import bisect, datetime as _dtm
    today_str = _dtm.date.today().isoformat()
    try:
        set_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "set_data.json")
        if not os.path.exists(set_file):
            return
        with open(set_file, encoding="utf-8") as f:
            set_data = json.load(f)

        def _rs_raw(e):
            return calc_rs_raw(e.get("ret_1m"), e.get("ret_3m"),
                               e.get("ret_6m"), e.get("ret_1y"))

        stock_raws = []
        for s in set_data.get("stocks", []):
            r = _rs_raw(s)
            if r is not None:
                stock_raws.append(r)
        stock_raws.sort()
        ns = len(stock_raws)

        for entry in result.values():
            raw = _rs_raw(entry)
            rs_val = None
            if raw is not None and ns > 0:
                rank = bisect.bisect_left(stock_raws, raw)
                rs_val = int(round(rank / ns * 99))
            entry["rs_set"] = rs_val

            # ── backfill rs_history จาก historical closes ────────
            closes = entry.get("closes", [])
            dates  = entry.get("dates",  [])
            hist   = entry.get("rs_history", [])

            if len(closes) >= 252 and len(hist) < 8:
                # คำนวณ rs_raw ทุก 5 วันทำการ ย้อนหลัง 52 สัปดาห์
                weekly = []
                step = 5
                for i in range(0, min(52 * step, len(closes) - 252), step):
                    pos = len(closes) - 1 - i
                    if pos < 252:
                        break
                    c = closes[:pos + 1]
                    def _ret(n, _c=c):
                        return (_c[-1] - _c[-(n+1)]) / _c[-(n+1)] * 100 if len(_c) > n and _c[-(n+1)] else None
                    rr = calc_rs_raw(_ret(21), _ret(63), _ret(126), _ret(250))
                    if rr is not None:
                        weekly.append({"date": dates[pos], "raw": rr})
                if weekly:
                    weekly.reverse()  # เรียงตามเวลา oldest → newest
                    raws = [e["raw"] for e in weekly]
                    mn, mx = min(raws), max(raws)
                    rng = mx - mn or 1
                    # normalize เป็น 0–99 ภายใน range ของดัชนีนั้น
                    hist = [{"date": e["date"],
                             "rs": int(round((e["raw"] - mn) / rng * 99))}
                            for e in weekly]

            # เพิ่ม entry วันนี้
            if rs_val is not None and (not hist or hist[-1]["date"] != today_str):
                # normalize rs_set (0–99 vs SET) เข้าไปด้วย
                hist.append({"date": today_str, "rs": rs_val})
            entry["rs_history"] = hist[-52:]

        print(f"[Indices] RS vs SET computed ({ns} stocks)")
    except Exception:
        print(f"[Indices] RS vs SET failed: {traceback.format_exc()}")


def _fetch_indices_tv(existing: dict, full_refresh: bool = False) -> dict:
    """ดึงข้อมูลดัชนีจาก TradingView WebSocket แบบ parallel
    full_refresh=True → ดึง 5000 bars (ประวัติเต็ม ~20 ปี)
    full_refresh=False → ดึง 30 bars (Quick Update)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import traceback as tb

    all_syms = list(INDEX_INFO.keys())
    updated  = time.strftime("%Y-%m-%d %H:%M")
    result   = dict(existing)

    if full_refresh:
        n_bars = 5000
    else:
        idx_last_dates = [v["dates"][-1] for v in existing.values() if v.get("dates")]
        if idx_last_dates:
            import pandas as _pd
            min_last_idx = min(idx_last_dates)
            gap_days = (_pd.Timestamp.now().normalize() - _pd.Timestamp(min_last_idx)).days
            n_bars = max(30, int(gap_days * 5 / 7) + 15)
            print(f"[Indices QU] gap={gap_days} days -> n_bars={n_bars}")
        else:
            n_bars = 30

    def _fetch_one(sym):
        info = INDEX_INFO.get(sym)
        if not info:
            return sym, None
        tv_sym = _yf_to_tv(sym)
        try:
            pairs = _fetch_tv_bars(tv_sym, n_bars=n_bars, timeout=8)
            if not pairs:
                print(f"[Indices] no data: {tv_sym}")
                return sym, None
            return sym, pairs
        except Exception:
            print(f"[Indices] {sym}: {tb.format_exc()}")
            return sym, None

    # ดึงแบบ parallel — สูงสุด 10 connections พร้อมกัน
    max_workers = 10 if not full_refresh else 5
    fetched = 0
    failed  = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, sym): sym for sym in all_syms}
        for future in as_completed(futures):
            sym, pairs = future.result()
            if pairs is None:
                failed += 1
                continue
            fetched += 1
            info = INDEX_INFO[sym]
            new_dates = [p[0] for p in pairs]
            new_vals  = [p[1] for p in pairs]

            entry = result.get(sym)
            # Full Refresh ที่ได้ bars กลับมาน้อยกว่าที่สะสมไว้มาก (TV throttle/
            # ตอบไม่ครบ) → ห้ามทับประวัติเดิม ให้ merge แบบ append แทน
            destructive_ok = (
                not entry
                or not entry.get("dates")
                or len(new_dates) >= len(entry["dates"]) * 0.9
            )
            if entry and (not full_refresh or not destructive_ok):
                if full_refresh and not destructive_ok:
                    print(f"[Indices] FR {sym}: ได้ {len(new_dates)} bars < 90% ของเดิม "
                          f"({len(entry['dates'])}) — append แทนการทับ")
                old_dates = entry["dates"]
                old_vals  = entry["closes"]
                last_d    = old_dates[-1] if old_dates else ""
                added = 0
                for d, v in zip(new_dates, new_vals):
                    if d > last_d:
                        old_dates.append(d); old_vals.append(v)
                        last_d = d; added += 1
                print(f"[Indices] QU {sym} +{added}d -> {(old_dates or ['?'])[-1]}")
            else:
                old_dates = new_dates
                old_vals  = new_vals
                print(f"[Indices] {'FR' if full_refresh else 'NEW'} {sym} {len(old_vals)} bars")

            v = old_vals
            def _ret(n, _v=v):
                return round((_v[-1] - _v[-(n+1)]) / _v[-(n+1)] * 100, 2) if len(_v) > n else None

            result[sym] = {
                "sym": sym, "name": info["name"], "group": info["group"],
                "last": v[-1],
                "ret_1d": _ret(1), "ret_1w": _ret(5),
                "ret_1m": _ret(21), "ret_3m": _ret(63),
                "ret_6m": _ret(126), "ret_1y": _ret(250),
                "closes": old_vals, "dates": old_dates, "updated_at": updated,
            }

    stats = {"fetched": fetched, "failed": failed, "total": len(all_syms)}

    # ดึงไม่ได้เลยสักตัว (TV ล่ม) → ไม่เขียนไฟล์/ไม่ประทับเวลาใหม่
    # เพื่อไม่ให้ข้อมูลเก่าดูเหมือนเพิ่งอัปเดต
    if fetched == 0:
        print(f"[Indices] ดึงไม่สำเร็จทั้ง {failed} ดัชนี — คงข้อมูลเดิมไว้ ไม่บันทึกทับ")
        return result, stats

    # บันทึกไฟล์ก่อนคำนวณ RS — ถ้า RS พังข้อมูลราคาที่ดึงมาแล้วต้องไม่หาย
    _atomic_write_json(INDICES_FILE, {"updated_at": updated, "data": result})
    try:
        _compute_idx_rs(result)
        _atomic_write_json(INDICES_FILE, {"updated_at": updated, "data": result})
    except Exception:
        print(f"[Indices] compute RS failed (ข้อมูลราคาบันทึกแล้ว): {traceback.format_exc()}")
    print(f"[Indices] saved {len(result)} indices -> {INDICES_FILE} "
          f"(fetched={fetched}, failed={failed})")
    return result, stats


@app.route("/api/indices")
def get_indices():
    """เสิร์ฟข้อมูลดัชนีจากไฟล์ หรือดึงใหม่ถ้าไม่มีไฟล์"""
    global _indices_cache
    data = _indices_cache.get("data")
    first = next(iter(data.values()), {}) if data else {}
    # ส่งจาก memory cache ถ้ามี rs_set และ rs_history ครบแล้ว
    if data and first.get("rs_set") is not None and len(first.get("rs_history", [])) >= 4:
        return jsonify(data)
    # โหลดจากไฟล์ (หรือ recompute ถ้า rs_history ยังน้อย)
    if os.path.exists(INDICES_FILE):
        try:
            with open(INDICES_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            data = saved["data"]
            first2 = next(iter(data.values()), {})
            need_rs = first2.get("rs_set") is None or len(first2.get("rs_history", [])) < 4
            if need_rs and data:
                _compute_idx_rs(data)
                # บันทึกกลับไฟล์เพื่อ cache rs_history
                _atomic_write_json(INDICES_FILE,
                                   {"updated_at": saved.get("updated_at", ""), "data": data})
            _indices_cache["data"] = data
            return jsonify(data)
        except Exception:
            pass
    # ไม่มีไฟล์ — แจ้งให้ refresh
    return jsonify({"error": "ยังไม่มีข้อมูลดัชนี กรุณากด 'อัปเดตดัชนี' เพื่อดาวน์โหลด"}), 404


def _load_indices_existing() -> dict:
    """โหลดข้อมูลสะสมจากไฟล์ (ถ้ามี)"""
    if os.path.exists(INDICES_FILE):
        try:
            with open(INDICES_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            return saved.get("data", {})
        except Exception:
            pass
    return {}


@app.route("/api/indices-quick-update", methods=["POST"])
def indices_quick_update():
    """Quick Update — ดึง 30 bars ล่าสุดจาก TradingView แล้ว append"""
    import traceback as tb
    global _indices_cache
    try:
        existing = _load_indices_existing()
        result, stats = _fetch_indices_tv(existing, full_refresh=False)
        _indices_cache["data"] = result
        return jsonify({"ok": stats["fetched"] > 0, "count": len(result), **stats,
                        "warning": (f"ดึงไม่สำเร็จ {stats['failed']}/{stats['total']} ดัชนี"
                                    if stats["failed"] else None),
                        "updated_at": time.strftime("%Y-%m-%d %H:%M")})
    except Exception as e:
        return jsonify({"error": str(e), "trace": tb.format_exc()}), 500


@app.route("/api/indices-refresh", methods=["POST"])
def indices_refresh():
    """Full Refresh — ดึง 5000 bars (~20 ปี) จาก TradingView"""
    import traceback as tb
    global _indices_cache
    try:
        existing = _load_indices_existing()
        result, stats = _fetch_indices_tv(existing, full_refresh=True)
        _indices_cache["data"] = result
        return jsonify({"ok": stats["fetched"] > 0, "count": len(result), **stats,
                        "warning": (f"ดึงไม่สำเร็จ {stats['failed']}/{stats['total']} ดัชนี"
                                    if stats["failed"] else None)})
    except Exception as e:
        return jsonify({"error": str(e), "trace": tb.format_exc()}), 500


@app.route("/api/restart", methods=["POST"])
def restart_server():
    """Restart Flask process (Windows-safe: spawn new process then exit)"""
    def _do_restart():
        time.sleep(0.8)
        script = os.path.abspath(__file__)
        subprocess.Popen([sys.executable, script],
                         cwd=os.path.dirname(script))
        os._exit(0)
    threading.Thread(target=_do_restart, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/status")
def get_status():
    """ตรวจสอบสถานะ server + ข้อมูล"""
    has_data = os.path.exists(DATA_FILE)
    updated_at = None
    if has_data:
        try:
            # updated_at เป็น key แรกของไฟล์ — อ่านแค่หัวไฟล์พอ
            # (เดิม json.load ทั้ง 12.6MB ทุกครั้งที่ถูกเรียก)
            import re as _re
            with open(DATA_FILE, encoding="utf-8") as f:
                head = f.read(200)
            m = _re.search(r'"updated_at":\s*"([^"]+)"', head)
            updated_at = m.group(1) if m else None
        except Exception:
            pass
    return jsonify({
        "has_data": has_data,
        "updated_at": updated_at,
        "refresh_running": _state["running"],
    })


@app.route("/healthz")
def healthz():
    """Health check แบบเบา (~1ms) สำหรับ monitor/uptime check —
    ไม่แตะไฟล์ใหญ่ ตอบจาก SQLite meta + สถานะ process"""
    return jsonify({
        "ok":                True,
        "db":                price_store.db_exists(BASE_DIR),
        "prices_updated_at": price_store.get_meta(BASE_DIR, "updated_at"),
        "refresh_running":   _state["running"],
    })


# ============================================================
# Background refresh
# ============================================================

def _run_refresh(period="max"):
    # สำรองข้อมูลเดิมไว้ก่อน
    has_backup = False
    if os.path.exists(DATA_FILE):
        try:
            shutil.copy2(DATA_FILE, BACKUP_FILE)
            has_backup = True
        except Exception:
            pass

    try:
        import importlib
        sys.path.insert(0, BASE_DIR)
        from services import refresh as _refresh_svc
        importlib.reload(_refresh_svc)

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        _refresh_svc.run_with_progress(cb, BASE_DIR, period=period)

        # อัพเดท Indices (full history)
        _update(current=98, total=100, message="อัพเดท Indices...")
        try:
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv(existing, full_refresh=True)
            _indices_cache["data"] = result
        except Exception as e:
            print(f"[FullRefresh] Indices error: {e}")

        # อัพเดท Capital Flow
        try:
            _fetch_flow_data()
        except Exception as e:
            print(f"[FullRefresh] Capital Flow error: {e}")

        _update(running=False, done=True, message="เสร็จแล้ว!")

    except Exception as e:
        # ดึงข้อมูลใหม่ล้มเหลว — คืนค่าข้อมูลสำรอง
        if has_backup and os.path.exists(BACKUP_FILE):
            try:
                shutil.copy2(BACKUP_FILE, DATA_FILE)
                _update(running=False, done=True,
                        error=str(e),
                        message="ดึงข้อมูลใหม่ไม่สำเร็จ — ใช้ข้อมูลล่าสุดแทน")
            except Exception:
                _update(running=False, done=True, error=str(e),
                        message=f"เกิดข้อผิดพลาด: {e}")
        else:
            _update(running=False, done=True, error=str(e),
                    message=f"เกิดข้อผิดพลาด: {e}")


_MARKET_STATS_FILE = os.path.join(BASE_DIR, "set_market_stats.json")

@app.route("/api/market-stats")
def market_stats():
    if not os.path.exists(_MARKET_STATS_FILE):
        return jsonify({"error": "ไม่พบ set_market_stats.json — รัน import_market_stats.py ก่อน"}), 404
    with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
        return jsonify(json.load(f))


@app.route("/api/market-stats-meta")
def market_stats_meta():
    """วันที่ล่าสุดที่กดอัปเดต P/E & P/BV — ใช้เช็คฝั่ง UI ว่าควร pop-up เตือนหรือยัง"""
    if not os.path.exists(_MARKET_STATS_FILE):
        return jsonify({"updated_at": None})
    from datetime import datetime as _dt
    mtime = os.path.getmtime(_MARKET_STATS_FILE)
    return jsonify({"updated_at": _dt.fromtimestamp(mtime).strftime("%Y-%m-%d")})


@app.route("/api/refresh-market-stats", methods=["POST"])
def refresh_market_stats():
    """อ่าน Table_PE.xls + Table_PBV.xls แล้วสร้าง set_market_stats.json ใหม่"""
    import math
    import numpy as np
    import pandas as pd
    from datetime import datetime as _dt

    PE_FILE  = os.path.join(BASE_DIR, "Table_PE.xls")
    PBV_FILE = os.path.join(BASE_DIR, "Table_PBV.xls")

    missing = [f for f in [PE_FILE, PBV_FILE] if not os.path.exists(f)]
    if missing:
        return jsonify({"ok": False, "error": f"ไม่พบไฟล์: {', '.join(os.path.basename(f) for f in missing)}"}), 400

    def read_xls(path):
        tables = pd.read_html(path, header=None)
        t = tables[1].copy()
        t.columns = t.iloc[0]
        t = t.iloc[1:].reset_index(drop=True)
        t.columns = [str(c).strip() for c in t.columns]
        return t

    def parse_ym(s):
        try:
            return _dt.strptime(str(s).strip(), "%b-%Y").strftime("%Y-%m")
        except Exception:
            return None

    def calc_stats(values):
        v = [x for x in values if x is not None and not (isinstance(x, float) and math.isnan(x))]
        if not v:
            return {}
        arr = sorted(v)
        avg = sum(v) / len(v)
        std = float(np.std(v))
        current = v[-1]
        pct = round(sum(1 for x in arr if x <= current) / len(arr) * 100, 1)
        zscore = round((current - avg) / std, 2) if std else 0
        return {
            "current": round(current, 2), "min": round(min(arr), 2),
            "max": round(max(arr), 2), "avg": round(avg, 2),
            "median": round(arr[len(arr)//2], 2), "std": round(std, 2),
            "zscore": zscore, "percentile": pct,
            "bands": {
                "+3σ": round(avg+3*std,2), "+2σ": round(avg+2*std,2),
                "+1σ": round(avg+std,2),   "-1σ": round(avg-std,2),
                "-2σ": round(avg-2*std,2), "-3σ": round(avg-3*std,2),
            },
        }

    def process(df):
        col_date = "Month-Year"
        data_cols = [c for c in df.columns if c != col_date]
        result = {"dates": [], "series": {c: [] for c in data_cols}}
        rows = []
        for _, row in df.iterrows():
            d = parse_ym(row[col_date])
            if not d:
                continue
            vals = {}
            for c in data_cols:
                try:
                    val = float(row[c])
                    vals[c] = round(val, 2) if not math.isnan(val) else None
                except Exception:
                    vals[c] = None
            rows.append((d, vals))
        rows.sort(key=lambda x: x[0])
        for d, vals in rows:
            result["dates"].append(d)
            for c in data_cols:
                result["series"][c].append(vals.get(c))
        return result

    try:
        pe_data  = process(read_xls(PE_FILE))
        pbv_data = process(read_xls(PBV_FILE))
    except Exception as e:
        return jsonify({"ok": False, "error": f"อ่านไฟล์ไม่สำเร็จ: {e}"}), 500

    # check if newer than current
    old_latest = None
    if os.path.exists(_MARKET_STATS_FILE):
        try:
            with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
                old = json.load(f)
            old_latest = old.get("pe", {}).get("dates", [None])[-1]
        except Exception:
            pass

    new_latest = pe_data["dates"][-1] if pe_data["dates"] else None

    output = {
        "updated_at": _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pe":  {"dates": pe_data["dates"],  "series": pe_data["series"],
                "stats": {k: calc_stats(v) for k, v in pe_data["series"].items()}},
        "pbv": {"dates": pbv_data["dates"], "series": pbv_data["series"],
                "stats": {k: calc_stats(v) for k, v in pbv_data["series"].items()}},
    }

    _atomic_write_json(_MARKET_STATS_FILE, output)

    pe_cur  = output["pe"]["stats"].get("SET", {})
    pbv_cur = output["pbv"]["stats"].get("SET", {})
    return jsonify({
        "ok": True,
        "new_data": new_latest != old_latest,
        "pe_range":  f"{pe_data['dates'][0]} – {pe_data['dates'][-1]}",
        "pbv_range": f"{pbv_data['dates'][0]} – {pbv_data['dates'][-1]}",
        "pe_months":  len(pe_data["dates"]),
        "pbv_months": len(pbv_data["dates"]),
        "pe_current":  pe_cur.get("current"),
        "pbv_current": pbv_cur.get("current"),
        "pe_zscore":   pe_cur.get("zscore"),
        "pbv_zscore":  pbv_cur.get("zscore"),
        "updated_at":  output["updated_at"],
        "old_latest":  old_latest,
        "new_latest":  new_latest,
    })


@app.route("/api/stock-valuation-stats")
def stock_valuation_stats():
    """คำนวณ cross-sectional PE/PBV stats รายตัวและรายกลุ่ม"""
    import math, statistics as st

    if not os.path.exists(DATA_FILE):
        return jsonify({"error": "ไม่มีข้อมูล"}), 404
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    stocks = data.get("stocks", [])

    def trim_outliers(vals, lo_pct=5, hi_pct=95):
        """ตัด outlier ด้วย percentile"""
        if not vals:
            return vals
        s = sorted(vals)
        n = len(s)
        lo = s[max(0, int(n * lo_pct / 100))]
        hi = s[min(n - 1, int(n * hi_pct / 100))]
        return [v for v in vals if lo <= v <= hi]

    def calc_dist(vals):
        v = trim_outliers([x for x in vals if x and x > 0])
        if len(v) < 3:
            return None
        avg = st.mean(v)
        std = st.pstdev(v)
        med = st.median(v)
        return {
            "n":      len(v),
            "avg":    round(avg, 2),
            "median": round(med, 2),
            "std":    round(std, 2),
            "min":    round(min(v), 2),
            "max":    round(max(v), 2),
            "bands": {
                "+3σ": round(avg + 3*std, 2),
                "+2σ": round(avg + 2*std, 2),
                "+1σ": round(avg + std,   2),
                "-1σ": round(avg - std,   2),
                "-2σ": round(avg - 2*std, 2),
                "-3σ": round(avg - 3*std, 2),
            },
        }

    # ── Market-wide ──────────────────────────────────────────────────────
    all_pe  = [s.get("pe")  for s in stocks]
    all_pbv = [s.get("pbv") for s in stocks]
    market = {
        "pe":  calc_dist(all_pe),
        "pbv": calc_dist(all_pbv),
    }

    # ── Per-sector ───────────────────────────────────────────────────────
    sector_map: dict = {}
    for s in stocks:
        sec = s.get("sector") or "Unknown"
        sector_map.setdefault(sec, {"pe": [], "pbv": [], "stocks": []})
        sector_map[sec]["pe"].append(s.get("pe"))
        sector_map[sec]["pbv"].append(s.get("pbv"))
        sector_map[sec]["stocks"].append(s.get("symbol"))

    sectors = {}
    for sec, d in sector_map.items():
        pd = calc_dist(d["pe"])
        bd = calc_dist(d["pbv"])
        if pd or bd:
            sectors[sec] = {"pe": pd, "pbv": bd, "n_stocks": len(d["stocks"])}

    # ── Per-stock z-scores ───────────────────────────────────────────────
    def zscore(val, dist):
        if not val or not dist or not dist["std"]:
            return None
        return round((val - dist["avg"]) / dist["std"], 2)

    stock_scores = []
    for s in stocks:
        sec = s.get("sector") or "Unknown"
        sd = sectors.get(sec, {})
        pe_z_mkt = zscore(s.get("pe"),  market["pe"])
        pe_z_sec = zscore(s.get("pe"),  sd.get("pe"))
        pbv_z_mkt = zscore(s.get("pbv"), market["pbv"])
        pbv_z_sec = zscore(s.get("pbv"), sd.get("pbv"))
        stock_scores.append({
            "symbol":    s.get("symbol"),
            "name":      s.get("name"),
            "sector":    sec,
            "pe":        s.get("pe"),
            "pbv":       s.get("pbv"),
            "pe_z_mkt":  pe_z_mkt,
            "pe_z_sec":  pe_z_sec,
            "pbv_z_mkt": pbv_z_mkt,
            "pbv_z_sec": pbv_z_sec,
        })

    return jsonify({
        "market":  market,
        "sectors": sectors,
        "stocks":  stock_scores,
    })


@app.route("/backtest-report")
def backtest_report():
    """รายงานผล backtest RS+RRG+Regime ฉบับเต็ม (static HTML)"""
    resp = send_file(os.path.join(BASE_DIR, "backtest_report.html"))
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/rotation-alerts")
def rotation_alerts():
    """Quadrant-change alerts ของ Rotation map — อ่านจาก rotation_state.json"""
    from services.rotation import load_state, CONFIRM_DAYS, DEAD_ZONE_PCT
    state = load_state(BASE_DIR)
    pending = []
    for key, e in state.get("groups", {}).items():
        p = e.get("pending")
        if p:
            gtype, name = key.split(":", 1)
            pending.append({"type": gtype, "name": name,
                            "from": e.get("confirmed"), "to": p["quadrant"],
                            "days": p["days"], "need": CONFIRM_DAYS,
                            "since": p["first_date"]})
    pending.sort(key=lambda x: -x["days"])
    return jsonify({
        "transitions":    state.get("transitions", [])[:20],
        "pending":        pending,
        "last_processed": state.get("last_processed"),
        "rules": {"confirm_days": CONFIRM_DAYS, "dead_zone_pct": DEAD_ZONE_PCT,
                  "axes": "x=ret_3m, y=ret_1m"},
    })


_breadth_cache: dict = {}

@app.route("/api/breadth")
def market_breadth():
    """Market Breadth รายวัน (% above EMA, NH/NL, McClellan)
    รับ query param ?range=1y|3y|5y|all (default 1y) — cache แยกต่อ range ใน memory,
    clear ทั้งหมดหลัง refresh"""
    from services.breadth import RANGE_DAYS
    rng = request.args.get("range", "1y")
    if rng not in RANGE_DAYS:
        rng = "1y"
    if _breadth_cache.get(rng):
        return jsonify(_breadth_cache[rng])
    try:
        from services.breadth import compute_breadth
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng])
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณา Full Refresh ก่อน"}), 404
        _breadth_cache[rng] = data
        return jsonify(data)
    except Exception as e:
        print(f"[Breadth] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


_market_internals_cache: dict = {}

@app.route("/api/market-internals")
def market_internals():
    """
    คำนวณ 52W New High / New Low count ต่อวัน ย้อนหลัง 63 วันทำการ (~3 เดือน)
    จาก SQLite price store — cache ผลลัพธ์ใน memory, expire เมื่อ Quick Update เสร็จ
    (result-cache ใช้ event invalidation: ถูก clear หลัง refresh — ไม่พึ่ง mtime)
    """
    if _market_internals_cache.get("data"):
        return jsonify(_market_internals_cache["data"])

    if not (price_store.db_exists(BASE_DIR)
            or os.path.exists(os.path.join(BASE_DIR, "set_history.json"))):
        return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณา Full Refresh ก่อน"}), 404

    try:
        import pandas as pd

        # สร้าง dict: ticker -> pd.Series ของ close (indexed by date string)
        all_series = {}
        for ticker, data in price_store.iter_all_series(BASE_DIR):
            dates  = data.get("dates", [])
            closes = data.get("closes", [])
            if len(dates) < 260 or len(closes) < 260:
                continue
            s = pd.Series(closes, index=pd.to_datetime(dates))
            all_series[ticker] = s

        if not all_series:
            return jsonify({"error": "ข้อมูลไม่เพียงพอ"}), 500

        # หาวันซื้อขายล่าสุด 63 วัน
        sample = next(iter(all_series.values()))
        trade_dates = sorted(sample.index[-70:])  # เผื่อ buffer
        recent_dates = trade_dates[-63:]

        new_high_counts = []
        new_low_counts  = []
        date_labels     = []

        for dt in recent_dates:
            nh = 0
            nl = 0
            for ticker, s in all_series.items():
                try:
                    loc = s.index.get_loc(dt)
                    if loc < 252:
                        continue
                    current_price = float(s.iloc[loc])
                    window_52w    = s.iloc[loc - 252 : loc]
                    if len(window_52w) < 200:
                        continue
                    if current_price >= float(window_52w.max()):
                        nh += 1
                    elif current_price <= float(window_52w.min()):
                        nl += 1
                except Exception:
                    continue
            new_high_counts.append(nh)
            new_low_counts.append(nl)
            date_labels.append(str(dt)[:10])

        result = {
            "dates":      date_labels,
            "new_highs":  new_high_counts,
            "new_lows":   new_low_counts,
        }
        _market_internals_cache["data"] = result
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _run_quick():
    try:
        import importlib
        sys.path.insert(0, BASE_DIR)
        from services import refresh as _refresh_svc
        importlib.reload(_refresh_svc)

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        _refresh_svc.run_quick_update(cb, BASE_DIR)
        _market_internals_cache.clear()
        _breadth_cache.clear()
        _insider_cache.clear()
        _major_cache.clear()

        # อัพเดท Indices
        _update(current=95, total=100, message="อัพเดท Indices...")
        try:
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv(existing, full_refresh=False)
            _indices_cache["data"] = result
        except Exception as e:
            print(f"[QuickUpdate] Indices error: {e}")

        # อัพเดท short sales + NVDR ประจำวัน
        _update(current=97, total=100, message="อัพเดท Short Sales...")
        short_sales_daily_update()
        _update(current=99, total=100, message="อัพเดท NVDR...")
        nvdr_daily_update()

        # อัพเดท Capital Flow
        try:
            _fetch_flow_data()
        except Exception as e:
            print(f"[QuickUpdate] Capital Flow error: {e}")

        _update(running=False, done=True, message="Quick Update เสร็จแล้ว!")

    except Exception as e:
        _update(running=False, done=True, error=str(e),
                message=f"เกิดข้อผิดพลาด: {e}")


# ============================================================
# SEC Insider / Major-Holder endpoints
# ============================================================

_insider_cache: dict = {}
_major_cache:   dict = {}
_SEC_CACHE_TTL = 6 * 3600   # 6 hours

@app.route("/api/insider-trades")
def insider_trades():
    """ดึงการซื้อขายหุ้นของผู้บริหาร (แบบ 59) — cache 6 ชม."""
    from datetime import datetime as _dt, timedelta as _td
    import re as _re

    days = int(request.args.get("days", 30))
    days = max(1, min(days, 365))
    cache_key = f"r59_{days}"

    cached = _insider_cache.get(cache_key)
    if cached and (time.time() - cached["ts"] < _SEC_CACHE_TTL):
        return jsonify(cached["data"])

    UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
    URL = "https://market.sec.or.th/public/idisc/th/r59"

    try:
        vs, vsg, ev = _sec_viewstate(URL, UA)
        date_to   = _dt.now()
        date_from = date_to - _td(days=days)
        df = _sec_post(URL, {
            "__VIEWSTATE": vs,
            "__VIEWSTATEGENERATOR": vsg,
            "__EVENTVALIDATION": ev,
            "ctl00$CPH$rblDateType": "T",
            "ctl00$CPH$BSDateFrom": _thai_date(date_from),
            "ctl00$CPH$BSDateTo":   _thai_date(date_to),
            "ctl00$CPH$btSearch":   "Search",
            "ctl00$CPH$ddlCompany": "",
        }, UA)

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
            action = "buy" if "ซื้อ" in method else \
                     "sell" if "ขาย" in method else "other"
            try:    qty = int(str(vals[5]).replace(",", ""))
            except: qty = 0
            try:
                price = float(str(vals[6]).replace(",", ""))
                if price != price: price = None  # NaN check
            except: price = None

            # parse date dd/mm/yyyy (Buddhist year)
            raw_date = str(vals[4]) if len(vals) > 4 else ""
            trade_date = ""
            m = _re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_date)
            if m:
                dd, mm, yy = m.groups()
                ce_year = int(yy) - 543
                trade_date = f"{ce_year}-{mm.zfill(2)}-{dd.zfill(2)}"

            records.append({
                "symbol":     sym,
                "company":    company,
                "name":       str(vals[1]),
                "relation":   str(vals[2]),
                "sec_type":   str(vals[3]),
                "trade_date": trade_date,
                "qty":        qty,
                "price":      price,
                "method":     method,
                "action":     action,
            })

        result = {
            "records": records,
            "days": days,
            "from": date_from.strftime("%Y-%m-%d"),
            "to":   date_to.strftime("%Y-%m-%d"),
            "fetched_at": _dt.now().strftime("%H:%M น. %d/%m/%Y"),
        }
        _insider_cache[cache_key] = {"ts": time.time(), "data": result}
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/major-changes")
def major_changes():
    """ดึงการเปลี่ยนแปลงผู้ถือหุ้นรายใหญ่ (แบบ 246-2) — cache 6 ชม."""
    from datetime import datetime as _dt, timedelta as _td
    import re as _re

    days = int(request.args.get("days", 30))
    days = max(1, min(days, 365))
    cache_key = f"r246_{days}"

    cached = _major_cache.get(cache_key)
    if cached and (time.time() - cached["ts"] < _SEC_CACHE_TTL):
        return jsonify(cached["data"])

    UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
    URL = "https://market.sec.or.th/public/idisc/th/r246"

    try:
        vs, vsg, ev = _sec_viewstate(URL, UA)
        date_to   = _dt.now()
        date_from = date_to - _td(days=days)
        df = _sec_post(URL, {
            "__VIEWSTATE": vs,
            "__VIEWSTATEGENERATOR": vsg,
            "__EVENTVALIDATION": ev,
            "ctl00$CPH$BsCompany": "",
            "ctl00$CPH$BsCompany_t": "",
            "ctl00$CPH$BsCompany_v": "",
            "ctl00$CPH$txtSearchPerson": "",
            "ctl00$CPH$rblDateType": "T",
            "ctl00$CPH$BSDateFrom": _thai_date(date_from),
            "ctl00$CPH$BSDateTo":   _thai_date(date_to),
            "ctl00$CPH$btSearch":   "Search",
        }, UA)

        records = []
        for _, row in df.iterrows():
            vals = row.tolist()
            if len(vals) < 5:
                continue
            sym = str(vals[0]).strip().upper()
            if not sym or sym == "NAN" or sym == "หลักทรัพย์":
                continue
            # symbol อาจมี format "GULF" โดยตรง หรือ "GULF (หุ้นสามัญ)"
            sym_clean = sym.split()[0].split("(")[0].strip()

            holder = str(vals[1])
            method = str(vals[2])
            action = "buy" if "ได้มา" in method else \
                     "sell" if "จำหน่าย" in method else "other"

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
                ce_year = int(yy) - 543
                trade_date = f"{ce_year}-{mm.zfill(2)}-{dd.zfill(2)}"

            records.append({
                "symbol":     sym_clean,
                "holder":     holder,
                "method":     method,
                "sec_type":   str(vals[3]) if len(vals) > 3 else "",
                "pct_before": pct_before,
                "pct_change": pct_change,
                "pct_after":  pct_after,
                "trade_date": trade_date,
                "action":     action,
            })

        result = {
            "records": records,
            "days": days,
            "from": date_from.strftime("%Y-%m-%d"),
            "to":   date_to.strftime("%Y-%m-%d"),
            "fetched_at": _dt.now().strftime("%H:%M น. %d/%m/%Y"),
        }
        _major_cache[cache_key] = {"ts": time.time(), "data": result}
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# Short Sales
# ============================================================

_SHORT_DATA_FILE = os.path.join(BASE_DIR, "short_sales_data.json")
_short_data_cache = None
_short_data_ts    = 0.0
_SHORT_DATA_TTL   = 3600  # re-read file ทุก 1 ชั่วโมง

def _load_short_data():
    global _short_data_cache, _short_data_ts
    if not os.path.exists(_SHORT_DATA_FILE):
        return None
    mtime = os.path.getmtime(_SHORT_DATA_FILE)
    if _short_data_cache and mtime == _short_data_ts:
        return _short_data_cache
    with open(_SHORT_DATA_FILE, encoding="utf-8") as f:
        _short_data_cache = json.load(f)
    _short_data_ts = mtime
    return _short_data_cache


def short_sales_daily_update():
    """ดึงข้อมูล short sales วันนี้จาก API แล้ว append ลง short_sales_data.json"""
    import urllib.request as _ur, ssl as _ssl
    from datetime import datetime as _dt

    if not os.path.exists(_SHORT_DATA_FILE):
        return

    try:
        ctx = _ssl._create_unverified_context()
        BASE = "https://www.set.or.th"
        main_req = _ur.Request(
            BASE + "/th/market/statistics/short-sales/total-short-sales",
            headers={"User-Agent": "Mozilla/5.0 Chrome/125.0"},
        )
        with _ur.urlopen(main_req, context=ctx, timeout=15) as r:
            cookie = r.getheader("Set-Cookie", "")

        hdr = {
            "User-Agent": "Mozilla/5.0 Chrome/125.0",
            "Accept": "application/json",
            "Referer": BASE + "/th/market/statistics/short-sales/total-short-sales",
            "Cookie": cookie,
        }
        req = _ur.Request(BASE + "/api/set/shortsales/statistics/list", headers=hdr)
        with _ur.urlopen(req, context=ctx, timeout=15) as r:
            resp = json.loads(r.read().decode("utf-8", "ignore"))

        trade_date = resp.get("tradingBeginDate", "")[:10]
        if not trade_date:
            return

        with open(_SHORT_DATA_FILE, encoding="utf-8") as f:
            data = json.load(f)

        stocks = data.get("stocks", {})
        updated = 0
        for item in resp.get("shortSales", []):
            sym = item.get("symbol")
            if not sym:
                continue
            snap = {
                "date":        trade_date,
                "pct_vol":     round(item.get("percentVolume") or 0, 4),    # API ให้เป็น % แล้ว (0-100)
                "pct_value":   round(item.get("percentValue") or 0, 4),     # API ให้เป็น % แล้ว (0-100)
                "short_pos":   int(item.get("totalShortPosition") or 0),
                "short_pos_pct": round(item.get("percentShortPosition") or 0, 4),  # API ส่งเป็น % แล้ว
            }
            if sym not in stocks:
                stocks[sym] = {
                    "period_vol": 0, "period_local_vol": 0, "period_nvdr_vol": 0,
                    "period_value": 0, "period_pct_value": 0,
                    "short_pos": 0, "short_pos_local": 0, "short_pos_nvdr": 0,
                    "short_pos_pct": 0, "daily": [],
                }
            # อัพเดท short_pos ปัจจุบัน
            stocks[sym]["short_pos"]     = snap["short_pos"]
            stocks[sym]["short_pos_pct"] = snap["short_pos_pct"]
            # append snapshot (ไม่ซ้ำวัน)
            daily = stocks[sym].setdefault("daily", [])
            if not daily or daily[-1].get("date") != trade_date:
                daily.append(snap)
                # เก็บแค่ 365 วัน
                if len(daily) > 365:
                    stocks[sym]["daily"] = daily[-365:]
            updated += 1

        # ── อัปเดตยอดสะสมรายงวด YTD จาก API เดียวกัน (fromDate/toDate) ──
        # แทนการ import Excel มือ — พิสูจน์แล้วว่าค่าตรงกับ Excel 100%
        # (API snap fromDate 01/01 ไปวันทำการแรกของปีให้เอง)
        try:
            y, m, d_ = trade_date[:4], trade_date[5:7], trade_date[8:10]
            # fromDate ต้องเป็น "วันทำการ" ไม่งั้น API ตอบ 400 — ใช้ period_from
            # เดิม (ปีเดียวกัน) ก่อน แล้วค่อยไล่หาวันทำการแรกของปี 1-10 ม.ค.
            candidates = []
            old_from = (data.get("period_from") or "")
            if old_from.startswith(y):
                candidates.append(f"{old_from[8:10]}/{old_from[5:7]}/{y}")
            candidates += [f"{dd:02d}/01/{y}" for dd in range(1, 11)]
            presp = None
            for fd in candidates:
                try:
                    purl = (BASE + "/api/set/shortsales/statistics/list"
                            + f"?fromDate={fd}&toDate={d_}/{m}/{y}")
                    with _ur.urlopen(_ur.Request(purl, headers=hdr),
                                     context=ctx, timeout=25) as r:
                        presp = json.loads(r.read().decode("utf-8", "ignore"))
                    break
                except Exception:
                    continue
            if presp is None:
                raise ValueError("ไม่พบ fromDate ที่ API ยอมรับ")
            p_from = (presp.get("tradingBeginDate") or "")[:10]
            p_to   = (presp.get("tradingEndDate") or "")[:10]
            p_items = presp.get("shortSales") or []
            if p_from and p_items:
                # ล้างยอดงวดเดิมก่อน (กันค้างข้ามปี/หุ้นที่ไม่มี short ในงวดใหม่)
                for s in stocks.values():
                    s["period_vol"] = s["period_local_vol"] = s["period_nvdr_vol"] = 0
                    s["period_value"] = s["period_pct_value"] = 0
                for item in p_items:
                    sym = item.get("symbol")
                    if not sym:
                        continue
                    s = stocks.setdefault(sym, {
                        "period_vol": 0, "period_local_vol": 0, "period_nvdr_vol": 0,
                        "period_value": 0, "period_pct_value": 0,
                        "short_pos": 0, "short_pos_local": 0, "short_pos_nvdr": 0,
                        "short_pos_pct": 0, "daily": [],
                    })
                    s["period_vol"]       = int(item.get("totalVolume") or 0)
                    s["period_local_vol"] = int(item.get("localVolume") or 0)
                    s["period_nvdr_vol"]  = int(item.get("nvdrVolume") or 0)
                    s["period_value"]     = round((item.get("totalValue") or 0) / 1e6, 2)
                    s["period_pct_value"] = round(item.get("percentValue") or 0, 4)
                    s["short_pos_local"]  = int(item.get("localShortPosition") or 0)
                    s["short_pos_nvdr"]   = int(item.get("nvdrShortPosition") or 0)
                data["period_from"] = p_from
                data["period_to"]   = p_to
                print(f"[short-sales] period YTD {p_from} -> {p_to}: {len(p_items)} stocks")
        except Exception as pe:
            print(f"[short-sales] period update failed (ใช้ยอดงวดเดิมไปก่อน): {pe}")

        data["stocks"]          = stocks
        data["last_api_update"] = trade_date
        _atomic_write_json(_SHORT_DATA_FILE, data)

        global _short_data_cache, _short_data_ts
        _short_data_cache = None  # invalidate cache
        print(f"[short-sales] updated {updated} stocks ({trade_date})")

    except Exception as e:
        print(f"[short-sales] daily update error: {e}")


# ── Capital Flow (siamchart.com) ─────────────────────────────────────────────
_flow_cache: dict = {}
_FLOW_CACHE_TTL = 4 * 3600

def _fetch_flow_data():
    """ดึงและ parse ข้อมูล Capital Flow จาก siamchart.com — อัพเดท _flow_cache"""
    import urllib.request as _ur, ssl as _ssl, re as _re, ast as _ast
    ctx = _ssl._create_unverified_context()
    req = _ur.Request(
        "https://siamchart.com/stock-summary/",
        headers={"User-Agent": "Mozilla/5.0 Chrome/125.0",
                 "Accept": "text/html,application/xhtml+xml"},
    )
    with _ur.urlopen(req, context=ctx, timeout=15) as r:
        html = r.read().decode("utf-8", "ignore")

    m = _re.search(r'var\s+market_data\s*=\s*(\[.*?\]);', html, _re.DOTALL)
    if not m:
        raise ValueError("ไม่พบ market_data ในหน้า siamchart")

    raw = _ast.literal_eval(m.group(1))

    rows = []
    for item in raw:
        if len(item) < 5:
            continue
        try:
            date_str = str(item[0])[:10]
            fund    = float(item[1]) if item[1] not in ('', None) else 0.0
            foreign = float(item[2]) if item[2] not in ('', None) else 0.0
            retail  = float(item[3]) if item[3] not in ('', None) else 0.0
            set_val = float(item[4]) if item[4] not in ('', None) else None
            rows.append({
                "date": date_str,
                "fund":    round(fund, 2),
                "foreign": round(foreign, 2),
                "retail":  round(retail, 2),
                "set":     set_val,
            })
        except (ValueError, TypeError):
            continue

    # siamchart ส่งข้อมูล newest-first — ต้อง sort เก่า→ใหม่ก่อน ไม่งั้น chg กลับเครื่องหมายและเลื่อนวัน
    rows.sort(key=lambda r: r["date"])
    for i in range(1, len(rows)):
        prev = rows[i-1]["set"]
        curr = rows[i]["set"]
        rows[i]["chg"] = round(curr - prev, 2) if prev and curr else None
    if rows:
        rows[0]["chg"] = None

    result = {"rows": rows, "fetched_at": time.strftime("%Y-%m-%d %H:%M")}
    _flow_cache["data"] = result
    _flow_cache["ts"]   = time.time()
    return result


@app.route("/api/market-flow")
def market_flow():
    """ดึงข้อมูล net buy/sell รายวัน จาก siamchart.com"""
    now = time.time()
    if _flow_cache.get("data") and now - _flow_cache.get("ts", 0) < _FLOW_CACHE_TTL:
        return jsonify(_flow_cache["data"])
    try:
        return jsonify(_fetch_flow_data())
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/short-sales")
def short_sales():
    data = _load_short_data()
    if not data:
        return jsonify({"error": "ไม่พบข้อมูล กรุณารัน import_short_sales.py ก่อน"}), 404

    stocks = data.get("stocks", {})
    # ส่งเฉพาะ field ที่ frontend ต้องการ (ไม่ส่ง daily array ทั้งหมด — ใหญ่เกินไป)
    out = {}
    for sym, v in stocks.items():
        daily = v.get("daily", [])
        out[sym] = {
            "period_vol":       v.get("period_vol", 0),
            "period_pct_value": v.get("period_pct_value", 0),
            "short_pos":        v.get("short_pos", 0),
            "short_pos_pct":    v.get("short_pos_pct", 0),
            "daily_count":      len(daily),
            "last_snap":        daily[-1] if daily else None,
            "prev_snap":        daily[-2] if len(daily) >= 2 else None,
        }

    return jsonify({
        "period_from":     data.get("period_from"),
        "period_to":       data.get("period_to"),
        "last_api_update": data.get("last_api_update"),
        "stocks":          out,
    })


@app.route("/api/short-sales/<symbol>")
def short_sales_symbol(symbol):
    """คืน daily snapshots ของหุ้นตัวเดียว (สำหรับ chart ใน popup)"""
    data = _load_short_data()
    if not data:
        return jsonify({"error": "no data"}), 404
    sym = symbol.upper()
    v = data.get("stocks", {}).get(sym)
    if not v:
        return jsonify({"error": "not found"}), 404
    return jsonify({**v, "symbol": sym,
                    "period_from": data.get("period_from"),
                    "period_to":   data.get("period_to")})


# ============================================================
# NVDR
# ============================================================

_NVDR_DATA_FILE  = os.path.join(BASE_DIR, "nvdr_data.json")
_nvdr_data_cache = None
_nvdr_data_ts    = 0.0

def _load_nvdr_data():
    global _nvdr_data_cache, _nvdr_data_ts
    if not os.path.exists(_NVDR_DATA_FILE):
        return None
    mtime = os.path.getmtime(_NVDR_DATA_FILE)
    if _nvdr_data_cache and mtime == _nvdr_data_ts:
        return _nvdr_data_cache
    with open(_NVDR_DATA_FILE, encoding="utf-8") as f:
        _nvdr_data_cache = json.load(f)
    _nvdr_data_ts = mtime
    return _nvdr_data_cache


def _fetch_nvdr_outstanding():
    """ดึง NVDR Outstanding Share จาก SET API"""
    import urllib.request as _ur, ssl as _ssl
    ctx = _ssl._create_unverified_context()
    BASE = "https://www.set.or.th"
    UA   = "Mozilla/5.0 Chrome/125.0"
    req  = _ur.Request(BASE + "/th/market/statistics/nvdr/outstanding-share",
                       headers={"User-Agent": UA})
    with _ur.urlopen(req, context=ctx, timeout=15) as r:
        cookie = r.getheader("Set-Cookie", "")
    hdr = {"User-Agent": UA, "Accept": "application/json",
           "Referer": BASE + "/th/market/statistics/nvdr/outstanding-share",
           "Cookie": cookie}
    req2 = _ur.Request(BASE + "/api/set/nvdr-trade/outstanding-share", headers=hdr)
    with _ur.urlopen(req2, context=ctx, timeout=15) as r:
        d = json.loads(r.read().decode("utf-8", "ignore"))
    return d.get("date", "")[:10], d.get("outstandings", [])


def nvdr_daily_update():
    """อัพเดท NVDR data ประจำวัน — เรียกตอน Quick Update"""
    from datetime import datetime as _dt
    try:
        trade_date, items = _fetch_nvdr_outstanding()
        if not trade_date or not items:
            return

        # โหลดหรือสร้างไฟล์ใหม่
        if os.path.exists(_NVDR_DATA_FILE):
            with open(_NVDR_DATA_FILE, encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"updated_at": None, "stocks": {}}

        stocks = data.get("stocks", {})
        updated = 0
        for item in items:
            sym  = item.get("symbol")
            if not sym:
                continue
            pct  = item.get("percentOfPaidUpCapital") or 0
            shr  = int(item.get("nvdrInvestment") or 0)
            paid = int(item.get("paidUpCapitalShares") or 0)
            snap = {"date": trade_date, "nvdr_pct": round(pct, 4), "nvdr_shares": shr}

            if sym not in stocks:
                stocks[sym] = {"nvdr_pct": 0, "nvdr_shares": 0,
                               "paid_up_shares": paid, "daily": []}
            stocks[sym]["nvdr_pct"]      = round(pct, 4)
            stocks[sym]["nvdr_shares"]   = shr
            stocks[sym]["paid_up_shares"] = paid

            daily = stocks[sym].setdefault("daily", [])
            if not daily or daily[-1].get("date") != trade_date:
                daily.append(snap)
                if len(daily) > 365:
                    stocks[sym]["daily"] = daily[-365:]
            updated += 1

        data["stocks"]     = stocks
        data["updated_at"] = trade_date
        _atomic_write_json(_NVDR_DATA_FILE, data)
        global _nvdr_data_cache
        _nvdr_data_cache = None
        print(f"[nvdr] updated {updated} stocks ({trade_date})")

    except Exception as e:
        print(f"[nvdr] update error: {e}")


@app.route("/api/prices")
def get_prices():
    """คืน {symbol: price} ทุกหุ้น (SET + DR underlying) แบบ lightweight — ใช้โดย price alert checker"""
    prices = {}
    updated_at = None

    # SET stocks from set_data.json
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                data = json.load(f)
            for s in data.get("stocks", []):
                if s.get("symbol") and s.get("price") is not None:
                    prices[s["symbol"]] = s["price"]
            updated_at = data.get("updated_at")
        except Exception:
            pass

    # DR underlying stocks from cache (ราคา foreign currency)
    dr_result = _dr_cache.get("result")
    if dr_result:
        for s in dr_result.get("stocks", []):
            if s.get("sym") and s.get("price") is not None:
                prices["DR:" + s["sym"]] = s["price"]

    if not prices:
        return jsonify({"error": "no data"}), 404
    return jsonify({"prices": prices, "updated_at": updated_at})


@app.route("/api/nvdr")
def nvdr_summary():
    """คืน NVDR% ทุกหุ้น (compact) + prev/last snap"""
    data = _load_nvdr_data()
    if not data:
        # ดึงสดถ้ายังไม่มีไฟล์
        try:
            trade_date, items = _fetch_nvdr_outstanding()
            out = {}
            for item in items:
                sym = item.get("symbol")
                if not sym: continue
                pct = item.get("percentOfPaidUpCapital") or 0
                out[sym] = {"nvdr_pct": round(pct, 4),
                            "nvdr_shares": int(item.get("nvdrInvestment") or 0),
                            "daily_count": 0, "last_snap": None, "prev_snap": None}
            return jsonify({"updated_at": trade_date, "stocks": out})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    stocks = data.get("stocks", {})
    out = {}
    for sym, v in stocks.items():
        daily = v.get("daily", [])
        out[sym] = {
            "nvdr_pct":    v.get("nvdr_pct", 0),
            "nvdr_shares": v.get("nvdr_shares", 0),
            "daily_count": len(daily),
            "last_snap":   daily[-1] if daily else None,
            "prev_snap":   daily[-2] if len(daily) >= 2 else None,
            # 21 snapshots ล่าสุดแบบ compact [date, pct, shares] —
            # ให้ frontend คำนวณ delta สะสม 5/20 snapshots ได้
            "daily_tail":  [[d.get("date"), d.get("nvdr_pct"), d.get("nvdr_shares", 0)]
                            for d in daily[-21:]],
        }
    return jsonify({"updated_at": data.get("updated_at"), "stocks": out})


@app.route("/api/nvdr/<symbol>")
def nvdr_symbol(symbol):
    """คืน NVDR history ของหุ้นตัวเดียว"""
    data = _load_nvdr_data()
    if not data:
        return jsonify({"error": "no data"}), 404
    sym = symbol.upper()
    v = data.get("stocks", {}).get(sym)
    if not v:
        return jsonify({"error": "not found"}), 404
    return jsonify({**v, "symbol": sym, "updated_at": data.get("updated_at")})


# ============================================================
# Main
# ============================================================

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


def _wait_port_free(port, timeout=12):
    """Port guard: Windows ยอมให้ bind ซ้อนได้ (SO_REUSEADDR) ทำให้เกิด
    server ผีหลายตัวแล้ว request วิ่งเข้าตัวเก่า — เช็คก่อน start
    รอสั้นๆ เผื่อเป็นจังหวะ /api/restart ที่ตัวเก่ากำลังจะปิด (0.8s)"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            busy = s.connect_ex(("127.0.0.1", port)) == 0
        finally:
            s.close()
        if not busy:
            return True
        time.sleep(0.5)
    return False


if __name__ == "__main__":
    port = 5001

    if not _wait_port_free(port):
        print(f"[!] พอร์ต {port} มี server อื่นรันอยู่แล้ว — ไม่ start ซ้อน")
        print(f"    ปิดตัวเก่าก่อน (Task Manager -> python) หรือใช้ตัวที่รันอยู่ได้เลย")
        sys.exit(1)

    local_ip = get_local_ip()

    # โหลด DR cache จากไฟล์ก่อนเริ่ม server
    _load_dr_cache_from_file()

    print("=" * 50)
    print("  SET Dashboard Server")
    print("=" * 50)
    print(f"  Local:   http://localhost:{port}")
    print(f"  Network: http://{local_ip}:{port}  (iPad/mobile)")
    print("=" * 50)
    print("  Press Ctrl+C to stop\n")

    try:
        from waitress import serve
        print("  Server: waitress (production WSGI, 12 threads)\n")
        logging.getLogger("waitress").setLevel(logging.INFO)
        # channel_timeout สูงเพื่อ SSE /api/progress ที่เปิดค้างระหว่าง
        # Full Refresh (10+ นาที)
        serve(app, host="0.0.0.0", port=port, threads=12, channel_timeout=1200)
    except ImportError:
        print("  Server: Flask dev (ติดตั้ง waitress เพื่อใช้ production server)\n")
        app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
