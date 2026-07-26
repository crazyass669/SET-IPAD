"""
SET Dashboard — Flask Web Server
รัน: python app.py
หรือดับเบิ้ลคลิก start.bat
"""

import json
import os
import random
import re
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
from core import run_log
from core import delisted_log

# Band cache — เก็บผล mrlikestock.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_band_cache: dict = {}
_BAND_CACHE_TTL = 6 * 3600

# DR cache — เก็บราคา underlying foreign stocks ไว้ 4 ชั่วโมง
_dr_cache: dict = {}
_DR_CACHE_TTL = 4 * 3600

# DR diff-check cache — ผล /api/set/dr/list เทียบ _DR_STATIC ไว้ 6 ชั่วโมง (กดเช็คบ่อยไม่ต้องยิง SET.or.th ซ้ำ)
_dr_diff_cache: dict = {}
_DR_DIFF_CACHE_TTL = 6 * 3600

# US index (S&P500/Dow/Nasdaq100) diff-check cache — เทียบ Wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_us_index_diff_cache: dict = {}
_US_INDEX_DIFF_CACHE_TTL = 6 * 3600

# HK index (HSI/HSCEI/HSTECH) diff-check cache — เทียบ Wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_hk_index_diff_cache: dict = {}
_HK_INDEX_DIFF_CACHE_TTL = 6 * 3600

# JP index (Nikkei 225) diff-check cache — เทียบ ja.wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_jp_index_diff_cache: dict = {}
_JP_INDEX_DIFF_CACHE_TTL = 6 * 3600

# Heatmap cache (mkt_cap + % เปลี่ยนแปลงรายวันของหุ้นในแต่ละดัชนี) — แยก slot ต่อดัชนี
# TTL สั้นกว่า cache อื่นเพราะ % เปลี่ยนแปลงต้องสดพอสมควร แต่ไม่สดเกินจนต้องยิง Yahoo ทุกครั้งที่เปิดหน้า
_heatmap_cache: dict = {}
_HEATMAP_CACHE_TTL = 15 * 60

# Heatmap HK cache — เหมือน _heatmap_cache ของ US แต่ % เปลี่ยนแปลงคำนวณจาก hk_prices.db
# ที่มีอยู่แล้ว (ไม่ยิง Yahoo สด) เลยไม่จำเป็นต้องสั้นเท่า US — ใช้ TTL เดียวกันเผื่ออนาคต
_hk_heatmap_cache: dict = {}
_HK_HEATMAP_CACHE_TTL = 15 * 60

# Heatmap JP cache — ดัชนีเดียว (NIKKEI225) ต่างจาก US/HK ที่มีหลายดัชนีให้เลือก แต่ pattern
# เดียวกับ HK (chg_1d/chg_1w จาก jp_prices.db ในเครื่อง, mkt_cap ยิง Yahoo สด cache แยก)
_jp_heatmap_cache: dict = {}
_JP_HEATMAP_CACHE_TTL = 15 * 60

# ข่าวรายหุ้น (รวม SET.or.th + Yahoo + Google News) — cache ต่อ (symbol, is_dr) 15 นาที
# ข่าวไม่ต้องสดวินาทีต่อวินาที แต่ก็ไม่ควรยิง 3 แหล่งซ้ำทุกครั้งที่พิมพ์ค้นหา
_stock_news_cache: dict = {}
_STOCK_NEWS_CACHE_TTL = 15 * 60

# Market cap เปลี่ยนช้ามาก (ต่างจากราคา) — cache แยกลงไฟล์ต่างหาก อายุ 1 วัน ลดเวลาโหลด
# ครั้งแรกจาก ~30-60 วิ (ยิง fast_info ทีละ ticker) เหลือแค่เวลา batch ราคา ~5 วิ
_MKTCAP_CACHE_TTL = 24 * 3600


def _mktcap_cache_path():
    return os.path.join(BASE_DIR, "data", "us_heatmap_mktcap_cache.json")


def _load_mktcap_cache():
    try:
        with open(_mktcap_cache_path(), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_mktcap_cache(data):
    p = _mktcap_cache_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, p)


def _hk_mktcap_cache_path():
    return os.path.join(BASE_DIR, "data", "hk_heatmap_mktcap_cache.json")


def _load_hk_mktcap_cache():
    try:
        with open(_hk_mktcap_cache_path(), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_hk_mktcap_cache(data):
    p = _hk_mktcap_cache_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, p)


def _jp_mktcap_cache_path():
    return os.path.join(BASE_DIR, "data", "jp_heatmap_mktcap_cache.json")


def _load_jp_mktcap_cache():
    try:
        with open(_jp_mktcap_cache_path(), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_jp_mktcap_cache(data):
    p = _jp_mktcap_cache_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, p)

# Financials cache — งบการเงิน cache 24 ชั่วโมง (ข้อมูลไม่เปลี่ยนบ่อย)
_fin_cache: dict = {}
_FIN_CACHE_TTL = 24 * 3600

# P/E-P/BV รายวันของตลาด (scrape จากหน้า overview ของ SET.or.th) — cache 3 ชม.
# พอ (ตัวเลขอัพเดทแค่วันละครั้งหลังตลาดปิดฝั่ง SET เอง กดถี่กว่านั้นก็ได้ค่าเดิม)
_set_daily_val_cache: dict = {"result": None, "ts": 0}
_SET_DAILY_VAL_TTL = 3 * 3600

# Financials analytics cache — growth score/PEG/FCF yield ทั้งตลาด (bulk)
# event-invalidate ตอน sync งบการเงินเสร็จ + TTL 24h กันค้างข้าม restart
_fin_analytics_cache: dict = {}
_FIN_ANALYTICS_CACHE_TTL = 24 * 3600

# Indices cache — ดัชนีราคากลุ่ม SET/MAI cache 4 ชั่วโมง
_indices_cache: dict = {}
_INDICES_CACHE_TTL = 4 * 3600

from flask import Flask, jsonify, send_file, Response, request

# สูตรคำนวณกลาง — ห้าม copy สูตรมาวางในไฟล์นี้ ให้ import จาก core.metrics เท่านั้น
from core.metrics import calc_rs_raw, calc_ema

# HTTP clients / static universe — แยกไว้ที่ sources/ (Phase 2 refactor)
from sources.tradingview import INDEX_INFO, _yf_to_tv, _fetch_tv_bars
from sources.dr_universe import _DR_STATIC, is_latest_bar_stable, region_today_date, load_dr_universe, sync_dr_universe
from sources import sec_store
from sources import financials_store
from sources import factor_snapshot
from sources import dr_descriptions
from sources import us_index_membership
from sources import hk_index_membership


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


_price_analytics_cache: dict = {}   # symbol -> (ts, result)
_PRICE_ANALYTICS_TTL = 6 * 3600     # ราคาไม่เปลี่ยนระหว่างวันมากพอจะต้องคำนวณใหม่ถี่


@app.route("/api/price-analytics/<symbol>")
def price_analytics_endpoint(symbol):
    """วิเคราะห์ราคาระยะยาว (seasonality/drawdown/CAGR) จาก set_prices.db ย้อนถึง 1983
    — ใช้ Adj Close (return) + close ดิบ (drawdown/ATH) cache 6 ชม. ต่อหุ้น"""
    from sources import price_analytics
    sym = symbol.upper().strip()
    cached = _price_analytics_cache.get(sym)
    if cached and (time.time() - cached[0] < _PRICE_ANALYTICS_TTL):
        return jsonify(cached[1])
    result = price_analytics.build_for_symbol(BASE_DIR, sym + ".BK")
    if not result:
        return jsonify({"error": f"ข้อมูลราคาไม่พอวิเคราะห์ {symbol} (ต้องมีอย่างน้อย ~1 ปี)"}), 404
    _price_analytics_cache[sym] = (time.time(), result)
    return jsonify(result)


@app.route("/api/price-analytics-yf/<yf_ticker>")
def price_analytics_yf_endpoint(yf_ticker):
    """เหมือน /api/price-analytics แต่สำหรับหุ้น DR — ไม่มี set_prices.db ของหุ้นต่างประเทศ
    เก็บไว้ในเครื่อง เลยดึงราคาย้อนหลังสดจาก yfinance (ตัวเดียวกับที่ /api/dr-history ใช้
    วาดกราฟ Max อยู่แล้ว) แล้วป้อนเข้า price_analytics.analyze() ตัวเดียวกับหุ้นไทย —
    cache 6 ชม. ต่อ ticker กันยิง yfinance ซ้ำถี่ๆ"""
    from sources import price_analytics
    yft = yf_ticker.upper().strip()
    cache_key = f"YF:{yft}"
    cached = _price_analytics_cache.get(cache_key)
    if cached and (time.time() - cached[0] < _PRICE_ANALYTICS_TTL):
        return jsonify(cached[1])
    try:
        import yfinance as yf
        hist = yf.Ticker(yft).history(period="max", auto_adjust=False)
        if hist.empty:
            return jsonify({"error": f"ไม่พบข้อมูลราคา {yft}"}), 404
        nz = lambda v: round(float(v), 6) if v == v else None   # v==v -> False เฉพาะ NaN
        adj_col = hist["Adj Close"] if "Adj Close" in hist.columns else hist["Close"]
        ohlc = {
            "dates": [str(d)[:10] for d in hist.index],
            "closes": [nz(c) for c in hist["Close"]],
            "adj_closes": [nz(c) for c in adj_col],
        }
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    result = price_analytics.analyze(ohlc)
    if not result:
        return jsonify({"error": f"ข้อมูลราคาไม่พอวิเคราะห์ {yft} (ต้องมีอย่างน้อย ~1 ปี)"}), 404
    _price_analytics_cache[cache_key] = (time.time(), result)
    return jsonify(result)


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


def _dr_light(result, refreshing=False):
    """ตัด dates/closes (ประวัติราคาเต็มทุกวัน — ~98% ของ payload 33MB) ออกจาก response
    /api/dr — frontend ใช้แค่ close100 (sparkline) + ohlc30 (แท่งเทียนย่อ) ส่วนกราฟ
    full history ดึงแยกรายตัวจาก /api/dr-history ซึ่งอ่านจาก _dr_cache ฝั่ง server
    (cache ในหน่วยความจำ/ไฟล์ยังเก็บเต็มเหมือนเดิม — quick-update ก็ใช้ dates ต่อได้)"""
    out = {"stocks": [{k: v for k, v in s.items() if k not in ("dates", "closes", "_full_vols")}
                      for s in result.get("stocks", [])],
           "ts": result.get("ts")}
    if refreshing:
        out["refreshing"] = True   # ให้ UI ติดป้าย 'กำลังอัพเดทเบื้องหลัง' แล้ว poll ซ้ำ
    return out


_dr_rebuild_lock = threading.Lock()


def _kick_dr_rebuild():
    """rebuild DR cache ใน background thread — ไม่ start ซ้อนถ้ามีตัวหนึ่งรันอยู่แล้ว"""
    if _dr_rebuild_lock.locked():
        return

    def _bg():
        try:
            _rebuild_dr_cache()
            print("[DR] background refresh เสร็จ")
        except Exception as e:
            print(f"[DR] background refresh ล้มเหลว: {e}")

    threading.Thread(target=_bg, daemon=True).start()


@app.route("/api/dr")
def get_dr_data():
    """ดึงราคา underlying foreign stocks ของ DR/DRx ทั้งหมด — cache 4 ชั่วโมง

    cache หมดอายุ: ตอบข้อมูลเก่าทันที (flag refreshing=true) แล้ว rebuild เบื้องหลัง
    (stale-while-revalidate) — ผู้ใช้ไม่ต้องรอ 1-2 นาทีเหมือนเดิมอีก
    ?fresh=1 = บังคับทำสดแบบ blocking (ใช้ตอน bake static site — ห้ามได้ข้อมูลเก่า)"""
    fresh = request.args.get("fresh") == "1"
    cached = _dr_cache.get("result")
    if not fresh and cached and cached.get("stocks") and _dr_cache.get("ts"):
        age = time.time() - _dr_cache["ts"]
        if age < _DR_CACHE_TTL:
            return jsonify(_dr_light(cached))
        _kick_dr_rebuild()
        return jsonify(_dr_light(cached, refreshing=True))

    # ไม่มี cache เลย (รันครั้งแรกสุดของเครื่อง) หรือ fresh=1 — ทำสดแบบ blocking
    try:
        result = _rebuild_dr_cache()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(_dr_light(result))


def _rebuild_dr_cache():
    """ดึงราคา+คำนวณ DR ทั้ง universe แล้วอัพเดท _dr_cache + dr_cache.json — คืน result เต็ม
    lock กันรันซ้อน (เรียกได้ทั้งจาก request ตรงและ background thread)"""
    with _dr_rebuild_lock:
        # อีก thread เพิ่ง rebuild เสร็จระหว่างเรารอ lock — ใช้ผลนั้นเลย ไม่ยิงซ้ำ
        if (_dr_cache.get("result") and _dr_cache.get("ts")
                and time.time() - _dr_cache["ts"] < 120):
            return _dr_cache["result"]
        return _dr_do_rebuild()


def _dr_do_rebuild():
    import yfinance as yf
    import pandas as pd
    from datetime import datetime as _dt

    # sync ลิสต์ DR กับ SET ก่อนดึงราคา — DR/underlying ออกใหม่ถูกเพิ่มอัตโนมัติ
    # (ล้มเหลวก็ไม่เป็นไร ใช้ universe เดิมไปก่อน)
    try:
        _sync_stats = sync_dr_universe(BASE_DIR)
        if _sync_stats.get("appended") or _sync_stats.get("added"):
            print(f"[DR-sync] อัปเดตลิสต์: series ใหม่ {_sync_stats['appended']}, "
                  f"underlying ใหม่ {_sync_stats['added']}, ยัง map ไม่ได้ {_sync_stats['unmapped']}")
    except Exception as e:
        print(f"[DR-sync] ข้ามรอบนี้ (sync ล้มเหลว): {e}")
    _dr_universe = load_dr_universe(BASE_DIR)

    yf_tickers = list({s["yf"] for s in _dr_universe})

    raw = yf.download(
        yf_tickers,
        period="max",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    # market cap: เดิมยิง fast_info ทีละตัวแบบ sequential ~283 รอบในลูปด้านล่าง
    # (~1-2 นาที — ตัวการหลักที่ทำให้โหลดหน้า DR ครั้งแรกช้า) — เปลี่ยนเป็นขนาน
    # 12 threads เหลือ ~15 วิ และถ้าดึงพลาดใช้ค่ารอบก่อนจาก cache แทน
    # (market cap เปลี่ยนช้า ค่าเก่าอายุไม่กี่ชั่วโมงใช้แทนได้สบาย)
    from concurrent.futures import ThreadPoolExecutor

    def _mc_one(t):
        try:
            v = getattr(yf.Ticker(t).fast_info, "market_cap", None)
            return t, (float(v) if v else None)
        except Exception:
            return t, None

    mkt_map = {}
    try:
        with ThreadPoolExecutor(max_workers=12) as _mc_ex:
            mkt_map = dict(_mc_ex.map(_mc_one, yf_tickers))
    except Exception as e:
        print(f"[DR] market cap batch ล้มเหลว (ใช้ค่าเก่าจาก cache): {e}")
    _old_mc = {s["sym"]: s.get("mkt_cap")
               for s in (_dr_cache.get("result") or {}).get("stocks", [])}

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
    for stock in _dr_universe:
        yticker = stock["yf"]
        try:
            close  = _series(yticker, "Close")
            open_s = _series(yticker, "Open")
            high_s = _series(yticker, "High")
            low_s  = _series(yticker, "Low")
            vol_s  = _series(yticker, "Volume")
            if len(close) < 2:
                continue

            # ตลาดยังไม่ปิดจริง/ยังอยู่ pre-market-after-hours -> แท่งล่าสุดยังไม่นิ่ง
            # (Yahoo เอาราคาที่กำลังไหลไปทับ Close ของแท่งนี้เรื่อยๆ) ตัดทิ้งไปใช้
            # แท่งก่อนหน้าที่ freeze แล้วแทน ให้ตรงกับ "ราคาปิด" จริงๆ — แต่เก็บค่าที่ยัง
            # ไม่นิ่งไว้แยกเป็น live_price/live_chg ก่อนตัด เพื่อโชว์เป็น "ราคาล่าสุด
            # (ไม่นิ่ง)" คู่กับราคาปิดที่นิ่งแทนที่จะบังคับเลือกออกันข้าง (ผู้ใช้ขอ)
            #
            # ตัดเฉพาะตอนแท่งล่าสุดเป็นของ "วันนี้จริง" ในตลาดนั้นเท่านั้น — ยืนยันบั๊กจริง:
            # หุ้น/ETF ปริมาณเบามาก (เช่น 3422.HK, FUEKIVND.VN) บางวันไม่มีเทรดเลยจนตลาด
            # ปิด แท่งล่าสุดที่ Yahoo มีให้จึงเป็นของ "เมื่อวาน" (ปิดแล้วจริง ไม่ใช่ราคาไหล)
            # ถ้าตัดทิ้งแบบเดิม (เช็คแค่ตลาดกำลังเปิดอยู่ไหม ไม่เช็ควันที่ของแท่ง) จะเผลอ
            # ตัดราคาปิดจริงทิ้ง เหลือ <2 แท่ง แล้ว skip ทั้งตัว ทำให้ราคาค้างไม่อัพเดทถาวร
            live_price = None
            if not is_latest_bar_stable(stock["region"]) and len(close):
                last_bar_date  = close.index[-1].date()
                today_in_region = region_today_date(stock["region"])
                if today_in_region is not None and last_bar_date == today_in_region:
                    live_price = float(close.iloc[-1])
                    close  = close.iloc[:-1]
                    if len(open_s): open_s = open_s.iloc[:-1]
                    if len(high_s): high_s = high_s.iloc[:-1]
                    if len(low_s):  low_s  = low_s.iloc[:-1]
                    if len(vol_s):  vol_s  = vol_s.iloc[:-1]
                    if len(close) < 2:
                        continue

            price = float(close.iloc[-1])
            prev  = float(close.iloc[-2])
            chg   = (price - prev) / prev * 100
            live_chg = round((live_price - price) / price * 100, 2) if live_price and price else None

            close100 = [round(float(x), 4) for x in close.tail(100).tolist()]

            # เก็บ full price history สำหรับ chart popup (date + price)
            dates_all  = [str(d)[:10] for d in close.index.tolist()]
            closes_all = [round(float(x), 6) for x in close.tolist()]

            # above_ema50/200 + price/vol_history ~500/260 แท่ง (เหมือนหุ้นไทยใน
            # set_data_fetcher.process_stock) — ให้ Screener ฝั่ง client เรียก
            # _enrichTechSignals() แบบเดียวกับหุ้นไทยได้ (EMA/SMA cross, RSI rebound,
            # bullish volume) แทนที่จะ hardcode null ทิ้งแบบเดิม (EMA200 ต้องการ
            # warmup ~300 แท่งหลัง seed จึงจะ converge ถูกต้อง เลยเก็บยาวกว่า close100)
            ema50  = calc_ema(close, 50)
            ema200 = calc_ema(close, 200)
            above_ema50  = bool(price > ema50)  if ema50  is not None else None
            above_ema200 = bool(price > ema200) if ema200 is not None else None
            _hist_bars = min(len(close), 500)
            price_history = [
                [d, round(float(p), 4 if p < 1 else 2)]
                for d, p in zip(dates_all[-_hist_bars:], close.tail(_hist_bars).tolist())
            ]
            vol_history = [int(v) for v in vol_s.tail(260).tolist()] if len(vol_s) else []
            vol_today   = int(vol_s.iloc[-1]) if len(vol_s) else None
            vol_avg20   = int(vol_s.tail(21).iloc[:-1].mean()) if len(vol_s) >= 21 else None

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

            # Market cap จาก batch ขนานด้านบน — พลาดก็ใช้ค่ารอบก่อน (best-effort)
            mkt_cap = mkt_map.get(yticker)
            if mkt_cap is None:
                mkt_cap = _old_mc.get(stock["sym"])

            results.append({
                "sym":      stock["sym"],
                "name":     stock["name"],
                "region":   stock["region"],
                "ind":      stock["ind"],
                "yf":       stock["yf"],
                "price":    round(price, 2),
                "chg":      round(chg, 2),
                "live_price": round(live_price, 2) if live_price is not None else None,
                "live_chg":   live_chg,
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
                "etf":      stock.get("etf", False),
                "close100": close100,
                "ohlc30":   ohlc30,
                "dates":    dates_all,
                "closes":   closes_all,
                "above_ema50":   above_ema50,
                "above_ema200":  above_ema200,
                "price_history": price_history,
                "vol_history":   vol_history,
                "vol_today":     vol_today,
                "vol_avg20":     vol_avg20,
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
    return result


@app.route("/api/dr/check-updates")
def dr_check_updates():
    """เทียบ DR ที่ซื้อขายอยู่จริงบน SET.or.th (/api/set/dr/list) กับ _DR_STATIC
    ที่ curate ด้วยมือ — รายงานตัวใหม่/ตัวที่ถูกถอดเท่านั้น ไม่แก้ _DR_STATIC ให้อัตโนมัติ
    (ยังต้องมีคนใส่ industry/region/yf ticker ให้ครบก่อนเพิ่มจริง)"""
    cached = _dr_diff_cache.get("result")
    if cached and (time.time() - _dr_diff_cache.get("ts", 0) < _DR_DIFF_CACHE_TTL):
        return jsonify(cached)
    try:
        from sources.dr_universe import check_dr_diff
        result = check_dr_diff(BASE_DIR)
        _dr_diff_cache["result"] = result
        _dr_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
            _universe = load_dr_universe(BASE_DIR)
            yf_tickers = list({s["yf"] for s in _universe})

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
            for st in _universe:
                sym, yticker = st["sym"], st["yf"]
                try:
                    close  = _series(yticker, "Close")
                    open_s = _series(yticker, "Open")
                    high_s = _series(yticker, "High")
                    low_s  = _series(yticker, "Low")
                    vol_s  = _series(yticker, "Volume")
                    if len(close) < 2:
                        continue

                    # แท่งล่าสุดยังไม่นิ่ง (pre-market/after-hours) -> ตัดทิ้ง
                    # กันดันค่าที่ยังไหลอยู่เข้า history ถาวร (รอบถัดไปค่อยดึงใหม่)
                    # แต่เก็บไว้แยกเป็น live_price/live_chg ก่อนตัด — โชว์เป็น "ราคาล่าสุด
                    # (ไม่นิ่ง)" คู่กับราคาปิดที่นิ่งแทนการบังคับเลือกอย่างใดอย่างหนึ่ง
                    #
                    # ตัดเฉพาะตอนแท่งล่าสุดเป็นของ "วันนี้จริง" เท่านั้น — ยืนยันบั๊กจริง:
                    # หุ้น/ETF ปริมาณเบามาก (3422.HK, FUEKIVND.VN) บางวันไม่มีเทรดเลย
                    # แท่งล่าสุดที่ได้จึงเป็นของ "เมื่อวาน" (ปิดแล้วจริง) เดิมเช็คแค่ตลาด
                    # กำลังเปิดอยู่ไหม ไม่เช็ควันที่ของแท่ง เลยตัดราคาปิดจริงทิ้งจนเหลือ
                    # <2 แท่ง แล้ว skip ทั้งตัว → ราคาค้างไม่อัพเดทถาวรทุกรอบ (นี่คือสาเหตุ
                    # ที่ผู้ใช้รายงานว่าบางหุ้นไม่อัพเดทราคาเลย)
                    live_price = None
                    if not is_latest_bar_stable(st["region"]) and len(close):
                        last_bar_date   = close.index[-1].date()
                        today_in_region = region_today_date(st["region"])
                        if today_in_region is not None and last_bar_date == today_in_region:
                            live_price = float(close.iloc[-1])
                            close  = close.iloc[:-1]
                            if len(open_s): open_s = open_s.iloc[:-1]
                            if len(high_s): high_s = high_s.iloc[:-1]
                            if len(low_s):  low_s  = low_s.iloc[:-1]
                            if len(vol_s):  vol_s  = vol_s.iloc[:-1]
                            if len(close) < 2:
                                continue

                    price = float(close.iloc[-1])
                    prev  = float(close.iloc[-2])
                    chg   = round((price - prev) / prev * 100, 2) if prev else 0
                    live_chg = round((live_price - price) / price * 100, 2) if live_price and price else None

                    entry = stock_map.get(sym)
                    if entry:
                        entry["price"] = round(price, 2)
                        entry["chg"]   = chg
                        if live_price is not None:
                            entry["live_price"] = round(live_price, 2)
                            entry["live_chg"]   = live_chg
                        else:
                            entry.pop("live_price", None)
                            entry.pop("live_chg", None)
                        new_closes_raw = [round(float(c), 4) for c in close.tolist()]
                        new_dates_raw  = [str(d)[:10] for d in close.index.tolist()]
                        new_vols_raw   = [int(v) for v in vol_s.tolist()] if len(vol_s) == len(close) else [0] * len(close)
                        # อัปเดต close100
                        old100 = entry.get("close100", [])
                        entry["close100"] = (old100 + new_closes_raw)[-100:]
                        # อัปเดต full history (+ volume คู่กัน สำหรับ _bullish_vol ฝั่ง client)
                        old_dates  = entry.get("dates", [])
                        old_closes = entry.get("closes", [])
                        old_vols   = entry.get("_full_vols", [0] * len(old_dates))
                        for dt, cl, vv in zip(new_dates_raw, new_closes_raw, new_vols_raw):
                            if not old_dates or dt > old_dates[-1]:
                                old_dates.append(dt)
                                old_closes.append(cl)
                                old_vols.append(vv)
                        entry["dates"]  = old_dates
                        entry["closes"] = old_closes
                        entry["_full_vols"] = old_vols
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
                        # อัปเดต above_ema50/200 + price_history/vol_history (ให้ Screener
                        # เรียก _enrichTechSignals() กับ DR ได้แบบเดียวกับหุ้นไทย — ดู
                        # comment เต็มใน _dr_do_rebuild)
                        _close_series = pd.Series(old_closes)
                        _ema50_q  = calc_ema(_close_series, 50)
                        _ema200_q = calc_ema(_close_series, 200)
                        entry["above_ema50"]  = bool(price > _ema50_q)  if _ema50_q  is not None else None
                        entry["above_ema200"] = bool(price > _ema200_q) if _ema200_q is not None else None
                        _hist_bars_q = min(len(old_closes), 500)
                        entry["price_history"] = [
                            [d, round(float(p), 4 if p < 1 else 2)]
                            for d, p in zip(old_dates[-_hist_bars_q:], old_closes[-_hist_bars_q:])
                        ]
                        entry["vol_history"] = old_vols[-260:]
                        entry["vol_today"]   = old_vols[-1] if old_vols else None
                        entry["vol_avg20"]   = (round(sum(old_vols[-21:-1]) / 20)
                                                 if len(old_vols) >= 21 else None)
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


@app.route("/api/us-index-full-refresh", methods=["POST"])
def us_index_full_refresh():
    """ดึงราคา OHLC ย้อนหลังสูงสุด (period=max) ของสมาชิกดัชนี S&P 500 + Dow + Nasdaq 100
    ทั้งหมด (union ไม่ซ้ำ ~518 ตัว) ลง us_prices.db — ปุ่มแยกต่างหาก ใช้เฉพาะกดมือ
    (ไม่ผูกเข้า Quick Update ประจำวัน เพราะ full history โหลดนานกว่า gap-update มาก)
    ใช้ progress state ร่วมกับงานยาวอื่นๆ (_state/_update/_lock — ดู /api/refresh)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา US Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_us_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_us_index_full_refresh():
    try:
        from sources import us_index_membership
        from sources.yahoo import fetch_all_batch
        from core import us_store

        tickers = us_index_membership.all_tickers(BASE_DIR)
        if not tickers:
            raise ValueError("ไม่พบรายชื่อดัชนี US ใน data/us_index_membership.json — รัน sync ก่อน")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        data = fetch_all_batch(tickers, callback=cb, period="max")
        # ขั้นเขียน DB ใช้เวลาหลายนาที (หลายล้านแถวใน transaction เดียว) — บอกสถานะ
        # ให้ชัด ไม่งั้น progress ค้างที่ "batch 6/6" จนดูเหมือนแฮงค์
        _update(current=517, total=518,
                message=f"กำลังบันทึก {len(data)} ตัวลง us_prices.db (หลายนาที อย่าเพิ่งปิด)...")
        us_store.init_db(BASE_DIR)
        us_store.upsert_bars(BASE_DIR, data)

        missing = len(tickers) - len(data)
        msg = f"เสร็จแล้ว! ดึงราคา US Index ย้อนหลังสูงสุด {len(data)}/{len(tickers)} ตัว"
        if missing:
            msg += f" (ขาด {missing} ตัว)"
        _update(running=False, done=True, message=msg)
        run_log.record_run(BASE_DIR, "us_index_full_refresh", True, msg)
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "us_index_full_refresh", False, str(e))

    # คำนวณ RS/EMA/Stage/52W ใหม่จากราคาที่เพิ่งดึง — ทำนอก try/except หลักเพื่อให้รันแม้
    # ราคาบางตัวพลาด (best-effort, ไม่ทำให้ job หลักถูกมองว่า error)
    try:
        from sources import us_index_metrics
        n_metrics = us_index_metrics.build(BASE_DIR)
        _us_breadth_cache.clear()
        print(f"[US Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[US Index] metrics build error: {e}")


def _run_index_gap_update(membership, store, region, label, progress_cb=None):
    """ดึงเฉพาะวันที่ขาดของสมาชิกดัชนี (gap-update, เร็ว) ต่างจาก full-refresh ที่ดึง
    ย้อนหลังสูงสุดทั้งประวัติ (ใช้เฉพาะกดมือ) ใช้จาก Quick Update ประจำวัน — คืนจำนวน
    ticker ที่อัพเดทสำเร็จ

    เดิมมี _run_us_index_gap_update/_run_hk_index_gap_update แยกกัน 2 ฟังก์ชัน
    เหมือนกันทุกบรรทัดยกเว้น module/region code — รวมเป็นฟังก์ชันเดียวรับ membership/
    store module + region code ("US"/"HK") + label ไว้ขึ้น log แทน"""
    from sources.yahoo import fetch_all_batch, fetch_gap_batch
    from sources.dr_universe import is_latest_bar_stable, region_today_date
    from services.refresh import detect_ca_mismatch, _repair_ca_tickers

    tickers = membership.all_tickers(BASE_DIR)
    if not tickers:
        return 0

    # last_dates อาจมี ticker ที่ถูกถอดจากดัชนีไปแล้วค้างอยู่ (ไม่อัพเดทต่อ) — ถ้าเอา
    # ไปหา min ทั้งก้อนจะยิ่งลากวันเริ่มดึง (start) ให้เก่าขึ้นเรื่อยๆ ทุกวันที่ผ่านไป
    # ต้อง filter เหลือเฉพาะ ticker ในดัชนีปัจจุบันก่อน (ตัวที่ถูกถอดไม่ต้องอัพเดทอยู่แล้ว)
    last_dates_all = store.get_last_dates(BASE_DIR)
    if not last_dates_all:
        # DB ยังว่าง (เครื่องใหม่/ยังไม่เคยดึง) — ถ้าปล่อยต่อ ทุกตัวจะกลายเป็น "หุ้นใหม่"
        # แล้ว Quick Update ประจำวันแอบกลายเป็น full backfill period='max' หลายร้อยตัว
        # (งานหนักที่ตั้งใจให้กดปุ่ม Index Max เองเท่านั้น)
        print(f"[{label}] {store.DB_FILE} ยังว่าง — ข้าม gap-update (กดปุ่ม Index Max ก่อน)")
        return 0
    last_dates = {t: d for t, d in last_dates_all.items() if t in set(tickers)}
    new_tickers = [t for t in tickers if t not in last_dates]

    if last_dates:
        import pandas as pd
        min_last = min(last_dates.values())
        # ไม่ +1 วัน — ต้องดึงแท่งล่าสุดที่เก็บไว้แล้วซ้ำ (overlap) ด้วย ไม่งั้นไม่มี
        # แท่งให้เทียบตรวจ split (ดู detect_ca_mismatch) เหมือน dr_quick_update
        start = pd.to_datetime(min_last).strftime("%Y-%m-%d")
        data = fetch_gap_batch(list(last_dates.keys()), start, callback=progress_cb)
    else:
        data = {}   # ไม่มี ticker เก่าเลย (DB ว่าง/รอบแรก) — new_tickers ด้านล่างครอบคลุมหมด

    # หุ้นเข้าดัชนีใหม่ (ไม่มีราคาเก่าเลย) — ต้อง backfill เต็มประวัติแยกต่างหาก ไม่งั้น
    # gap-update ปกติจะดึงแค่ไม่กี่วันล่าสุด ทำให้ RS/EMA200/52W คำนวณไม่ได้อีกนาน
    if new_tickers:
        print(f"[{label}] หุ้นเข้าดัชนีใหม่ {len(new_tickers)} ตัว — backfill เต็มประวัติ: {new_tickers}")
        data.update(fetch_all_batch(new_tickers, callback=progress_cb, period="max"))

    # Split detector: เทียบแท่ง overlap ก่อนบันทึก — ถ้า Yahoo เพิ่งปรับราคาย้อนหลัง
    # (แตกพาร์ ฯลฯ) ต้อง refetch เต็มเฉพาะตัว ไม่งั้น series จะเป็นฐานเก่าต่อฐานใหม่
    # (ดู detect_ca_mismatch ใน services/refresh.py — ใช้ตัวเดียวกับหุ้นไทย)
    suspects = detect_ca_mismatch(BASE_DIR, data, store=store)
    if suspects:
        print(f"[{label} CA] พบ overlap mismatch: {suspects}")
        repaired = _repair_ca_tickers(BASE_DIR, data, suspects, progress_cb or (lambda *a: None))
        for t in repaired:
            store.delete_ticker_bars(BASE_DIR, t)

    # ตัดแท่งล่าสุดทิ้งถ้ายังไม่นิ่ง (ตลาดกำลังเปิด/pre-market/after-hours) — เหตุผล
    # เดียวกับ dr_quick_update (ดูคอมเมนต์ยาวตรงนั้น) timezone ต่างกันตาม region
    if not is_latest_bar_stable(region):
        today = region_today_date(region)
        if today is not None:
            for t in list(data.keys()):
                close = data[t].get("close")
                if close is None or len(close) == 0 or close.index[-1].date() != today:
                    continue
                for k in ("open", "high", "low", "close", "adj_close", "volume"):
                    s = data[t].get(k)
                    if s is not None and len(s):
                        data[t][k] = s.iloc[:-1]
                if len(data[t]["close"]) == 0:
                    del data[t]

    if data:
        store.upsert_bars(BASE_DIR, data)
    return len(data)


def _run_us_index_gap_update(progress_cb=None):
    from sources import us_index_membership
    from core import us_store
    return _run_index_gap_update(us_index_membership, us_store, "US", "US Index", progress_cb)


@app.route("/api/us-index-metrics")
def us_index_metrics_route():
    """RS/EMA/Stage/52W ของสมาชิกดัชนี S&P 500 + Dow + Nasdaq 100 (cache — คำนวณล่วงหน้า
    ตอน Quick Update / US Index Max ที่ sources/us_index_metrics.py ดู field ที่ได้ที่
    set_data_fetcher.process_stock() + core.metrics.rank_rs() — เหมือนหุ้นไทยทุกประการ
    ต่างแค่ field เสริม in_sp500/in_dow/in_ndx สำหรับกรองตามดัชนี"""
    from sources import us_index_metrics
    return jsonify(us_index_metrics.load_local(BASE_DIR))


@app.route("/api/us-sector-ranks")
def us_sector_ranks():
    """จัดอันดับ Sector (GICS) ของสมาชิกดัชนี US ตาม RS/return เฉลี่ย — reuse
    core.metrics.summarize_groups() ตัวเดียวกับหน้า "Sectors" ของหุ้นไทย (ห้ามเขียนสูตรซ้ำ)
    query param index=SP500|DOW|NDX (default SP500)"""
    from sources import us_index_metrics
    from core.metrics import summarize_groups
    idx = (request.args.get("index") or "SP500").upper()
    flag = {"SP500": "in_sp500", "DOW": "in_dow", "NDX": "in_ndx"}.get(idx, "in_sp500")
    stocks = [s for s in us_index_metrics.load_local(BASE_DIR).get("stocks", []) if s.get(flag)]
    return jsonify({"sectors": summarize_groups(stocks, "sector")})


@app.route("/api/us-history/<symbol>")
def get_us_history(symbol):
    """ส่ง full price history ของหุ้นดัชนี US (S&P500/Dow/NDX) จาก us_prices.db —
    ใช้เติมกราฟ 5Y/Max ใน chart modal (ตัวเดียวกับ /api/history ของหุ้นไทย แค่คนละ DB)"""
    from core import us_store
    ticker = symbol.upper().strip()
    data = us_store.get_ohlc_series(BASE_DIR, ticker)
    if not data:
        return jsonify({"error": f"ไม่พบข้อมูล {symbol} — กรุณากด US Index Max ก่อน"}), 404
    return jsonify({"dates": data["dates"], "closes": data["closes"], "volumes": data["volumes"]})


@app.route("/api/hk-index-full-refresh", methods=["POST"])
def hk_index_full_refresh():
    """ดึงราคา OHLC ย้อนหลังสูงสุด (period=max) ของสมาชิกดัชนี HSI + HSCEI + HSTECH
    ทั้งหมด (union ไม่ซ้ำ ~105 ตัว) ลง hk_prices.db — ปุ่มแยกต่างหาก ใช้เฉพาะกดมือ
    (ไม่ผูกเข้า Quick Update ประจำวัน เพราะ full history โหลดนานกว่า gap-update มาก)
    ใช้ progress state ร่วมกับงานยาวอื่นๆ (_state/_update/_lock — ดู /api/refresh)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา HK Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_hk_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_hk_index_full_refresh():
    try:
        from sources import hk_index_membership
        from sources.yahoo import fetch_all_batch
        from core import hk_store

        tickers = hk_index_membership.all_tickers(BASE_DIR)
        if not tickers:
            raise ValueError("ไม่พบรายชื่อดัชนี HK ใน data/hk_index_membership.json — รัน sync ก่อน")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        data = fetch_all_batch(tickers, callback=cb, period="max")
        _update(current=104, total=105,
                message=f"กำลังบันทึก {len(data)} ตัวลง hk_prices.db (หลายนาที อย่าเพิ่งปิด)...")
        hk_store.init_db(BASE_DIR)
        hk_store.upsert_bars(BASE_DIR, data)

        missing = len(tickers) - len(data)
        msg = f"เสร็จแล้ว! ดึงราคา HK Index ย้อนหลังสูงสุด {len(data)}/{len(tickers)} ตัว"
        if missing:
            msg += f" (ขาด {missing} ตัว)"
        _update(running=False, done=True, message=msg)
        run_log.record_run(BASE_DIR, "hk_index_full_refresh", True, msg)
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hk_index_full_refresh", False, str(e))

    # คำนวณ RS/EMA/Stage/52W ใหม่จากราคาที่เพิ่งดึง — ทำนอก try/except หลักเพื่อให้รันแม้
    # ราคาบางตัวพลาด (best-effort, ไม่ทำให้ job หลักถูกมองว่า error)
    try:
        from sources import hk_index_metrics
        n_metrics = hk_index_metrics.build(BASE_DIR)
        _hk_breadth_cache.clear()
        print(f"[HK Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[HK Index] metrics build error: {e}")


def _run_hk_index_gap_update(progress_cb=None):
    from sources import hk_index_membership
    from core import hk_store
    return _run_index_gap_update(hk_index_membership, hk_store, "HK", "HK Index", progress_cb)


@app.route("/api/hk-index-metrics")
def hk_index_metrics_route():
    """RS/EMA/Stage/52W ของสมาชิกดัชนี HSI + HSCEI + HSTECH (cache — คำนวณล่วงหน้า
    ตอน Quick Update / HK Index Max ที่ sources/hk_index_metrics.py ดู field ที่ได้ที่
    set_data_fetcher.process_stock() + core.metrics.rank_rs() — เหมือนหุ้นไทย/US ทุกประการ
    ต่างแค่ field เสริม in_hsi/in_hscei/in_hstech สำหรับกรองตามดัชนี"""
    from sources import hk_index_metrics
    return jsonify(hk_index_metrics.load_local(BASE_DIR))


@app.route("/api/hk-sector-ranks")
def hk_sector_ranks():
    """จัดอันดับ Sector ของสมาชิกดัชนี HK ตาม RS/return เฉลี่ย — reuse
    core.metrics.summarize_groups() ตัวเดียวกับหน้า "Sectors" ของหุ้นไทย/US (ห้ามเขียนสูตรซ้ำ)
    query param index=HSI|HSCEI|HSTECH (default HSI)"""
    from sources import hk_index_metrics
    from core.metrics import summarize_groups
    idx = (request.args.get("index") or "HSI").upper()
    flag = {"HSI": "in_hsi", "HSCEI": "in_hscei", "HSTECH": "in_hstech"}.get(idx, "in_hsi")
    stocks = [s for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", []) if s.get(flag)]
    return jsonify({"sectors": summarize_groups(stocks, "sector")})


@app.route("/api/hk-history/<symbol>")
def get_hk_history(symbol):
    """ส่ง full price history ของหุ้นดัชนี HK (HSI/HSCEI/HSTECH) จาก hk_prices.db —
    ใช้เติมกราฟ 5Y/Max ใน chart modal (ตัวเดียวกับ /api/us-history แค่คนละ DB)"""
    from core import hk_store
    ticker = symbol.upper().strip()
    data = hk_store.get_ohlc_series(BASE_DIR, ticker)
    if not data:
        return jsonify({"error": f"ไม่พบข้อมูล {symbol} — กรุณากด HK Index Max ก่อน"}), 404
    return jsonify({"dates": data["dates"], "closes": data["closes"], "volumes": data["volumes"]})


@app.route("/api/jp-index-full-refresh", methods=["POST"])
def jp_index_full_refresh():
    """ดึงราคา OHLC ย้อนหลังสูงสุด (period=max) ของสมาชิกดัชนี Nikkei 225 (~225 ตัว)
    ลง jp_prices.db — ปุ่มแยกต่างหาก ใช้เฉพาะกดมือ (ดู hk_index_full_refresh)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา JP Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_jp_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_jp_index_full_refresh():
    try:
        from sources import jp_index_membership
        from sources.yahoo import fetch_all_batch
        from core import jp_store

        tickers = jp_index_membership.all_tickers(BASE_DIR)
        if not tickers:
            raise ValueError("ไม่พบรายชื่อดัชนี JP ใน data/jp_index_membership.json — รัน sync ก่อน")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        data = fetch_all_batch(tickers, callback=cb, period="max")
        _update(current=len(tickers) - 1, total=len(tickers),
                message=f"กำลังบันทึก {len(data)} ตัวลง jp_prices.db (หลายนาที อย่าเพิ่งปิด)...")
        jp_store.init_db(BASE_DIR)
        jp_store.upsert_bars(BASE_DIR, data)

        missing = len(tickers) - len(data)
        msg = f"เสร็จแล้ว! ดึงราคา JP Index ย้อนหลังสูงสุด {len(data)}/{len(tickers)} ตัว"
        if missing:
            msg += f" (ขาด {missing} ตัว)"
        _update(running=False, done=True, message=msg)
        run_log.record_run(BASE_DIR, "jp_index_full_refresh", True, msg)
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "jp_index_full_refresh", False, str(e))

    try:
        from sources import jp_index_metrics
        n_metrics = jp_index_metrics.build(BASE_DIR)
        print(f"[JP Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[JP Index] metrics build error: {e}")


def _run_jp_index_gap_update(progress_cb=None):
    from sources import jp_index_membership
    from core import jp_store
    return _run_index_gap_update(jp_index_membership, jp_store, "JP", "JP Index", progress_cb)


@app.route("/api/jp-index-metrics")
def jp_index_metrics_route():
    """RS/EMA/Stage/52W ของสมาชิกดัชนี Nikkei 225 (cache — คำนวณล่วงหน้าตอน Quick Update /
    JP Index Max ที่ sources/jp_index_metrics.py ดู field ที่ได้ที่
    set_data_fetcher.process_stock() + core.metrics.rank_rs() — เหมือนหุ้นไทย/US/HK ทุกประการ
    ต่างแค่ field เสริม in_nikkei225 สำหรับกรองตามดัชนี"""
    from sources import jp_index_metrics
    return jsonify(jp_index_metrics.load_local(BASE_DIR))


@app.route("/api/jp-history/<symbol>")
def get_jp_history(symbol):
    """ส่ง full price history ของหุ้นดัชนี JP (Nikkei 225) จาก jp_prices.db — ใช้เติมกราฟ
    5Y/Max ใน chart modal (ตัวเดียวกับ /api/us-history/hk-history แค่คนละ DB)"""
    from core import jp_store
    ticker = symbol.upper().strip()
    data = jp_store.get_ohlc_series(BASE_DIR, ticker)
    if not data:
        return jsonify({"error": f"ไม่พบข้อมูล {symbol} — กรุณากด JP Index Max ก่อน"}), 404
    return jsonify({"dates": data["dates"], "closes": data["closes"], "volumes": data["volumes"]})


# ============================================================
# Hedge Holdings (13F / superinvestors จาก Dataroma)
# ดู sources/dataroma.py — cache: data/hedge_holdings.json
# ============================================================
@app.route("/api/hedge/managers")
def hedge_managers():
    """เสิร์ฟ cache การถือครองทุกกอง (managers + holdings) ให้ client คำนวณ overlap เอง"""
    from sources import dataroma
    data = dataroma.load_cache(BASE_DIR)
    if not data:
        return jsonify({"error": "ยังไม่มีข้อมูล กดปุ่ม 'อัพเดท Hedge Holdings' เพื่อดึงครั้งแรก"}), 404
    return jsonify(data)


@app.route("/api/hedge-status")
def hedge_status():
    """สถานะสั้นๆ ของ cache — วันที่ดึงล่าสุด + จำนวนกอง (ไว้โชว์บนหน้า)"""
    from sources import dataroma
    data = dataroma.load_cache(BASE_DIR)
    if not data:
        return jsonify({"cached": False})
    return jsonify({"cached": True, "generated_at": data.get("generated_at"),
                    "manager_count": data.get("manager_count")})


@app.route("/api/hedge-refresh", methods=["POST"])
def hedge_refresh():
    """ขูดการถือครองทุกกองจาก Dataroma ใหม่ (~84 กอง, ใช้เวลาหลายนาที) — background thread
    ใช้ progress state ร่วม (_state/_update/_lock — ดู /api/progress)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึง Hedge Holdings จาก Dataroma...")
    threading.Thread(target=_run_hedge_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_hedge_refresh():
    try:
        from sources import dataroma

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        payload = dataroma.refresh_all(BASE_DIR, callback=cb)
        msg = f"เสร็จแล้ว! ดึง Hedge Holdings {payload['manager_count']} กอง"
        _update(running=False, done=True, message=msg)
        run_log.record_run(BASE_DIR, "hedge_refresh", True, msg)
    except Exception as e:  # noqa: BLE001
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hedge_refresh", False, str(e))


def _hedge_norm(sym):
    """Dataroma ใช้จุด (BRK.B) — คลัง US ใช้ขีด (BRK-B) ให้ตรง yfinance/us_prices.db"""
    return (sym or "").upper().replace(".", "-").strip()


def _hedge_us_covered():
    """set ของ ticker US (normalize แล้ว) ที่ "มีในคลัง" = สมาชิกดัชนีหลัก (us_prices.db) ∪
    หุ้นที่เคยดึง on-demand แล้ว (table mirror_ondemand, market=US) — ใช้เช็คว่าตัวไหนยังขาด"""
    covered = set()
    try:
        from sources import us_index_membership
        covered |= {_hedge_norm(t) for t in us_index_membership.all_tickers(BASE_DIR)}
    except Exception:
        pass
    try:
        import sqlite3
        con = sqlite3.connect(os.path.join(BASE_DIR, "financials.db"))
        try:
            covered |= {_hedge_norm(r[0]) for r in
                        con.execute("SELECT symbol FROM mirror_ondemand WHERE market='US'")}
        finally:
            con.close()
    except Exception:
        pass
    return covered


@app.route("/api/hedge-coverage")
def hedge_coverage():
    """คืนรายชื่อหุ้น (จาก cache hedge) ที่ "มีในคลัง" แล้ว — ให้ client ติด badge + นับตัวที่ขาด
    ก่อนกดดึง (covered = subset ของ hedge universe เพื่อ payload เล็ก)"""
    from sources import dataroma
    data = dataroma.load_cache(BASE_DIR)
    if not data:
        return jsonify({"covered": []})
    hedge_syms = set()
    for m in data.get("managers", {}).values():
        for h in m.get("holdings", []):
            if h.get("sym"):
                hedge_syms.add(_hedge_norm(h["sym"]))
    covered = _hedge_us_covered()
    return jsonify({"covered": sorted(hedge_syms & covered),
                    "total_universe": len(hedge_syms)})


@app.route("/api/hedge-fetch-missing", methods=["POST"])
def hedge_fetch_missing():
    """ดึงหุ้น US ที่ส่งมา (list) ตัวที่ยังไม่มีในคลัง → เข้า mirror_ondemand (Tearsheet/Peer/
    Screener+ ใช้ได้) — background thread, progress ผ่าน _state ร่วม
    body: {"symbols": ["HHH","AER",...]} (รูปแบบ Dataroma มีจุดได้ จะ normalize เอง)"""
    syms = []
    if request.is_json:
        syms = request.json.get("symbols") or []
    syms = [_hedge_norm(s) for s in syms if s]
    if not syms:
        return jsonify({"error": "ไม่มีรายชื่อหุ้นที่จะดึง"}), 400
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเตรียมดึงหุ้น consensus เข้าคลัง...")
    threading.Thread(target=_run_hedge_fetch_missing, args=(syms,), daemon=True).start()
    return jsonify({"ok": True})


def _run_hedge_fetch_missing(syms):
    try:
        from sources import mirror_ondemand
        covered = _hedge_us_covered()
        todo = [s for s in dict.fromkeys(syms) if s not in covered]  # ไม่ซ้ำ + ตัดที่มีแล้ว
        total = len(todo)
        if not total:
            _update(running=False, done=True, current=0, total=0,
                    message="หุ้นในรายการมีในคลังครบแล้ว ไม่มีอะไรต้องดึง")
            return
        ok = fail = 0
        failed = []
        for i, sym in enumerate(todo, 1):
            _update(current=i, total=total, message=f"ดึง {sym} เข้าคลัง ({i}/{total}) · สำเร็จ {ok}")
            try:
                row = mirror_ondemand.fetch_header(BASE_DIR, "US", sym, force=False)
                if row:
                    ok += 1
                else:
                    fail += 1; failed.append(sym)
            except Exception:  # noqa: BLE001 — ตัวเดียวพังไม่ล้มทั้ง batch
                fail += 1; failed.append(sym)
        msg = f"เสร็จแล้ว! ดึงเข้าคลังสำเร็จ {ok}/{total} ตัว"
        if fail:
            msg += f" · ไม่สำเร็จ {fail} (เช่น {', '.join(failed[:8])}{'...' if len(failed) > 8 else ''})"
        _update(running=False, done=True, message=msg)
        run_log.record_run(BASE_DIR, "hedge_fetch_missing", True, msg)
    except Exception as e:  # noqa: BLE001
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hedge_fetch_missing", False, str(e))


@app.route("/api/dr-history/<symbol>")
def get_dr_history(symbol):
    """ดึง price history สำหรับ DR stock — เสิร์ฟจาก cache ก่อน ไม่ต้อง fetch yfinance ซ้ำ"""
    import yfinance as yf
    sym = symbol.upper().strip()
    dr_entry = next((s for s in load_dr_universe(BASE_DIR) if s["sym"] == sym), None)
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


@app.route("/api/dr-descriptions")
def dr_descriptions_route():
    """คำอธิบายบริษัท (EN + แปลไทย) ของหุ้น DR ทั้งหมด — มาจาก Yahoo Finance
    (longBusinessSummary) ไม่ใช่ Finnomena ดังนั้น bake เป็น static ให้เว็บมือถือ
    ได้ตามกฎที่ตกลงกันไว้ (ดู run_static_update.py)"""
    return jsonify(dr_descriptions.load_all(BASE_DIR))


@app.route("/api/dr-description/<symbol>")
def dr_description_one(symbol):
    """คำอธิบายบริษัทของหุ้นตัวเดียว แบบ lazy — cache hit (สด ≤180 วัน) ตอบทันทีจาก
    ไฟล์ local, cache miss/เก่า ดึง+แปลสดตอนนั้นเลย (ตัวเดียวเร็วพอไม่ต้องผ่าน
    background thread) ครอบคลุมทั้งหุ้น DR ที่ curate ไว้ และหุ้น mirror US/HK ทั่วไป
    (ระบุ ?market=US หรือ HK ให้ตอน symbol ไม่อยู่ใน DR universe — frontend รู้จาก
    currency ของงบที่โหลดอยู่แล้ว)"""
    sym = symbol.upper().strip()
    market = request.args.get("market")
    record, err = dr_descriptions.fetch_one(BASE_DIR, sym, market=market)
    if not record:
        return jsonify({"sym": sym, "error": err or "ไม่พบข้อมูล"}), 404
    return jsonify({"sym": sym, **record})


@app.route("/api/resolve-yf/<symbol>")
def resolve_yf(symbol):
    """หา yfinance ticker ของ symbol เพื่อสร้างลิงก์ TradingView — เบามาก (ไม่ยิง yfinance/
    Google Translate) ใช้ logic เดียวกับ dr_descriptions (DR universe ก่อน ไม่เจอเดาจาก
    market) แยกจาก /api/dr-description เพราะจะได้ไม่ต้องพึ่งผลลัพธ์ description สำเร็จ"""
    sym = symbol.upper().strip()
    market = request.args.get("market")
    yf_ticker, is_etf = dr_descriptions.resolve_yf_ticker(BASE_DIR, sym, market=market)
    if not yf_ticker:
        return jsonify({"sym": sym, "error": "ไม่ทราบตลาดของหุ้นนี้"}), 404
    return jsonify({"sym": sym, "yf": yf_ticker, "is_etf": is_etf})


@app.route("/api/live-price/<symbol>")
def live_price(symbol):
    """ราคาล่าสุดจาก Yahoo Finance แบบเบา (fast_info — ไม่โหลด history เต็ม) ใช้คำนวณ
    PE/PBV band แบบสด ("มูลค่าเทียบอดีตตัวเอง") ในหน้างบการเงิน — งวด Finnomena
    ล่าสุดอาจเป็นราคา ณ สิ้นไตรมาสที่ผ่านมาแล้ว ไม่ใช่ราคาวันนี้"""
    import yfinance as yf
    sym = symbol.upper().strip()
    is_dr = request.args.get("is_dr") == "1"
    market = request.args.get("market")
    yf_override = request.args.get("yf")
    if yf_override:
        # ผู้เรียกรู้ ticker จริงอยู่แล้ว (เช่น /api/tearsheet ที่ resolve DR/mirror ให้แล้ว)
        # ข้าม resolve ซ้ำ กันเดาผิดสำหรับ US/HK mirror ที่ sym ดิบไม่มี suffix
        yf_ticker = yf_override.upper().strip()
    elif is_dr:
        yf_ticker, is_etf = dr_descriptions.resolve_yf_ticker(BASE_DIR, sym, market=market)
        if not yf_ticker:
            return jsonify({"sym": sym, "error": "ไม่ทราบตลาดของหุ้นนี้"}), 404
    else:
        yf_ticker = sym + ".BK"
    try:
        fi = yf.Ticker(yf_ticker).fast_info
        price = getattr(fi, "last_price", None)
        if price is None:
            return jsonify({"sym": sym, "error": "ไม่พบราคา"}), 404
        return jsonify({"sym": sym, "yf": yf_ticker, "price": float(price),
                        "currency": getattr(fi, "currency", None)})
    except Exception as e:
        return jsonify({"sym": sym, "error": str(e)}), 500


@app.route("/api/dr-description-sync", methods=["POST"])
def start_dr_description_sync():
    """ดึงคำอธิบายบริษัท DR ทั้งหมดจาก Yahoo Finance + แปลไทย (local-only ปุ่มกด —
    ผลลัพธ์ dr_descriptions.json ค่อย bake ขึ้น GitHub ทีหลังตอน push ปกติ)
    ใช้ progress overlay + /api/progress ร่วมกับ job อื่น (กันรันซ้อน)"""
    force = False
    if request.is_json:
        body = request.json or {}
        force = bool(body.get("force"))
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=0,
                      message="กำลังเริ่มดึงคำอธิบายบริษัท DR...")

    def _run():
        try:
            def cb(current, total, msg):
                _update(current=current, total=total, message=msg)
            result = dr_descriptions.sync_all(BASE_DIR, force=force, callback=cb)
            _update(running=False, done=True,
                    message=f"เสร็จแล้ว! ดึงใหม่ {result['ok']} · ข้าม {result['skipped']} (มีอยู่แล้ว ไม่เก่า)"
                            + (f" · ล้มเหลว {result['fail']}" if result["fail"] else ""))
        except Exception as e:
            _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True})


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

    # หา yfinance ticker: ค้นใน DR universe (static+auto) ก่อน ไม่เจอ → ใช้ .BK
    dr_entry = next((s for s in load_dr_universe(BASE_DIR) if s["sym"] == sym), None)
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


def _finnomena_annual_status(annual, sym, is_dr):
    """ตรวจทาน 'งบรายปีรวมจากไตรมาส' Finnomena กับ Yahoo รายปี — คืน (status, median_diff)

    เดิมใช้กัน (404) หุ้นที่ยอดไม่ตรงปีปฏิทิน แต่สำรวจจริงพบว่าที่ 'ไม่ตรง' มี 2 แบบ
    และทั้งคู่ยังมีประโยชน์ถ้าติดป้ายให้ถูก:
      calendar     : ยอดตรงปีต่อปี (มัธยฐานต่าง ≤3%) — หุ้นปีบัญชี ธ.ค. ปกติ ~0%
      fiscal_shift : ยอดตรงเมื่อเลื่อนป้ายปี ±1 (บริษัทปีบัญชีไม่ตรงปฏิทิน เช่น NVDA/CRM
                     จบ ม.ค. — ผลรวม 'ปีบัญชี' ถูกต้อง แค่ป้ายปีเหลื่อมกับแหล่งปีปฏิทิน)
      mismatch     : มีปีทับกันแต่ยอดไม่ตรงทั้งตรงและเลื่อน — มักเป็นธนาคาร/ประกันที่
                     นิยาม 'รายได้' ต่างแหล่ง (เช่น BAC gross vs net interest income)
                     ตัวเลขใช้ดูแนวโน้มภายในชุดเดียวกันได้ แต่อย่าเทียบข้ามแหล่ง
      unverified   : ไม่มีงบ Yahoo ให้เทียบ (หุ้น mirror US/HK นอกพอร์ต)"""
    ya = financials_store.get(BASE_DIR, sym, "yahoo", is_dr=is_dr)
    arev = {int(k[:4]): v for k, v in (annual.get("income", {}).get("Total Revenue", {}) or {}).items()}
    yrev = {int(d[:4]): v for d, v in ((ya or {}).get("income", {}).get("Total Revenue", {}) or {}).items()}

    def _median_diff(shift):
        diffs = sorted(abs(arev[y] - yrev[y + shift]) / abs(yrev[y + shift])
                       for y in arev if yrev.get(y + shift))
        return diffs[len(diffs) // 2] if diffs else None

    if not ya or not arev or not yrev:
        return "unverified", None
    d0 = _median_diff(0)
    if d0 is None:
        return "unverified", None
    if d0 <= 0.03:
        return "calendar", d0
    shifted = [d for d in (_median_diff(1), _median_diff(-1)) if d is not None]
    ds = min(shifted) if shifted else None
    if ds is not None and ds <= 0.035:
        return "fiscal_shift", ds

    # ยอดไม่ตรงทั้งตรงและเลื่อนป้าย — แยกสาเหตุจากเดือนสิ้นปีบัญชีของ Yahoo:
    # FYE != ธ.ค. = 'calendar_window': พิสูจน์แล้ว (MSFT/NIKE เทียบ Yahoo รายไตรมาส
    # ต่าง 0.0%) ว่า Finnomena จัดกลุ่มเป็น 'ปีปฏิทิน' ซึ่งถูกต้องในตัวเอง แค่คนละ
    # หน้าต่างกับปีบัญชีบริษัทที่ Yahoo รายปีใช้ — ถ้ามี yahoo_q สะสมพอ ยืนยันซ้ำ
    # ด้วยผลรวมปีปฏิทินจริงแล้วอัพเกรดเป็น 'calendar_verified'
    from collections import Counter, defaultdict
    months = [d[5:7] for d in ya.get("income", {}).get("Total Revenue", {})]
    fye = Counter(months).most_common(1)[0][0] if months else "12"
    if fye != "12":
        yq = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr)
        qrow = (yq or {}).get("income", {}).get("Total Revenue", {})
        by_cal = defaultdict(list)
        for d, v in qrow.items():
            by_cal[int(d[:4])].append(v)
        diffs = sorted(abs(arev[y] - sum(vs)) / abs(sum(vs))
                       for y, vs in by_cal.items()
                       if len(vs) == 4 and arev.get(y) and sum(vs))
        if diffs and diffs[len(diffs) // 2] <= 0.02:
            return "calendar_verified", diffs[len(diffs) // 2]
        return "calendar_window", d0
    return "definition", d0


@app.route("/api/financials-full/<symbol>")
def get_financials_full(symbol):
    """งบการเงินฉบับเต็ม (ทุก field) จาก DB local — sync ล่วงหน้าด้วย /api/financials/sync-all
    ถ้ายังไม่เคย sync ตัวนี้มาก่อน จะดึงสดครั้งเดียวแล้วเก็บลง DB ให้ (สะดวกตอนทดสอบ/หุ้นใหม่)"""
    sym = symbol.upper().strip()
    source = request.args.get("source", "yahoo")
    if source not in ("yahoo", "yahoo_q", "finnomena_q", "finnomena_y", "set"):
        return jsonify({"error": "source ต้องเป็น yahoo, yahoo_q, finnomena_q, finnomena_y หรือ set เท่านั้น"}), 400
    is_dr = request.args.get("is_dr") == "1"
    # ใช้เฉพาะตอน source=yahoo/yahoo_q + is_dr แต่ symbol ไม่อยู่ใน DR universe ที่ curate
    # ไว้ (หุ้น mirror US/HK ทั่วไป) — กัน fetch_yahoo_* พลาดไปดึงเป็นหุ้นไทย .BK
    # (ดูคอมเมนต์ resolve_yf_ticker ใน dr_descriptions.py — logic เดียวกัน)
    market = request.args.get("market")

    # ETF/กองทุนไม่มีงบการเงินแบบบริษัท — บอกสาเหตุชัดๆ แทน error ว่างเปล่า
    if is_dr:
        _entry = next((s for s in load_dr_universe(BASE_DIR) if s["sym"] == sym), None)
        if _entry and _entry.get("etf"):
            return jsonify({"error": f"{sym} เป็น ETF/กองทุน ({_entry.get('name', '')}) — "
                                     "ไม่มีงบการเงินแบบบริษัท (งบกำไรขาดทุน/งบดุล/กระแสเงินสด "
                                     "มีเฉพาะหุ้นสามัญ) ดูราคาและผลตอบแทนได้ที่หน้า DR/DRx"}), 404

    # finnomena_y = งบรายปี รวมสดจากไตรมาส Finnomena (ไม่เก็บแยก — คำนวณจาก finnomena_q)
    # ได้ประวัติลึก ~16-20 ปี ต่างจาก Yahoo รายปีที่ให้แค่ ~5 ปี
    if source == "finnomena_y":
        q = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr)
        if not q:
            try:
                fresh = financials_store.fetch_finnomena_quarterly(sym, is_dr=is_dr)
                financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh, is_dr=is_dr)
                q = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr)
            except Exception as e:
                return jsonify({"error": str(e)}), 404
        annual = financials_store.build_annual_from_quarterly(q)
        if not annual:
            return jsonify({"error": f"{sym}: ยังไม่มีปีบัญชีที่ครบ 4 ไตรมาสใน Finnomena"}), 404
        # ไม่กัน (404) อีกต่อไป — แสดงเสมอพร้อม 'สถานะการตรวจทานกับ Yahoo' ให้ UI ติดป้าย
        # (calendar/fiscal_shift/mismatch/unverified — ดู _finnomena_annual_status)
        # เดิมหุ้นปีบัญชีไม่ตรงปฏิทิน (55 ตัว) + mirror US/HK ทั้งหมด (~5,100 ตัว) โดนกันหมด
        # ทั้งที่ยอดรวม 'ปีบัญชี' ของ Finnomena ถูกต้องในตัวเอง แค่ป้ายปีเหลื่อม/ไม่มีตัวเทียบ
        status, med = _finnomena_annual_status(annual, sym, is_dr)
        annual["fy_status"] = status
        if med is not None:
            annual["fy_diff_pct"] = round(med * 100, 1)
        annual["synced_at"] = (q or {}).get("synced_at")
        return jsonify(annual)

    def _with_quality(data):
        # ติดป้ายเตือนถ้าราคา/BVPS ของ Finnomena ต่างจากแหล่งอิสระอื่น (ราคาจริงใน
        # {market}_prices.db, BVPS คำนวณเองจากงบ Yahoo) เกินเกณฑ์ — ดู check_valuation_quality
        # (เจอจริงว่า Finnomena มีข้อมูลค้างช่วงหุ้นมีปัญหา เช่น NWR/TSR) ไม่บล็อกการใช้งาน
        # แค่ให้ UI โชว์คำเตือนก่อนผู้ใช้เอาไปตัดสินใจ
        if data and source == "finnomena_q":
            try:
                data["valuation_quality"] = financials_store.check_valuation_quality(
                    BASE_DIR, sym, data, market=market, is_dr=is_dr)
            except Exception:
                pass
        return data

    data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr)
    if data:
        return jsonify(_with_quality(data))

    try:
        if source == "yahoo":
            payload = financials_store.fetch_yahoo_full(sym, is_dr=is_dr, market=market)
        elif source == "yahoo_q":
            payload = financials_store.fetch_yahoo_quarterly(sym, is_dr=is_dr, market=market)
        elif source == "finnomena_q":
            payload = financials_store.fetch_finnomena_quarterly(sym, is_dr=is_dr)
        else:
            payload = financials_store.fetch_set_full(sym)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    financials_store.upsert(BASE_DIR, sym, source, payload, is_dr=is_dr)
    data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr)
    return jsonify(_with_quality(data))


_DIVIDENDS_STALE_DAYS = 30   # ตามแผน PLAN_stock_study_suite.txt งาน #5


@app.route("/api/dividends/<market>/<symbol>")
def get_dividends_endpoint(market, symbol):
    """ประวัติปันผล + สถิติ (streak/CAGR/YoY/ความถี่/yield รายปี) — เก็บใน financials.db
    (local-only) ดึงสดจาก yfinance ครั้งแรกหรือเมื่อข้อมูลเก่าเกิน 30 วัน, ?refresh=1 บังคับดึงสด
    yield รายปีคำนวณจากราคาปิดในเครื่อง — TH จาก set_prices.db, US จาก us_prices.db, HK จาก
    hk_prices.db (เฉพาะสมาชิกดัชนีหลักที่มีราคาโหลดไว้แล้ว) DR ยังไม่รองรับ (ต้องมี price series
    ในเครื่องของ underlying ก่อน ยังไม่มี local store แยกให้)"""
    from sources import dividend_stats
    from datetime import datetime as _dt, timedelta as _td
    mkt = (market or "TH").upper()
    sym = symbol.upper().strip()
    force = request.args.get("refresh") == "1"

    rows, synced_at = financials_store.get_dividends(BASE_DIR, sym, mkt)
    stale = True
    if synced_at:
        try:
            stale = (_dt.now() - _dt.fromisoformat(synced_at)) > _td(days=_DIVIDENDS_STALE_DAYS)
        except ValueError:
            stale = True

    fetch_error = None
    if force or rows is None or stale:
        try:
            fresh = financials_store.fetch_dividends(sym, market=mkt)
            financials_store.save_dividends(BASE_DIR, sym, mkt, fresh)   # เขียน synced_at เสมอแม้ fresh=[]
            rows, synced_at = financials_store.get_dividends(BASE_DIR, sym, mkt)
            stale = False
        except Exception as e:
            fetch_error = str(e)

    if not rows:
        msg = f"ไม่พบประวัติปันผลของ {sym}" + (f" ({fetch_error})" if fetch_error else "")
        return jsonify({"error": msg}), 404

    price_series = None
    if mkt in ("TH", "SET"):
        price_series = price_store.get_series(BASE_DIR, sym + ".BK")
    elif mkt == "US":
        from core import us_store
        data = us_store.get_ohlc_series(BASE_DIR, sym)
        if data:
            price_series = {"dates": data["dates"], "closes": data["closes"]}
    elif mkt == "HK":
        from core import hk_store
        # hk_prices.db เก็บ ticker แบบมี suffix ".HK" (ตรงกับ hk_index_metrics.json) ต่างจาก
        # ตาราง dividends ที่เก็บรหัสดิบไม่มี suffix (ดู sync_dividends_batch/_mirror_sym) —
        # ต้องแปลงกลับก่อนเสมอ ไม่งั้นจะหาราคาไม่เจอเงียบๆ (บั๊กคลาสเดียวกับ .HK suffix mismatch
        # ที่เจอมาก่อนใน US/HK support ของ Tearsheet/Peer Compare)
        data = hk_store.get_ohlc_series(BASE_DIR, sym.zfill(4) + ".HK")
        if data:
            price_series = {"dates": data["dates"], "closes": data["closes"]}

    # Payout Ratio (DPS÷EPS) — EPS มาจาก factor_snapshot (คำนวณไว้แล้วตอน build_snapshot/
    # build_mirror_snapshot, ดู factor_snapshot._latest_eps) เลือก snapshot table ให้ตรงตลาด
    eps_value = eps_date = None
    try:
        if mkt in ("TH", "SET"):
            snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}
        elif mkt == "DR":
            snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=True)}
        elif mkt in ("US", "HK"):
            snap_rows = {r["symbol"]: r for r in factor_snapshot.get_mirror_snapshot(BASE_DIR, mkt)}
        else:
            snap_rows = {}
        f = snap_rows.get(sym)
        if f:
            eps_value, eps_date = f.get("eps_latest"), f.get("eps_latest_date")
    except Exception:
        pass
    payout_ratio_pct = dividend_stats.compute_payout_ratio(rows, eps_value, eps_date)

    stats = dividend_stats.compute_dividend_stats(rows, price_series=price_series)
    return jsonify({
        "symbol": sym, "market": mkt, "synced_at": synced_at, "stale": stale,
        "fetch_error": fetch_error,
        "payout_ratio_pct": payout_ratio_pct, "eps_latest": eps_value, "eps_latest_date": eps_date,
        **(stats or {}),
    })


_CALENDAR_STALE_DAYS = 7   # ปฏิทินเปลี่ยนเร็วกว่าปันผล (ประกาศใหม่ได้ตลอด) — sync ถี่กว่า


@app.route("/api/calendar-events-all")
def get_all_calendar_events_endpoint():
    """ปฏิทินรวมทุกหุ้นที่เคย sync ไว้ใน local DB (ไม่ใช่แค่ watchlist) — ใช้กับตัวกรอง
    "ทั้งหมดที่มีข้อมูล"/"ตามตลาด" หน้า 📅 ปฏิทิน อ่านจาก cache เท่านั้น ไม่ fetch สด (เบา ไม่มี
    rate-limit risk) ?market=TH|US|HK|DR กรองเฉพาะตลาดนั้น ไม่ส่ง = ทุกตลาด"""
    from datetime import date as _date
    market = request.args.get("market")
    today_iso = _date.today().isoformat()
    events = financials_store.get_all_calendar_events(BASE_DIR, from_date=today_iso, market=market)
    return jsonify({"events": events})


@app.route("/api/calendar-events/<market>/<symbol>")
def get_calendar_events_endpoint(market, symbol):
    """ปฏิทิน XD/pay (SET.or.th, หุ้นไทยเท่านั้น, confirmed) + earnings (yfinance, ทุกตลาด,
    estimated) ของหุ้นตัวเดียว — เก็บใน financials.db (local-only), stale เกิน 7 วันดึงสดใหม่
    เอง, ?refresh=1 บังคับ ดู PLAN_stock_study_suite.txt งาน #4"""
    from datetime import datetime as _dt, timedelta as _td, date as _date
    mkt = (market or "TH").upper()
    sym = symbol.upper().strip()
    force = request.args.get("refresh") == "1"
    today_iso = _date.today().isoformat()

    rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=today_iso)
    stale = True
    if synced_at:
        try:
            stale = (_dt.now() - _dt.fromisoformat(synced_at)) > _td(days=_CALENDAR_STALE_DAYS)
        except ValueError:
            stale = True

    fetch_error = None
    if force or rows is None or stale:
        try:
            fresh = financials_store.fetch_calendar_events(sym, market=mkt)
            financials_store.save_calendar_events(BASE_DIR, sym, mkt, fresh)
            rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=today_iso)
            stale = False
        except Exception as e:
            fetch_error = str(e)

    return jsonify({
        "symbol": sym, "market": mkt, "synced_at": synced_at, "stale": stale,
        "fetch_error": fetch_error, "events": rows or [],
    })


@app.route("/api/financials-meta")
def financials_meta():
    """วันที่ sync งบการเงินเต็มล่าสุด — ใช้เช็คฝั่ง UI ว่าถึงรอบเตือนอัพเดท (~2 เดือน) หรือยัง"""
    return jsonify(financials_store.get_meta_summary(BASE_DIR))


_FIN_UNIVERSE_STALE_DAYS = 180   # ราคาไม่ขยับเกินนี้ = น่าจะแขวน SP/เพิกถอน/ฟื้นฟูกิจการถาวร
                                  # (ตรวจจริง: 295 หุ้น "ขาด" ในหน้าตรวจครบถ้วน 270 ตัวไม่เทรดมา
                                  # เกิน 30 วัน, 240 ตัวเกินปี — sync ซ้ำเท่าไหร่ก็ไม่มีทางได้ข้อมูล
                                  # เพราะ Yahoo/SET.or.th ไม่มีงบให้บริษัทที่หยุดดำเนินการ)


def _financials_universe():
    last_dates = price_store.get_last_dates(BASE_DIR)
    tickers = sorted(last_dates.keys())
    syms = [t[:-3] if t.endswith(".BK") else t for t in tickers]
    # ตัดตราสารที่ "ไม่มีงบการเงินเป็นของตัวเอง" หรือ "ไม่มีทางมีงบอีกแล้ว" ออกจาก universe
    # — เดิมโดนนับรวมแล้วค้างเป็น "หุ้นขาด" ในหน้าตรวจสอบความครบถ้วน ให้ผู้ใช้กดดึงซ้ำฟรีตลอดไป
    # โดยไม่มีทางสำเร็จ (ตรวจกับ Yahoo/SET.or.th ตรงๆ แล้วคืน 404/ไม่มีข้อมูลจริงสม่ำเสมอ):
    #   1) ใบ DR ที่เทรดบน SET (เช่น SP50001) — รายชื่อชัวร์จาก field drs ของ DR universe
    #   2) DW (derivative warrants เช่น ADVA01CB, BANP01CC) — จับด้วยรูปแบบชื่อมาตรฐาน
    #      underlying + เลขโบรก 2 หลัก + C/P + รุ่น 1 ตัวอักษร (บังคับ prefix ≥2 ตัวอักษร
    #      กันหุ้นจริงชื่อสั้นอย่าง 24CS หลุดเข้า pattern — ตรวจกับ universe จริงแล้วไม่มีหุ้นจริงโดน)
    #   3) ดัชนีรายกลุ่มอุตสาหกรรมขึ้นต้นด้วย "!" (เช่น !AGRO) — ติดมากับ import หุ้น delisted
    #   4) ราคาไม่ขยับเกิน _FIN_UNIVERSE_STALE_DAYS วัน — แขวน SP/เพิกถอน/ฟื้นฟูกิจการถาวร
    dr_series = {d for s in load_dr_universe(BASE_DIR) for d in (s.get("drs") or [])}
    dw_pat = re.compile(r"^[A-Z0-9]{2,}\d{2}[CP][A-Z]$")
    from datetime import datetime as _dt, timedelta as _td
    cutoff = _dt.now() - _td(days=_FIN_UNIVERSE_STALE_DAYS)

    def _is_stale(sym):
        d = last_dates.get(sym + ".BK") or last_dates.get(sym)
        if not d:
            return False
        try:
            return _dt.strptime(d, "%Y-%m-%d") < cutoff
        except (TypeError, ValueError):
            return False

    out = []
    for s in syms:
        if s in dr_series or dw_pat.match(s) or s.startswith("!"):
            continue
        if _is_stale(s):
            # บันทึกไว้ให้ backtest รุ่นถัดไปรู้ว่าหุ้นนี้ "ยังอยู่จริง" ถึงวันไหน
            # (upsert — เก็บวันที่ตรวจพบครั้งแรก ไม่ทับทุกรอบที่ universe กรองซ้ำ)
            delisted_log.record_delisted(
                BASE_DIR, s, "TH", f"ราคาไม่ขยับเกิน {_FIN_UNIVERSE_STALE_DAYS} วัน (แขวน SP/เพิกถอน)",
                last_seen=last_dates.get(s + ".BK") or last_dates.get(s))
            continue
        out.append(s)
    return out


def _dr_financials_universe():
    """หุ้นต่างประเทศ (underlying ของ DR/DRx) — มีข้อมูลจาก Yahoo Finance เท่านั้น
    ไม่มี SET.or.th เพราะเป็นหุ้นต่างประเทศ (ไม่ใช่หุ้นไทย)
    ตัด ETF ออก — กองทุน/ETF ไม่มีงบการเงินแบบบริษัท (โชว์ note แยกในหน้า UI แทน)"""
    return sorted(s["sym"] for s in load_dr_universe(BASE_DIR) if not s.get("etf"))


@app.route("/api/financials-coverage")
def financials_coverage():
    """เทียบ universe หุ้นทั้งหมดกับที่มีข้อมูลจริงใน DB แล้วต่อแหล่ง (yahoo/set)
    ใช้เช็คว่า sync ครบหรือยัง หุ้นไหนโดนบล็อค/ยังไม่มีข้อมูล — คืน missing แยกตาม source
    ?universe=dr เช็คเฉพาะหุ้นต่างประเทศ (มีแค่ yahoo — SET.or.th ไม่มีข้อมูลหุ้นต่างประเทศ)"""
    if request.args.get("universe") == "dr":
        symbols = _dr_financials_universe()
        coverage = financials_store.get_coverage(BASE_DIR, symbols, sources=("yahoo",), is_dr=True)
        # ETF/กองทุนไม่มีงบการเงินแบบบริษัท — รายงานแยกพร้อมเหตุผล (ไม่นับเป็น missing)
        coverage["excluded_etf"] = sorted(
            [{"sym": s["sym"], "name": s.get("name", ""),
              "reason": "ETF/กองทุน — ไม่มีงบการเงินแบบบริษัท"}
             for s in load_dr_universe(BASE_DIR) if s.get("etf")],
            key=lambda x: x["sym"])
    else:
        symbols = _financials_universe()
        coverage = financials_store.get_coverage(BASE_DIR, symbols)
    return jsonify(coverage)


def _run_financials_sync(symbols=None, sources=None, is_dr=False, min_age_days=None):
    try:
        if is_dr and not symbols:
            # เช็ค SET.or.th ก่อนดึงงบ — DR ออกใหม่ถูกเพิ่มเข้า universe อัตโนมัติ
            # ไม่งั้นปุ่ม "ดึงเฉพาะที่ขาด/เก่า" มองไม่เห็นหุ้นใหม่จนกว่าหน้า DR
            # จะ rebuild ราคา (sync ล้มเหลวก็ใช้ลิสต์เดิมไปก่อน ไม่ให้งานล่ม)
            _update(message="เช็คหุ้น DR ใหม่จาก SET.or.th ก่อนดึงงบ...")
            try:
                st = sync_dr_universe(BASE_DIR)
                if st.get("added") or st.get("appended"):
                    _dr_diff_cache.clear()   # ลิสต์เปลี่ยน — ผลเช็คหุ้นใหม่เดิมไม่ตรงแล้ว
                    _dr_cache.clear()        # ล้าง cache ราคา — เปิดหน้า DR รอบหน้า rebuild เห็นหุ้นใหม่เลย
                    print(f"[DR-sync] ก่อนดึงงบ: underlying ใหม่ {st.get('added', 0)}, "
                          f"series ใหม่ {st.get('appended', 0)}, ยัง map ไม่ได้ {st.get('unmapped', 0)}")
            except Exception as e:
                print(f"[DR-sync] ข้าม (sync ล้มเหลว ใช้ลิสต์เดิม): {e}")
            symbols = _dr_financials_universe()
        target = symbols if symbols else _financials_universe()
        # yahoo_q = งบรายไตรมาส (สะสมทุกรอบ sync — ใช้กรอง QoQ/YoY-Q ใน Screener)
        # finnomena_q = งบไตรมาสย้อนยาว ~20 ปี (backfill ครั้งเดียวได้ streak/เร่งตัว/TTM เต็มสูตร)
        srcs = tuple(sources) if sources else ("yahoo", "set", "yahoo_q", "finnomena_q")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        result = financials_store.sync_all(BASE_DIR, target, sources=srcs, callback=cb,
                                           is_dr=is_dr, min_age_days=min_age_days)
        _fin_analytics_cache.clear()   # ข้อมูลเปลี่ยน — บังคับคำนวณ growth/PEG/FCF ใหม่รอบถัดไป
        skipped = result.get("skipped", 0)
        _update(running=False, done=True,
                message=f"เสร็จแล้ว! สำเร็จ {result['ok']}/{result['total']}"
                        + (f" · ข้าม {skipped} คู่ (ดึงไปแล้วไม่เกิน {min_age_days} วัน)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']} — อาจโดนบล็อคชั่วคราวหรือแหล่งข้อมูลไม่มีจริง ลองอีกครั้งได้)" if result["fail"] else ""))
    except Exception as e:
        _update(running=False, done=True, error=str(e),
                message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/financials/sync-all", methods=["POST"])
def start_financials_sync():
    symbols = None
    sources = None
    is_dr = False
    min_age_days = None
    if request.is_json:
        body = request.json or {}
        if body.get("universe") == "dr":
            # ไม่ resolve รายชื่อที่นี่ — _run_financials_sync จะ sync ลิสต์กับ
            # SET.or.th ก่อนแล้วค่อยสร้างรายชื่อ (เห็นหุ้น DR ออกใหม่ทันที)
            sources = ["yahoo", "yahoo_q", "finnomena_q"]   # DR ไม่มี SET.or.th; finnomena ครอบ US/HK
            is_dr = True
        else:
            body_symbols = body.get("symbols")
            if body_symbols:
                symbols = [str(s).upper().strip() for s in body_symbols]
        body_sources = body.get("sources")
        if body_sources:
            sources = body_sources
        # incremental sync: ข้ามคู่ (หุ้น, แหล่ง) ที่ดึงไปแล้วไม่เกิน N วัน — ใช้กับปุ่ม
        # "ดึงเฉพาะที่ขาด/เก่า" แทนปุ่มดึงเต็มที่ยิงทุกตัวซ้ำทุกครั้ง (ดู sync_all min_age_days)
        raw_age = body.get("min_age_days")
        if raw_age is not None:
            try:
                min_age_days = max(0, int(raw_age))
            except (TypeError, ValueError):
                min_age_days = None
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        label = ("กำลังเริ่ม sync งบการเงิน" + (" (หุ้นต่างประเทศ DR)" if is_dr else " (เฉพาะที่ขาด)" if symbols else "")
                + (f" — ข้ามของที่ดึงไปแล้วไม่เกิน {min_age_days} วัน" if min_age_days is not None else "") + "...")
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message=label)
    threading.Thread(target=_run_financials_sync, args=(symbols, sources, is_dr, min_age_days), daemon=True).start()
    return jsonify({"ok": True})


def _compute_fin_analytics_for(symbols, is_dr, pe_map, mktcap_map, yahoo_only=False, fin_sector_syms=None):
    """คำนวณ growth/PEG/FCF/streak/ratio ของ symbol กลุ่มเดียว (หุ้นไทย หรือ DR)
    แยก percentile ranking กันคนละ universe — ไม่งั้นหุ้นไทยจะถูกเทียบ growth score
    กับหุ้นเทคยักษ์ใหญ่ระดับโลกที่ปนอยู่ใน DR (และกลับกัน)

    yahoo_only=True: ไม่แตะ Finnomena เลย (ใช้ yahoo_q ล้วนสำหรับข้อมูลรายไตรมาส) —
    ใช้ตอน bake ไฟล์ static สำหรับเว็บมือถือ/ไอแพด ที่ไม่มี financials.db เต็ม
    ผลต่าง: quarters_available ตื้นกว่ามาก (Yahoo ให้ปกติ ~5 ไตรมาสต่อหุ้น สะสม
    เพิ่มทีละรอบ sync) ทำให้ signal ที่ต้องมองย้อนหลายไตรมาส (กำไรเร่งตัว/TTM margin
    delta ซึ่งต้องการ ≥8 ไตรมาส) ไม่มีค่า — ฝั่ง frontend รู้เรื่องนี้อยู่แล้วและปิด
    ช่องกรองพวกนั้นไว้เฉพาะบนเว็บ (ดู _FIN_STATIC_UNAVAILABLE_IDS ใน dashboard.js)
    ส่วน PEG จะ fallback ไปใช้ CAGR รายปีแทน TTM เมื่อคำนวณ TTM ไม่ได้"""
    from core.metrics import rank_percentile
    result = {}
    growth_raw = {}
    for sym in symbols:
        payload = financials_store.get(BASE_DIR, sym, "yahoo", is_dr=is_dr)
        if not payload:
            continue
        gs = financials_store.compute_growth_score(payload)
        fcf = financials_store.compute_fcf_metrics(payload, mktcap_map.get(sym))
        streaks = financials_store.compute_growth_streaks(payload)
        ratios = financials_store.compute_ratio_trends(payload)
        q_finn = None
        if yahoo_only:
            q_used = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr)
            qg = financials_store.compute_quarterly_growth(q_used)
        else:
            # การเติบโตรายไตรมาส (QoQ / YoY-Q / streak / เร่งตัว) จากงบ quarterly — Finnomena
            # ลึกกว่า Yahoo เสมอเมื่อมี (ตรวจแล้วทุกกรณี ~60-80 ไตรมาส vs ~5-6) จึงเช็คแค่ตัวเดียว
            # ก่อน ไม่ต้อง get() ทั้งคู่ทุกครั้ง — ก่อนนี้ทำให้ endpoint นี้ช้าลงเพราะ get() แต่ละครั้ง
            # เปิด/ปิด sqlite connection ใหม่ (สังเกตได้จาก /api/financials-analytics ช้าลงเป็น ~23 วิ
            # ตอน cache miss จนแข่งกับปุ่ม "งบการเงิน" ในหน้า DR ที่รอ _finAnalyticsData โหลดเสร็จ)
            q_finn = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr)
            q_used = q_finn if q_finn else financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr)
            qg = financials_store.compute_quarterly_growth(q_used)
            # Finnomena สั้นผิดปกติ (<8 ไตรมาส เช่นหุ้นที่ Finnomena เพิ่งเริ่มเก็บ) — เช็ค yahoo_q
            # เผื่อลึกกว่า ให้เลือกแหล่งแบบเดียวกับ factor_snapshot (ไม่งั้นสองเมนูต่างกันได้)
            if q_finn is not None and qg["quarters_available"] < 8:
                q_yah = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr)
                qg_y = financials_store.compute_quarterly_growth(q_yah)
                if qg_y["quarters_available"] > qg["quarters_available"]:
                    qg, q_used = qg_y, q_yah
        # เป็นบวกติดกัน (รายได้/กำไร/EBITDA/OCF) + OCF>กำไรสุทธิงวดล่าสุด — เวอร์ชันรันบนเครื่อง
        # ใช้ Finnomena รายไตรมาสล้วน (เหมือน factor_snapshot.compute_positive_streaks)
        # ส่วน yahoo_only (bake เว็บมือถือ/ไอแพด ไม่มี Finnomena) สลับไปนับจากงบ Yahoo
        # "รายปี" แทน (payload เดิมที่ดึงมาแล้วด้านบน) — field ชื่อเดียวกันแต่หน่วยกลายเป็นปี
        # ไม่ใช่ไตรมาส ฝั่ง frontend (_patchPosStreakForStatic) แก้ label/tooltip ให้ตรงแล้ว
        pq = financials_store.compute_positive_streaks(payload if yahoo_only else q_finn)
        pe = pe_map.get(sym)
        # PEG = PE ÷ %โตของ 'กำไร' TTM — นิยามเดียวกับ Screener+ (เดิมใช้ CAGR รายได้
        # หลายปีซึ่งไม่ตรงนิยามสากลและทำให้ค่า PEG สองเมนูไม่ตรงกัน) มีค่าเฉพาะ
        # growth 1-200%: ต่ำกว่านั้น PEG ระเบิด / เกินนั้นเป็น base effect หลอกตา
        tg = financials_store.compute_ttm_growth(q_used)
        g = tg["profit_ttm_yoy"]
        if yahoo_only and g is None:
            # Yahoo รายไตรมาสมักมีแค่ ~5 งวด ไม่พอคำนวณ TTM (ต้องการ ≥8) — fallback ไป
            # กำไรโตเฉลี่ยรายปี (CAGR) แทน ยังมีประโยชน์แต่นิยามไม่เหมือน TTM เป๊ะ
            # (บอกผู้ใช้ผ่าน tooltip ฝั่ง frontend แล้ว กันเข้าใจผิดว่าตรงกับเวอร์ชันเครื่อง)
            g = gs.get("profit_cagr")
        peg = (pe / g) if (pe and pe > 0 and g is not None and 1 <= g <= 200) else None
        # P/S = Market Cap / รายได้ปีล่าสุด (Yahoo) — ใช้ได้แม้หุ้นขาดทุนที่ PE คำนวณไม่ได้
        rev_row = payload.get("income", {}).get("Total Revenue", {})
        latest_rev = rev_row[max(rev_row)] if rev_row else None
        mc = mktcap_map.get(sym)
        ps = (mc / latest_rev) if (mc and latest_rev and latest_rev > 0) else None
        # F-Score/Z-Score — mkt_cap ที่นี่มาจาก set_data.json (สดกว่า Finnomena valuation
        # ที่ factor_snapshot ใช้) ธนาคาร/เงินทุน/ประกันไทย ไม่แสดง Z-Score (ดู
        # factor_snapshot._financial_sector_symbols — งบดุลตีความไม่ได้กับสูตร Altman)
        fscore = financials_store.compute_fscore(payload)
        if (not is_dr) and fin_sector_syms and sym in fin_sector_syms:
            zscore = {"z_score": None, "z_variant": None, "z_zone": None}
        else:
            zscore = financials_store.compute_zscore(payload, mc, variant=("Z" if is_dr else "Z2"))
        result[sym] = {**gs, **fcf, **streaks, **ratios, **qg, **tg, **pq, "peg": peg, "ps": ps,
                       "f_score": fscore["f_score"], "f_score_max": fscore["f_score_max"],
                       "f_score_detail": fscore["f_score_detail"],
                       "z_score": zscore["z_score"], "z_variant": zscore["z_variant"],
                       "z_zone": zscore["z_zone"]}
        growth_raw[sym] = gs["growth_score"]

    percentiles = rank_percentile(growth_raw)
    for sym, pct in percentiles.items():
        result[sym]["growth_percentile"] = pct
    return result


@app.route("/api/financials-analytics")
def financials_analytics():
    """Growth Score / PEG / FCF Yield / Dividend Coverage ทั้งตลาด — คำนวณจาก financials.db
    (local-only) ผสาน pe/mkt_cap จาก set_data.json — cache ใน memory (event-invalidate ตอน
    sync งบการเงินเสร็จ + TTL กันค้างข้าม restart)

    คืนแยก {"set": {...}, "dr": {...}} เพราะ symbol อาจชื่อชนกันข้ามสอง universe
    (เช่น 'META' มีทั้งหุ้นไทย mai และ underlying ของ DR) — merge รวม dict เดียวจะ
    เขียนทับข้อมูลกันฝั่งใดฝั่งหนึ่งผิด

    ?source=yahoo : คำนวณจาก Yahoo ล้วน ไม่แตะ Finnomena เลย (ดู yahoo_only ใน
    _compute_fin_analytics_for) — ใช้ตอน bake ไฟล์ static สำหรับเว็บมือถือ/ไอแพด
    cache แยกจากโหมดปกติ (คนละผลลัพธ์กัน)"""
    yahoo_only = request.args.get("source") == "yahoo"
    cache_key = "yahoo" if yahoo_only else "default"
    slot = _fin_analytics_cache.setdefault(cache_key, {})
    cached = slot.get("result")
    if cached and (time.time() - slot.get("ts", 0) < _FIN_ANALYTICS_CACHE_TTL):
        return jsonify(cached)

    pe_map, mktcap_map = {}, {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                for s in json.load(f).get("stocks", []):
                    pe_map[s["symbol"]] = s.get("pe")
                    mktcap_map[s["symbol"]] = s.get("mkt_cap")
        except Exception:
            pass

    set_symbols = financials_store.get_synced_symbols(BASE_DIR, "yahoo", is_dr=False)
    dr_symbols = financials_store.get_synced_symbols(BASE_DIR, "yahoo", is_dr=True)
    fin_sector_syms = factor_snapshot._financial_sector_symbols(BASE_DIR)
    result = {
        "set": _compute_fin_analytics_for(set_symbols, False, pe_map, mktcap_map, yahoo_only=yahoo_only, fin_sector_syms=fin_sector_syms),
        "dr": _compute_fin_analytics_for(dr_symbols, True, pe_map, mktcap_map, yahoo_only=yahoo_only),
    }

    slot["result"] = result
    slot["ts"] = time.time()
    return jsonify(result)


@app.route("/api/factor-screener")
def factor_screener():
    """Deep Screener — ตารางปัจจัยพื้นฐานต่อหุ้น (รวม Finnomena+Yahoo+SET) จาก factor_snapshot
    (local-only, precompute ด้วย build_snapshot.py) overlay pe/mkt_cap สดจาก set_data.json
    เพื่อคำนวณ peg/fcf_yield ที่อิงราคาปัจจุบัน — คืน {rows: [...], meta: {...}}

    ?universe=us / hk : คืนหุ้น mirror ทั้งตลาด (นอก universe หลัก, งบ Finnomena ล้วน)
    แทนชุดหลัก (ไทย+DR) — โหลดแยก เพราะชุดใหญ่ (US ~11k, HK ~2k)"""
    uni = (request.args.get("universe") or "").lower()
    if uni in ("us", "hk"):
        # ชุด mirror ใหญ่มาก (US ~17k) — กรองฝั่ง server ส่งเฉพาะผลลัพธ์ (≤ limit)
        rows = factor_snapshot.get_mirror_snapshot(BASE_DIR, uni.upper())

        # overlay technical (rs/EMA200/52W high/rvol/stage) ให้เฉพาะตัวที่อยู่ใน
        # S&P500+Dow+NDX (~518 ตัว จาก us_index_metrics.json — ราคาราย daily จาก
        # us_prices.db) / HSI+HSCEI+HSTECH (~105 ตัว จาก hk_index_metrics.json —
        # ราคาราย daily จาก hk_prices.db) — mirror ตัวอื่นๆ นอกดัชนีหลักยังไม่มีราคา
        # รายวันเก็บไว้ เลยยังเป็น None เหมือนเดิม
        if uni == "us":
            from sources import us_index_metrics
            _us_by_sym = {s["symbol"]: s for s in us_index_metrics.load_local(BASE_DIR).get("stocks", [])}
            for r in rows:
                s = _us_by_sym.get(r["symbol"])
                r["rs"] = s.get("rs_score") if s else None
                r["pct_vs_ema200"] = (round((s["price"] / s["ema200"] - 1) * 100, 2)
                                       if s and s.get("price") and s.get("ema200") else None)
                r["pct_off_high52"] = (round((s["price"] / s["high_52w"] - 1) * 100, 2)
                                        if s and s.get("price") and s.get("high_52w") else None)
                r["rvol"] = (round(s["vol_today"] / s["vol_avg20"], 4)
                             if s and s.get("vol_today") and s.get("vol_avg20") else None)
                r["stage"] = s.get("stage") if s else None
        elif uni == "hk":
            from sources import hk_index_metrics
            # symbol ในชุด mirror เป็นรหัสดิบ (เช่น "0700") ส่วน hk_index_metrics ใช้
            # รูปแบบ yfinance "0700.HK" — ต่อ ".HK" ก่อน lookup
            _hk_by_sym = {s["symbol"]: s for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", [])}
            for r in rows:
                s = _hk_by_sym.get(f"{r['symbol']}.HK")
                r["rs"] = s.get("rs_score") if s else None
                r["pct_vs_ema200"] = (round((s["price"] / s["ema200"] - 1) * 100, 2)
                                       if s and s.get("price") and s.get("ema200") else None)
                r["pct_off_high52"] = (round((s["price"] / s["high_52w"] - 1) * 100, 2)
                                        if s and s.get("price") and s.get("high_52w") else None)
                r["rvol"] = (round(s["vol_today"] / s["vol_avg20"], 4)
                             if s and s.get("vol_today") and s.get("vol_avg20") else None)
                r["stage"] = s.get("stage") if s else None
        try:
            filters = json.loads(request.args.get("filters") or "[]")
        except Exception:
            filters = []
        sort_key = request.args.get("sort") or "roe"
        sort_dir = 1 if request.args.get("dir") == "asc" else -1
        try:
            limit = max(1, min(int(request.args.get("limit", 500)), 1000))
        except Exception:
            limit = 500

        def _keep(r):
            for c in filters:
                v = r.get(c.get("k"))
                cmp = c.get("cmp")
                if cmp == "bool":
                    if not v:
                        return False
                    continue
                if cmp == "eq":
                    if v != c.get("v"):
                        return False
                    continue
                if v is None or isinstance(v, str):
                    # nullOk (risk filter ชุดตัดความเสี่ยง): ไม่มีข้อมูล = ผ่าน
                    # (ตัดเฉพาะตัวที่ 'รู้ว่าแย่' ไม่ใช่ตัวที่ไม่มีข้อมูล)
                    if v is None and c.get("nullOk"):
                        continue
                    return False
                cv = c.get("v")
                if cmp == "gte" and v < cv:
                    return False
                if cmp == "lte" and v > cv:
                    return False
            return True

        matched = [r for r in rows if _keep(r)]
        total = len(matched)
        # เรียงค่า None ไว้ท้ายเสมอ (ทั้ง asc/desc) — แยก non-null ออกมาเรียงก่อน
        non_null = [r for r in matched if r.get(sort_key) is not None]
        null_rows = [r for r in matched if r.get(sort_key) is None]
        try:
            non_null.sort(key=lambda r: r.get(sort_key), reverse=(sort_dir < 0))
        except TypeError:
            # คอลัมน์ปนชนิด (เลข+string) — เรียงแบบ string ทั้งชุดแทนที่จะพัง
            non_null.sort(key=lambda r: str(r.get(sort_key)), reverse=(sort_dir < 0))
        matched = non_null + null_rows
        return jsonify({"rows": matched[:limit], "total": total, "returned": min(total, limit),
                        "meta": factor_snapshot.mirror_snapshot_meta(BASE_DIR)})

    rows = factor_snapshot.get_snapshot(BASE_DIR)
    if not rows:
        return jsonify({"rows": [], "meta": {"computed_at": None, "count": 0,
                        "note": "ยังไม่มี factor snapshot — รัน build_snapshot.py ก่อน (local-only)"}})

    th_map = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                for s in json.load(f).get("stocks", []):
                    th_map[s["symbol"]] = s
        except Exception:
            pass
    dr_map = {}
    try:
        with open(os.path.join(BASE_DIR, "dr_cache.json"), encoding="utf-8") as f:
            for s in json.load(f).get("stocks", []):
                dr_map[s["sym"]] = s
    except Exception:
        pass

    def _pct_vs(price, base):
        return round((price / base - 1) * 100, 2) if (price and base and base > 0) else None

    # overlay ค่าที่อิงราคาปัจจุบัน — หุ้นไทยจาก set_data.json, DR จาก dr_cache.json
    # รวม technical (RS / %เหนือ EMA200 / %จาก high 52wk / RVOL) สำหรับ screen
    # แบบ CANSLIM ครบสูตร (พื้นฐาน x ราคานำตลาด) — mirror US/HK ไม่มีราคารายวัน
    for r in rows:
        if not r["is_dr"]:
            s = th_map.get(r["symbol"]) or {}
            pe = s.get("pe")
            mc = s.get("mkt_cap")
            r["pe_live"] = pe
            r["mkt_cap"] = mc
            # PEG = PE สด ÷ กำไรโต TTM (นิยามเดียวกับ snapshot/mirror) — ถ้าไม่มี pe สด
            # คงค่า peg จาก snapshot (PE Finnomena งวดล่าสุด) ไว้ตามเดิม
            g = r.get("profit_ttm_yoy")
            # เงื่อนไขเดียวกับ _calc_peg ใน factor_snapshot (growth 1-200% เท่านั้น —
            # เกินนั้นเป็น base effect ทำ PEG จิ๋วหลอกตา)
            if pe and pe > 0 and g is not None and 1 <= g <= 200:
                r["peg"] = round(pe / g, 2)
            r["fcf_yield"] = (r["fcf"] / mc * 100) if (r.get("fcf") is not None and mc) else None
            r["rs"] = s.get("rs_score")
            r["sector"] = s.get("sector")
            r["pct_vs_ema200"] = _pct_vs(s.get("price"), s.get("ema200"))
            r["pct_off_high52"] = _pct_vs(s.get("price"), s.get("high_52w"))
            va = s.get("vol_avg20")
            r["rvol"] = round(s["vol_today"] / va, 4) if (s.get("vol_today") is not None and va) else None
        else:
            e = dr_map.get(r["symbol"]) or {}
            r["rs"] = e.get("rs_score")   # RS จัดอันดับภายใน universe DR ด้วยกัน
            r["pct_off_high52"] = _pct_vs(e.get("price"), e.get("high_52w"))
            r["pct_vs_ema200"] = None     # dr_cache ไม่เก็บ EMA200/volume เฉลี่ย
            r["rvol"] = None

    # percentile ภายใน sector (หุ้นไทย — sector จาก set_data.json): PE ต่ำ=ถูกกว่าเพื่อน
    # ในกลุ่ม, ROE สูง=เด่นกว่ากลุ่ม — แก้จุดอ่อน "PE/ROE เทียบข้ามอุตสาหกรรมไม่ได้"
    # ต้องมีเพื่อนร่วม sector >= 5 ตัวถึงจัดอันดับ (น้อยกว่านั้น percentile ไม่มีนัยยะ)
    from core.metrics import rank_percentile
    by_sector = {}
    for r in rows:
        if not r["is_dr"] and r.get("sector"):
            by_sector.setdefault(r["sector"], []).append(r)
    for sec_rows in by_sector.values():
        if len(sec_rows) < 5:
            continue
        pe_pct = rank_percentile({r["symbol"]: (r.get("pe_live") or r.get("pe_value")) for r in sec_rows})
        roe_pct = rank_percentile({r["symbol"]: r.get("roe") for r in sec_rows})
        for r in sec_rows:
            r["pe_sector_pctile"] = pe_pct.get(r["symbol"])
            r["roe_sector_pctile"] = roe_pct.get(r["symbol"])
    return jsonify({"rows": rows, "meta": factor_snapshot.snapshot_meta(BASE_DIR)})


@app.route("/api/peer-compare")
def peer_compare():
    """🆚 เทียบเพื่อนร่วม sector/industry ในตารางเดียว — งาน #3 ของ PLAN_stock_study_suite.txt

    ?symbol=CPALL          : ใช้ sector ของหุ้นนี้เป็นกลุ่ม (หุ้นตั้งต้น pin บนสุดฝั่ง frontend)
    ?sector=...&level=...  : เลือกกลุ่มตรง ๆ ไม่ต้องมีหุ้นตั้งต้น
    ?level=sector|industry : ชั้นการจัดกลุ่ม (default sector) — auto ขยับเป็น industry เอง
                              ถ้ากลุ่ม sector มีสมาชิก < 4 ตัว (percentile ไม่มีนัยยะ)
    ?market=TH|US|HK|DR    : default TH · US/HK สมาชิกดัชนีหลัก (us/hk_index_metrics.json
                              ~623 ตัว) ใช้ sector ที่มีอยู่แล้ว · ถ้า symbol ที่ระบุเป็นหุ้น
                              mirror นอกดัชนีหลัก ดึง sector แบบ on-demand ให้ตัวนั้นแล้ว pin
                              เข้ากลุ่ม sector เดียวกัน (peer ที่เห็นยังจำกัดแค่สมาชิกดัชนีหลัก
                              เท่านั้น — ยังไม่มี sector ของหุ้นอื่นนอกดัชนีแบบ bulk) · DR:
                              resolve symbol เป็น underlying US/HK ผ่าน dr_universe ก่อน
                              (ต้องระบุ symbol เสมอ, ไม่รองรับ sector โดยตรง — region อื่นที่ไม่ใช่
                              US/HK ยัง 501)
                              level เป็น 'sector' อย่างเดียวเสมอ (index metrics มีแค่ชั้นเดียว
                              ไม่มี industry ย่อยแบบ set_data.json)

    ตัวเลข factor มาจาก factor_snapshot/factor_snapshot_mirror (local-only) — ไม่คำนวณใหม่"""
    symbol = (request.args.get("symbol") or "").upper().strip()
    sector_q = (request.args.get("sector") or "").strip()
    level = request.args.get("level") or "sector"
    if level not in ("sector", "industry"):
        level = "sector"
    mkt = (request.args.get("market") or "TH").upper()
    if mkt not in ("TH", "US", "HK", "JP", "DR"):
        mkt = "TH"

    dr_symbol = None
    if mkt == "DR":
        if not symbol:
            return jsonify({"rows": [], "median": None, "count": 0,
                            "meta": {"note": "market=DR ต้องระบุ symbol เสมอ"}})
        from sources.dr_universe import load_dr_universe
        entry = next((e for e in load_dr_universe(BASE_DIR) if e["sym"] == symbol), None)
        if not entry:
            return jsonify({"rows": [], "median": None, "count": 0,
                            "meta": {"note": f"ไม่พบหุ้น DR {symbol}"}})
        region = entry.get("region")
        if region not in ("US", "HK"):
            return jsonify({"rows": [], "median": None, "count": 0,
                            "meta": {"note": f"หุ้น DR {symbol} (underlying ตลาด {region or '?'})"
                                             f" ยังไม่รองรับ — รองรับเฉพาะ underlying ตลาด US/HK"}}), 501
        dr_symbol = symbol
        mkt = region
        symbol = entry["yf"].upper()

    if mkt != "TH":
        level = "sector"   # index metrics มีแค่ sector ชั้นเดียว ไม่มี industry ให้ widen

    th_map = _tearsheet_universe_map(mkt)
    ondemand_note = None
    if symbol and mkt != "TH" and symbol not in th_map:
        # HK ใช้ "0700.HK" ในดัชนีหลักแต่ mirror_candidates/fetch_header ต้องการรหัสดิบ (ดู
        # _mirror_sym) — ตัดก่อนเช็ค/ก่อนส่งเข้า yfinance เสมอ
        raw_symbol = _mirror_sym(mkt, symbol)
        if any(name == raw_symbol for ex, name, _sid in financials_store.mirror_candidates((mkt,))):
            from sources import mirror_ondemand
            hdr = mirror_ondemand.fetch_header(BASE_DIR, mkt, raw_symbol)
            if hdr and hdr.get("sector"):
                th_map = dict(th_map)   # สำเนา local — ไม่แก้ dict ที่ _tearsheet_universe_map คืนมา
                th_map[symbol] = hdr
                ondemand_note = (f"{symbol} ไม่ใช่สมาชิกดัชนีหลัก — sector ดึงแบบ on-demand,"
                                  f" peer ที่เห็นยังจำกัดแค่สมาชิกดัชนีหลักเท่านั้น")
    if not th_map:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": f"ไม่มีข้อมูล universe ของตลาด {mkt}"}})

    base = th_map.get(symbol) if symbol else None
    if sector_q:
        group_key = sector_q
    elif base:
        group_key = base.get(level) or base.get("sector")
        level = level if base.get(level) else "sector"
    else:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": "ต้องระบุ symbol หรือ sector"}})
    if not group_key:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": f"หุ้น {symbol} ไม่มีข้อมูล {level}"}})

    members = [sym for sym, s in th_map.items() if s.get(level) == group_key]
    widened = False
    if level == "sector" and len(members) < 4:
        ind = (base or {}).get("industry") if base else None
        if not ind and sector_q:
            # ผู้ใช้เลือก sector ตรง ๆ (ไม่มีหุ้นตั้งต้น) — หา industry จากสมาชิกกลุ่มเดิม
            sample = th_map.get(members[0]) if members else None
            ind = sample.get("industry") if sample else None
        if ind:
            wide_members = [sym for sym, s in th_map.items() if s.get("industry") == ind]
            if len(wide_members) > len(members):
                members, level, group_key, widened = wide_members, "industry", ind, True

    if mkt == "TH":
        snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}
        fin_sector_syms = factor_snapshot._financial_sector_symbols(BASE_DIR)
    else:
        snap_rows = {r["symbol"]: r for r in factor_snapshot.get_mirror_snapshot(BASE_DIR, mkt)}
        # เช็คด้วยชื่อ GICS sector ทุกรูปแบบที่พบจริงใน us/hk_index_metrics.json (ไม่ใช่แค่
        # "Financials" เดี่ยวๆ — HK มี "Finance" แยกต่างหากสำหรับ HSBC/HKEX/AIA/BOCHK ด้วย)
        fin_sector_syms = {sy for sy, s2 in th_map.items()
                           if s2.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES}

    rows = []
    for sym in members:
        s = th_map.get(sym) or {}
        f = snap_rows.get(_mirror_sym(mkt, sym)) or {}
        is_fin = sym in fin_sector_syms
        mc = s.get("mkt_cap")
        if mc is None and mkt != "TH" and s.get("price") and f.get("shares_out"):
            mc = s["price"] * f["shares_out"]   # ดู comment เดียวกันใน /api/tearsheet
        # US/HK: factor_snapshot_mirror ไม่รู้จัก sector ตอน build (คำนวณจากทั้ง mirror universe
        # ไม่ใช่แค่สมาชิกดัชนีหลักที่มี sector) — บังคับซ่อน Z-Score เองตรงนี้แทนสำหรับกลุ่มการเงิน
        z_score, z_zone = f.get("z_score"), f.get("z_zone")
        z_reason = f.get("z_excluded_reason")
        if is_fin and mkt != "TH":
            z_score, z_zone = None, None
            z_reason = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"
        row = {
            "symbol": sym, "name": s.get("name"), "sector": s.get("sector"),
            "industry": s.get("industry"), "mkt_cap": mc,
            "pe": s.get("pe") if s.get("pe") is not None else f.get("pe_value"),
            "pbv": s.get("pbv") if s.get("pbv") is not None else f.get("pbv_value"),
            "div_yield": s.get("div_yield"),
            "peg": f.get("peg"), "ps_value": f.get("ps_value"),
            "roe": f.get("roe"), "roa": f.get("roa"),
            "gross_margin": f.get("gross_margin"), "net_margin": f.get("net_margin"),
            "de_ratio": f.get("de_ratio"), "interest_coverage": f.get("interest_coverage"),
            "ocf_ni_ratio": f.get("ocf_ni_ratio"),
            "f_score": f.get("f_score"), "f_score_max": f.get("f_score_max"),
            "z_score": z_score, "z_zone": z_zone, "z_variant": f.get("z_variant"),
            "z_excluded_reason": z_reason,
            "rev_cagr": f.get("rev_cagr"), "profit_cagr": f.get("profit_cagr"),
            "rev_ttm_yoy": f.get("rev_ttm_yoy"), "profit_ttm_yoy": f.get("profit_ttm_yoy"),
            "revenue_streak": f.get("revenue_streak"), "growth_score": f.get("growth_score"),
            "rule_of_40": f.get("rule_of_40"),
            "quarters_available": f.get("quarters_available"),
            "is_financial_sector": is_fin,
            "has_financials": _mirror_sym(mkt, sym) in snap_rows,
        }
        rows.append(row)

    num_cols = ["mkt_cap", "pe", "pbv", "div_yield", "peg", "ps_value", "roe", "roa",
                "gross_margin", "net_margin", "de_ratio", "interest_coverage", "ocf_ni_ratio",
                "f_score", "z_score", "rev_cagr", "profit_cagr", "rev_ttm_yoy", "profit_ttm_yoy",
                "revenue_streak", "growth_score", "rule_of_40"]
    median = {}
    for c in num_cols:
        vals = sorted(v for v in (r.get(c) for r in rows) if isinstance(v, (int, float)))
        n = len(vals)
        if n:
            mid = n // 2
            median[c] = vals[mid] if n % 2 else round((vals[mid - 1] + vals[mid]) / 2, 3)
        else:
            median[c] = None

    rows.sort(key=lambda r: (r.get("mkt_cap") or 0), reverse=True)
    computed_at = (factor_snapshot.snapshot_meta(BASE_DIR).get("computed_at") if mkt == "TH"
                   else factor_snapshot.mirror_snapshot_meta(BASE_DIR).get("computed_at"))
    return jsonify({
        "rows": rows, "median": median, "count": len(rows),
        "meta": {"level": level, "group": group_key, "widened": widened,
                 "base_symbol": symbol or None, "market": mkt, "computed_at": computed_at,
                 "ondemand_note": ondemand_note, "dr_symbol": dr_symbol},
    })


def _mirror_sym(mkt, sym):
    """แปลง symbol ให้ตรงกับ key ที่ factor_snapshot_mirror ใช้จริง — hk_index_metrics.json/
    jp_index_metrics.json เก็บ symbol แบบ '0700.HK'/'7203.T' (ให้ยิง yfinance ตรงๆ ได้) แต่
    namespace mirror ('FINN:HK:0700'/'FINN:JP:7203' ทั้ง finnomena_q/yahoo) ใช้รหัสดิบไม่มี
    suffix เสมอ — ต้องตัด suffix ก่อน lookup ทุกครั้ง ไม่งั้น snap_rows.get(sym) จะไม่เจออะไรเลย
    (US ไม่มีปัญหานี้ ไม่มี suffix ต่อท้ายอยู่แล้ว)"""
    if mkt == "HK":
        return sym.replace(".HK", "")
    if mkt == "JP":
        return sym.replace(".T", "")
    return sym


def _tearsheet_universe_map(mkt):
    """คืน {symbol: entry} ของตลาดที่ tearsheet รองรับ — TH จาก set_data.json (ทุกหุ้น),
    US/HK จาก us_index_metrics.json/hk_index_metrics.json (เฉพาะสมาชิกดัชนีหลัก S&P500+
    Dow+NDX / HSI+HSCEI+HSTECH ~623 ตัว — scope ที่ตัดสินใจไว้ใน PLAN_stock_study_suite.txt
    เพราะ mirror ทั้งก้อนไม่มีราคารายวัน/sector เก็บไว้ ยกเว้นกลุ่มดัชนีหลักนี้)
    field ชื่อเดียวกันทุกไฟล์ (ret_1d/rs_score/stage/price_history ฯลฯ) โค้ดข้างล่างเลยใช้ร่วมกันได้"""
    out = {}
    if mkt == "TH":
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    for s in json.load(f).get("stocks", []):
                        out[s["symbol"]] = s
            except Exception:
                pass
    elif mkt == "US":
        from sources import us_index_metrics
        for s in us_index_metrics.load_local(BASE_DIR).get("stocks", []):
            out[s["symbol"]] = s
    elif mkt == "HK":
        from sources import hk_index_metrics
        for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", []):
            out[s["symbol"]] = s
    elif mkt == "JP":
        from sources import jp_index_metrics
        for s in jp_index_metrics.load_local(BASE_DIR).get("stocks", []):
            out[s["symbol"]] = s
    return out


@app.route("/api/tearsheet/<market>/<symbol>")
def tearsheet(market, symbol):
    """📋 Tearsheet (งาน #1 ใน PLAN_stock_study_suite.txt) — header + valuation + quality + DCF
    input แบบเบา รวมใน call เดียว ส่วนหนัก (เงินทุน/ฤดูกาล/ข่าว) ให้หน้าเรียก endpoint เดิมแยกเอง
    async (/api/financials-analytics, /api/insider-trades, /api/price-analytics, /api/stock-news ฯลฯ)

    market=TH: universe ทั้งหมด (set_data.json) · market=US/HK: สมาชิกดัชนีหลัก
    (us_index_metrics.json/hk_index_metrics.json) ใช้ราคา/RS ที่คำนวณไว้ล่วงหน้าแล้ว ส่วนหุ้น
    mirror อื่นนอกดัชนีหลัก (~4,485 ตัว) ดึงแบบ on-demand ตรงนี้ (sources/mirror_ondemand.py —
    ราคา 2 ปีย้อนหลัง + sector จาก Yahoo สด, cache ไว้ 1 วัน, ไม่มี pipeline ราคารายวันถาวร)
    404 ถ้าไม่ใช่สมาชิก mirror universe เลย (กัน junk ticker ยิง Yahoo ฟรี) หรือดึงราคา
    ไม่สำเร็จ (ticker ผิด/ข้อมูลไม่พอ)

    market=DR: สัญลักษณ์ DR (เช่น AAPL, APPL — รหัส underlying ตาม field 'sym' ใน dr_universe
    ไม่ใช่รหัส DR ตัวจริงอย่าง AAPL01/APPL03) — resolve เป็น underlying US/HK ตัวจริงผ่าน
    dr_universe (field 'yf'/'region') แล้ววิ่งต่อผ่าน flow US/HK ปกติทั้งหมด (สมาชิกดัชนีหลัก/
    on-demand เหมือนเดิม) รองรับเฉพาะ underlying ตลาด US/HK เท่านั้น (region อื่น เช่น JP/VN/EU
    ยังไม่มี cohort ให้เทียบ RS — 501)"""
    from sources import dividend_stats
    mkt = market.upper()
    sym = symbol.upper().strip()
    if mkt not in ("TH", "US", "HK", "JP", "DR"):
        return jsonify({"error": f"ไม่รู้จักตลาด {mkt}"}), 400

    dr_symbol = None
    lite = False          # cohort-free path สำหรับ DR underlying ตลาด JP/VN/SG/EU/TW/CN
    lite_yf = None        # yf ticker จริงของ underlying (เช่น 2243.T) ใช้ทำลิงก์ภายนอก + ดึงราคา
    lite_dr_sym = None    # DR sym (เช่น GSEMI) = key ของ factor ใน namespace DR:
    if mkt == "DR":
        from sources.dr_universe import load_dr_universe
        entry = next((e for e in load_dr_universe(BASE_DIR) if e["sym"] == sym), None)
        if not entry:
            return jsonify({"error": f"ไม่พบหุ้น DR {sym}"}), 404
        region = entry.get("region")
        dr_symbol = sym
        if region in ("US", "HK"):
            mkt = region
            sym = entry["yf"].upper()
        elif region:
            # ตลาดยังไม่มี price cohort ให้เทียบ RS — เปิด Tearsheet แบบ lite (งบ/F-Z/DCF/ปันผล
            # /valuation ครบ, ไม่มี RS/เทียบเพื่อน) ดู mirror_ondemand.fetch_header_lite
            lite = True
            mkt = region
            lite_yf = entry["yf"].upper()
            lite_dr_sym = sym
        else:
            return jsonify({"error": f"หุ้น DR {sym} ไม่มีข้อมูล region — เปิด Tearsheet ไม่ได้"}), 501

    if lite:
        from sources import mirror_ondemand
        th_map = {}       # ไม่มี cohort/universe ของตลาดนี้ -> sector median = None
        s = mirror_ondemand.fetch_header_lite(BASE_DIR, lite_yf, lite_dr_sym, mkt,
                                              name_hint=entry.get("name"))
        if not s:
            return jsonify({"error": f"ดึงราคาหุ้น DR {lite_dr_sym} ({mkt}: {lite_yf}) ไม่สำเร็จ"
                                      f" — Yahoo อาจไม่มีข้อมูล underlying ตัวนี้"}), 404
    else:
        th_map = _tearsheet_universe_map(mkt)
        s = th_map.get(sym)
        if not s and mkt != "TH":
            # HK ใช้ "0700.HK" ในดัชนีหลัก (yfinance ticker ตรงๆ) แต่ mirror_candidates/fetch_header
            # ต้องการรหัสดิบไม่มี suffix (ดู _mirror_sym) — ตัดก่อนเช็ค/ก่อนส่งเข้า yfinance เสมอ
            raw_sym = _mirror_sym(mkt, sym)
            if not any(name == raw_sym for ex, name, _sid in financials_store.mirror_candidates((mkt,))):
                return jsonify({"error": f"ไม่พบหุ้น {sym} ในตลาด {mkt}"}), 404
            from sources import mirror_ondemand
            s = mirror_ondemand.fetch_header(BASE_DIR, mkt, raw_sym)
            if not s:
                return jsonify({"error": f"ดึงราคาหุ้น {sym} ไม่สำเร็จ — ตรวจสอบชื่อย่ออีกครั้ง"
                                          f" หรือหุ้นนี้อาจข้อมูลไม่พอคำนวณ (เพิ่ง IPO/เทรดเบาบาง)"}), 404
        if not s:
            return jsonify({"error": f"ไม่พบหุ้น {sym}"}), 404

    # ticker จริงสำหรับดึงราคาสดจาก Yahoo (ใช้แก้ Valuation Snapshot ให้เป็นราคาสด แทน
    # ราคาสิ้นงวด/สิ้นวันของ snapshot) — DR ที่ resolve เป็น underlying แล้ว sym ตัวนี้คือ
    # yf ticker เต็มอยู่แล้ว (มี .HK ต่อท้ายให้ถ้าเป็น HK) ส่วน US/HK ที่เข้าตรงๆ (ไม่ผ่าน DR)
    # sym ยังเป็นรหัสดิบไม่มี suffix ต้องเติมเอง
    if lite:
        yf_symbol = lite_yf
    elif dr_symbol and mkt in ("US", "HK"):
        yf_symbol = sym
    elif mkt == "TH":
        yf_symbol = f"{sym}.BK"
    else:
        from sources import mirror_ondemand as _mo
        yf_symbol = _mo._yf_ticker(mkt, sym)

    header = {
        "symbol": sym, "name": s.get("name"), "sector": s.get("sector"), "industry": s.get("industry"),
        "is_reit": s.get("is_reit"),
        "price": s.get("price"), "ret_1d": s.get("ret_1d"), "ret_1w": s.get("ret_1w"),
        "ret_1m": s.get("ret_1m"), "ret_3m": s.get("ret_3m"), "ret_1y": s.get("ret_1y"),
        "ret_ytd": s.get("ret_ytd"),
        "rs_score": s.get("rs_score"), "rs_momentum": s.get("rs_momentum"), "stage": s.get("stage"),
        "high_52w": s.get("high_52w"), "low_52w": s.get("low_52w"),
        "pct_off_high52": (round((s["price"] / s["high_52w"] - 1) * 100, 2)
                            if s.get("price") and s.get("high_52w") else None),
        "above_ema50": s.get("above_ema50"), "above_ema200": s.get("above_ema200"),
        "ema200_slope_pct": s.get("ema200_slope_pct"), "atr14_pct": s.get("atr14_pct"),
        "sparkline": (s.get("price_history") or [])[-260:],
    }

    if lite:
        # factor มาจากงบ Yahoo ใน namespace DR: (is_dr=True) โดยตรง — ไม่มี mirror snapshot ของ
        # ตลาดนี้ (JP/VN/ฯลฯ ไม่มี pipeline) คำนวณสดตัวเดียว (fetch_header_lite sync งบให้แล้ว)
        snap_rows = {}
        fkey = lite_dr_sym
        f = factor_snapshot._factors_for(BASE_DIR, lite_dr_sym, is_dr=True) or {}
        if f:
            f.setdefault("div_cagr_5y", factor_snapshot._div_cagr_5y(BASE_DIR, lite_dr_sym, "DR"))
    else:
        if mkt == "TH":
            snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}
        else:
            snap_rows = {r["symbol"]: r for r in factor_snapshot.get_mirror_snapshot(BASE_DIR, mkt)}
        fkey = _mirror_sym(mkt, sym)
        f = snap_rows.get(fkey) or {}

    def _label(pct):
        if pct is None:
            return None
        if pct <= 25:
            return "cheap"
        if pct >= 75:
            return "expensive"
        return "normal"

    sector = s.get("sector")
    sec_syms = [sym2 for sym2, s2 in th_map.items() if sector and s2.get("sector") == sector]

    def _sector_median(getter):
        vals = [v for v in (getter(sym2) for sym2 in sec_syms) if isinstance(v, (int, float))]
        if not vals:
            return None
        vals.sort()
        n = len(vals)
        mid = n // 2
        return vals[mid] if n % 2 else round((vals[mid - 1] + vals[mid]) / 2, 3)

    valuation = {
        "pe": {
            "value": s.get("pe") if s.get("pe") is not None else f.get("pe_value"),
            "percentile": f.get("pe_percentile"), "label": _label(f.get("pe_percentile")),
            "mean": f.get("pe_mean"), "median": f.get("pe_median"), "n": f.get("pe_n"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("pe")
                                             if (th_map.get(sy) or {}).get("pe") is not None
                                             else (snap_rows.get(sy) or {}).get("pe_value")),
        },
        "pbv": {
            "value": s.get("pbv") if s.get("pbv") is not None else f.get("pbv_value"),
            "percentile": f.get("pbv_percentile"), "label": _label(f.get("pbv_percentile")),
            "mean": f.get("pbv_mean"), "median": f.get("pbv_median"), "n": f.get("pbv_n"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("pbv")
                                             if (th_map.get(sy) or {}).get("pbv") is not None
                                             else (snap_rows.get(sy) or {}).get("pbv_value")),
        },
        "ps": {
            "value": f.get("ps_value"), "percentile": f.get("ps_percentile"),
            "label": _label(f.get("ps_percentile")),
            "mean": f.get("ps_mean"), "median": f.get("ps_median"), "n": f.get("ps_n"),
            "sector_median": _sector_median(lambda sy: (snap_rows.get(sy) or {}).get("ps_value")),
        },
        "div_yield": {
            "value": s.get("div_yield"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("div_yield")),
        },
        "ev_ebitda": {
            "value": f.get("ev_ebitda_value"), "percentile": f.get("ev_ebitda_percentile"),
            "label": _label(f.get("ev_ebitda_percentile")),
            "mean": f.get("ev_ebitda_mean"), "median": f.get("ev_ebitda_median"),
            "sector_median": _sector_median(lambda sy: (snap_rows.get(sy) or {}).get("ev_ebitda_value")),
        },
    }

    mkt_cap = s.get("mkt_cap")
    if mkt_cap is None and mkt != "TH" and s.get("price") and f.get("shares_out"):
        # us/hk_index_metrics.json ไม่เก็บ mkt_cap เลย (ยืนยันแล้ว 0/518 US, 0/105 HK) —
        # คำนวณเองจาก price สด × shares_out ล่าสุด (งบ Yahoo annual) แทน ไม่งั้น DCF/fcf_yield
        # จะ "ข้อมูลไม่พอ" ทุกตัวทั้งที่มี FCF จริง
        mkt_cap = s["price"] * f["shares_out"]
    fcf = f.get("fcf")
    if mkt == "TH":
        is_financial = sym in factor_snapshot._financial_sector_symbols(BASE_DIR)
    else:
        # US/HK ไม่มีลิสต์สถาบันการเงินคิวเรตแบบ TH — ใช้ชื่อ GICS sector จาก
        # us/hk_index_metrics.json แทน (ครอบทุกรูปแบบชื่อที่พบจริง — ดู FINANCIAL_SECTOR_NAMES,
        # HK มีทั้ง "Financials"/"Finance" แยกกัน เช็คแค่ตัวเดียวจะหลุด HSBC/HKEX/AIA/BOCHK)
        is_financial = (s.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES)

    # US/HK: factor_snapshot_mirror ไม่รู้จัก sector ตอน build (คำนวณจากทั้ง mirror universe ไม่ใช่
    # แค่สมาชิกดัชนีหลักที่มี sector) — บังคับซ่อน Z-Score เองตรงนี้แทนสำหรับกลุ่มการเงิน (เหมือนที่
    # ทำใน /api/peer-compare)
    z_score, z_zone, z_reason = f.get("z_score"), f.get("z_zone"), f.get("z_excluded_reason")
    if is_financial and mkt != "TH":
        z_score, z_zone = None, None
        z_reason = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"

    quality = {
        "roe": f.get("roe"), "de_ratio": f.get("de_ratio"), "gross_margin": f.get("gross_margin"),
        "fcf_yield": round(fcf / mkt_cap * 100, 2) if (fcf is not None and mkt_cap) else None,
        "dividend_coverage": f.get("dividend_coverage"),
        "f_score": f.get("f_score"), "f_score_max": f.get("f_score_max"),
        "f_score_detail": f.get("f_score_detail"),
        "z_score": z_score, "z_variant": f.get("z_variant"), "z_zone": z_zone,
        "z_excluded_reason": z_reason,
    }

    # _mirror_sym ตัด suffix ".HK" ก่อนเสมอ — ตาราง dividends เก็บรหัสดิบไม่มี suffix
    # เหมือน factor_snapshot mirror (ดู sync_dividends_batch/get_dividends_endpoint)
    # lite: dividends อยู่ใต้ market "DR" (key = DR sym) ถ้าเคย sync จากหน้าปันผล/batch
    div_rows, _div_synced = financials_store.get_dividends(BASE_DIR, fkey, "DR" if lite else mkt)
    dividend = {
        "yield": s.get("div_yield"),
        "cagr_5y": f.get("div_cagr_5y"),
        "growth_streak_y": f.get("div_growth_streak_y"),
        "coverage": f.get("dividend_coverage"),
        "payout_ratio_pct": dividend_stats.compute_payout_ratio(
            div_rows, f.get("eps_latest"), f.get("eps_latest_date")),
    }

    # ตลาด lite (VN/SG/EU/TW/CN — DR underlying ไม่มี cohort) ยังไม่ตั้ง discount rate เฉพาะ —
    # ใช้ 9.0 กลางๆ ปรับเองได้ใน UI · JP=7.0 ตลาดพัฒนาแล้ว ดอกเบี้ย/cost of equity ต่ำกว่า US/HK
    discount_rate_default = {"TH": 9.0, "US": 8.5, "HK": 9.5, "JP": 7.0}.get(mkt, 9.0)
    dcf = {
        "fcf": fcf, "mkt_cap": mkt_cap, "net_cash": f.get("net_cash"), "price": s.get("price"),
        "rev_cagr": f.get("rev_cagr"), "profit_cagr": f.get("profit_cagr"),
        "is_financial_sector": is_financial,
        "discount_rate_default": discount_rate_default, "terminal_growth_default": 2.5,
    }

    # เติมเครื่องมือประเมินมูลค่าทางเลือก (นอกจาก DCF) — PEG / Graham Number / DDM / Justified P-B
    # คำนวณฝั่ง client ทั้งหมด (เหมือน DCF) ที่นี่แค่ส่งวัตถุดิบ + ค่าเริ่มต้นที่ผู้ใช้ปรับเองได้
    growth_pct_default = f.get("profit_ttm_yoy")
    if growth_pct_default is None:
        growth_pct_default = f.get("profit_cagr")
    valuation_models = {
        "eps": f.get("eps_latest"), "bvps": f.get("bvps"), "roe": f.get("roe"),
        "growth_pct_default": growth_pct_default,
        "discount_rate_default": discount_rate_default, "terminal_growth_default": 2.5,
    }

    meta_computed_at = (factor_snapshot.snapshot_meta(BASE_DIR).get("computed_at") if mkt == "TH"
                         else factor_snapshot.mirror_snapshot_meta(BASE_DIR).get("computed_at"))
    return jsonify({
        "symbol": sym, "market": mkt, "header": header, "valuation": valuation, "quality": quality,
        "dividend": dividend, "dcf": dcf, "valuation_models": valuation_models, "dr_symbol": dr_symbol,
        "lite": lite, "underlying_yf": lite_yf, "yf_symbol": yf_symbol,
        "meta": {"computed_at": meta_computed_at,
                 "has_factors": bool(f) if lite else (_mirror_sym(mkt, sym) in snap_rows)},
    })


@app.route("/api/track-search", methods=["POST"])
def track_search():
    """จำหุ้นที่ถูกค้นในหน้างบการเงิน — ใช้เลือกหุ้น 'ค้นบ่อย' มาอัพเดทในโหมดเบา"""
    body = request.json or {}
    sym = (body.get("symbol") or "").upper().strip()
    if sym:
        try:
            financials_store.record_search(BASE_DIR, sym)
        except Exception:
            pass
    return jsonify({"ok": True})


@app.route("/api/mirror-symbols")
def mirror_symbols():
    """รายชื่อ symbol หุ้น US/HK ทั้งตลาด (mirror) สำหรับ datalist ค้นหาในหน้างบการเงิน"""
    return jsonify(factor_snapshot.get_mirror_symbols(BASE_DIR))


@app.route("/api/us-index-membership")
def get_us_index_membership():
    """รายชื่อ ticker ที่อยู่ใน S&P 500 / Dow Jones / Nasdaq 100 (ไฟล์ local อัพเดทได้ผ่าน
    ปุ่ม "ดึงเฉพาะที่ขาด/เก่า" ในหน้างบการเงิน — ดู /api/us-index-sync) ใช้กรองรายการ
    browse หุ้น US — คืน {SP500:[...], DOW:[...], NDX:[...]}"""
    path = os.path.join(BASE_DIR, "data", "us_index_membership.json")
    if not os.path.exists(path):
        return jsonify({})
    try:
        with open(path, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({})


@app.route("/api/us-index-check-updates")
def us_index_check_updates():
    """เทียบรายชื่อ S&P 500 / Dow Jones / Nasdaq 100 สดจาก Wikipedia กับไฟล์ local — รายงาน
    ตัวใหม่/ตัวที่ถูกถอดต่อดัชนี ไม่แก้ไฟล์ local ให้ (คู่กับ dr_check_updates ของหน้า DR
    แต่ใช้กับ 3 ดัชนี US แทน underlying ของ DR)"""
    cached = _us_index_diff_cache.get("result")
    if cached and (time.time() - _us_index_diff_cache.get("ts", 0) < _US_INDEX_DIFF_CACHE_TTL):
        return jsonify(cached)
    try:
        mirror_us = factor_snapshot.get_mirror_symbols(BASE_DIR).get("US", [])
        result, _live = us_index_membership.diff_membership(BASE_DIR, mirror_us=mirror_us)
        _us_index_diff_cache["result"] = result
        _us_index_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/us-index-sync", methods=["POST"])
def start_us_index_sync():
    """ปุ่ม "ดึงเฉพาะที่ขาด/เก่า (local)" ของดัชนี US — เช็ค Wikipedia แล้วอัพเดทไฟล์
    local ให้ตรง จากนั้นดึงงบการเงินเฉพาะ ticker ในดัชนีที่ยังไม่อยู่ใน mirror list หลัก
    (factor_mirror) ผ่าน Yahoo Finance โดยตรง (ข้ามคู่ที่เพิ่งดึงไปไม่เกิน min_age_days วัน)"""
    min_age_days = None
    if request.is_json:
        body = request.json or {}
        raw_age = body.get("min_age_days")
        if raw_age is not None:
            try:
                min_age_days = max(0, int(raw_age))
            except (TypeError, ValueError):
                min_age_days = None
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=0,
                      message="กำลังเช็คดัชนี S&P500/Dow/Nasdaq100 จาก Wikipedia...")
    threading.Thread(target=_run_us_index_sync, args=(min_age_days,), daemon=True).start()
    return jsonify({"ok": True})


def _load_mirror_names_us():
    """ชื่อบริษัทหุ้น US จาก mirror_names.json — {} ถ้ายังไม่ได้สร้างไฟล์/อ่านไม่ได้"""
    path = os.path.join(BASE_DIR, "mirror_names.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return (json.load(f) or {}).get("US", {})
    except Exception:
        return {}


def _run_us_index_sync(min_age_days=None):
    try:
        diff, live = us_index_membership.sync_membership(BASE_DIR)
        _us_index_diff_cache.clear()   # ลิสต์เปลี่ยน — ผลเช็คหุ้นใหม่เดิมไม่ตรงแล้ว
        added_n = sum(len(v["new"]) for v in diff.values())
        removed_n = sum(len(v["removed"]) for v in diff.values())

        mirror_us = set(factor_snapshot.get_mirror_symbols(BASE_DIR).get("US", []))
        extra = sorted({s for lst in live.values() for s in lst} - mirror_us)

        def cb(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · {msg}")

        if extra:
            result = financials_store.sync_all(BASE_DIR, extra, sources=("yahoo_q", "yahoo"),
                                               callback=cb, is_dr=True, market="US",
                                               min_age_days=min_age_days)
            _fin_analytics_cache.clear()
        else:
            result = {"ok": 0, "fail": 0, "total": 0, "skipped": 0}

        # เติมชื่อบริษัทของตัวที่ยังไม่มีชื่อ (ไม่อยู่ใน mirror_names.json) จาก payload ที่เพิ่งดึงมา
        # ให้ปุ่มกรองดัชนีในหน้างบการเงินโชว์ชื่อได้ครบ ไม่ใช่แค่ symbol เฉยๆ
        local = us_index_membership.load_local(BASE_DIR)
        mirror_names_us = _load_mirror_names_us()
        extra_names = dict(local.get("extra_names") or {})
        for sym in extra:
            if sym in mirror_names_us or sym in extra_names:
                continue
            payload = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=True) \
                or financials_store.get(BASE_DIR, sym, "yahoo", is_dr=True)
            if payload and payload.get("name") and payload["name"] != sym:
                extra_names[sym] = payload["name"]
        local["extra_names"] = extra_names
        us_index_membership.save_local(BASE_DIR, local)

        skipped = result.get("skipped", 0)
        _update(running=False, done=True,
                message=f"เสร็จแล้ว! ดัชนีอัพเดท +{added_n}/-{removed_n} · งบการเงิน {result['ok']}/{result['total']} สำเร็จ"
                        + (f" · ข้าม {skipped} คู่ (ดึงไปแล้วไม่เกิน {min_age_days} วัน)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']})" if result["fail"] else ""))
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/hk-index-membership")
def get_hk_index_membership():
    """รายชื่อ ticker ที่อยู่ใน HSI / HSCEI / HSTECH (ไฟล์ local อัพเดทได้ผ่าน
    ปุ่ม "ดึงเฉพาะที่ขาด/เก่า" ในหน้างบการเงิน — ดู /api/hk-index-sync) ใช้กรองรายการ
    browse หุ้น HK — คืน {HSI:[...], HSCEI:[...], HSTECH:[...]}"""
    path = os.path.join(BASE_DIR, "data", "hk_index_membership.json")
    if not os.path.exists(path):
        return jsonify({})
    try:
        with open(path, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({})


@app.route("/api/hk-index-check-updates")
def hk_index_check_updates():
    """เทียบรายชื่อ HSI / HSCEI / HSTECH สดจาก Wikipedia กับไฟล์ local — รายงาน
    ตัวใหม่/ตัวที่ถูกถอดต่อดัชนี ไม่แก้ไฟล์ local ให้ (คู่กับ us_index_check_updates
    แต่ใช้กับ 3 ดัชนี HK แทน)"""
    cached = _hk_index_diff_cache.get("result")
    if cached and (time.time() - _hk_index_diff_cache.get("ts", 0) < _HK_INDEX_DIFF_CACHE_TTL):
        return jsonify(cached)
    try:
        mirror_hk = factor_snapshot.get_mirror_symbols(BASE_DIR).get("HK", [])
        result, _live = hk_index_membership.diff_membership(BASE_DIR, mirror_hk=mirror_hk)
        _hk_index_diff_cache["result"] = result
        _hk_index_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/hk-index-sync", methods=["POST"])
def start_hk_index_sync():
    """ปุ่ม "ดึงเฉพาะที่ขาด/เก่า (local)" ของดัชนี HK — เช็ค Wikipedia แล้วอัพเดทไฟล์
    local ให้ตรง จากนั้นดึงงบการเงินเฉพาะ ticker ในดัชนีที่ยังไม่อยู่ใน mirror list หลัก
    (factor_mirror) ผ่าน Yahoo Finance โดยตรง (ข้ามคู่ที่เพิ่งดึงไปไม่เกิน min_age_days วัน)"""
    min_age_days = None
    if request.is_json:
        body = request.json or {}
        raw_age = body.get("min_age_days")
        if raw_age is not None:
            try:
                min_age_days = max(0, int(raw_age))
            except (TypeError, ValueError):
                min_age_days = None
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=0,
                      message="กำลังเช็คดัชนี HSI/HSCEI/HSTECH จาก Wikipedia...")
    threading.Thread(target=_run_hk_index_sync, args=(min_age_days,), daemon=True).start()
    return jsonify({"ok": True})


def _load_mirror_names_hk():
    """ชื่อบริษัทหุ้น HK จาก mirror_names.json — {} ถ้ายังไม่ได้สร้างไฟล์/อ่านไม่ได้"""
    path = os.path.join(BASE_DIR, "mirror_names.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return (json.load(f) or {}).get("HK", {})
    except Exception:
        return {}


def _run_hk_index_sync(min_age_days=None):
    try:
        diff, live = hk_index_membership.sync_membership(BASE_DIR)
        _hk_index_diff_cache.clear()   # ลิสต์เปลี่ยน — ผลเช็คหุ้นใหม่เดิมไม่ตรงแล้ว
        added_n = sum(len(v["new"]) for v in diff.values())
        removed_n = sum(len(v["removed"]) for v in diff.values())

        # mirror เก็บชื่อตาม Finnomena (เลขล้วน มีทั้งเติม 0 นำหน้าและไม่เติม เช่น "0700"/"799")
        # ส่วน membership เป็น "0700.HK" — ต้อง normalize ก่อนเทียบ ไม่งั้น extra = ทั้งดัชนี
        # ทุกครั้ง (ยิง Yahoo ซ้ำหมด) และ key ที่เก็บ ("0700.HK") จะไม่ตรงกับที่หน้างบ/
        # โมดัลกราฟ query ("0700" — ดู _loadCmFin ฝั่ง JS ที่ตัด .HK ก่อน fetch)
        def _hk_code(s):
            s = s.upper()
            if s.endswith(".HK"):
                s = s[:-3]
            return s.lstrip("0") or "0"
        mirror_hk = {_hk_code(s) for s in factor_snapshot.get_mirror_symbols(BASE_DIR).get("HK", [])}
        members = {s for k in ("HSI", "HSCEI", "HSTECH") for s in live.get(k, [])}
        extra = sorted(s[:-3] for s in members if _hk_code(s) not in mirror_hk)

        def cb(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · {msg}")

        if extra:
            result = financials_store.sync_all(BASE_DIR, extra, sources=("yahoo_q", "yahoo"),
                                               callback=cb, is_dr=True, market="HK",
                                               min_age_days=min_age_days)
            _fin_analytics_cache.clear()
        else:
            result = {"ok": 0, "fail": 0, "total": 0, "skipped": 0}

        # เติมชื่อบริษัทของตัวที่ยังไม่มีชื่อ (ไม่อยู่ใน mirror_names.json) จาก payload ที่เพิ่งดึงมา
        # ให้ปุ่มกรองดัชนีในหน้างบการเงินโชว์ชื่อได้ครบ ไม่ใช่แค่ symbol เฉยๆ
        local = hk_index_membership.load_local(BASE_DIR)
        mirror_names_hk = _load_mirror_names_hk()
        extra_names = dict(local.get("extra_names") or {})
        for sym in extra:
            if sym in mirror_names_hk or sym in extra_names:
                continue
            payload = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=True) \
                or financials_store.get(BASE_DIR, sym, "yahoo", is_dr=True)
            if payload and payload.get("name") and payload["name"] != sym:
                extra_names[sym] = payload["name"]
        local["extra_names"] = extra_names
        hk_index_membership.save_local(BASE_DIR, local)

        skipped = result.get("skipped", 0)
        _update(running=False, done=True,
                message=f"เสร็จแล้ว! ดัชนีอัพเดท +{added_n}/-{removed_n} · งบการเงิน {result['ok']}/{result['total']} สำเร็จ"
                        + (f" · ข้าม {skipped} คู่ (ดึงไปแล้วไม่เกิน {min_age_days} วัน)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']})" if result["fail"] else ""))
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/jp-index-membership")
def get_jp_index_membership():
    """รายชื่อ ticker ที่อยู่ใน Nikkei 225 (ไฟล์ local อัพเดทได้ผ่านปุ่ม "ดึงเฉพาะที่ขาด/
    เก่า" — ดู /api/jp-index-sync) ใช้กรองรายการ browse หุ้น JP — คืน {NIKKEI225:[...]}"""
    path = os.path.join(BASE_DIR, "data", "jp_index_membership.json")
    if not os.path.exists(path):
        return jsonify({})
    try:
        with open(path, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({})


@app.route("/api/jp-index-check-updates")
def jp_index_check_updates():
    """เทียบรายชื่อ Nikkei 225 สดจาก ja.wikipedia กับไฟล์ local — รายงานตัวใหม่/ตัวที่ถูกถอด
    ไม่แก้ไฟล์ local ให้ (คู่กับ hk_index_check_updates)"""
    cached = _jp_index_diff_cache.get("result")
    if cached and (time.time() - _jp_index_diff_cache.get("ts", 0) < _JP_INDEX_DIFF_CACHE_TTL):
        return jsonify(cached)
    try:
        from sources import jp_index_membership
        result, _live = jp_index_membership.diff_membership(BASE_DIR)
        _jp_index_diff_cache["result"] = result
        _jp_index_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/jp-index-sync", methods=["POST"])
def start_jp_index_sync():
    """ปุ่ม "ดึงเฉพาะที่ขาด/เก่า (local)" ของดัชนี JP — เช็ค ja.wikipedia แล้วอัพเดทไฟล์ local
    ให้ตรง จากนั้นดึงงบการเงิน (ผ่าน Yahoo Finance) ให้สมาชิก Nikkei 225 ทุกตัวที่ยังไม่มีข้อมูล
    (ใช้ `sync_mirror_yahoo_index` — skip-if-exists, ไม่มี min_age_days re-fetch แบบ
    `_run_hk_index_sync`/`sync_all` เพราะ scope เล็กแค่ 225 ตัว ไม่ต้องการ incremental refresh
    ซับซ้อน) แล้ว rebuild factor_snapshot_mirror ให้ Tearsheet/Peer Compare/F-Score-Z-Score
    เห็นข้อมูล — **ไม่ใช้ build_mirror_snapshot() ปกติ** (ผูกกับ finnomena_q ที่ไม่มีข้อมูล JP
    เลย) ใช้ build_mirror_snapshot_yahoo_only() แทน"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=0,
                      message="กำลังเช็คดัชนี Nikkei 225 จาก Wikipedia...")
    threading.Thread(target=_run_jp_index_sync, daemon=True).start()
    return jsonify({"ok": True})


def _run_jp_index_sync():
    try:
        from sources import jp_index_membership
        diff, _live = jp_index_membership.sync_membership(BASE_DIR)
        _jp_index_diff_cache.clear()
        added_n = sum(len(v["new"]) for v in diff.values())
        removed_n = sum(len(v["removed"]) for v in diff.values())

        # รหัสดิบไม่มี suffix (namespace mirror ใช้เสมอ) ดู _mirror_sym/sync_mirror_yahoo_index
        tickers = [t[:-2] for t in jp_index_membership.all_tickers(BASE_DIR) if t.endswith(".T")]
        # ราคาปัจจุบัน (ต่อ ticker รหัสดิบ) จาก jp_index_metrics.json — ใช้คำนวณ mkt_cap ให้
        # Z-Score variant 'Z' เพราะไม่มี finnomena_q ให้ _factors_for หา mkt_cap เองแบบ US/HK
        from sources import jp_index_metrics
        price_by_ticker = {s["symbol"][:-2]: s["price"] for s in
                            jp_index_metrics.load_local(BASE_DIR).get("stocks", [])
                            if s.get("price") and s["symbol"].endswith(".T")}

        def cb(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · {msg}")

        result = financials_store.sync_mirror_yahoo_index(BASE_DIR, {"JP": tickers}, callback=cb)
        _update(message="กำลัง rebuild factor snapshot JP...")
        n_mirror = factor_snapshot.build_mirror_snapshot_yahoo_only(
            BASE_DIR, "JP", tickers, price_by_ticker=price_by_ticker)
        _fin_analytics_cache.clear()

        _update(running=False, done=True,
                message=f"เสร็จแล้ว! ดัชนี Nikkei 225 อัพเดท +{added_n}/-{removed_n} · "
                        f"งบการเงิน {result['ok']} ตัว (ข้าม {result['skipped']} ที่มีอยู่แล้ว"
                        + (f", ล้มเหลว {result['fail']}" if result["fail"] else "") + f") · "
                        f"factor snapshot: {n_mirror} ตัว")
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/hk-index-heatmap")
def hk_index_heatmap():
    """Heatmap ของ HSI / HSCEI / HSTECH — ?index=HSI|HSCEI|HSTECH
    คืน {rows:[{symbol, name, mkt_cap, chg_1d, chg_1w}], ts, requested, missing}
    ต่างจาก us_index_heatmap ตรงที่ chg_1d/chg_1w คำนวณจากราคาใน hk_prices.db ที่มีอยู่แล้ว
    (Quick Update อัพเดทให้ทุกวัน) ไม่ต้องยิง Yahoo สดเหมือน US — เหลือแค่ market cap ที่ต้อง
    ยิง fast_info สดอยู่ดี (ไม่มีเก็บใน DB ราคา) cache ไฟล์แยก 1 วันเหมือนของ US"""
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor
    from core import hk_store

    index_key = (request.args.get("index") or "HSI").upper()
    if index_key not in ("HSI", "HSCEI", "HSTECH"):
        return jsonify({"error": "index ต้องเป็น HSI, HSCEI หรือ HSTECH เท่านั้น"}), 400
    force = request.args.get("force") == "1"

    cached = _hk_heatmap_cache.get(index_key)
    if not force and cached and (time.time() - cached["ts"] < _HK_HEATMAP_CACHE_TTL):
        return jsonify(cached)

    local = hk_index_membership.load_local(BASE_DIR)
    tickers = local.get(index_key) or []
    if not tickers:
        return jsonify({"error": f"ยังไม่มีรายชื่อ {index_key} ในเครื่อง — กด \"ดึงเฉพาะที่ขาด/เก่า\" ในหน้างบการเงินก่อน"}), 404

    # % เปลี่ยนแปลง 1 วัน/1 สัปดาห์ — คำนวณจาก hk_prices.db ตรงๆ (มีราคาอยู่แล้วจาก Quick
    # Update ทุกวัน ไม่ต้องยิง Yahoo สดเหมือน US heatmap)
    # ใช้แค่ 6 แท่งล่าสุดต่อตัว (chg_1d/chg_1w) — iter_recent_series เปิด connection
    # เดียวอ่านทุก ticker แทนเปิดทีละ connection ต่อตัวด้วย get_ohlc_series (~105 ครั้ง)
    chg1d_map, chg1w_map = {}, {}
    recent = dict(hk_store.iter_recent_series(BASE_DIR, warmup_rows=6))
    for t in tickers:
        series = recent.get(t)
        if not series or not series.get("closes"):
            continue
        closes = [c for c in series["closes"] if c is not None]
        if len(closes) >= 2:
            chg1d_map[t] = round((closes[-1] / closes[-2] - 1) * 100, 2)
        if len(closes) >= 6:
            chg1w_map[t] = round((closes[-1] / closes[-6] - 1) * 100, 2)
        elif len(closes) >= 2:
            chg1w_map[t] = round((closes[-1] / closes[0] - 1) * 100, 2)

    # Market cap — fast_info ต้องยิงทีละ ticker (ไม่มีใน hk_prices.db) ใช้ disk cache
    # อายุ 1 วันก่อน (เปลี่ยนช้า) ยิง Yahoo เฉพาะตัวที่ cache หมดอายุ/ไม่มี ขนาน 12 threads
    def _mc_one(t):
        try:
            v = getattr(yf.Ticker(t).fast_info, "market_cap", None)
            return t, (float(v) if v else None)
        except Exception:
            return t, None

    mktcap_cache = _load_hk_mktcap_cache()
    now_ts = time.time()
    stale = [t for t in tickers if force
             or t not in mktcap_cache
             or now_ts - mktcap_cache[t].get("ts", 0) >= _MKTCAP_CACHE_TTL]

    if stale:
        try:
            with ThreadPoolExecutor(max_workers=12) as ex:
                for t, mc in ex.map(_mc_one, stale):
                    if mc is not None:
                        mktcap_cache[t] = {"mc": mc, "ts": now_ts}
        except Exception as e:
            print(f"[HK Heatmap] market cap batch {index_key} ล้มเหลว: {e}")
        _save_hk_mktcap_cache(mktcap_cache)

    mkt_map = {t: mktcap_cache[t]["mc"] for t in tickers if t in mktcap_cache}

    mirror_names_hk = _load_mirror_names_hk()
    extra_names = local.get("extra_names") or {}
    # HSTECH ไม่มี *_sector จาก Wikipedia (bullet list ไม่มีคอลัมน์ industry) — ใช้แหล่ง
    # เดียวกับ hk_index_metrics.build(): รวม sector ของ HSI/HSCEI แล้วเติมจาก dr_universe
    sector_map = dict(local.get(f"{index_key}_sector") or {})
    if not sector_map:
        for k in ("HSI_sector", "HSCEI_sector"):
            sector_map.update(local.get(k) or {})
        for e in load_dr_universe(BASE_DIR):
            yf_t = (e.get("yf") or "").upper()
            if yf_t.endswith(".HK") and yf_t not in sector_map and e.get("ind"):
                sector_map[yf_t] = e["ind"]

    rows = []
    for t in tickers:
        mc = mkt_map.get(t)
        chg_1d = chg1d_map.get(t)
        chg_1w = chg1w_map.get(t)
        if mc is None and chg_1d is None:
            continue   # โดนบล็อค/ไม่มีข้อมูลจริงทั้งคู่ — ข้ามแทนที่จะโชว์กล่องว่าง
        # ชื่อบริษัท: mirror_names/extra_names เก็บด้วยรหัสไม่มี .HK (ตาม Finnomena — มีทั้ง
        # แบบเติม 0 นำหน้าและไม่เติม) ส่วน t เป็น "0700.HK" — ลองทั้งสองแบบ
        code = t[:-3] if t.endswith(".HK") else t
        rows.append({
            "symbol": t,
            "name": mirror_names_hk.get(code) or mirror_names_hk.get(code.lstrip("0") or "0")
                    or extra_names.get(code) or t,
            "mkt_cap": mc,
            "chg_1d": chg_1d,
            "chg_1w": chg_1w,
            "sector": sector_map.get(t) or "อื่นๆ",
        })

    result = {"rows": rows, "ts": now_ts, "requested": len(tickers), "missing": len(tickers) - len(rows)}
    _hk_heatmap_cache[index_key] = result
    return jsonify(result)


@app.route("/api/jp-index-heatmap")
def jp_index_heatmap():
    """Heatmap ของ Nikkei 225 — ดัชนีเดียว (ไม่มี ?index= ต่างจาก US/HK ที่มีหลายดัชนีให้เลือก)
    คืน {rows:[{symbol, name, mkt_cap, chg_1d, chg_1w, sector}], ts, requested, missing}
    ก็อปโครงจาก hk_index_heatmap ทั้งหมด — chg_1d/chg_1w คำนวณจาก jp_prices.db ในเครื่อง
    (Quick Update อัพเดทให้ทุกวัน) mkt_cap ยิง fast_info สด cache แยกไฟล์ 1 วันเหมือน HK/US"""
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor
    from core import jp_store
    from sources import jp_index_membership, jp_index_metrics

    force = request.args.get("force") == "1"

    cached = _jp_heatmap_cache.get("NIKKEI225")
    if not force and cached and (time.time() - cached["ts"] < _JP_HEATMAP_CACHE_TTL):
        return jsonify(cached)

    local = jp_index_membership.load_local(BASE_DIR)
    tickers = local.get("NIKKEI225") or []
    if not tickers:
        return jsonify({"error": "ยังไม่มีรายชื่อ NIKKEI225 ในเครื่อง — กด \"📈 JP Index Max\" ก่อน"}), 404

    # % เปลี่ยนแปลง 1 วัน/1 สัปดาห์ — คำนวณจาก jp_prices.db ตรงๆ (มีราคาอยู่แล้วจาก Quick
    # Update ทุกวัน ไม่ต้องยิง Yahoo สดเหมือน US heatmap) ดู hk_index_heatmap สำหรับเหตุผลเดียวกัน
    chg1d_map, chg1w_map = {}, {}
    recent = dict(jp_store.iter_recent_series(BASE_DIR, warmup_rows=6))
    for t in tickers:
        series = recent.get(t)
        if not series or not series.get("closes"):
            continue
        closes = [c for c in series["closes"] if c is not None]
        if len(closes) >= 2:
            chg1d_map[t] = round((closes[-1] / closes[-2] - 1) * 100, 2)
        if len(closes) >= 6:
            chg1w_map[t] = round((closes[-1] / closes[-6] - 1) * 100, 2)
        elif len(closes) >= 2:
            chg1w_map[t] = round((closes[-1] / closes[0] - 1) * 100, 2)

    # Market cap — fast_info ต้องยิงทีละ ticker (ไม่มีใน jp_prices.db) ใช้ disk cache
    # อายุ 1 วันก่อน (เปลี่ยนช้า) ยิง Yahoo เฉพาะตัวที่ cache หมดอายุ/ไม่มี ขนาน 12 threads
    def _mc_one(t):
        try:
            v = getattr(yf.Ticker(t).fast_info, "market_cap", None)
            return t, (float(v) if v else None)
        except Exception:
            return t, None

    mktcap_cache = _load_jp_mktcap_cache()
    now_ts = time.time()
    stale = [t for t in tickers if force
             or t not in mktcap_cache
             or now_ts - mktcap_cache[t].get("ts", 0) >= _MKTCAP_CACHE_TTL]

    if stale:
        try:
            with ThreadPoolExecutor(max_workers=12) as ex:
                for t, mc in ex.map(_mc_one, stale):
                    if mc is not None:
                        mktcap_cache[t] = {"mc": mc, "ts": now_ts}
        except Exception as e:
            print(f"[JP Heatmap] market cap batch ล้มเหลว: {e}")
        _save_jp_mktcap_cache(mktcap_cache)

    mkt_map = {t: mktcap_cache[t]["mc"] for t in tickers if t in mktcap_cache}

    # ชื่อบริษัท: jp_index_membership ไม่เก็บชื่อ (แค่ ticker+sector) — ใช้ name ที่
    # jp_index_metrics คำนวณไว้แล้วจาก dr_universe/Yahoo ตอน sync งบ
    metrics_local = jp_index_metrics.load_local(BASE_DIR)
    name_map = {s["symbol"]: s.get("name") for s in metrics_local.get("stocks", [])}
    sector_map = dict(local.get("NIKKEI225_sector") or {})

    rows = []
    for t in tickers:
        mc = mkt_map.get(t)
        chg_1d = chg1d_map.get(t)
        chg_1w = chg1w_map.get(t)
        if mc is None and chg_1d is None:
            continue   # โดนบล็อค/ไม่มีข้อมูลจริงทั้งคู่ — ข้ามแทนที่จะโชว์กล่องว่าง
        rows.append({
            "symbol": t,
            "name": name_map.get(t) or t,
            "mkt_cap": mc,
            "chg_1d": chg_1d,
            "chg_1w": chg_1w,
            "sector": sector_map.get(t) or "อื่นๆ",
        })

    result = {"rows": rows, "ts": now_ts, "requested": len(tickers), "missing": len(tickers) - len(rows)}
    _jp_heatmap_cache["NIKKEI225"] = result
    return jsonify(result)


@app.route("/api/us-index-heatmap")
def us_index_heatmap():
    """Heatmap ของ S&P 500 / Dow Jones / Nasdaq 100 — ?index=SP500|DOW|NDX
    คืน {rows:[{symbol, name, mkt_cap, chg_1d, chg_1w}], ts, requested, missing}
    mkt_cap ใช้กำหนดขนาดกล่อง, chg_1d/chg_1w ใช้กำหนดสี (ฝั่ง client สลับดูได้ทั้งคู่โดยไม่ต้องยิงซ้ำ)
    ดึงสดจาก Yahoo (ไม่มีข้อมูลนี้ใน financials.db ที่ sync ไว้ล่วงหน้า — เป็นราคา/มูลค่าตลาด
    ปัจจุบัน ไม่ใช่งบการเงิน) ราคา cache ไว้ 15 นาทีต่อดัชนี ส่วน market cap cache แยกไฟล์ 1 วัน
    (เปลี่ยนช้ากว่าราคามาก) กันยิง Yahoo ทุกครั้งที่เปิดหน้า"""
    import yfinance as yf
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor

    index_key = (request.args.get("index") or "SP500").upper()
    if index_key not in ("SP500", "DOW", "NDX"):
        return jsonify({"error": "index ต้องเป็น SP500, DOW หรือ NDX เท่านั้น"}), 400
    force = request.args.get("force") == "1"

    cached = _heatmap_cache.get(index_key)
    if not force and cached and (time.time() - cached["ts"] < _HEATMAP_CACHE_TTL):
        return jsonify(cached)

    local = us_index_membership.load_local(BASE_DIR)
    tickers = local.get(index_key) or []
    if not tickers:
        return jsonify({"error": f"ยังไม่มีรายชื่อ {index_key} ในเครื่อง — กด \"ดึงเฉพาะที่ขาด/เก่า\" ในหน้างบการเงินก่อน"}), 404

    yf_tickers = tickers   # ticker ในไฟล์ local เป็นรูปแบบ yfinance อยู่แล้ว (BRK-B ไม่ใช่ BRK.B)

    # % เปลี่ยนแปลง 1 วัน/1 สัปดาห์ — ดาวน์โหลดพร้อมกันทั้งชุด (เร็วกว่ายิงทีละตัวมาก เหมือน dr_quick_update)
    # period="1mo" ให้ครบทั้งสองช่วงในการยิงครั้งเดียว (1w = เทียบย้อน 5 วันทำการ)
    chg1d_map, chg1w_map = {}, {}
    try:
        raw = yf.download(yf_tickers, period="1mo", auto_adjust=True,
                          progress=False, group_by="ticker", threads=True)
        is_multi = len(yf_tickers) > 1
        for t in yf_tickers:
            try:
                closes = (raw[t]["Close"] if is_multi else raw["Close"]).dropna()
                if len(closes) >= 2:
                    chg1d_map[t] = round((closes.iloc[-1] / closes.iloc[-2] - 1) * 100, 2)
                if len(closes) >= 6:
                    chg1w_map[t] = round((closes.iloc[-1] / closes.iloc[-6] - 1) * 100, 2)
                elif len(closes) >= 2:
                    chg1w_map[t] = round((closes.iloc[-1] / closes.iloc[0] - 1) * 100, 2)
            except (KeyError, TypeError):
                continue
    except Exception as e:
        print(f"[Heatmap] ดาวน์โหลดราคา {index_key} ล้มเหลว: {e}")

    # Market cap — fast_info ต้องยิงทีละ ticker (ไม่รวมอยู่ใน batch download ด้านบน)
    # ใช้ disk cache อายุ 1 วันก่อน (เปลี่ยนช้า) ยิง Yahoo เฉพาะตัวที่ cache หมดอายุ/ไม่มี
    # ขนาน 12 threads เหมือน DR rebuild (ดู _mc_one) ลดจาก sequential ~1-2 นาทีเหลือ ~15-20 วิ
    def _mc_one(t):
        try:
            v = getattr(yf.Ticker(t).fast_info, "market_cap", None)
            return t, (float(v) if v else None)
        except Exception:
            return t, None

    mktcap_cache = _load_mktcap_cache()
    now_ts = time.time()
    stale = [t for t in yf_tickers if force
             or t not in mktcap_cache
             or now_ts - mktcap_cache[t].get("ts", 0) >= _MKTCAP_CACHE_TTL]

    if stale:
        try:
            with ThreadPoolExecutor(max_workers=12) as ex:
                for t, mc in ex.map(_mc_one, stale):
                    if mc is not None:
                        mktcap_cache[t] = {"mc": mc, "ts": now_ts}
        except Exception as e:
            print(f"[Heatmap] market cap batch {index_key} ล้มเหลว: {e}")
        _save_mktcap_cache(mktcap_cache)

    mkt_map = {t: mktcap_cache[t]["mc"] for t in yf_tickers if t in mktcap_cache}

    mirror_names_us = _load_mirror_names_us()
    extra_names = local.get("extra_names") or {}
    sector_map = local.get(f"{index_key}_sector") or {}

    rows = []
    for t in tickers:
        mc = mkt_map.get(t)
        chg_1d = chg1d_map.get(t)
        chg_1w = chg1w_map.get(t)
        if mc is None and chg_1d is None:
            continue   # โดนบล็อค/ไม่มีข้อมูลจริงทั้งคู่ — ข้ามแทนที่จะโชว์กล่องว่าง
        rows.append({
            "symbol": t,
            "name": mirror_names_us.get(t) or extra_names.get(t) or t,
            "mkt_cap": mc,
            "chg_1d": chg_1d,
            "chg_1w": chg_1w,
            "sector": sector_map.get(t) or "อื่นๆ",
        })

    result = {"rows": rows, "ts": now_ts, "requested": len(tickers), "missing": len(tickers) - len(rows)}
    # chg1d_map ว่างเปล่า = yf.download ทั้งชุดล้มเหลว (โดนบล็อค/เน็ตปัญหา) — ถ้า cache
    # ผลลัพธ์สีเทาทั้งกระดานนี้ไว้ 15 นาทีจะยิ่งแย่ (ผู้ใช้ต้องรอนานกว่าจะได้ลองใหม่)
    # ปล่อยให้ request ถัดไปยิง Yahoo ใหม่แทน — cache เฉพาะตอนที่ได้ราคาจริงมาบ้าง
    if chg1d_map:
        _heatmap_cache[index_key] = result
    return jsonify(result)


# ============================================================
# ข่าวรายหุ้น — รวม 3 แหล่งในหน้าเดียว (เมนู "📰 ข่าวหุ้น")
#   1) SET.or.th — ประกาศทางการของบริษัท (หุ้นไทยเท่านั้น)
#   2) Yahoo Finance — ข่าวสำนักต่างประเทศ (ครบสำหรับ US/HK, หุ้นไทยมีบ้าง)
#   3) Google News RSS — ข่าวสื่อไทย/สากลทุกสำนัก (ครอบคลุมสุด ไม่ต้องมี API key)
# ============================================================

def _news_resolve_yf(sym, is_dr, market):
    """หา yf ticker สำหรับดึงข่าว Yahoo — ใช้ logic เดียวกับ fetch_yahoo_full ของ financials_store"""
    if not is_dr:
        return sym + ".BK"
    entry = next((s for s in load_dr_universe(BASE_DIR) if s["sym"] == sym), None)
    if entry and entry.get("yf"):
        return entry["yf"]
    if market == "HK":
        return sym.zfill(4) + ".HK"
    return sym


def _news_from_set(sym):
    """ประกาศทางการจาก SET.or.th — คืน [] เงียบๆ ถ้าพัง (แหล่งอื่นยังใช้ได้)"""
    import urllib.parse
    from sources.set_api import _bootstrap_headers, _get_json
    ctx, hdr = _bootstrap_headers()
    d = _get_json(ctx, hdr, f"/api/set/news/{urllib.parse.quote(sym)}/list?lang=th")
    rows = []
    for n in (d.get("newsInfoList") or []):
        if not n.get("headline"):
            continue
        rows.append({
            "title": n["headline"],
            "url": n.get("url") or "",
            "ts": (n.get("datetime") or "")[:19],
            "source": "set",
            "publisher": "SET.or.th (ประกาศบริษัท)",
            "summary": "",
        })
    return rows


def _news_from_yahoo(yf_ticker):
    """ข่าวจาก Yahoo Finance ผ่าน yfinance — รองรับทั้ง payload รุ่นใหม่ (ห่อใน 'content')
    และรุ่นเก่า (field แบนราบ providerPublishTime เป็น epoch)"""
    import yfinance as yf
    from datetime import datetime
    rows = []
    for n in (yf.Ticker(yf_ticker).news or []):
        c = n.get("content") or n   # รุ่นใหม่ห่อใน content, รุ่นเก่าอยู่ชั้นนอกเลย
        title = c.get("title")
        if not title:
            continue
        url = (((c.get("canonicalUrl") or {}).get("url"))
               or ((c.get("clickThroughUrl") or {}).get("url"))
               or n.get("link") or "")
        ts = (c.get("pubDate") or c.get("displayTime") or "")[:19]
        if not ts and n.get("providerPublishTime"):
            ts = datetime.fromtimestamp(n["providerPublishTime"]).strftime("%Y-%m-%dT%H:%M:%S")
        publisher = ((c.get("provider") or {}).get("displayName")) or n.get("publisher") or "Yahoo Finance"
        summary = re.sub(r"<[^>]+>", "", c.get("summary") or c.get("description") or "")[:250]
        rows.append({"title": title, "url": url, "ts": ts, "source": "yahoo",
                     "publisher": publisher, "summary": summary})
    return rows


def _news_from_google(query, lang_th):
    """Google News RSS — ไม่ต้องมี key · lang_th=True ใช้ feed ไทย (ครอบคลุมสื่อหุ้นไทย
    อย่าง HoonVision/ข่าวหุ้น/ทันหุ้น), False ใช้ feed อังกฤษ (สำนักสากล)"""
    import urllib.parse
    import urllib.request as _ur
    import ssl as _ssl
    import xml.etree.ElementTree as _ET
    from email.utils import parsedate_to_datetime
    loc = "hl=th&gl=TH&ceid=TH:th" if lang_th else "hl=en-US&gl=US&ceid=US:en"
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&{loc}"
    req = _ur.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"})
    ctx = _ssl.create_default_context()
    with _ur.urlopen(req, context=ctx, timeout=20) as r:
        xml = r.read().decode("utf-8", "ignore")
    rows = []
    for it in _ET.fromstring(xml).findall(".//item"):
        title = it.findtext("title") or ""
        if not title:
            continue
        ts = ""
        try:
            ts = parsedate_to_datetime(it.findtext("pubDate")).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
        src_el = it.find("source")
        # Google News ต่อท้าย title ด้วย " - ชื่อสำนัก" — ตัดออกเมื่อรู้ชื่อสำนักจาก tag แล้ว
        publisher = src_el.text if src_el is not None and src_el.text else "Google News"
        if title.endswith(" - " + publisher):
            title = title[:-(len(publisher) + 3)]
        rows.append({"title": title, "url": it.findtext("link") or "", "ts": ts,
                     "source": "google", "publisher": publisher, "summary": ""})
    return rows


@app.route("/api/stock-news/<symbol>")
def stock_news(symbol):
    """ข่าวรวมของหุ้นตัวเดียว — ?is_dr=1&market=US|HK สำหรับหุ้นต่างประเทศ
    คืน {rows: [{title,url,ts,source,publisher,summary}], ts, errors: {src: msg}}
    เรียงใหม่สุดก่อน · dedupe หัวข้อซ้ำข้ามแหล่ง · แหล่งไหนพังไม่ล้มทั้งก้อน (รายงานใน errors)"""
    from concurrent.futures import ThreadPoolExecutor

    sym = symbol.upper().strip()
    is_dr = request.args.get("is_dr") == "1"
    market = (request.args.get("market") or "US").upper()

    cache_key = (sym, is_dr)
    cached = _stock_news_cache.get(cache_key)
    if cached and (time.time() - cached["ts"] < _STOCK_NEWS_CACHE_TTL):
        return jsonify(cached)

    yf_ticker = _news_resolve_yf(sym, is_dr, market)
    # คำค้น Google: หุ้นไทยเติม "หุ้น" กันชนคำสามัญ (ALL/GIFT/EE) · หุ้นนอกใช้ "stock"
    g_query = f"{sym} หุ้น" if not is_dr else f"{sym} stock"

    jobs = {"yahoo": lambda: _news_from_yahoo(yf_ticker),
            "google": lambda: _news_from_google(g_query, lang_th=not is_dr)}
    if not is_dr:
        jobs["set"] = lambda: _news_from_set(sym)

    rows, errors = [], {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {name: ex.submit(fn) for name, fn in jobs.items()}
        for name, f in futs.items():
            try:
                rows.extend(f.result(timeout=30))
            except Exception as e:
                errors[name] = str(e)[:120]

    # dedupe หัวข้อซ้ำข้ามแหล่ง (Google มักเจอข่าวเดียวกับ Yahoo) — คงตัวที่เจอก่อนตามลำดับ
    # แหล่ง (yahoo มี summary ครบกว่า) · เทียบแบบตัดช่องว่าง/ตัวพิมพ์
    seen, deduped = set(), []
    for r in sorted(rows, key=lambda r: r["source"]!= "yahoo"):
        k = re.sub(r"\s+", "", r["title"].lower())[:80]
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    deduped.sort(key=lambda r: r["ts"] or "0000", reverse=True)

    result = {"rows": deduped[:80], "ts": time.time(), "symbol": sym, "errors": errors}
    _stock_news_cache[cache_key] = result
    return jsonify(result)


@app.route("/api/mirror-names")
def mirror_names():
    """ชื่อบริษัทของหุ้น US/HK ที่มีงบ (จาก mirror_names.json — สร้างด้วย build_mirror_names.py)
    คืน {US: {ticker: name}, HK: {...}} · ว่างถ้ายังไม่ได้สร้างไฟล์"""
    path = os.path.join(BASE_DIR, "mirror_names.json")
    if not os.path.exists(path):
        return jsonify({})
    try:
        with open(path, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify({})


@app.route("/api/financials-dq-check/<symbol>")
def financials_dq_check(symbol):
    """เทียบตัวเลขงบการเงิน Yahoo vs SET.or.th ของหุ้นตัวเดียว (on-demand, ไม่ cache — เร็วอยู่แล้ว)
    เฉพาะหุ้นไทย (DR ไม่มีข้อมูลจาก SET.or.th)"""
    sym = symbol.upper().strip()
    payload_yahoo = financials_store.get(BASE_DIR, sym, "yahoo")
    payload_set = financials_store.get(BASE_DIR, sym, "set")
    return jsonify(financials_store.compare_sources(payload_yahoo, payload_set))


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

    def _fetch_one(sym, timeout=8):
        info = INDEX_INFO.get(sym)
        if not info:
            return sym, None
        tv_sym = _yf_to_tv(sym)
        try:
            pairs = _fetch_tv_bars(tv_sym, n_bars=n_bars, timeout=timeout)
            if not pairs:
                print(f"[Indices] no data: {tv_sym}")
                return sym, None
            return sym, pairs
        except Exception:
            print(f"[Indices] {sym}: {tb.format_exc()}")
            return sym, None

    def _merge_one(sym, pairs):
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

    # ดึงแบบ parallel — สูงสุด 10 connections พร้อมกัน
    max_workers = 10 if not full_refresh else 5
    fetched = 0
    failed_syms = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, sym): sym for sym in all_syms}
        for future in as_completed(futures):
            sym, pairs = future.result()
            if pairs is None:
                failed_syms.append(sym)
                continue
            fetched += 1
            _merge_one(sym, pairs)

    # retry รอบสอง: TV WebSocket แบบขนาน 10 ตัวพร้อมกัน flaky เป็นปกติ (บางรอบล้ม
    # 10+/50 ตัวแบบสุ่ม) — ตัวที่ล้มซ้ำหลายรอบติดกันจะค้างเป็นข้อมูลเก่าเงียบๆ
    # (เคยเกิดกับ SET Index/SET100 ค้าง 2 วัน) ดึงซ้ำทีละตัว + timeout ยาวขึ้น ชัวร์กว่ามาก
    if failed_syms:
        print(f"[Indices] retry {len(failed_syms)} ดัชนีที่ล้มรอบแรก (ทีละตัว): "
              f"{', '.join(failed_syms[:10])}{'...' if len(failed_syms) > 10 else ''}")
        still_failed = []
        for sym in failed_syms:
            _, pairs = _fetch_one(sym, timeout=15)
            if pairs is None:
                still_failed.append(sym)
            else:
                fetched += 1
                _merge_one(sym, pairs)
        failed_syms = still_failed

    failed = len(failed_syms)
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
# DATA HEALTH — ภาพรวมความสดของทุกแหล่งข้อมูลในจอเดียว (local-only)
# เกณฑ์ (warn/red ชม.) อ้างอิงรอบอัพเดทจริงที่บันทึกไว้ใน คู่มือ-อัพเดทข้อมูล.txt
# ============================================================
from datetime import datetime as _dh_dt, timedelta

def _dh_mtime(path):
    try:
        return _dh_dt.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None

def _dh_parse(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return _dh_dt.strptime(s, fmt)
        except Exception:
            continue
    return None

def _dh_age_hours(dt):
    if dt is None:
        return None
    return (_dh_dt.now() - dt).total_seconds() / 3600

def _dh_business_age_hours(dt):
    """อายุแบบตัดชั่วโมงวันเสาร์-อาทิตย์ออก — ใช้เทียบ threshold เท่านั้น (ไม่ใช่ตัวที่
    แสดงผลให้ผู้ใช้เห็น) กันไม่ให้ prices/flow ขึ้น warn/red หลอกทุกวันจันทร์เช้าหรือช่วง
    วันหยุดยาว ทั้งที่จริงๆ ไม่มีข้อมูลใหม่ให้ดึงในวันหยุดอยู่แล้ว"""
    if dt is None:
        return None
    now = _dh_dt.now()
    total_h = (now - dt).total_seconds() / 3600
    if total_h <= 0:
        return max(total_h, 0.0)
    weekend_h = 0.0
    d = dt.date()
    end_d = now.date()
    while d <= end_d:
        if d.weekday() >= 5:  # 5=เสาร์, 6=อาทิตย์
            day_start = _dh_dt.combine(d, _dh_dt.min.time())
            day_end = day_start + timedelta(days=1)
            overlap_start = max(day_start, dt)
            overlap_end = min(day_end, now)
            if overlap_end > overlap_start:
                weekend_h += (overlap_end - overlap_start).total_seconds() / 3600
        d += timedelta(days=1)
    return max(total_h - weekend_h, 0.0)

def _dh_oldest_mtime(paths):
    """mtime ที่ "เก่าที่สุด" ในกลุ่มไฟล์ — ใช้กับชุดไฟล์ที่ควรถูกเขียนพร้อมกันในรอบเดียว
    (เช่น data/*.json ที่ bake จาก run_static_update.py) ถ้าไฟล์ไหนตกรอบไป จะถูก
    จับได้ทันทีแทนที่จะถูกกลบด้วยไฟล์อื่นที่สดกว่า · คืน (mtime, ชื่อไฟล์ที่เก่าสุด)
    ถ้ามีไฟล์ไหนหายไปเลย คืน (None, ชื่อไฟล์นั้น)"""
    oldest, oldest_name = None, None
    for p in paths:
        dt = _dh_mtime(p)
        if dt is None:
            return None, os.path.basename(p)
        if oldest is None or dt < oldest:
            oldest, oldest_name = dt, os.path.basename(p)
    return oldest, oldest_name


def _dh_quality_item(key, label, status, note, last_at=None):
    """รายการเช็ค "คุณภาพ" (ไม่ใช่ความสด) — ไฟล์ถูกเขียนทับด้วยข้อมูลว่าง/พร่องจะยัง
    ขึ้นเขียวถ้าดูแค่ mtime ตัวนี้เลยเปิดไฟล์อ่านจริงแล้วนับจำนวน/ดูวันล่าสุดข้างใน"""
    return {"key": key, "label": label, "category": "คุณภาพข้อมูล (ไม่ใช่แค่ความสด)",
            "last_at": last_at, "age_hours": None, "status": status, "note": note}


def _dh_item(key, label, category, dt, warn_h, red_h,
             missing_note="ไม่พบไฟล์ / ยังไม่เคยอัพเดท", optional=False):
    age_h = _dh_age_hours(dt)
    status_age_h = _dh_business_age_hours(dt)
    if age_h is None:
        # optional=True: แหล่งข้อมูลที่ไม่ใช่ทุกเครื่องต้องมี (เช่น ยังไม่เคยเปิดหน้านั้นๆ)
        # ไม่ควรถูกนับเป็น 🔴 เท่ากับ "เคยมีแล้วค้าง" — แยกเป็น na (⚪ ยังไม่ใช้งาน)
        status = "na" if optional else "red"
    elif status_age_h >= red_h:
        status = "red"
    elif status_age_h >= warn_h:
        status = "warn"
    else:
        status = "ok"
    return {
        "key": key, "label": label, "category": category,
        "last_at": dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None,
        "age_hours": round(age_h, 1) if age_h is not None else None,
        "status": status,
        "note": None if dt else missing_note,
    }


@app.route("/api/data-health")
def data_health():
    """สถานะความสดของไฟล์/DB หลักทุกตัวที่ dashboard ใช้ — เกณฑ์ ok/warn/red
    ต่อรายการ (ดู PLAN_universe_data_health.txt ส่วนที่ 6 งาน 1)"""
    items = []

    # ราคา/เทคนิค — auto 3 รอบ/วัน (จ-ศ), gap วันหยุดสุดสัปดาห์ ~59.5 ชม.
    items.append(_dh_item(
        "prices", "ราคา/RS/เทคนิค (หุ้นไทย)", "ราคา/เทคนิค",
        _dh_parse(price_store.get_meta(BASE_DIR, "updated_at")), 30, 72))

    items.append(_dh_item(
        "indices", "ดัชนีกลุ่ม (TradingView)", "ราคา/เทคนิค",
        _dh_mtime(os.path.join(BASE_DIR, "indices_cache.json")), 30, 96))

    items.append(_dh_item(
        "dr_cache", "DR/DRx (ราคา underlying)", "ราคา/เทคนิค",
        _dh_mtime(DR_CACHE_FILE), 24, 168,
        missing_note="ยังไม่เคยเปิดหน้า DR/DRx ในเครื่องนี้", optional=True))

    # Flow / เจ้าของ
    items.append(_dh_item(
        "market_flow", "Capital Flow", "Flow/เจ้าของ",
        _dh_mtime(os.path.join(BASE_DIR, "market_flow_data.json")), 30, 96))

    items.append(_dh_item(
        "s50_flow", "S50 Futures Flow", "Flow/เจ้าของ",
        _dh_mtime(_S50_FLOW_FILE), 30, 96))

    items.append(_dh_item(
        "bond_flow", "Bond Flow", "Flow/เจ้าของ",
        _dh_mtime(_BOND_FLOW_FILE), 30, 96))

    items.append(_dh_item(
        "short_sales", "Short Sales", "Flow/เจ้าของ",
        _dh_mtime(_SHORT_DATA_FILE), 48, 120))

    items.append(_dh_item(
        "nvdr", "NVDR", "Flow/เจ้าของ",
        _dh_mtime(os.path.join(BASE_DIR, "nvdr_data.json")), 48, 120))

    _insider_at = _dh_parse(sec_store._get_meta(BASE_DIR, "insider_last_synced_at"))
    items.append(_dh_item(
        "insider", "Insider / ผู้ถือหุ้นใหญ่", "Flow/เจ้าของ",
        _insider_at, 48, 168))

    # Hedge Holdings (13F superinvestors จาก Dataroma) — ยื่นรายไตรมาส ดีเลย์ ~45 วัน
    # เกณฑ์เตือนจึงหลวมกว่า flow อื่น (แนะนำกดรีเฟรชเดือนละครั้งพอ — ดู sources/dataroma.py)
    from sources import dataroma as _dataroma_dh
    _hedge_cache = _dataroma_dh.load_cache(BASE_DIR)
    items.append(_dh_item(
        "hedge_holdings", "Hedge Holdings 13F (Dataroma)", "Flow/เจ้าของ",
        _dh_parse(_hedge_cache.get("generated_at")) if _hedge_cache else None,
        45 * 24, 100 * 24,
        missing_note="ยังไม่เคยดึง Hedge Holdings ในเครื่องนี้ — กดปุ่ม '⟳ อัพเดท Hedge Holdings' "
                      "ในหน้า 🐋 Hedge Holdings", optional=True))

    # หุ้นเข้าใหม่/ถูกถอด
    items.append(_dh_item(
        "set_universe", "รายชื่อหุ้น SET (listedCompanies xls)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "listedCompanies_en_US.xls")), 45 * 24, 90 * 24))

    items.append(_dh_item(
        "us_index_membership", "สมาชิกดัชนี US (S&P500/DOW/NDX)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "us_index_membership.json")), 45 * 24, 90 * 24))

    items.append(_dh_item(
        "hk_index_membership", "สมาชิกดัชนี HK (HSI/HSCEI/HSTECH)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "hk_index_membership.json")), 45 * 24, 90 * 24))

    items.append(_dh_item(
        "jp_index_membership", "สมาชิกดัชนี JP (Nikkei 225)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "jp_index_membership.json")), 45 * 24, 90 * 24))

    # ราคา/metrics หุ้นดัชนี US/HK/JP — auto ผ่าน Quick Update/Index Max, เกณฑ์เดียวกับ
    # ราคาหุ้นไทย (30/72 ชม.) เพราะ upsert_bars() stamp 'updated_at' รอบเดียวกัน
    from core import us_store as _us_store, hk_store as _hk_store, jp_store as _jp_store
    items.append(_dh_item(
        "us_prices", "ราคาหุ้นดัชนี US (us_prices.db)", "ราคา/เทคนิค",
        _dh_parse(_us_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น US ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "hk_prices", "ราคาหุ้นดัชนี HK (hk_prices.db)", "ราคา/เทคนิค",
        _dh_parse(_hk_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น HK ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "jp_prices", "ราคาหุ้นดัชนี JP (jp_prices.db)", "ราคา/เทคนิค",
        _dh_parse(_jp_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น JP ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "us_index_metrics", "Metrics หุ้นดัชนี US (us_index_metrics.json)", "ราคา/เทคนิค",
        _dh_mtime(os.path.join(BASE_DIR, "data", "us_index_metrics.json")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น US ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "hk_index_metrics", "Metrics หุ้นดัชนี HK (hk_index_metrics.json)", "ราคา/เทคนิค",
        _dh_mtime(os.path.join(BASE_DIR, "data", "hk_index_metrics.json")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น HK ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "jp_index_metrics", "Metrics หุ้นดัชนี JP (jp_index_metrics.json)", "ราคา/เทคนิค",
        _dh_mtime(os.path.join(BASE_DIR, "data", "jp_index_metrics.json")), 30, 72,
        missing_note="ยังไม่เคยอัพเดทหุ้น JP ในเครื่องนี้", optional=True))

    # งบการเงิน (local-only)
    _fin_summary = financials_store.get_meta_summary(BASE_DIR)
    items.append(_dh_item(
        "financials", "งบการเงิน หุ้นไทย+DR (financials.db)", "งบการเงิน",
        _dh_parse(_fin_summary.get("last_synced_at")), 100 * 24, 150 * 24,
        missing_note="ยังไม่เคยรัน update_financials.py"))

    # เก็บเป็น JSON string {"at": "YYYY-MM-DD HH:MM", "ok":.., "empty":.., "fail":.., "total":.., "force":..}
    # ไม่ใช่ plain datetime string เหมือน meta อื่น — ต้องแกะก่อน parse
    _mirror_raw = financials_store._get_meta(BASE_DIR, "finnomena_mirror_last")
    _mirror_at = None
    _mirror_info = {}
    if _mirror_raw:
        try:
            _mirror_info = json.loads(_mirror_raw)
            _mirror_at = _dh_parse(_mirror_info.get("at"))
        except Exception:
            pass
    _mirror_item = _dh_item(
        "mirror", "Mirror งบ US/HK ทั้งตลาด (Finnomena)", "งบการเงิน",
        _mirror_at, 100 * 24, 200 * 24,
        missing_note="ยังไม่เคยรัน mirror_finnomena.py")
    # ตัว detector "Finnomena อาจเปลี่ยน/ปิด API" — force run (ยิงซ้ำทุกตัวที่มีงบ
    # เพื่อดึงงวดใหม่) ปกติต้องได้ ok > 0 เสมอถ้ามี candidate เยอะ ถ้า ok=0 ทั้งที่ total
    # เยอะ = parser พังหรือ API เปลี่ยนรูปแบบ ไม่ใช่แค่ "ไม่มีงวดใหม่ตามฤดูกาล"
    if _mirror_info.get("force") and _mirror_info.get("total", 0) > 50 and _mirror_info.get("ok", 0) == 0:
        _mirror_item["status"] = "red"
        _mirror_item["note"] = ("⚠ รอบ force ล่าสุดดึงงบสำเร็จ 0 ตัวจาก "
                                 f"{_mirror_info['total']} ตัว — Finnomena อาจเปลี่ยน/ปิด API "
                                 "(ไม่ใช่แค่ไม่มีงวดใหม่ตามฤดูกาล) ดู PLAN_universe_data_health.txt งาน 4")
    items.append(_mirror_item)

    # Valuation ตลาด — ปกติช้ากว่าปัจจุบัน ~1 เดือน (ไม่ใช่ค้าง) เตือนเมื่อ >=2 เดือน
    _pe_status = "ok"
    _pe_last = None
    _pe_note = None
    if os.path.exists(_MARKET_STATS_FILE):
        try:
            with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
                _mstats = json.load(f)
            _pe_dates = (_mstats.get("pe", {}) or {}).get("dates") or []
            _pe_last = _pe_dates[-1] if _pe_dates else None
            if _pe_last:
                _py, _pm = (int(x) for x in _pe_last.split("-"))
                _now = _dh_dt.now()
                _months_old = (_now.year * 12 + _now.month) - (_py * 12 + _pm)
                _pe_status = "ok" if _months_old <= 1 else ("warn" if _months_old == 2 else "red")
            else:
                _pe_status = "red"
            # เช็ค updated_at คู่กับเดือนข้อมูลด้วย — ถ้าสคริปต์ตายเงียบๆ (เช่น source
            # เปลี่ยนรูปแบบ) เดือนข้อมูลจะยัง "ok" ได้นานถึง ~2 เดือนกว่าจะรู้ตัว แต่ถ้าไฟล์
            # เองไม่ถูกเขียนทับมา >45 วันทั้งที่ควรรันทุกเดือน ก็เป็นสัญญาณค้างได้เร็วกว่า
            _pe_updated_at = _dh_parse(_mstats.get("updated_at"))
            if _pe_updated_at:
                _pe_file_age_h = _dh_business_age_hours(_pe_updated_at)
                if _pe_file_age_h >= 60 * 24 and _pe_status == "ok":
                    _pe_status = "warn"
                if _pe_file_age_h >= 90 * 24:
                    _pe_status = "red"
        except Exception:
            _pe_status = "red"
            _pe_note = "อ่านไฟล์ไม่ได้"
    else:
        _pe_status = "red"
        _pe_note = "ไม่พบไฟล์ / ยังไม่เคยอัพเดท"
    items.append({
        "key": "market_stats", "label": "P/E & P/BV ตลาด (SET)", "category": "Valuation",
        "last_at": _pe_last, "age_hours": None, "status": _pe_status, "note": _pe_note,
    })

    # สำรอง financials.db + set_prices.db + sec_filings.db + delisted_log.json
    # นอกเครื่อง (external drive/cloud sync) — บันทึกโดย backup_financials_offsite.py
    # ผ่าน core.run_log เดียวกับกลไกอัพเดทอื่น ความเสี่ยงสูงสุดของระบบ: ไฟล์เหล่านี้
    # สร้างใหม่ไม่ได้ถ้า Finnomena ปิด API หรือหุ้นถูก delist ไปแล้ว
    _backup_status = run_log.read_status(BASE_DIR).get("offsite_backup")
    items.append(_dh_item(
        "offsite_backup", "สำรองไฟล์สร้างใหม่ไม่ได้นอกเครื่อง", "งบการเงิน",
        _dh_parse(_backup_status.get("at")) if _backup_status else None,
        35 * 24, 60 * 24,
        missing_note="ยังไม่เคยรัน python backup_financials_offsite.py <ปลายทาง> — "
                      "financials.db/set_prices.db สร้างใหม่ไม่ได้ถ้า Finnomena ปิด API "
                      "หรือหุ้น delisted (ดู PLAN_universe_data_health.txt งาน 4)"))

    # ── เว็บมือถือ (GitHub Pages) — ไฟล์ bake ใน data/ ────────────────────
    # ก่อนหน้านี้ไม่มีการเช็คเลยสักไฟล์: ถ้า run_static_update.py / GitHub Actions
    # ตายเงียบๆ หน้านี้จะเขียวหมดทั้งที่เว็บมือถือเสิร์ฟข้อมูลค้างไปเรื่อยๆ
    _DATA_DIR = os.path.join(BASE_DIR, "data")
    _bake_core = ["set_data.json", "breadth_1y.json", "indices_data.json",
                  "market_flow.json", "nvdr_data.json", "short_sales.json",
                  "insider_trades_30.json", "market_stats.json"]
    _bake_at, _bake_which = _dh_oldest_mtime([os.path.join(_DATA_DIR, f) for f in _bake_core])
    _bake_item = _dh_item(
        "static_bake", "ข้อมูล bake เว็บมือถือ (data/*.json)", "เว็บมือถือ (GitHub Pages)",
        _bake_at, 30, 96,
        missing_note=f"ไม่พบ data/{_bake_which} — ยังไม่เคยรัน run_static_update.py")
    if _bake_at is not None:
        # แสดงชื่อไฟล์ที่เก่าสุดเสมอ ไม่ใช่เฉพาะตอนเตือน — เวลาไฟล์ตัวเดียวตกรอบ
        # (เช่น endpoint นั้น error) จะเห็นได้ว่าเป็นตัวไหนโดยไม่ต้องไปไล่ดูโฟลเดอร์เอง
        _bake_item["note"] = (f"เก่าสุด: {_bake_which} · เวลานี้คือ mtime ของสำเนาในเครื่อง "
                              "(อัพเดทตอนรัน run_static_update.py เอง หรือ git pull "
                              "หลัง GitHub Actions commit)")
    items.append(_bake_item)

    _bake_run = run_log.read_status(BASE_DIR).get("static_bake")
    items.append(_dh_item(
        "static_bake_run", "รอบรัน run_static_update.py ล่าสุด (ในเครื่อง)",
        "เว็บมือถือ (GitHub Pages)",
        _dh_parse(_bake_run.get("at")) if _bake_run else None, 30 * 24, 90 * 24,
        missing_note="ยังไม่เคยรัน run_static_update.py ในเครื่องนี้ (ปกติรันบน GitHub "
                      "Actions — logs/ เป็น local-only ไม่ตามมาจาก CI)", optional=True))
    if _bake_run and not _bake_run.get("ok"):
        for _it in items:
            if _it["key"] == "static_bake_run":
                _it["status"] = "red"
                _it["note"] = f"⚠ รอบล่าสุดล้มเหลว: {str(_bake_run.get('message'))[:200]}"

    # ── ไฟล์ประกอบอื่นๆ ที่หน้าเว็บใช้จริงแต่เดิมไม่มีใครเฝ้า ──────────────
    items.append(_dh_item(
        "set_history", "ประวัติราคา/สถิติย้อนหลัง (set_history.json)", "ราคา/เทคนิค",
        _dh_mtime(HISTORY_FILE), 30 * 24, 90 * 24,
        missing_note="ยังไม่เคยสร้าง set_history.json", optional=True))

    items.append(_dh_item(
        "fin_analytics_yahoo", "Analytics งบจาก Yahoo (financials_analytics_yahoo.json)",
        "งบการเงิน",
        _dh_mtime(os.path.join(_DATA_DIR, "financials_analytics_yahoo.json")),
        100 * 24, 150 * 24, missing_note="ยังไม่เคยสร้าง", optional=True))

    items.append(_dh_item(
        "stock_valuation_stats", "P/E-P/BV รายตัว (stock_valuation_stats.json)", "Valuation",
        _dh_mtime(os.path.join(_DATA_DIR, "stock_valuation_stats.json")), 45 * 24, 90 * 24))

    # mirror_names.json = ชื่อบริษัท US/HK ที่ใช้โชว์ใน Screener+/Tearsheet — ถ้าไม่ rebuild
    # หลัง mirror ได้หุ้นใหม่ ตัวใหม่จะโชว์แค่ ticker เปล่าๆ (ไม่พังแต่ดูไม่รู้เรื่อง)
    items.append(_dh_item(
        "mirror_names", "ชื่อบริษัท mirror US/HK (mirror_names.json)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "mirror_names.json")), 100 * 24, 200 * 24,
        missing_note="ยังไม่เคยรัน build_mirror_names.py", optional=True))

    items.append(_dh_item(
        "dr_universe", "รายชื่อ DR/DRx (dr_universe_auto.json)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "dr_universe_auto.json")), 45 * 24, 90 * 24,
        missing_note="ยังไม่เคยเช็คหุ้น DR ใหม่ในเครื่องนี้", optional=True))

    items.append(_dh_item(
        "dr_descriptions", "คำอธิบายบริษัท DR (dr_descriptions.json)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "dr_descriptions.json")), 180 * 24, 365 * 24,
        missing_note="ยังไม่เคยดึงคำอธิบาย DR", optional=True))

    # ── คุณภาพข้อมูล (เปิดไฟล์อ่านจริง ไม่ใช่แค่ mtime) ────────────────────
    # ทุก item ข้างบนวัดแค่ "ถูกเขียนล่าสุดเมื่อไหร่" — ไฟล์ที่ถูกเขียนทับด้วยข้อมูล
    # ว่าง/พร่อง (source เปลี่ยนรูปแบบ, parser คืน list ว่าง) จะยังขึ้นเขียวสนิท
    try:
        with open(os.path.join(BASE_DIR, "set_data.json"), encoding="utf-8") as f:
            _sd = json.load(f)
        _n_stocks = len(_sd.get("stocks") or [])
        _q_status = "ok" if _n_stocks >= 800 else ("warn" if _n_stocks >= 500 else "red")
        items.append(_dh_quality_item(
            "q_set_data", "จำนวนหุ้นใน set_data.json", _q_status,
            f"{_n_stocks} ตัว (ปกติ ~900-950 · <800 = เริ่มผิดปกติ, <500 = พร่องชัดเจน)",
            last_at=_sd.get("data_as_of")))
    except Exception as e:
        items.append(_dh_quality_item(
            "q_set_data", "จำนวนหุ้นใน set_data.json", "red", f"อ่านไฟล์ไม่ได้: {str(e)[:120]}"))

    try:
        _bake_sd_path = os.path.join(_DATA_DIR, "set_data.json")
        _bake_mb = os.path.getsize(_bake_sd_path) / (1024 * 1024)
        with open(_bake_sd_path, encoding="utf-8") as f:
            _bsd = json.load(f)
        _bn = len(_bsd.get("stocks") or [])
        # ไฟล์ bake ถูก _slim_set_data() ตัดฟิลด์ออก เลยเล็กกว่าตัวเต็มมาก (~7MB vs 12MB)
        # แต่ถ้าต่ำกว่า 1MB = แทบแน่นอนว่าเป็น payload ว่าง/พร่อง ไม่ใช่แค่ slim
        _q2 = "ok" if (_bake_mb >= 1.0 and _bn >= 800) else ("warn" if _bn >= 500 else "red")
        items.append(_dh_quality_item(
            "q_bake_set_data", "ขนาด/ความครบของ data/set_data.json (เว็บมือถือ)", _q2,
            f"{_bake_mb:.1f} MB · {_bn} ตัว (<1 MB หรือ <500 ตัว = payload ว่าง)",
            last_at=_bsd.get("data_as_of")))
    except Exception as e:
        items.append(_dh_quality_item(
            "q_bake_set_data", "ขนาด/ความครบของ data/set_data.json (เว็บมือถือ)", "red",
            f"อ่านไฟล์ไม่ได้: {str(e)[:120]}"))

    try:
        with open(os.path.join(_DATA_DIR, "breadth_1y.json"), encoding="utf-8") as f:
            _bd = json.load(f)
        _bd_dates = _bd.get("dates") or []
        _bd_last = _bd_dates[-1] if _bd_dates else None
        if not _bd_last:
            items.append(_dh_quality_item("q_breadth", "วันล่าสุดในข้อมูล breadth (เว็บมือถือ)",
                                          "red", "ไม่มีวันที่ในไฟล์เลย (payload ว่าง)"))
        else:
            # _dh_parse รู้จักแค่ "%Y-%m-%d[ %H:%M[:%S]]" — ถ้ารูปแบบวันที่ในไฟล์เปลี่ยน
            # (เช่นกลายเป็น ISO "…T00:00:00") จะได้ None แล้วอายุกลายเป็น 0 = เขียวสนิท
            # ทั้งที่ข้อมูลอาจค้างมาเป็นเดือน — แยกเคส "อ่านวันที่ไม่ออก" ออกมาเตือนแทน
            _bd_age_h = _dh_business_age_hours(_dh_parse(_bd_last))
            if _bd_age_h is None:
                items.append(_dh_quality_item(
                    "q_breadth", "วันล่าสุดในข้อมูล breadth (เว็บมือถือ)", "warn",
                    f"อ่านรูปแบบวันที่ไม่ออก: {str(_bd_last)[:40]!r} "
                    f"(คาดว่า YYYY-MM-DD) · {len(_bd_dates)} จุด — เช็คความสดไม่ได้",
                    last_at=_bd_last))
            else:
                _bd_days = _bd_age_h / 24
                _q3 = "ok" if _bd_days < 5 else ("warn" if _bd_days < 14 else "red")
                items.append(_dh_quality_item(
                    "q_breadth", "วันล่าสุดในข้อมูล breadth (เว็บมือถือ)", _q3,
                    f"ข้อมูลถึง {_bd_last} ({_bd_days:.0f} วันทำการก่อน) · {len(_bd_dates)} จุด",
                    last_at=_bd_last))
    except Exception as e:
        items.append(_dh_quality_item("q_breadth", "วันล่าสุดในข้อมูล breadth (เว็บมือถือ)",
                                      "red", f"อ่านไฟล์ไม่ได้: {str(e)[:120]}"))

    summary = {"ok": 0, "warn": 0, "red": 0, "na": 0}
    for it in items:
        summary[it["status"]] += 1

    return jsonify({
        "checked_at": _dh_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        "items": items,
        "summary": summary,
    })


@app.route("/api/data-health-ping")
def data_health_ping():
    """ยิงทดสอบแหล่งข้อมูลภายนอกทีละ 1 request เบาๆ (timeout สั้น) — ใช้ตอบคำถาม
    'ตอนนี้ดึงจาก SET/Yahoo/Finnomena/TradingView ได้จริงไหม' ไม่ได้ผูกกับ mtime"""
    import urllib.request as _dh_ur
    import ssl as _dh_ssl
    from concurrent.futures import ThreadPoolExecutor

    def _ping(key, label, url, headers=None, timeout=6, insecure=False):
        t0 = time.time()
        try:
            req = _dh_ur.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
            # insecure=True เฉพาะแหล่งที่โค้ดดึงจริงก็ข้าม verify อยู่แล้ว (siamchart) —
            # ไม่งั้นปิงจะขึ้นแดงเพราะ cert ทั้งที่การดึงจริงยังทำงานได้ปกติ
            ctx = _dh_ssl._create_unverified_context() if insecure else _dh_ssl.create_default_context()
            with _dh_ur.urlopen(req, context=ctx, timeout=timeout) as r:
                code = r.getcode()
                r.read(256)  # แค่พิสูจน์ว่า body มาจริง ไม่ต้องอ่านทั้งหมด
            ms = round((time.time() - t0) * 1000)
            return {"key": key, "label": label, "ok": 200 <= code < 400,
                    "http_status": code, "ms": ms, "error": None}
        except Exception as e:
            ms = round((time.time() - t0) * 1000)
            return {"key": key, "label": label, "ok": False,
                    "http_status": None, "ms": ms, "error": str(e)[:160]}

    # ยิงขนานทั้ง 4 แหล่ง (ไม่ใช่ serial) — ถ้าเน็ตล่มทั้งหมด กรณีที่คนกดปุ่มนี้บ่อยที่สุด
    # จะรอแค่ ~timeout เดียว (~6 วิ) แทนที่จะรอสะสมทีละตัวจนถึง ~24 วิ
    targets = [
        ("set", "SET.or.th", "https://www.set.or.th/en/market/product/stock/quote/ptt/price"),
        ("yahoo", "Yahoo Finance", "https://query1.finance.yahoo.com/v8/finance/chart/PTT.BK"),
        ("finnomena", "Finnomena", "https://www.finnomena.com/fn3/api/stock/list?exchange=TH"),
        # TradingView ดึงข้อมูลจริงผ่าน WebSocket (ดู sources/tradingview.py) — ปิงนี้เช็คแค่
        # "เข้าถึงโดเมนได้ไหม" (เครือข่าย/บล็อก) ไม่ใช่การพิสูจน์ endpoint ข้อมูลเต็มรูปแบบ
        ("tradingview", "TradingView (reachability)", "https://www.tradingview.com/"),
        # siamchart = แหล่งหลักของ Capital Flow (มี fallback ไป SET API ทางการอยู่แล้ว
        # ดู _fetch_flow_siamchart) ปิงหน้าเดียวกับที่ดึงจริงเพื่อให้รู้ว่ากำลังใช้ fallback อยู่ไหม
        ("siamchart", "siamchart (Capital Flow)", "https://siamchart.com/stock-summary/",
         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
          "Referer": "https://siamchart.com/"}, 8, True),
    ]
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        results = list(ex.map(lambda t: _ping(*t), targets))

    return jsonify({"checked_at": _dh_dt.now().strftime("%Y-%m-%d %H:%M:%S"), "results": results})


# (ชื่อไฟล์ต้นทาง, label ไทย) — ต้องตรงกับ FILES ใน backup_financials_offsite.py
_OFFSITE_BACKUP_FILES = [
    ("financials.db", "งบการเงิน (financials.db)"),
    ("set_prices.db", "ราคาหุ้น + หุ้นเพิกถอน (set_prices.db)"),
    ("sec_filings.db", "SEC filings / insider (sec_filings.db)"),
    ("delisted_log.json", "ประวัติหุ้นเข้า/ออก (delisted_log.json)"),
]


@app.route("/api/backup-files-status")
def backup_files_status():
    """แยกรายไฟล์ว่า backup_financials_offsite.py สำรองแต่ละตัวล่าสุดวันไหน/ขนาดเท่าไหร่
    — ต่างจาก item 'offsite_backup' ใน /api/data-health (บอกแค่ 'สำรองล่าสุดเมื่อไหร่'
    รวมทั้งรอบ) ตัวนี้อ่าน dest_dir จาก run_log แล้วสแกนโฟลเดอร์ปลายทางจริง ทำให้เห็นได้
    ถ้าไฟล์ไหนไม่ได้ถูกสำรองจริง (เช่น sec_filings.db ไม่มีตอนรันรอบนั้น)"""
    status = run_log.read_status(BASE_DIR).get("offsite_backup")
    if not status:
        return jsonify({"dest_dir": None, "files": [],
                         "note": "ยังไม่เคยรัน backup_financials_offsite.py"})

    dest_dir = None
    try:
        dest_dir = (json.loads(status.get("message") or "{}")).get("dest_dir")
    except Exception:
        pass
    # เผื่อ record เดิม (ก่อนแก้ให้เก็บ dest_dir เป็น JSON) ยังเป็น plain string อยู่ —
    # ใช้ path เดียวกับที่ task_backup_offsite.bat เรียกจริงเป็น fallback แทนที่จะ
    # ต้องรอรอบสำรองถัดไปกว่าจะเห็นตารางนี้
    if not dest_dir:
        dest_dir = r"C:\Users\joeki\OneDrive\SET_Dashboard_Backup"

    if not os.path.isdir(dest_dir):
        return jsonify({"dest_dir": dest_dir, "files": [],
                         "note": f"เข้าไม่ถึงโฟลเดอร์ปลายทาง{(' ' + dest_dir) if dest_dir else ''} — "
                                 "external drive ถอดอยู่หรือ OneDrive ยังไม่ sync?"})

    try:
        entries = os.listdir(dest_dir)
    except Exception as e:
        return jsonify({"dest_dir": dest_dir, "files": [], "note": f"อ่านโฟลเดอร์ไม่ได้: {e}"})

    files = []
    for fname, label in _OFFSITE_BACKUP_FILES:
        stem, ext = os.path.splitext(fname)
        matches = sorted(f for f in entries if f.startswith(f"{stem}_") and f.endswith(ext))
        if not matches:
            files.append({"file": fname, "label": label, "last_backup_at": None,
                          "age_hours": None, "size_mb": None, "versions_kept": 0})
            continue
        latest = matches[-1]
        latest_path = os.path.join(dest_dir, latest)
        try:
            mtime = _dh_dt.fromtimestamp(os.path.getmtime(latest_path))
            size_mb = round(os.path.getsize(latest_path) / 1024 / 1024, 1)
        except Exception:
            mtime, size_mb = None, None
        files.append({
            "file": fname, "label": label,
            "last_backup_at": mtime.strftime("%Y-%m-%d %H:%M:%S") if mtime else None,
            "age_hours": round((_dh_dt.now() - mtime).total_seconds() / 3600, 1) if mtime else None,
            "size_mb": size_mb,
            "versions_kept": len(matches),
            "latest_filename": latest,
        })

    return jsonify({"dest_dir": dest_dir, "files": files, "note": None})


_UPDATE_STATUS_LABEL = {
    "quick_update":   "⚡ Quick Update",
    "full_refresh":   "⟳ Full Refresh",
    "financials_sync": "🔄 อัพเดทงบการเงิน (update_financials.py)",
    "mirror_finnomena": "📥 Mirror US/HK ทั้งตลาด (mirror_finnomena.py)",
    "us_index_full_refresh": "📈 ดึงราคา US Index ย้อนหลังสูงสุด",
    "hk_index_full_refresh": "📈 ดึงราคา HK Index ย้อนหลังสูงสุด",
    "jp_index_full_refresh": "📈 ดึงราคา JP Index ย้อนหลังสูงสุด",
    "offsite_backup":  "🛟 สำรองไฟล์สร้างใหม่ไม่ได้นอกเครื่อง (backup_financials_offsite.py)",
    "hedge_refresh":   "🐋 อัพเดท Hedge Holdings (Dataroma)",
    "hedge_fetch_missing": "⬇️ ดึงหุ้น Hedge Holdings ที่ยังไม่มีเข้าคลัง",
}

@app.route("/api/update-status")
def update_status():
    """ผลการรันล่าสุดของแต่ละกลไกอัพเดท (สำเร็จ/ล้มเหลว) — เขียนโดย core.run_log
    ตอนจบ Quick Update/Full Refresh (ในแอป) และ update_financials.py/mirror_finnomena.py
    (สคริปต์ local) ใช้ทำ banner เตือนตอนเปิดแอปถ้ารอบล่าสุดล้มเหลว แม้ผู้ใช้ไม่ได้
    เฝ้าหน้าจอตอนรัน — ไม่ครอบคลุม GitHub Actions (ใช้ email แจ้งเตือนของ GitHub เอง
    เพราะ logs/ เป็น local-only ไม่ตามไปที่ CI runner)"""
    raw = run_log.read_status(BASE_DIR)
    failed = [{"source": k, "label": _UPDATE_STATUS_LABEL.get(k, k), **v}
              for k, v in raw.items() if not v.get("ok")]
    return jsonify({"all": raw, "failed": failed})


# หุ้นไทยเข้าใหม่/ถูกถอด — เทียบรายชื่อสดจาก SET.or.th กับ universe งบการเงินในเครื่อง
# (คู่แนวเดียวกับ _dr_diff_cache/check_dr_diff แต่ใช้กับหุ้นไทยแทน DR)
_set_universe_diff_cache: dict = {}
_SET_UNIVERSE_DIFF_TTL = 6 * 3600

@app.route("/api/set-universe-check-updates")
def set_universe_check_updates():
    """เทียบรายชื่อหุ้น SET+mai ที่ซื้อขายจริง (โหลดสดจาก SET.or.th) กับ universe
    ที่ใช้ดึงงบการเงินในเครื่อง (_financials_universe — กรอง DW/DR series/ดัชนีกลุ่ม/
    หุ้นแขวนถาวรออกแล้ว) รายงานตัวใหม่/ตัวที่หายไปเท่านั้น ไม่แก้อะไรให้อัตโนมัติ"""
    cached = _set_universe_diff_cache.get("result")
    if cached and (time.time() - _set_universe_diff_cache.get("ts", 0) < _SET_UNIVERSE_DIFF_TTL):
        return jsonify(cached)
    try:
        from set_data_fetcher import load_set_symbols
        live = load_set_symbols(BASE_DIR)
        live_map = {s["symbol"]: s for s in live}
        local_syms = set(_financials_universe())
        new_syms = sorted(set(live_map.keys()) - local_syms)
        removed_syms = sorted(local_syms - set(live_map.keys()))
        tracked_delisted = sum(1 for k in delisted_log.read_log(BASE_DIR) if k.startswith("TH:"))
        result = {
            "live_count": len(live_map), "local_count": len(local_syms),
            "new": [{"symbol": s, "name": live_map[s]["name"], "market": live_map[s]["market"],
                     "sector": live_map[s]["sector"]} for s in new_syms],
            "removed": removed_syms,
            "tracked_delisted_count": tracked_delisted,
        }
        _set_universe_diff_cache["result"] = result
        _set_universe_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# หุ้น US/HK ตัวใหม่ที่ Finnomena มีแต่ mirror ในเครื่องยังไม่มี — เทียบ /stock/list
# สดกับ FINN: namespace ใน financials.db (ไม่ดึงงบจริง แค่รายงานจำนวน — กด sync
# แยกทีหลังผ่าน /api/mirror-sync-new)
_mirror_diff_cache: dict = {}
_MIRROR_DIFF_TTL = 6 * 3600

@app.route("/api/mirror-check-updates")
def mirror_check_updates():
    cached = _mirror_diff_cache.get("result")
    if cached and (time.time() - _mirror_diff_cache.get("ts", 0) < _MIRROR_DIFF_TTL):
        return jsonify(cached)
    try:
        cands = financials_store.mirror_candidates(("US", "HK"))
        con = financials_store._connect(BASE_DIR)
        try:
            have = {r[0] for r in con.execute(
                "SELECT symbol FROM financials WHERE source='finnomena_q' AND symbol LIKE 'FINN:%'")}
        finally:
            con.close()
        new_by_ex = {"US": [], "HK": []}
        live_counts = {"US": 0, "HK": 0}
        for ex, name, sid in cands:
            live_counts[ex] += 1
            if f"FINN:{ex}:{name}" not in have:
                new_by_ex[ex].append(name)
        result = {
            "live_counts": live_counts,
            "new_counts": {ex: len(v) for ex, v in new_by_ex.items()},
            "new_samples": {ex: v[:40] for ex, v in new_by_ex.items()},
        }
        _mirror_diff_cache["result"] = result
        _mirror_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _run_mirror_sync_new():
    try:
        cached = _mirror_diff_cache.get("result") or {}
        exs = tuple(ex for ex, n in (cached.get("new_counts") or {}).items() if n > 0) or ("US", "HK")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        result = financials_store.mirror_finnomena(BASE_DIR, exchanges=exs, callback=cb, force=False)
        _mirror_diff_cache.clear()   # ตัวใหม่ถูกดึงแล้ว — เช็คครั้งหน้าต้องได้ผลใหม่
        _fin_analytics_cache.clear()
        _update(running=False, done=True,
                message=f"เสร็จแล้ว! ดึงงบได้ {result['ok']} ตัว · ไม่มีงบ {result['empty']} ตัว"
                        + (f" (ล้มเหลว {result['fail']} — ลองอีกครั้งได้)" if result["fail"] else ""))
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/mirror-sync-new", methods=["POST"])
def mirror_sync_new():
    """ดึงงบเฉพาะหุ้น US/HK ตัวใหม่ที่ /api/mirror-check-updates เจอ (ไม่ force ทั้งตลาด —
    ต่างจาก mirror_finnomena.py force ที่ยิงซ้ำทุกตัวเพื่อ refresh งวดใหม่)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่ม sync หุ้น US/HK ตัวใหม่...")
    threading.Thread(target=_run_mirror_sync_new, daemon=True).start()
    return jsonify({"ok": True})


def _mirror_yahoo_tickers_by_ex():
    """ticker ของ mirror universe US/HK ที่ 'ใช้งานได้จริง' (~5,108 ตัว — ผ่านเกณฑ์คุณภาพ
    เดียวกับที่ factor_snapshot.build_mirror_snapshot ใช้กรอง Screener+ อยู่แล้ว: มีงบ Finnomena
    >= 12 ไตรมาส + มี PE ในประวัติจริง ไม่ใช่ OTC/delisted) จัดกลุ่ม {"US":[...], "HK":[...]}
    ให้ sync_mirror_yahoo_index ใช้ตรงๆ — **ไม่ใช่** financials_store.mirror_candidates()
    ดิบ (~31,000 ตัว รวม OTC/foreign-share ขยะที่ไม่คุ้ม sync งบ Yahoo ให้เลย)"""
    return factor_snapshot.get_mirror_symbols(BASE_DIR)


def _run_mirror_yahoo_index_sync(limit=None):
    """งาน #1/#3 US/HK support (PLAN_stock_study_suite.txt) — sync งบ Yahoo annual ให้หุ้น
    mirror US/HK ทั้ง universe (~5,108 ตัว จาก mirror_candidates ไม่ใช่แค่สมาชิกดัชนีหลัก
    ~623 ตัวเหมือนเดิม) แล้ว rebuild factor_snapshot_mirror ให้ Screener+/Tearsheet/Peer
    Compare/F-Score-Z-Score เห็นข้อมูลใหม่ — limit=None (ปุ่มกดมือ "ทั้ง universe") ปล่อยรัน
    จนครบ ยาวได้เป็นชั่วโมง, limit=300 (เรียกจาก Full Refresh) resume เองทุกรอบ"""
    try:
        tickers_by_ex = _mirror_yahoo_tickers_by_ex()

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        result = financials_store.sync_mirror_yahoo_index(
            BASE_DIR, tickers_by_ex, limit=limit, callback=cb)

        _update(message="Sync งบเสร็จ — กำลัง rebuild factor snapshot mirror...")
        mirror_counts = factor_snapshot.build_mirror_snapshot(BASE_DIR, exchanges=("US", "HK"))
        _fin_analytics_cache.clear()

        _update(running=False, done=True,
                message=f"เสร็จแล้ว! ดึงงบ Yahoo ได้ {result['ok']} ตัว "
                        f"(ข้าม {result['skipped']} ที่มีอยู่แล้ว"
                        + (f", ล้มเหลว {result['fail']}" if result["fail"] else "") + ") · "
                        f"rebuild mirror snapshot: US {mirror_counts.get('US', 0)} / HK {mirror_counts.get('HK', 0)} ตัว")
    except Exception as e:
        _update(running=False, done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")


@app.route("/api/mirror-yahoo-index-sync", methods=["POST"])
def mirror_yahoo_index_sync():
    """เริ่ม sync งบ Yahoo annual ของหุ้น US/HK ทั้ง mirror universe (~5,108 ตัว, งาน US/HK
    support) — job เดียวกับระบบ progress bar เดิม (ใช้ _state/_lock ร่วมกับ /api/refresh,
    /api/mirror-sync-new) กดมือเป็นครั้งคราว ใช้เวลานาน (ชั่วโมง) ต่างจากที่ Full Refresh
    เรียกเองแบบ limit=300/รอบ"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่ม sync งบ Yahoo หุ้น US/HK ทั้ง mirror universe...")
    threading.Thread(target=_run_mirror_yahoo_index_sync, daemon=True).start()
    return jsonify({"ok": True})


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

        # อัพเดท Indices (full history) + Capital Flow ล้มได้โดยไม่ทำให้ทั้งรอบ
        # ล้ม (ราคาหุ้น SET หลักได้ไปแล้ว) แต่ต้องโผล่ในผลลัพธ์รอบนี้ ไม่งั้น
        # run_log บันทึกว่า "สำเร็จ" ทั้งที่มีส่วนล้มเงียบๆ — ผู้ใช้ไม่มีทางรู้
        warnings = []

        _update(current=98, total=100, message="อัพเดท Indices...")
        try:
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv(existing, full_refresh=True)
            _indices_cache["data"] = result
        except Exception as e:
            print(f"[FullRefresh] Indices error: {e}")
            warnings.append(f"Indices ล้มเหลว: {e}")

        # อัพเดท Capital Flow
        try:
            _fetch_flow_data()
        except Exception as e:
            print(f"[FullRefresh] Capital Flow error: {e}")
            warnings.append(f"Capital Flow ล้มเหลว: {e}")

        # Sync งบ Yahoo annual mirror US/HK ทั้ง universe (~5,108 ตัว, ขยายจากเดิมที่จำกัดแค่
        # สมาชิกดัชนีหลัก ~623 ตัว) — limit=300/รอบกัน Full Refresh ยืดยาวเกินไป resume เองรอบ
        # ถัดไปเพราะ sync_mirror_yahoo_index ข้ามตัวที่มี source='yahoo' อยู่แล้วเสมอ ไม่ rebuild
        # mirror snapshot เองตรงนี้ — รวมกับผลของ batch dividends ด้านล่าง rebuild ทีเดียว
        mirror_yahoo_changed = False
        try:
            def _myc_cb(current, total, msg):
                _update(current=80 + int(current / max(total, 1) * 8), total=100, message=msg)

            myc_result = financials_store.sync_mirror_yahoo_index(
                BASE_DIR, _mirror_yahoo_tickers_by_ex(), limit=300, callback=_myc_cb)
            mirror_yahoo_changed = bool(myc_result["ok"] or myc_result["fail"])
        except Exception as e:
            print(f"[FullRefresh] Mirror Yahoo sync error: {e}")
            warnings.append(f"Sync งบ Yahoo mirror US/HK ล้มเหลว: {e}")

        # Batch fetch ประวัติปันผล (งาน #5 เฟส B) — เดิม fetch สดเฉพาะตอนเปิดหน้า "💵 ปันผล"
        # ทีละตัวเท่านั้น ทำให้ div_cagr_5y ใน Screener+/Tearsheet ว่างเปล่าเกือบทั้ง universe
        # จำกัด limit=500/รอบกัน Full Refresh ยืดยาวเกินไป — ตัวที่เหลือ resume เองรอบถัดไป
        # เพราะ sync_dividends_batch ข้าม (symbol,market) ที่ sync ไปแล้วภายใน 30 วันเสมอ
        try:
            def _div_cb(current, total, msg):
                _update(current=90 + int(current / max(total, 1) * 8), total=100, message=msg)

            from sources import us_index_metrics, hk_index_metrics
            us_syms = [s["symbol"] for s in us_index_metrics.load_local(BASE_DIR).get("stocks", [])]
            hk_syms = [s["symbol"].replace(".HK", "") for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", [])]
            div_result = financials_store.sync_dividends_batch(
                BASE_DIR,
                {"TH": _financials_universe(), "DR": _dr_financials_universe(),
                 "US": us_syms, "HK": hk_syms},
                min_age_days=30, limit=500, callback=_div_cb)
            if div_result["ok"] or div_result["fail"] or mirror_yahoo_changed:
                _update(message="Batch fetch เสร็จ — กำลัง rebuild factor snapshot...")
                factor_snapshot.build_snapshot(BASE_DIR)
                factor_snapshot.build_mirror_snapshot(BASE_DIR, exchanges=("US", "HK"))
                _fin_analytics_cache.clear()
        except Exception as e:
            print(f"[FullRefresh] Batch dividends error: {e}")
            warnings.append(f"Batch fetch ปันผลล้มเหลว: {e}")

        final_msg = "เสร็จแล้ว!" if not warnings else "เสร็จแล้ว (มีบางส่วนล้มเหลว: " + "; ".join(warnings) + ")"
        _update(running=False, done=True, message=final_msg)
        run_log.record_run(BASE_DIR, "full_refresh", True, final_msg)

    except Exception as e:
        # ดึงข้อมูลใหม่ล้มเหลว — คืนค่าข้อมูลสำรอง
        run_log.record_run(BASE_DIR, "full_refresh", False, str(e))
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
    """เดือนล่าสุดที่มีข้อมูล P/E & P/BV จริง (ตาม Table_PE.xls ที่ import ล่าสุด) —
    ใช้เช็คฝั่ง UI ว่าข้อมูลตกรุ่นหรือยัง เดิมใช้ mtime ของไฟล์ซึ่งผิด: บนเวอร์ชันเว็บ
    ไฟล์นี้ถูก regenerate ทุกรอบ GitHub Actions (ไม่ว่า Table_PE.xls จะมีข้อมูลใหม่จริง
    หรือไม่) เลย mtime รีเซ็ตเป็น "วันนี้" ตลอด ทำให้ข้อมูลค้างหลายเดือนก็ยังดูเหมือนสด"""
    if not os.path.exists(_MARKET_STATS_FILE):
        return jsonify({"updated_at": None})
    with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
        data = json.load(f)
    pe_latest = (data.get("pe", {}).get("dates") or [None])[-1]
    pbv_latest = (data.get("pbv", {}).get("dates") or [None])[-1]
    return jsonify({"updated_at": pe_latest, "pe_date": pe_latest, "pbv_date": pbv_latest})


@app.route("/api/set-daily-valuation")
def set_daily_valuation():
    """P/E, P/BV, Div Yield, EPS รายวันของตลาด SET/mai — scrape จากหน้า overview
    ของ SET.or.th โดยตรง (server-rendered, ไม่ใช่ Finnomena) เสริมข้างๆ ค่าจาก
    Table_PE.xls/Table_PBV.xls ที่เป็นรายเดือน (ดู sources/set_daily_valuation.py)"""
    now = time.time()
    cached = _set_daily_val_cache.get("result")
    if cached and (now - _set_daily_val_cache.get("ts", 0) < _SET_DAILY_VAL_TTL):
        return jsonify(cached)
    from sources import set_daily_valuation as sdv
    result = sdv.fetch()
    if not result:
        if cached:
            return jsonify(cached)   # ดึงใหม่ล้มเหลว — ใช้ค่าเก่าไปก่อนดีกว่าไม่มีเลย
        return jsonify({"error": "ดึงข้อมูลจาก SET.or.th ไม่สำเร็จ (โครงสร้างหน้าเว็บอาจเปลี่ยน)"}), 502
    _set_daily_val_cache.update(result=result, ts=now)
    return jsonify(result)


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
    """Quadrant-change alerts ของ Rotation map — อ่านจาก rotation_state.json
    ส่งทั้งชุดหลัก (3M/1M) และชุดสัญญาณเร็ว (1M/1W, dead zone กว้างกว่า)"""
    from services.rotation import load_state, CONFIRM_DAYS, DEAD_ZONE_PCT, FAST_DEAD_ZONE_PCT
    state = load_state(BASE_DIR)

    def _pending_of(groups):
        pending = []
        for key, e in groups.items():
            p = e.get("pending")
            if p:
                gtype, name = key.split(":", 1)
                pending.append({"type": gtype, "name": name,
                                "from": e.get("confirmed"), "to": p["quadrant"],
                                "days": p["days"], "need": CONFIRM_DAYS,
                                "since": p["first_date"]})
        pending.sort(key=lambda x: -x["days"])
        return pending

    return jsonify({
        "transitions":      state.get("transitions", [])[:20],
        "pending":          _pending_of(state.get("groups", {})),
        "transitions_fast": state.get("transitions_fast", [])[:20],
        "pending_fast":     _pending_of(state.get("groups_fast", {})),
        "last_processed":   state.get("last_processed"),
        "rules": {"confirm_days": CONFIRM_DAYS, "dead_zone_pct": DEAD_ZONE_PCT,
                  "axes": "x=ret_3m, y=ret_1m"},
        "rules_fast": {"confirm_days": CONFIRM_DAYS, "dead_zone_pct": FAST_DEAD_ZONE_PCT,
                       "axes": "x=ret_1m, y=ret_1w"},
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


_us_breadth_cache: dict = {}

@app.route("/api/us-breadth")
def us_market_breadth():
    """Market Breadth รายวันของหุ้น US (% above EMA50/200) จาก us_prices.db —
    reuse services.breadth.compute_breadth ตัวเดียวกับหุ้นไทย, แค่สลับ store module
    รับ query param ?range=1y|3y|5y|all (default 1y) — cache แยกต่อ range ใน memory,
    clear ทั้งหมดหลัง US Index refresh/gap-update"""
    from services.breadth import RANGE_DAYS
    rng = request.args.get("range", "1y")
    if rng not in RANGE_DAYS:
        rng = "1y"
    if _us_breadth_cache.get(rng):
        return jsonify(_us_breadth_cache[rng])
    try:
        from services.breadth import compute_breadth
        from core import us_store
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng], store=us_store)
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณากด 📈 US Index Max ก่อน"}), 404
        _us_breadth_cache[rng] = data
        return jsonify(data)
    except Exception as e:
        print(f"[USBreadth] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


_hk_breadth_cache: dict = {}

@app.route("/api/hk-breadth")
def hk_market_breadth():
    """Market Breadth รายวันของหุ้น HK (% above EMA50/200) จาก hk_prices.db —
    reuse services.breadth.compute_breadth ตัวเดียวกับหุ้นไทย/US, แค่สลับ store module
    รับ query param ?range=1y|3y|5y|all (default 1y) — cache แยกต่อ range ใน memory,
    clear ทั้งหมดหลัง HK Index refresh/gap-update"""
    from services.breadth import RANGE_DAYS
    rng = request.args.get("range", "1y")
    if rng not in RANGE_DAYS:
        rng = "1y"
    if _hk_breadth_cache.get(rng):
        return jsonify(_hk_breadth_cache[rng])
    try:
        from services.breadth import compute_breadth
        from core import hk_store
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng], store=hk_store)
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณากด 📈 HK Index Max ก่อน"}), 404
        _hk_breadth_cache[rng] = data
        return jsonify(data)
    except Exception as e:
        print(f"[HKBreadth] {traceback.format_exc()}")
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
        # ใช้ compute_breadth (vectorized pandas — ~1-2 วิ) แทน loop เดิมที่ทำ
        # .get_loc() ทีละหุ้นทีละวัน (~1,198 หุ้น x 63 วัน ~25 วิ) และยังผูก
        # calendar กับหุ้นตัวเดียวที่อาจถูกแขวน/เลิกเทรดแล้ว
        from services.breadth import compute_breadth
        breadth = compute_breadth(BASE_DIR, days=63)
        if not breadth:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณา Full Refresh ก่อน"}), 404

        result = {
            "dates":      breadth["dates"],
            "new_highs":  breadth["nh"],
            "new_lows":   breadth["nl"],
        }
        _market_internals_cache["data"] = result
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


def _run_quick():
    failed_steps = []

    def _sub_step(name, current, msg, fn):
        """รันขั้นตอนเสริม (non-critical) — ล้มแล้วไม่ทำให้ Quick Update ทั้งรอบพัง
        แต่บันทึกชื่อไว้โชว์ในข้อความสรุป + run_log กันเงียบหาย"""
        _update(current=current, total=100, message=msg)
        try:
            fn()
        except Exception as e:
            print(f"[QuickUpdate] {name} error: {e}")
            failed_steps.append(name)

    try:
        import importlib
        from services import refresh as _refresh_svc
        importlib.reload(_refresh_svc)

        # run_quick_update ใช้ตั้งแต่ 0-90% ของ progress bar เอง — ขั้นตอนเสริม
        # ด้านล่างไล่ 90-99% ต่อ ไม่งั้นแถบวิ่งถอยหลัง (99% -> 93% -> ...)
        def cb(current, total, msg):
            pct = (current / total * 90) if total > 0 else 0
            _update(current=round(pct), total=100, message=msg)

        _refresh_svc.run_quick_update(cb, BASE_DIR)
        _market_internals_cache.clear()
        _breadth_cache.clear()

        def _insider():
            from sources import sec_store as _sec_store
            _sec_store.sync_insider_trades(BASE_DIR)
            _sec_store.sync_major_changes(BASE_DIR)
        _sub_step("Insider/ผู้ถือหุ้นใหญ่", 91, "อัพเดท Insider/ผู้ถือหุ้นใหญ่...", _insider)

        def _indices():
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv(existing, full_refresh=False)
            _indices_cache["data"] = result
        _sub_step("Indices", 93, "อัพเดท Indices...", _indices)

        # อัพเดทราคา US Index (S&P500/Dow/NDX gap-update) + คำนวณ RS/EMA/Stage/52W ใหม่
        def _us_index():
            def _us_cb(current, total, msg):
                _update(message=f"US Index: {msg}")
            n_us = _run_us_index_gap_update(progress_cb=_us_cb)
            from sources import us_index_metrics
            us_index_metrics.build(BASE_DIR)
            _us_breadth_cache.clear()
            print(f"[QuickUpdate] US Index: gap-updated {n_us} ticker, metrics rebuilt")
        _sub_step("US Index", 95, "อัพเดทราคา US Index...", _us_index)

        # อัพเดทราคา HK Index (HSI/HSCEI/HSTECH gap-update) + คำนวณ RS/EMA/Stage/52W ใหม่
        def _hk_index():
            def _hk_cb(current, total, msg):
                _update(message=f"HK Index: {msg}")
            n_hk = _run_hk_index_gap_update(progress_cb=_hk_cb)
            from sources import hk_index_metrics
            hk_index_metrics.build(BASE_DIR)
            _hk_breadth_cache.clear()
            _hk_heatmap_cache.clear()
            print(f"[QuickUpdate] HK Index: gap-updated {n_hk} ticker, metrics rebuilt")
        _sub_step("HK Index", 96, "อัพเดทราคา HK Index...", _hk_index)

        # อัพเดทราคา JP Index (Nikkei 225 gap-update) + คำนวณ RS/EMA/Stage/52W ใหม่
        def _jp_index():
            def _jp_cb(current, total, msg):
                _update(message=f"JP Index: {msg}")
            n_jp = _run_jp_index_gap_update(progress_cb=_jp_cb)
            from sources import jp_index_metrics
            jp_index_metrics.build(BASE_DIR)
            print(f"[QuickUpdate] JP Index: gap-updated {n_jp} ticker, metrics rebuilt")
        _sub_step("JP Index", 97, "อัพเดทราคา JP Index...", _jp_index)

        # อัพเดท short sales + NVDR ประจำวัน
        _sub_step("Short Sales", 97, "อัพเดท Short Sales...", short_sales_daily_update)
        _sub_step("NVDR", 98, "อัพเดท NVDR...", nvdr_daily_update)

        # อัพเดท Capital Flow
        _sub_step("Capital Flow", 99, "อัพเดท Capital Flow...", _fetch_flow_data)

        # อัพเดท S50 Futures Flow — TFEX ให้แค่ "วันล่าสุดวันเดียว" ไม่มี API ประวัติ
        # ต้องสะสมเองทุกวัน ถ้าไม่ดึงตอน Quick Update (พึ่งแค่คนเปิดหน้าเว็บ) วันที่
        # ไม่มีใครเปิดหน้าตอน TFEX ยังโชว์วันนั้นอยู่ จะหายไปถาวร (ดู _fetch_flow_s50_data)
        _sub_step("S50 Futures Flow", 99, "อัพเดท S50 Futures Flow...", _fetch_flow_s50_data)

        # อัพเดท Thai Bond Flow — ThaiBMA คืนประวัติเต็มทุกครั้ง (ไม่เสี่ยงข้อมูลหาย
        # แบบ S50) แต่เดิมพึ่งแค่คนเปิดหน้าเว็บ เลยมักค้างจนกว่าจะมีคนเข้าไปดู
        _sub_step("Bond Flow", 99, "อัพเดท Bond Flow...", _fetch_flow_bond_data)

        if failed_steps:
            summary = "Quick Update เสร็จแล้ว (⚠️ ล้มเหลว: " + ", ".join(failed_steps) + ")"
        else:
            summary = "Quick Update เสร็จแล้ว!"
        _update(running=False, done=True, message=summary)
        run_log.record_run(BASE_DIR, "quick_update", not failed_steps, summary)

    except Exception as e:
        _update(running=False, done=True, error=str(e),
                message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "quick_update", False, str(e))


# ============================================================
# SEC Insider / Major-Holder endpoints — เก็บสะสมใน sec_filings.db
# (sources/sec_store.py) sync ตอน Quick Update / auto-update cron
# ============================================================

@app.route("/api/insider-trades")
def insider_trades():
    """ผู้บริหารซื้อขายหุ้น (แบบ 59) — อ่านจากฐานข้อมูลสะสม (sec_filings.db)
    ไม่ยิง SEC สดทุกครั้งอีกต่อไป ข้อมูลใหม่เข้ามาจาก sync_insider_trades()
    (เรียกตอน Quick Update / auto-update cron) เท่านั้น"""
    from datetime import datetime as _dt

    days = int(request.args.get("days", 30))
    days = max(1, min(days, 365))

    if not sec_store.db_exists(BASE_DIR):
        # ยังไม่เคย sync เลย -> sync ครั้งแรก (ช้า ~2-3 นาที ดึงย้อนหลัง 180 วัน)
        try:
            sec_store.sync_insider_trades(BASE_DIR)
        except Exception as e:
            return jsonify({"error": f"sync ครั้งแรกล้มเหลว: {e}"}), 500

    records = sec_store.query_insider_trades(BASE_DIR, days)
    last_synced = sec_store._get_meta(BASE_DIR, "insider_last_synced_at")
    return jsonify({
        "records": records,
        "days": days,
        "from": (_dt.now() - __import__("datetime").timedelta(days=days)).strftime("%Y-%m-%d"),
        "to":   _dt.now().strftime("%Y-%m-%d"),
        "fetched_at": last_synced or "-",
    })


@app.route("/api/major-changes")
def major_changes():
    """ผู้ถือหุ้นรายใหญ่เปลี่ยนแปลง (แบบ 246-2) — อ่านจากฐานข้อมูลสะสม (sec_filings.db)"""
    from datetime import datetime as _dt

    days = int(request.args.get("days", 30))
    days = max(1, min(days, 365))

    if not sec_store.db_exists(BASE_DIR):
        try:
            sec_store.sync_major_changes(BASE_DIR)
        except Exception as e:
            return jsonify({"error": f"sync ครั้งแรกล้มเหลว: {e}"}), 500

    records = sec_store.query_major_changes(BASE_DIR, days)
    last_synced = sec_store._get_meta(BASE_DIR, "major_last_synced_at")
    return jsonify({
        "records": records,
        "days": days,
        "from": (_dt.now() - __import__("datetime").timedelta(days=days)).strftime("%Y-%m-%d"),
        "to":   _dt.now().strftime("%Y-%m-%d"),
        "fetched_at": last_synced or "-",
    })


@app.route("/api/insider-sync", methods=["POST"])
def insider_sync():
    """sync ฐานข้อมูลสะสม SEC filings แบบ manual (เรียกจากปุ่มในหน้า Insider)"""
    try:
        n1 = sec_store.sync_insider_trades(BASE_DIR)
        n2 = sec_store.sync_major_changes(BASE_DIR)
        return jsonify({"ok": True, "insider_fetched": n1, "major_fetched": n2})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
            raise ValueError(
                f"SET API ไม่คืน tradingBeginDate (อาจถูกบล็อค/รูปแบบ response เปลี่ยน) — "
                f"response keys: {list(resp.keys())}"
            )

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
        # ก.ค. 2026: SET เริ่มจำกัดข้อมูลย้อนหลังไว้ ~180 วันจากวันปัจจุบัน (เกินตอบ
        # 400 "Invalid selected period") — ครึ่งปีแรกยัง rebuild ทั้งปีได้ครั้งเดียว
        # แต่ตั้งแต่ ก.ค. ต้นปีหลุดหน้าต่างไปแล้ว ต้องสะสมเพิ่มทีละช่วงแทน:
        # ขอเฉพาะ (period_to เดิม + 1วัน -> วันล่าสุด) แล้วบวกยอดเข้ากับของเดิม
        try:
            from datetime import date as _dd, timedelta as _tdel
            end_d = _dd.fromisoformat(trade_date)

            def _query_range(cs, ce):
                """ยิง API ช่วง cs..ce — fromDate บางวัน API ไม่รับ (วันหยุด/นอกหน้าต่าง
                180 วัน) ขยับหน้าไปทีละวันสูงสุด 10 วัน คืน None ถ้าไม่ผ่านเลย"""
                for shift in range(10):
                    fd = cs + _tdel(days=shift)
                    if fd > ce:
                        return None
                    try:
                        purl = (BASE + "/api/set/shortsales/statistics/list"
                                + f"?fromDate={fd.strftime('%d/%m/%Y')}"
                                + f"&toDate={ce.strftime('%d/%m/%Y')}")
                        with _ur.urlopen(_ur.Request(purl, headers=hdr),
                                         context=ctx, timeout=25) as r:
                            return json.loads(r.read().decode("utf-8", "ignore"))
                    except Exception:
                        continue
                return None

            def _blank(sym):
                return stocks.setdefault(sym, {
                    "period_vol": 0, "period_local_vol": 0, "period_nvdr_vol": 0,
                    "period_value": 0, "period_pct_value": 0,
                    "short_pos": 0, "short_pos_local": 0, "short_pos_nvdr": 0,
                    "short_pos_pct": 0, "daily": [],
                })

            # (1) พยายาม rebuild ทั้งปีก่อน — แม่นสุดเพราะตั้งยอดใหม่จากศูนย์
            presp = _query_range(_dd(int(trade_date[:4]), 1, 1), end_d)
            if presp is not None and presp.get("shortSales"):
                p_from = (presp.get("tradingBeginDate") or "")[:10]
                p_to   = (presp.get("tradingEndDate") or "")[:10]
                # ล้างยอดงวดเดิมก่อน (กันค้างข้ามปี/หุ้นที่ไม่มี short ในงวดใหม่)
                for s in stocks.values():
                    s["period_vol"] = s["period_local_vol"] = s["period_nvdr_vol"] = 0
                    s["period_value"] = s["period_pct_value"] = 0
                for item in presp["shortSales"]:
                    sym = item.get("symbol")
                    if not sym:
                        continue
                    s = _blank(sym)
                    s["period_vol"]       = int(item.get("totalVolume") or 0)
                    s["period_local_vol"] = int(item.get("localVolume") or 0)
                    s["period_nvdr_vol"]  = int(item.get("nvdrVolume") or 0)
                    s["period_value"]     = round((item.get("totalValue") or 0) / 1e6, 2)
                    s["period_pct_value"] = round(item.get("percentValue") or 0, 4)
                    s["short_pos_local"]  = int(item.get("localShortPosition") or 0)
                    s["short_pos_nvdr"]   = int(item.get("nvdrShortPosition") or 0)
                data["period_from"] = p_from
                data["period_to"]   = p_to
                print(f"[short-sales] period YTD rebuild {p_from} -> {p_to}: "
                      f"{len(presp['shortSales'])} stocks")
            else:
                # (2) ต้นปีอยู่นอกหน้าต่าง 180 วันแล้ว — สะสมเพิ่มเฉพาะช่วงที่ยังไม่รวม
                old_to = (data.get("period_to") or "")
                if not old_to:
                    raise ValueError("ไม่มี period_to เดิมให้สะสมต่อ และ rebuild ทั้งปีไม่ผ่าน")
                inc_start = _dd.fromisoformat(old_to) + _tdel(days=1)
                if inc_start > end_d:
                    pass  # ยอดสะสมครอบถึงวันล่าสุดแล้ว ไม่ต้องทำอะไร
                else:
                    presp = _query_range(inc_start, end_d)
                    if presp is None:
                        raise ValueError(f"ขอช่วงเพิ่ม {inc_start} -> {end_d} ไม่สำเร็จ")
                    items = presp.get("shortSales") or []
                    new_to = (presp.get("tradingEndDate") or "")[:10]
                    for item in items:
                        sym = item.get("symbol")
                        if not sym:
                            continue
                        s = _blank(sym)
                        val_new = item.get("totalValue") or 0
                        pct_new = item.get("percentValue") or 0
                        old_val = (s.get("period_value") or 0) * 1e6
                        old_pct = s.get("period_pct_value") or 0
                        # ถอดฐานมูลค่าซื้อขาย (ตัวหารของ %) กลับจากค่าที่เก็บไว้ เพื่อรวม % ข้ามช่วง
                        turnover = (old_val / old_pct * 100) if old_pct > 0 else 0
                        if val_new and pct_new > 0:
                            turnover += val_new / pct_new * 100
                        s["period_vol"]       = (s.get("period_vol") or 0) + int(item.get("totalVolume") or 0)
                        s["period_local_vol"] = (s.get("period_local_vol") or 0) + int(item.get("localVolume") or 0)
                        s["period_nvdr_vol"]  = (s.get("period_nvdr_vol") or 0) + int(item.get("nvdrVolume") or 0)
                        total_val = old_val + val_new
                        s["period_value"]     = round(total_val / 1e6, 2)
                        s["period_pct_value"] = (round(total_val / turnover * 100, 4)
                                                 if turnover > 0 else 0)
                        s["short_pos_local"]  = int(item.get("localShortPosition") or 0)
                        s["short_pos_nvdr"]   = int(item.get("nvdrShortPosition") or 0)
                    if new_to:
                        data["period_to"] = new_to
                    print(f"[short-sales] period สะสมเพิ่ม {old_to} + ({inc_start} -> {new_to}): "
                          f"{len(items)} stocks")
        except Exception as pe:
            print(f"[short-sales] period update failed (ใช้ยอดงวดเดิมไปก่อน): {pe}")

        data["stocks"]          = stocks
        data["last_api_update"] = trade_date
        _atomic_write_json(_SHORT_DATA_FILE, data)

        global _short_data_cache, _short_data_ts
        _short_data_cache = None  # invalidate cache
        print(f"[short-sales] updated {updated} stocks ({trade_date})")

    except Exception as e:
        import traceback as _tb
        print(f"[short-sales] daily update error: {type(e).__name__}: {e}")
        print(_tb.format_exc())
        raise


# ── Capital Flow ─────────────────────────────────────────────────────────────
# แหล่งหลัก: siamchart.com (ประวัติยาวหลายปี) — แต่บล็อค IP ของ GitHub Actions
# ทำให้เวอร์ชันเว็บค้างข้อมูลเก่าถาวร (เกิดจริง: ค้างที่ 2026-07-07)
# แหล่งสำรอง: SET API ทางการ (investor type — ให้เฉพาะวันทำการล่าสุด แต่ CI ดึงได้
# เสมอเหมือน short sales) สะสมทุกแหล่งลง market_flow_data.json แล้ว commit โดย
# Actions — เว็บได้ข้อมูลใหม่ทุกรอบแม้ siamchart ล่ม
_flow_cache: dict = {}
_FLOW_CACHE_TTL = 4 * 3600
_MARKET_FLOW_FILE = os.path.join(BASE_DIR, "market_flow_data.json")


def _fetch_flow_siamchart():
    """ดึง+parse จาก siamchart.com — คืน list of rows (ไม่คำนวณ chg/ไม่แตะ cache)"""
    import urllib.request as _ur, ssl as _ssl, re as _re, ast as _ast
    ctx = _ssl._create_unverified_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://siamchart.com/",
    }
    html = None
    last_err = None
    for attempt in range(3):
        try:
            req = _ur.Request("https://siamchart.com/stock-summary/", headers=headers)
            with _ur.urlopen(req, context=ctx, timeout=20) as r:
                html = r.read().decode("utf-8", "ignore")
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(3)
    if html is None:
        raise RuntimeError(f"ดึง siamchart ไม่สำเร็จหลังลอง 3 ครั้ง: {last_err}")

    m = _re.search(r'var\s+market_data\s*=\s*(\[.*?\]);', html, _re.DOTALL)
    if not m:
        raise ValueError(f"ไม่พบ market_data ในหน้า siamchart (html {len(html)} bytes)")
    raw = _ast.literal_eval(m.group(1))

    rows = []
    for item in raw:
        if len(item) < 5:
            continue
        try:
            rows.append({
                "date":    str(item[0])[:10],
                "fund":    round(float(item[1]) if item[1] not in ('', None) else 0.0, 2),
                "foreign": round(float(item[2]) if item[2] not in ('', None) else 0.0, 2),
                "retail":  round(float(item[3]) if item[3] not in ('', None) else 0.0, 2),
                "set":     float(item[4]) if item[4] not in ('', None) else None,
            })
        except (ValueError, TypeError):
            continue
    return rows


def _fetch_flow_set_official():
    """Fallback: SET API ทางการ /api/set/market/SET/investor-type — วันทำการล่าสุดวันเดียว
    mapping ให้ตรง schema เดิม: fund = สถาบัน + บัญชีบริษัทหลักทรัพย์ (ตรงกับที่ UI ระบุ
    'กองทุน+โบรก'), retail = นักลงทุนทั่วไปในประเทศ, หน่วยล้านบาทเหมือน siamchart
    คืน row เดียว หรือ None ถ้าวันนั้นตลาดยังไม่ปิด (ตัวเลขระหว่างวันยังไหลอยู่)"""
    from sources.set_api import _bootstrap_headers, _get_json
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZoneInfo
    ctx, hdr = _bootstrap_headers()
    d = _get_json(ctx, hdr, "/api/set/market/SET/investor-type?lang=th")
    as_of = (d.get("asOfDate") or "")[:10]
    inv = {x.get("type"): x.get("netValue") for x in (d.get("investors") or [])}
    if not as_of or not inv:
        return None
    # ตลาดยังไม่ปิดของวันนั้น -> ข้าม (เก็บเฉพาะยอดสิ้นวันที่นิ่งแล้ว) — ใช้เวลาไทยเสมอ
    # ไม่ใช่เวลาเครื่อง เพราะ GitHub Actions รันด้วย UTC (เดิมใช้ _dt.now() เพี้ยนบน CI)
    now = _dt.now(_ZoneInfo("Asia/Bangkok"))
    if as_of == now.strftime("%Y-%m-%d") and (now.hour, now.minute) < (17, 45):
        return None

    def mb(k):
        return round((inv.get(k) or 0) / 1e6, 2)

    # ราคาปิด SET index ของวันนั้นจาก indices cache (ไม่มีก็ปล่อย None — แถวนี้จะถูก
    # siamchart ทับให้เองเมื่อฝั่งไหนดึง siamchart ได้)
    set_close = None
    try:
        e = (_load_indices_existing() or {}).get("^SET.BK") or {}
        set_close = dict(zip(e.get("dates", []), e.get("closes", []))).get(as_of)
    except Exception:
        pass
    return {"date": as_of, "fund": round(mb("institution") + mb("proprietary"), 2),
            "foreign": mb("foreign"), "retail": mb("individual"), "set": set_close}


def _fetch_flow_data():
    """รวมข้อมูล Capital Flow จากไฟล์สะสม + siamchart + SET API — คำนวณ chg,
    บันทึกไฟล์สะสม (atomic) และอัพเดท _flow_cache — ล้มเฉพาะเมื่อไม่มีข้อมูลเลยจริงๆ"""
    rows_by_date = {}
    try:
        with open(_MARKET_FLOW_FILE, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = {k: r0.get(k) for k in
                                                ("date", "fund", "foreign", "retail", "set")}
    except Exception:
        pass

    sources = []
    try:
        for r0 in _fetch_flow_siamchart():
            rows_by_date[r0["date"]] = r0        # siamchart เป็นแหล่งหลัก — ทับได้
        sources.append("siamchart")
    except Exception as e:
        print(f"[Flow] siamchart ไม่ได้ ({e}) — ลอง fallback SET API")
    try:
        row = _fetch_flow_set_official()
        if row and row["date"] not in rows_by_date:
            rows_by_date[row["date"]] = row      # เติมเฉพาะวันที่ยังไม่มี ไม่ทับ siamchart
            sources.append("set.or.th")
    except Exception as e:
        print(f"[Flow] SET investor-type ไม่ได้: {e}")

    if not rows_by_date:
        raise RuntimeError("ไม่มีข้อมูล Capital Flow จากทุกแหล่ง (siamchart + SET API + ไฟล์สะสม)")

    rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    for i in range(1, len(rows)):
        prev, curr = rows[i - 1].get("set"), rows[i].get("set")
        rows[i]["chg"] = round(curr - prev, 2) if prev and curr else None
    rows[0]["chg"] = None

    result = {"rows": rows, "fetched_at": time.strftime("%Y-%m-%d %H:%M"),
              "sources": sources or ["ไฟล์สะสม (ดึงใหม่ไม่สำเร็จทั้งสองแหล่ง)"]}
    if sources:   # ได้ของใหม่จริงค่อยเขียนไฟล์
        _atomic_write_json(_MARKET_FLOW_FILE,
                           {"rows": rows, "updated_at": result["fetched_at"]})
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



# ── S50 Futures flow (TFEX) ───────────────────────────────────────────────
# แหล่ง: หน้า tfex.co.th/en/market-data/historical-data/trading-by-investor-types
# (Nuxt SSR — ข้อมูลฝังอยู่ใน window.__NUXT__) ให้แค่ "วันล่าสุดวันเดียว" ต่อครั้ง
# (ไม่มี API ประวัติแบบเปิด — /api/set/tfex/... โดน Incapsula บล็อค) จึงต้องดึงทุกวัน
# แล้วสะสมเก็บเองในไฟล์ (เหมือน market_flow_data.json ของ SET)
_flow_s50_cache: dict = {}
_S50_FLOW_FILE = os.path.join(BASE_DIR, "s50_flow_data.json")


def _fetch_flow_tfex_today():
    """ดึง+parse หน้า TFEX investor-type — คืน row เดียว {date, fund, foreign, retail}
    (หน่วย: สัญญา ไม่ใช่ล้านบาท) หรือ None ถ้าหาข้อมูลไม่เจอ"""
    import urllib.request as _ur, ssl as _ssl, re as _re
    ctx = _ssl._create_unverified_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    req = _ur.Request(
        "https://www.tfex.co.th/en/market-data/historical-data/trading-by-investor-types",
        headers=headers)
    with _ur.urlopen(req, context=ctx, timeout=20) as r:
        html = r.read().decode("utf-8", "ignore")

    m = _re.search(r'stockName:"Equity Index Futures",'
                    r'institutionalInvestorBuy:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'institutionalInvestorSell:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'institutionalInvestorNet:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'foreignInvestorBuy:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'foreignInvestorSell:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'foreignInvestorNet:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'localIndividualBuy:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'localIndividualSell:(-?[\d.]+|[a-zA-Z_$]+),'
                    r'localIndividualNet:(-?[\d.]+|[a-zA-Z_$]+)', html)
    if not m:
        raise ValueError("ไม่พบ Equity Index Futures ในหน้า TFEX")

    def num(s):
        try:
            return float(s)
        except ValueError:
            return None   # ตัวแปรอ้างค่า null ของ nuxt (a/b/... ไม่ใช่ตัวเลข)

    inst_net, for_net, loc_net = num(m.group(3)), num(m.group(6)), num(m.group(9))

    dm = _re.search(r'As of (\d{1,2} \w{3} \d{4})', html)
    if not dm:
        raise ValueError("ไม่พบวันที่ (As of) ในหน้า TFEX")
    from datetime import datetime as _dt
    date = _dt.strptime(dm.group(1), "%d %b %Y").strftime("%Y-%m-%d")

    return {"date": date, "fund": inst_net, "foreign": for_net, "retail": loc_net}


_S50_GITHUB_RAW_URL = "https://raw.githubusercontent.com/crazyass669/SET-IPAD/main/s50_flow_data.json"


def _fetch_flow_s50_github_fallback():
    """ดึง s50_flow_data.json ที่ GitHub Actions commit ไว้ (cron รันวันละ 3 รอบ:
    06:00/13:10/18:30 ICT) มาใช้เติมวันที่ขาดบนเครื่อง — TFEX ให้แค่ "วันล่าสุดวันเดียว"
    ถ้าเครื่อง local ไม่ได้เปิดแอป/กด Quick Update พอดีตอน TFEX ยังโชว์วันนั้นอยู่ วันนั้น
    จะหายถาวร แต่ GitHub Actions มีโอกาสจับติดมากกว่าเพราะรันถี่กว่า 3 เท่า"""
    import urllib.request as _ur, ssl as _ssl
    ctx = _ssl._create_unverified_context()
    req = _ur.Request(_S50_GITHUB_RAW_URL, headers={"User-Agent": "Mozilla/5.0"})
    with _ur.urlopen(req, context=ctx, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8", "ignore"))
    return data.get("rows") or []


def _fetch_flow_s50_data():
    """รวมข้อมูล S50 Futures flow จากไฟล์สะสม + GitHub fallback + TFEX (วันล่าสุด) — เหมือน SET flow"""
    rows_by_date = {}
    try:
        with open(_S50_FLOW_FILE, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = r0
    except Exception:
        pass

    # เติมเฉพาะวันที่ขาดบนเครื่อง (ไม่ทับของเดิม — ไฟล์ local อาจถูกแก้ไขมือไว้แล้ว)
    try:
        for r0 in _fetch_flow_s50_github_fallback():
            if r0.get("date") and r0["date"] not in rows_by_date:
                rows_by_date[r0["date"]] = r0
    except Exception as e:
        print(f"[S50 Flow] GitHub fallback ไม่ได้ ({e})")

    sources = []
    try:
        row = _fetch_flow_tfex_today()
        rows_by_date[row["date"]] = row
        sources.append("tfex.co.th")
    except Exception as e:
        print(f"[S50 Flow] TFEX ไม่ได้ ({e})")

    if not rows_by_date:
        raise RuntimeError("ไม่มีข้อมูล S50 Futures flow (TFEX ไม่ได้ + ไม่มีไฟล์สะสม)")

    rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    result = {"rows": rows, "fetched_at": time.strftime("%Y-%m-%d %H:%M"),
              "sources": sources or ["ไฟล์สะสม (ดึงใหม่ไม่สำเร็จ)"]}
    if sources:
        _atomic_write_json(_S50_FLOW_FILE, {"rows": rows, "updated_at": result["fetched_at"]})
    _flow_s50_cache["data"] = result
    _flow_s50_cache["ts"] = time.time()
    return result


@app.route("/api/market-flow-s50")
def market_flow_s50():
    """ดึงข้อมูล net position รายวัน (สัญญา) จาก TFEX — สถาบัน/ต่างชาติ/ในประเทศ"""
    now = time.time()
    if _flow_s50_cache.get("data") and now - _flow_s50_cache.get("ts", 0) < _FLOW_CACHE_TTL:
        return jsonify(_flow_s50_cache["data"])
    try:
        return jsonify(_fetch_flow_s50_data())
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── Thai Bond flow (ThaiBMA) ──────────────────────────────────────────────
# แหล่ง: thaibma.or.th/nrdaily/GetNR/ — JSON เปิด ไม่ต้อง auth คืนประวัติเต็ม
# (ต่างจาก SET/S50 ที่ต้องสะสมเอง) หน่วย: ล้านบาท
_flow_bond_cache: dict = {}
_BOND_FLOW_FILE = os.path.join(BASE_DIR, "bond_flow_data.json")


def _fetch_flow_bond_data():
    """รวมข้อมูล Thai Bond flow จากไฟล์สะสม + ThaiBMA (merge by date เหมือน SET/S50 flow —
    เดิมเขียนทับไฟล์ทั้งไฟล์ด้วยผลลัพธ์ ThaiBMA ตรงๆ ถ้าวันไหน ThaiBMA ส่งประวัติสั้นลง
    (เช่นเหลือแค่ YTD) ข้อมูลย้อนหลังที่สะสมไว้จะหายถาวร — ตอนนี้ merge เข้าไฟล์เดิมแทน)"""
    import urllib.request as _ur, ssl as _ssl
    ctx = _ssl._create_unverified_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.thaibma.or.th/EN/Market/NR/NRDaily.aspx",
        "X-Requested-With": "XMLHttpRequest",
    }
    rows_by_date = {}
    try:
        with open(_BOND_FLOW_FILE, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = r0
    except Exception:
        pass

    sources = []
    try:
        req = _ur.Request("https://www.thaibma.or.th/nrdaily/GetNR/", headers=headers)
        with _ur.urlopen(req, context=ctx, timeout=20) as r:
            raw = json.loads(r.read().decode("utf-8", "ignore"))
        for item in raw:
            date = str(item.get("Asof") or "")[:10]
            net = item.get("NetFlow")
            if date and net is not None:
                rows_by_date[date] = {"date": date, "foreign": round(float(net), 2)}
        sources.append("thaibma.or.th")
    except Exception as e:
        print(f"[Bond Flow] ThaiBMA ไม่ได้ ({e}) — ใช้ไฟล์สะสม")

    if not rows_by_date:
        raise RuntimeError("ไม่มีข้อมูล Thai Bond flow (ThaiBMA ไม่ได้ + ไม่มีไฟล์สะสม)")

    rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    result = {"rows": rows, "fetched_at": time.strftime("%Y-%m-%d %H:%M"),
              "sources": sources or ["ไฟล์สะสม (ดึงใหม่ไม่สำเร็จ)"]}
    if sources:
        _atomic_write_json(_BOND_FLOW_FILE, {"rows": rows, "updated_at": result["fetched_at"]})
    _flow_bond_cache["data"] = result
    _flow_bond_cache["ts"] = time.time()
    return result


@app.route("/api/market-flow-bond")
def market_flow_bond():
    """ดึงข้อมูล net buy/sell รายวัน (ล้านบาท) ของต่างชาติ (NR) ในตลาดตราสารหนี้ไทย จาก ThaiBMA"""
    now = time.time()
    if _flow_bond_cache.get("data") and now - _flow_bond_cache.get("ts", 0) < _FLOW_CACHE_TTL:
        return jsonify(_flow_bond_cache["data"])
    try:
        return jsonify(_fetch_flow_bond_data())
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
            # 21 snapshots ล่าสุดแบบ compact [date, short_pos, short_pos_pct] —
            # ให้เวอร์ชันเว็บ (ที่ไม่มี endpoint /api/short-sales/<sym>) วาด trend
            # chart ใน detail panel ได้ (pattern เดียวกับ /api/nvdr daily_tail)
            "daily_tail":       [[d.get("date"), d.get("short_pos"), d.get("short_pos_pct")]
                                 for d in daily[-21:]],
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
            raise ValueError(
                f"SET API ไม่คืนข้อมูล NVDR (อาจถูกบล็อค/รูปแบบ response เปลี่ยน) — "
                f"trade_date={trade_date!r}, items={len(items) if items else 0}"
            )

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
        import traceback as _tb
        print(f"[nvdr] update error: {type(e).__name__}: {e}")
        print(_tb.format_exc())
        raise


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

    # US Index (S&P500/Dow/NDX) stocks from us_index_metrics.json (ราคา USD)
    from sources import us_index_metrics
    for s in us_index_metrics.load_local(BASE_DIR).get("stocks", []):
        if s.get("symbol") and s.get("price") is not None:
            prices["US:" + s["symbol"]] = s["price"]

    # HK Index (HSI/HSCEI/HSTECH) stocks from hk_index_metrics.json (ราคา HKD)
    from sources import hk_index_metrics
    for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", []):
        if s.get("symbol") and s.get("price") is not None:
            prices["HK:" + s["symbol"]] = s["price"]

    if not prices:
        return jsonify({"error": "no data"}), 404
    return jsonify({"prices": prices, "updated_at": updated_at})


_flow_signals_cache: dict = {"result": None, "ts": 0}
_FLOW_SIGNALS_TTL = 3600   # ข้อมูล short/nvdr/insider อัพเดตวันละครั้ง


@app.route("/api/flow-signals")
def flow_signals_endpoint():
    """รวมสัญญาณเงินทุน 3 ชั้น (insider + short + NVDR) ต่อหุ้น เรียงตาม confluence score
    — ข้อมูลสาธารณะ (SET + SEC) cache 1 ชม."""
    from sources import flow_signals
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    now = time.time()
    if _flow_signals_cache["result"] and (now - _flow_signals_cache["ts"] < _FLOW_SIGNALS_TTL):
        return jsonify(_flow_signals_cache["result"])
    rows = flow_signals.build_flow_signals(BASE_DIR)
    result = {"stocks": rows, "count": len(rows),
              "generated_at": _dt.now(_tz(_td(hours=7))).strftime("%Y-%m-%d %H:%M")}
    _flow_signals_cache["result"] = result
    _flow_signals_cache["ts"] = now
    return jsonify(result)


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

    # อุ่น cache ของ endpoint ที่คำนวณหนักไว้ล่วงหน้าใน background — เดิมผู้ใช้
    # ที่คลิกเมนูพวกนี้เป็น "คนแรก" หลังเปิด server ต้องรอคำนวณสด (วัดจริง:
    # financials-analytics ~15 วิ, market-internals ~10 วิ, breadth ~6 วิ)
    # ทุกตัว cache ในหน่วยความจำอยู่แล้ว (breadth/internals ไม่หมดอายุจนกว่า
    # จะ refresh ข้อมูล, analytics 24 ชม.) — อุ่นครั้งเดียวตอนเปิดก็เร็วทั้งวัน
    # หมายเหตุ: อยู่ใต้ __main__ เท่านั้น — run_static_update.py ที่ import app
    # ไป bake จะไม่สั่งอุ่นซ้ำ (มันเรียก endpoint เองอยู่แล้ว)
    def _warmup_caches():
        import time as _t
        _t.sleep(3)   # ให้ server เปิดพอร์ตเสร็จก่อน ค่อยเริ่มงานเบื้องหลัง
        tc = app.test_client()
        for ep in ("/api/market-flow", "/api/breadth?range=1y",
                   "/api/market-internals", "/api/financials-analytics",
                   "/api/us-breadth?range=1y", "/api/hk-breadth?range=1y"):
            try:
                t0 = _t.time()
                tc.get(ep)
                print(f"[Warmup] {ep} พร้อม ({_t.time() - t0:.0f} วิ)", flush=True)
            except Exception as e:
                print(f"[Warmup] {ep} ล้มเหลว (ไม่กระทบการใช้งาน): {e}", flush=True)

    threading.Thread(target=_warmup_caches, daemon=True).start()

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
