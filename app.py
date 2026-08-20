"""
SET Dashboard — Flask Web Server
รัน: python app.py
หรือดับเบิ้ลคลิก start.bat
"""

import json
import math
import os
import random
import re
import secrets
import shutil
import string
import subprocess
import threading
import time
import traceback
import sys
import socket
from collections import OrderedDict

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
from core import index_drift
from core.net import ssl_context


class _LRUCache(OrderedDict):
    """dict จำกัดขนาด — ใช้กับ cache ที่ key มาจาก URL/symbol ตรงๆ (ผู้ใช้ยิงคำขอ
    symbol แปลกๆ ซ้ำได้ไม่จำกัด) กันโตไม่มีเพดานจนกิน memory ยาว ๆ เกิน TTL ไม่ช่วย
    เพราะ entry ใหม่มาเรื่อย ๆ เร็วกว่าของเก่าหมดอายุ"""

    def __init__(self, maxsize):
        super().__init__()
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            self.popitem(last=False)


# Band cache — เก็บผล mrlikestock.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_band_cache = _LRUCache(2000)
_BAND_CACHE_TTL = 6 * 3600

# NP data cache — เก็บผลกำไรสุทธิรายไตรมาสจาก 9hoon.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_npdata_cache = _LRUCache(2000)
_NPDATA_CACHE_TTL = 6 * 3600

# DR cache — เก็บราคา underlying foreign stocks ไว้ 4 ชั่วโมง
_dr_cache: dict = {}
_DR_CACHE_TTL = 4 * 3600

# DR diff-check cache — ผล /api/set/dr/list เทียบ _DR_STATIC ไว้ 6 ชั่วโมง (กดเช็คบ่อยไม่ต้องยิง SET.or.th ซ้ำ)
_dr_diff_cache: dict = {}
_DR_DIFF_CACHE_TTL = 6 * 3600

# ETF cache — เก็บราคา+metadata ETF ที่จดทะเบียนบน SET โดยตรงไว้ 2 ชั่วโมง (universe
# เล็กแค่ ~13 ตัว rebuild เร็ว ไม่ต้องมี quick-update แบบ delta-merge เหมือน DR)
_etf_cache: dict = {}
_ETF_CACHE_TTL = 2 * 3600

# US index (S&P500/Dow/Nasdaq100) diff-check cache — เทียบ Wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_us_index_diff_cache: dict = {}
_US_INDEX_DIFF_CACHE_TTL = 6 * 3600

# HK index (HSI/HSCEI/HSTECH) diff-check cache — เทียบ Wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_hk_index_diff_cache: dict = {}
_HK_INDEX_DIFF_CACHE_TTL = 6 * 3600

# JP index (Nikkei 225) diff-check cache — เทียบ ja.wikipedia กับไฟล์ local ไว้ 6 ชั่วโมง
_jp_index_diff_cache: dict = {}
_JP_INDEX_DIFF_CACHE_TTL = 6 * 3600

# Heatmap US/HK/JP ไม่มี cache แยกของตัวเองอีกต่อไป — อ่านจาก <market>_index_metrics.json
# ตรงๆ (ดู /api/us-index-heatmap ฯลฯ) ซึ่ง load_local() ของแต่ละไฟล์มี mtime cache ของมันเอง
# อยู่แล้ว ไม่ต้องมี cache ซ้อน cache

# ข่าวรายหุ้น (รวม SET.or.th + Yahoo + Google News) — cache ต่อ (symbol, is_dr) 15 นาที
# ข่าวไม่ต้องสดวินาทีต่อวินาที แต่ก็ไม่ควรยิง 3 แหล่งซ้ำทุกครั้งที่พิมพ์ค้นหา
_stock_news_cache = _LRUCache(2000)
_STOCK_NEWS_CACHE_TTL = 15 * 60

# Financials cache — งบการเงิน cache 24 ชั่วโมง (ข้อมูลไม่เปลี่ยนบ่อย)
_fin_cache = _LRUCache(2000)
_FIN_CACHE_TTL = 24 * 3600

# P/E-P/BV รายวันของตลาด (scrape จากหน้า overview ของ SET.or.th) — cache 3 ชม.
# พอ (ตัวเลขอัพเดทแค่วันละครั้งหลังตลาดปิดฝั่ง SET เอง กดถี่กว่านั้นก็ได้ค่าเดิม)
_set_daily_val_cache: dict = {"result": None, "ts": 0}
_SET_DAILY_VAL_TTL = 3 * 3600

# Financials analytics cache — growth score/PEG/FCF yield ทั้งตลาด (bulk)
# event-invalidate ตอน sync งบการเงินเสร็จ + TTL 24h กันค้างข้าม restart
_fin_analytics_cache: dict = {}
_FIN_ANALYTICS_CACHE_TTL = 24 * 3600
# lock กันรันซ้อน — คำนวณสด ~15-23 วิ/ครั้ง (เปิด sqlite ทีละ symbol) ถ้าไม่มี lock
# หลายแท็บที่เปิดพร้อมกันตอน cache หมดอายุ/ว่าง (ทุกแท็บยิง /api/financials-analytics
# ตอนโหลดหน้าผ่าน loadFinAnalytics()) จะแย่งกันคำนวณซ้ำพร้อมกันหลายชุด แข่งกันเอง
# บน GIL จนแท็บหลังๆ ดูเหมือนค้าง (เจอจริงตอนเปิด 4-5 แท็บพร้อมกัน) — ใช้ pattern
# เดียวกับ _dr_rebuild_lock ของ /api/dr: แท็บแรกคำนวณ แท็บอื่นรอเฉยๆ แล้วได้ผลจาก cache
_fin_analytics_lock = threading.Lock()

# Sector compare cache — รวมรายได้/กำไร/ROE รายไตรมาสกลุ่มตาม Sector (SET) ทั้งตลาด
# (เมนู "ดัชนีกลุ่มอุตสาหกรรม SET & mai" มุมมอง "เปรียบเทียบ Sector") event-invalidate
# พร้อม _fin_analytics_cache ตอน sync งบการเงินหุ้นไทยเสร็จ + TTL 24h กันค้างข้าม restart
_sector_compare_cache: dict = {}
_SECTOR_COMPARE_CACHE_TTL = 24 * 3600
# lock แยกจาก _fin_analytics_lock โดยตั้งใจ — handler ของ endpoint นี้เรียก financials_analytics()
# ตรงๆ เพื่อ reuse ROE ต่อหุ้นที่คำนวณไว้แล้ว (ดูคอมเมนต์ที่ /api/sector-compare) ซึ่งจะไปขอ
# _fin_analytics_lock เองข้างใน — ถ้าใช้ lock ตัวเดียวกันครอบทั้งคู่จะ deadlock ทันที (Lock ไม่ reentrant)
_sector_compare_lock = threading.Lock()

# Market trend cache — แนวโน้มตลาดย้อนหลัง 20 ไตรมาส (หน้า "📈 แนวโน้มตลาด") คำนวณ ROE/Cash
# Quality เองจาก finnomena_q ไม่ได้ยืม financials_analytics() เหมือน sector-compare จึงไม่เสี่ยง
# deadlock กับ _fin_analytics_lock แต่ยังแยก cache/lock ของตัวเองเพื่อความชัดเจน
_market_trend_cache: dict = {}
_MARKET_TREND_CACHE_TTL = 24 * 3600
_market_trend_lock = threading.Lock()

# Sector trend cache — เทรนด์ย้อนหลัง 20 ไตรมาสของ sector เดียว (เปิดจาก modal รายละเอียด sector
# ใน "⚖ เปรียบเทียบ Sector") คีย์ตามชื่อ sector แยกกัน (ต่างจาก _market_trend_cache ที่มีก้อนเดียว
# ทั้งตลาด) เพราะ scope symbols ต่างกันทุก sector — TTL/invalidate pattern เดียวกับ cache อื่นๆ
# ที่พึ่งข้อมูลงบการเงิน
_sector_trend_cache: dict = {}
_SECTOR_TREND_CACHE_TTL = 24 * 3600
# lock แยกต่อ sector (ไม่ใช่ lock เดียวคุมทุก sector แบบ _sector_compare_cache/_market_trend_cache
# เพราะสองอันนั้นมีผลลัพธ์ก้อนเดียวทั้งตลาด แต่ตัวนี้คีย์ตาม sector) — เดิมเคยใช้ lock เดียวรวม แล้วเจอ
# ว่าคลิกดูหลาย sector ติดกันเร็วๆ หลัง cache ว่าง (เพิ่ง sync/restart) จะต่อคิวรอกันทั้งที่เป็นข้อมูล
# คนละ sector ไม่เกี่ยวกันเลย — เปลี่ยนเป็น lock แยกต่อ sector ผ่าน _sector_trend_locks (สร้างแบบ
# lazy ต่อคีย์) sector ต่างกันคำนวณพร้อมกันได้ ส่วน request ซ้ำ sector เดียวกันยังกันคำนวณซ้อนกันอยู่
# เหมือนเดิม (กัน thundering herd ต่อ sector หนึ่งๆ) universe sector มีจำกัด (~30 ตัว) dict นี้เลย
# ไม่มีปัญหาโตไม่หยุด
_sector_trend_locks: dict = {}
_sector_trend_locks_meta_lock = threading.Lock()


def _get_sector_trend_lock(sector):
    with _sector_trend_locks_meta_lock:
        if sector not in _sector_trend_locks:
            _sector_trend_locks[sector] = threading.Lock()
        return _sector_trend_locks[sector]


def _swr_get_or_refresh(cache, lock, ttl, compute_fn):
    """stale-while-revalidate accessor ใช้ร่วมกับ cache dict {"result":.., "ts":..} แบบ
    _sector_compare_cache/_market_trend_cache/_fin_analytics_cache[slot] — 3 ตัวนี้คำนวณสด
    ช้า (~6-13 วิ วัดจริง) แต่ TTL ยาว 24 ชม. เดิมพอ TTL หมดอายุ request แรกที่มาเจอต้องรอ
    คำนวณสดทั้งก้อนแบบ synchronous (มี cache 24 ชม. + event-invalidate ตอน sync ช่วยไว้อยู่แล้ว
    แต่ยังมีช่วงคาบเกี่ยว TTL หมดอายุเองตามเวลาที่ไม่มีใคร sync)

    - สด (อายุ < ttl): คืนค่า cache ทันที ไม่ทำอะไรเพิ่ม
    - เก่าแต่ยังมีค่าอยู่ (อายุ >= ttl): คืนค่าเก่าทันทีให้ request นี้ก่อน แล้วสั่งคำนวณใหม่ใน
      background thread (กันสั่งซ้อนด้วย flag "_revalidating" ใน cache เดียวกัน — burst ของ
      request ตอน TTL เพิ่งหมดอายุพร้อมกันจะสั่งคำนวณแค่ครั้งเดียว) ถ้าคำนวณใหม่พัง จะ log
      แล้วปล่อยให้ค่าเก่าอยู่ต่อ (ดีกว่าเดิมที่ error จะทำให้ endpoint ตอบ 500 ทันที)
    - ไม่เคยมีค่าเลย (cold — เพิ่ง restart/clear และยังไม่ถูก warmup แตะ): คำนวณ synchronous
      ใต้ lock เหมือนเดิมทุกประการ (double-checked pattern) — เพื่อไม่ให้ request แรกสุดได้
      ค่า None ไป ต่างจากเคส "เก่า" ที่มีของเก่าให้คืนก่อนได้"""
    result = cache.get("result")
    if result is not None:
        if time.time() - cache.get("ts", 0) < ttl:
            return result
        with lock:
            if not cache.get("_revalidating"):
                cache["_revalidating"] = True

                def _bg():
                    try:
                        new_result = compute_fn()
                        cache["result"] = new_result
                        cache["ts"] = time.time()
                    except Exception as e:
                        print(f"[SWR] คำนวณใหม่เบื้องหลังล้มเหลว (ใช้ค่าเก่าต่อ): {e}")
                    finally:
                        cache["_revalidating"] = False

                threading.Thread(target=_bg, daemon=True).start()
        return result

    with lock:
        result = cache.get("result")
        if result is not None and time.time() - cache.get("ts", 0) < ttl:
            return result
        result = compute_fn()
        cache["result"] = result
        cache["ts"] = time.time()
        return result


# Indices cache — ดัชนีราคากลุ่ม SET/MAI (invalidate แบบ event-driven ตอน refresh เขียนทับ ไม่ใช่ TTL)
_indices_cache: dict = {}
# กันกด Quick Update / Full Refresh ซ้อนกัน (เดิมไม่มี lock ต่างจาก endpoint งานหนักอื่นๆ)
_indices_job_lock = threading.Lock()
_indices_job_state = {"running": False}

from flask import Flask, jsonify, send_file, Response, request

# สูตรคำนวณกลาง — ห้าม copy สูตรมาวางในไฟล์นี้ ให้ import จาก core.metrics เท่านั้น
from core.metrics import calc_rs_raw, calc_ema, calc_return, calc_return_calendar

# HTTP clients / static universe — แยกไว้ที่ sources/ (Phase 2 refactor)
from sources.tradingview import INDEX_INFO, _yf_to_tv, _fetch_tv_bars
from sources.dr_universe import _DR_STATIC, is_latest_bar_stable, region_today_date, load_dr_universe, sync_dr_universe
from sources.etf_universe import fetch_etf_list_live
from sources import sec_store
from sources import financials_store
from sources import factor_snapshot
from sources import dcf_screener
from sources import dr_descriptions
from sources import us_index_membership
from sources import hk_index_membership


BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
# พอร์ตที่ server ตัวนี้ bind (ดู __main__ ท้ายไฟล์) — ยกมาเป็น module-level constant
# ให้ /api/kill-duplicate-servers อ้างอิงพอร์ตเดียวกันได้โดยไม่ต้อง hardcode ซ้ำ 2 จุด
SERVER_PORT  = 5001
DATA_FILE    = os.path.join(BASE_DIR, "set_data.json")
BACKUP_FILE  = os.path.join(BASE_DIR, "set_data_backup.json")
HTML_FILE    = os.path.join(BASE_DIR, "set_dashboard.html")
HISTORY_FILE = os.path.join(BASE_DIR, "set_history.json")
DR_CACHE_FILE = os.path.join(BASE_DIR, "dr_cache.json")
ETF_CACHE_FILE = os.path.join(BASE_DIR, "etf_cache.json")
WATCHLIST_FILE = os.path.join(BASE_DIR, "data", "watchlist.json")
PRICE_ALERTS_FILE = os.path.join(BASE_DIR, "data", "price_alerts.json")
TOKEN_FILE = os.path.join(BASE_DIR, ".dashboard_token")

# ── CSRF token: ทุก endpoint ที่ mutate (POST /api/*) ต้องแนบ header นี้มาด้วย —
# กัน third-party website ที่เปิดพร้อมกันยิง POST มาสั่ง restart/full-refresh/เขียนทับ
# ข้อมูลแบบ CSRF (LAN เปิด 0.0.0.0 ให้ iPad/มือถือเข้าได้ ไม่มี auth เดิม) token สุ่ม
# ต่อเครื่อง เก็บไฟล์ local (gitignored) ฝัง inline เข้าหน้า HTML ตอน serve (ดู index())
# เว็บอื่นอ่านค่านี้ไม่ได้เพราะไม่ได้โหลดหน้าเราจริง ๆ
def _load_or_create_token():
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            tok = f.read().strip()
            if tok:
                return tok
    except Exception:
        pass
    tok = secrets.token_urlsafe(32)
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(tok)
    except Exception:
        pass
    return tok


DASHBOARD_TOKEN = _load_or_create_token()

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
# กัน race: 2 request ยิง /api/dr-quick-update พร้อมกัน (เดิม check-then-set ไม่มี lock
# ต่างจาก _state/_indices_job_state ที่ห่อ with _lock: ครบ — เจอได้จริงตอนเปิดหลายแท็บ/
# iPad+PC พร้อมกันกด Quick Update DR ชนกันยิง Yahoo ซ้อน)
_dr_refresh_lock = threading.Lock()

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

def _load_etf_cache_from_file():
    """โหลด ETF cache จากไฟล์ตอน server เริ่มทำงาน"""
    if not os.path.exists(ETF_CACHE_FILE):
        return
    try:
        with open(ETF_CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        file_ts = os.path.getmtime(ETF_CACHE_FILE)
        _etf_cache["result"] = data
        _etf_cache["ts"] = file_ts
        print(f"[ETF] Loaded cache: {len(data.get('stocks', []))} ETFs from etf_cache.json")
    except Exception as e:
        print(f"[ETF] Failed to load cache: {e}")

def _save_etf_cache_to_file(result):
    try:
        _atomic_write_json(ETF_CACHE_FILE, result)
        print(f"[ETF] Saved cache: {len(result.get('stocks', []))} ETFs -> etf_cache.json")
    except Exception as e:
        print(f"[ETF] Failed to save cache: {e}")

# History: อ่านจาก SQLite ผ่าน core.store (point query ~6ms) — ไม่มี in-memory
# cache 434MB อีกต่อไป และไม่ต้องมี mtime invalidation (query ตรงทุกครั้ง)
from core import store as price_store

app = Flask(__name__)


@app.after_request
def _static_no_cache(resp):
    # /static (dashboard.js/css) ให้ browser revalidate ทุกครั้ง — werkzeug ใส่
    # ETag/Last-Modified ให้อยู่แล้ว จึงได้ 304 เมื่อไฟล์ไม่เปลี่ยน (ยังใช้ค่านี้ต่อแม้
    # dashboard.js/css จะเสิร์ฟผ่าน _serve_static_asset ด้านล่างแล้วก็ตาม — after_request
    # ทำงานกับทุก response ใต้ /static/ ไม่ว่ามาจาก view function ไหน)
    if request.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-cache"
    return resp


_static_asset_cache: dict = {}
_static_asset_lock = threading.Lock()


def _serve_static_asset(filename, mimetype):
    """เสิร์ฟ static asset ใหญ่ (dashboard.js/css) เอง แทน Flask static handler เริ่มต้นที่ส่ง
    raw bytes ตรงๆ ไม่บีบอัดเลย — วัดจริง dashboard.js ~1.9MB -> gzip ~455KB (ลด 76%) กระทบเวลา
    โหลดหน้าเว็บครั้งแรกผ่านเน็ตช้าโดยเฉพาะ (มือถือ/iPad) cache ทั้ง raw+gz ไว้ตาม mtime
    (pattern เดียวกับ _resolve_data_bytes ด้านล่างสำหรับ /api/data) ไม่ gzip ซ้ำทุก request

    เส้นทาง /static/<path:filename> ของ Flask ยังทำงานปกติสำหรับไฟล์อื่น (route ตรงตัวแบบนี้
    มีความจำเพาะสูงกว่า wildcard เลยถูกเลือกก่อนเฉพาะ 2 ไฟล์นี้เท่านั้น — ตรวจสอบแล้วด้วย
    test client ว่าไฟล์ static อื่นยังผ่าน handler เดิมได้ปกติ ไม่กระทบ)"""
    path = os.path.join(app.static_folder, filename)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return jsonify({"error": f"ไม่พบไฟล์ {filename}"}), 404

    with _static_asset_lock:
        cached = _static_asset_cache.get(filename)
        if not cached or cached["mtime"] != mtime:
            import gzip as _gzip
            with open(path, "rb") as f:
                raw = f.read()
            cached = {"mtime": mtime, "raw": raw, "gz": _gzip.compress(raw, compresslevel=6),
                      "etag": f'"{mtime}-{len(raw)}"'}
            _static_asset_cache[filename] = cached

    etag = cached["etag"]
    if request.headers.get("If-None-Match") == etag:
        return Response(status=304, headers={"ETag": etag})

    use_gzip = "gzip" in request.headers.get("Accept-Encoding", "").lower()
    headers = {"ETag": etag, "Vary": "Accept-Encoding"}
    if use_gzip:
        headers["Content-Encoding"] = "gzip"
    return Response(cached["gz"] if use_gzip else cached["raw"], mimetype=mimetype, headers=headers)


@app.route("/static/dashboard.js")
def static_dashboard_js():
    # mimetype ตรงกับที่ mimetypes.guess_type() ให้ (เหมือนที่ Flask static handler เดิมใช้) —
    # Werkzeug เติม "; charset=utf-8" ให้เองสำหรับ text/* อยู่แล้ว ไม่ต้องใส่ซ้ำ (ใส่ซ้ำจะได้
    # header เพี้ยนเป็น "text/javascript; charset=utf-8; charset=utf-8")
    return _serve_static_asset("dashboard.js", "text/javascript")


@app.route("/static/dashboard.css")
def static_dashboard_css():
    return _serve_static_asset("dashboard.css", "text/css")


@app.before_request
def _require_dashboard_token():
    # ทุก POST /api/* (endpoint ที่ mutate สถานะ/ไฟล์/process) ต้องแนบ token ที่ได้จาก
    # หน้า HTML จริงของเรา — กัน CSRF จากเว็บอื่นที่เปิดพร้อมกันใน browser เดียวกัน
    if request.method == "POST" and request.path.startswith("/api/"):
        if request.headers.get("X-Dashboard-Token") != DASHBOARD_TOKEN:
            return jsonify({"error": "missing/invalid dashboard token"}), 403


# โหลด DR/ETF cache จากไฟล์ตอน import (ใช้ได้ทั้ง __main__ และ WSGI)
_load_dr_cache_from_file()
_load_etf_cache_from_file()

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

# cache HTML ที่ inject token แล้วไว้ตาม mtime — กัน อ่าน+replace ไฟล์ทุก request
# (หน้านี้เอง Cache-Control ก็ no-store อยู่แล้ว นี่คือ cache ฝั่งเซิร์ฟเวอร์ ไม่ใช่ browser)
_index_html_cache = {"mtime": None, "bytes": None}


@app.route("/")
def index():
    mtime = os.path.getmtime(HTML_FILE)
    if _index_html_cache["mtime"] != mtime:
        with open(HTML_FILE, "rb") as f:
            raw = f.read()
        inject = f'<script>window.__DASH_TOKEN__={json.dumps(DASHBOARD_TOKEN)};</script>'.encode("utf-8")
        raw = raw.replace(b"<!--DASH_TOKEN-->", inject)
        _index_html_cache.update(mtime=mtime, bytes=raw)
    resp = Response(_index_html_cache["bytes"], mimetype="text/html")
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
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
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
        body = request.json
        p = body.get("period", "max") if isinstance(body, dict) else "max"
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
    """SSE endpoint — ส่ง progress ทุก 0.5 วิ

    Stall detection (ไม่ใช่ deadline ตายตัว): จับเวลาตั้งแต่ (current, total, message)
    'ไม่เปลี่ยนเลย' ไม่ใช่ตั้งแต่เปิด stream — เดิมนับจากเปิด stream ตรงๆ ทำให้งานที่รู้อยู่แล้วว่า
    ใช้เวลานานเกิน 20 นาทีโดยชอบธรรม (เช่น Mirror ทั้งตลาด force เป็นชั่วโมง, financials-update-all
    เฟส sync mirror US/HK หลายร้อยตัว) โดน error timeout ทั้งที่ backend ยังขยับ/รันต่อเนื่องอยู่จริง
    (เจอสด 2026-08-20: mirror index sync ยัง log ความคืบหน้าต่อเนื่อง แต่ SSE ยิง error ที่ 20 นาทีพอดี
    ทั้งที่ progress ไม่ได้ค้าง) — งานจริงไม่ได้หยุด แค่ SSE ตัดการรายงานผลให้ผู้ใช้เห็นก่อนเวลา"""
    def generate():
        STALL_TIMEOUT = 20 * 60   # ค้างจริง (current/total/message เดิมทุกตัว) เกินนี้ถือว่าผิดปกติ
        last_sig = None
        last_change = time.monotonic()
        while True:
            snap = _snapshot()
            yield f"data: {json.dumps(snap, ensure_ascii=False)}\n\n"
            if snap["done"] or snap["error"]:
                break
            sig = (snap.get("current"), snap.get("total"), snap.get("message"))
            now = time.monotonic()
            if sig != last_sig:
                last_sig = sig
                last_change = now
            elif now - last_change > STALL_TIMEOUT:
                yield f"data: {json.dumps({'done': True, 'error': 'timeout: ไม่มีความคืบหน้าเกิน 20 นาที'}, ensure_ascii=False)}\n\n"
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
    try:
        data = price_store.get_series(BASE_DIR, ticker)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if not data:
        return jsonify({"error": f"ไม่พบข้อมูล {symbol} — กรุณา Full Refresh ก่อน"}), 404
    return jsonify(data)


_price_analytics_cache = _LRUCache(2000)   # symbol -> (ts, result)
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
    try:
        result = price_analytics.build_for_symbol(BASE_DIR, sym + ".BK")
    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
        result = price_analytics.analyze(ohlc)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
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


@app.route("/api/npdata/<symbol>")
def get_npdata(symbol):
    """ดึงกำไรสุทธิรายไตรมาส + P/E, P/BV, มูลค่าตลาด จาก 9hoon.com สำหรับหุ้นที่ระบุ — cache 6 ชั่วโมง"""
    import requests as req, re as _re
    import lxml.html as _lh
    from datetime import datetime as _dt

    sym = symbol.upper().strip()

    cached = _npdata_cache.get(sym)
    if cached and (time.time() - cached["ts"] < _NPDATA_CACHE_TTL):
        result = dict(cached["data"])
        result["cached_at"] = cached["fetched_at"]
        return jsonify(result)

    try:
        r = req.get(
            "https://9hoon.com/aset/view_np.php",
            params={"symbol": sym},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=20,
        )
        html = r.text
        tree = _lh.fromstring(html)

        ticker_els = tree.xpath('//div[@class="ticker-text"]')
        if not ticker_els:
            return jsonify({"error": f"ไม่พบข้อมูลสำหรับ {sym} บน 9hoon.com"}), 404

        def _txt1(els):
            return els[0].text_content().strip() if els else None

        company_name  = _txt1(tree.xpath('//div[@class="company-name"]'))
        chips         = [c.text_content().strip() for c in tree.xpath('//span[contains(@class,"chip")]')]
        business_type = _txt1(tree.xpath('//div[@class="business-type"]'))

        def _stat_value(label):
            for cell in tree.xpath(f'//div[@class="stat-label" and normalize-space(text())="{label}"]'):
                parent = cell.getparent()
                vals = parent.xpath('./div[contains(@class,"stat-value")]')
                if vals:
                    return vals[0].text_content().strip()
            return None

        market_cap = _stat_value('มูลค่าตลาด')
        pe         = _stat_value('P/E')
        pbv        = _stat_value('P/BV')
        div_yield  = _stat_value('Div. Yield (12M)')

        latest_label = latest_val = None
        hero_tiles = tree.xpath('//div[contains(@class,"stat-tile") and contains(@class,"hero")]')
        if hero_tiles:
            lbl = hero_tiles[0].xpath('.//div[@class="stat-label"]')
            val = hero_tiles[0].xpath('.//div[contains(@class,"stat-value")]')
            latest_label = _txt1(lbl)
            latest_val   = _txt1(val)

        def _delta(label_contains):
            tiles = tree.xpath(
                f'//div[contains(@class,"delta-tile")]'
                f'[.//div[@class="stat-label" and contains(text(),"{label_contains}")]]'
            )
            if not tiles:
                return None
            val = tiles[0].xpath('.//div[contains(@class,"stat-value")]')
            sub = tiles[0].xpath('.//div[@class="stat-sub"]')
            return {"value": _txt1(val), "sub": _txt1(sub)}

        yoy = _delta('YoY')
        qoq = _delta('QoQ')

        # ตารางกำไรสุทธิรายปี/ไตรมาส (พร้อม EPS ต่อไตรมาส)
        def _cell(td):
            eps_divs = td.xpath('.//div[@class="eps-sub"]')
            eps_text = _txt1(eps_divs)
            eps = eps_text.strip('[] ').strip() if eps_text else None
            val_text = ' '.join(t.strip() for t in td.xpath('./text()') if t.strip())
            val = None if (not val_text or val_text == '-') else val_text
            return {"val": val, "eps": eps}

        table = []
        for tr in tree.xpath('//table//tbody/tr'):
            tds = tr.xpath('./td')
            if len(tds) < 6:
                continue
            table.append({
                "year":  tds[0].text_content().strip(),
                "q1":    _cell(tds[1]), "q2": _cell(tds[2]),
                "q3":    _cell(tds[3]), "q4": _cell(tds[4]),
                "total": _cell(tds[5]),
            })

        # ข้อมูลกราฟรายไตรมาสเทียบปี (ดึงจาก google.visualization.DataTable ที่ฝังในหน้า)
        chart_years = _re.findall(r"addColumn\('number',\s*'(\d{4})'\)", html)
        chart = {"years": chart_years, "quarters": []}
        rows_m = _re.search(r'data\.addRows\(\[(.*?)\]\);', html, _re.DOTALL)
        if rows_m:
            for qm in _re.finditer(r'\["(Q\d)",([^\]]*)\]', rows_m.group(1)):
                vals = [(float(v) if v.strip() != 'null' else None) for v in qm.group(2).split(',')]
                chart["quarters"].append({"q": qm.group(1), "vals": vals})

        result = {
            "symbol": sym,
            "company_name": company_name,
            "chips": chips,
            "business_type": business_type,
            "market_cap": market_cap,
            "pe": pe,
            "pbv": pbv,
            "div_yield": div_yield,
            "latest_label": latest_label,
            "latest_val": latest_val,
            "yoy": yoy,
            "qoq": qoq,
            "table": table,
            "chart": chart,
        }
        fetched_at = _dt.now().strftime("%H:%M น.")
        _npdata_cache[sym] = {"ts": time.time(), "fetched_at": fetched_at, "data": result}
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
           "ts": result.get("ts"),
           "warnings": result.get("warnings") or []}
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

    from sources.yahoo import _TimeoutSession

    # session เดียวใช้ร่วมกันทุก thread + บังคับ timeout ต่อ request — เดิม yf.download()
    # ก้อนนี้ไม่มี session เลย เป็นจุดเดียวใน DR pipeline ที่หลุด pattern _TimeoutSession
    # ของโปรเจกต์ ถ้า Yahoo ค้าง socket กลางทาง thread จะค้างไม่มีกำหนด ยึด
    # _dr_rebuild_lock ตลอดไป ซึ่ง /api/job-reset ตั้งใจไม่แตะ (ดูคอมเมนต์ที่นั่น) ต้อง
    # restart ทั้งเซิร์ฟเวอร์ถึงจะกู้คืนได้ — ดู sources/yahoo.py: REQUEST_TIMEOUT
    raw = yf.download(
        yf_tickers,
        period="max",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
        session=_TimeoutSession(),
    )

    # market cap: เดิมยิง fast_info ทีละตัวแบบ sequential ~283 รอบในลูปด้านล่าง
    # (~1-2 นาที — ตัวการหลักที่ทำให้โหลดหน้า DR ครั้งแรกช้า) — เปลี่ยนเป็นขนาน
    # 12 threads เหลือ ~15 วิ และถ้าดึงพลาดใช้ค่ารอบก่อนจาก cache แทน
    # (market cap เปลี่ยนช้า ค่าเก่าอายุไม่กี่ชั่วโมงใช้แทนได้สบาย)
    from concurrent.futures import ThreadPoolExecutor

    _mc_session = _TimeoutSession()

    def _mc_one(t):
        try:
            v = getattr(yf.Ticker(t, session=_mc_session).fast_info, "market_cap", None)
            return t, (float(v) if v else None), None
        except Exception as e:
            return t, None, str(e)

    mkt_map = {}
    mkt_cap_failed = []   # ticker ที่ fetch พลาดจริง (ต่าง จาก "ไม่มี market cap" ที่ v=None ปกติ)
    try:
        with ThreadPoolExecutor(max_workers=12) as _mc_ex:
            for t, v, err in _mc_ex.map(_mc_one, yf_tickers):
                mkt_map[t] = v
                if err is not None:
                    mkt_cap_failed.append(t)
    except Exception as e:
        print(f"[DR] market cap batch ล้มเหลว (ใช้ค่าเก่าจาก cache): {e}")
        mkt_cap_failed = list(yf_tickers)
    if mkt_cap_failed:
        # เดิม fallback เงียบๆ ใช้ค่าเก่าโดยไม่บอกใคร — print ให้เห็นใน server log อย่างน้อย
        # + ส่งสรุปกลับใน result["warnings"] ให้ UI แจ้งผู้ใช้ด้วย (ดูท้ายฟังก์ชัน)
        print(f"[DR] market cap ดึงไม่สำเร็จ {len(mkt_cap_failed)}/{len(yf_tickers)} ตัว (ใช้ค่าเก่าแทน): "
              + ", ".join(mkt_cap_failed[:15]) + (" ..." if len(mkt_cap_failed) > 15 else ""))
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
    price_failed = []   # sym ที่ดึงราคาไม่สำเร็จ/ข้อมูลไม่พอ — เก็บไว้แจ้งใน result["warnings"]
    for stock in _dr_universe:
        yticker = stock["yf"]
        try:
            close  = _series(yticker, "Close")
            open_s = _series(yticker, "Open")
            high_s = _series(yticker, "High")
            low_s  = _series(yticker, "Low")
            vol_s  = _series(yticker, "Volume")
            if len(close) < 2:
                price_failed.append(stock["sym"])
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
                        price_failed.append(stock["sym"])
                        continue

            price = float(close.iloc[-1])
            prev  = float(close.iloc[-2])
            chg   = (price - prev) / prev * 100 if prev else 0
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
            price_failed.append(stock["sym"])

    # RS rank within DR universe
    valid_rs = [r for r in results if r.get("rs_raw") is not None]
    valid_rs.sort(key=lambda x: x["rs_raw"])
    n_rs = len(valid_rs)
    for i, r in enumerate(valid_rs):
        r["rs_score"] = int(round(i / n_rs * 99)) if n_rs > 0 else None

    # แจ้งผู้ใช้ว่ามีตัวไหนบ้างที่ดึงพลาด (เดิม fallback เงียบๆ ใช้ค่าเก่า ไม่มีใครรู้ว่าพัง
    # ตรงไหน) — เก็บสั้นๆ พอโชว์เป็น banner ได้ ดูรายละเอียดเต็มใน server log (print ด้านบน)
    warnings = []
    if price_failed:
        warnings.append(f"ดึงราคาไม่สำเร็จ {len(price_failed)} ตัว: "
                         + ", ".join(price_failed[:10]) + (" ..." if len(price_failed) > 10 else ""))
    if mkt_cap_failed:
        warnings.append(f"Market cap ดึงไม่สำเร็จ {len(mkt_cap_failed)} ตัว (ใช้ค่าเก่าแทน)")

    result = {"stocks": results, "ts": _dt.now().isoformat(), "warnings": warnings}
    _dr_cache.update(result=result, ts=time.time())
    _save_dr_cache_to_file(result)
    return result


# ============================================================
# ETF (SET-listed) — ต่างจาก DR ตรงที่เป็นตราสารจดทะเบียนบน SET เอง ไม่ใช่หุ้นต่างประเทศ
# universe เล็ก (~13 ตัว) ดึงรายชื่อ+metadata สดจาก SET API ทุกรอบ (ไม่ curate มือแบบ DR)
# ============================================================

def _etf_light(result, refreshing=False):
    """ตัด dates/closes เต็มออกเหมือน _dr_light — ใช้ /api/etf-history แยกตอนเปิดกราฟเต็ม"""
    out = {"stocks": [{k: v for k, v in s.items() if k not in ("dates", "closes")}
                      for s in result.get("stocks", [])],
           "ts": result.get("ts"),
           "warnings": result.get("warnings") or []}
    if refreshing:
        out["refreshing"] = True
    return out


_etf_rebuild_lock = threading.Lock()


def _kick_etf_rebuild():
    if _etf_rebuild_lock.locked():
        return

    def _bg():
        try:
            _rebuild_etf_cache()
            print("[ETF] background refresh เสร็จ")
        except Exception as e:
            print(f"[ETF] background refresh ล้มเหลว: {e}")

    threading.Thread(target=_bg, daemon=True).start()


@app.route("/api/etf")
def get_etf_data():
    """ดึงราคา+metadata ETF ที่จดทะเบียนบน SET ทั้งหมด — cache 2 ชั่วโมง
    stale-while-revalidate เหมือน /api/dr — ?fresh=1 บังคับทำสดแบบ blocking"""
    fresh = request.args.get("fresh") == "1"
    cached = _etf_cache.get("result")
    if not fresh and cached and cached.get("stocks") and _etf_cache.get("ts"):
        age = time.time() - _etf_cache["ts"]
        if age < _ETF_CACHE_TTL:
            return jsonify(_etf_light(cached))
        _kick_etf_rebuild()
        return jsonify(_etf_light(cached, refreshing=True))

    try:
        result = _rebuild_etf_cache()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(_etf_light(result))


def _rebuild_etf_cache():
    with _etf_rebuild_lock:
        if (_etf_cache.get("result") and _etf_cache.get("ts")
                and time.time() - _etf_cache["ts"] < 120):
            return _etf_cache["result"]
        return _etf_do_rebuild()


def _etf_price_update(symbols):
    """ทำให้ etf_prices.db (core/etf_store.py) เป็นปัจจุบัน — SET API เป็นหลักสำหรับ
    วันล่าสุด เหมือน Quick Update ของหุ้นไทย (services/refresh.py::run_quick_update)
    yfinance ใช้แค่ (1) backfill ประวัติยาวของ ETF ตัวใหม่ที่ยังไม่เคยมีใน DB และ
    (2) สำรองตอน SET API ใช้ไม่ได้ — ต่างจาก US/HK/JP ที่พึ่ง yfinance ล้วนเพราะไม่มี
    SET API ของตลาดต่างประเทศ"""
    import pandas as pd
    from core import etf_store
    from sources.yahoo import fetch_all_batch, fetch_gap_batch

    etf_store.init_db(BASE_DIR)
    tickers = [f"{s}.BK" for s in symbols]
    last_dates = etf_store.get_last_dates(BASE_DIR)

    # 1) ตัวที่ยังไม่เคยมีใน DB เลย (ETF ใหม่/รอบแรกที่รันหลังอัพเกรด) — backfill
    # ประวัติเต็มผ่าน yfinance ครั้งเดียว (เร็วกว่าจะรอ SET API สะสมทีละวัน)
    new_tickers = [t for t in tickers if t not in last_dates]
    if new_tickers:
        print(f"[ETF] backfill ประวัติเต็ม {len(new_tickers)} ตัวใหม่ (yfinance)...")
        try:
            backfill = fetch_all_batch(new_tickers, period="max")
        except Exception as ex:
            print(f"[ETF] backfill yfinance ล้มเหลว: {ex}")
            backfill = {}
        if backfill:
            etf_store.upsert_bars(BASE_DIR, backfill)
            print(f"[ETF] backfill สำเร็จ {len(backfill)}/{len(new_tickers)} ตัว")
        missing = [t for t in new_tickers if t not in backfill]
        if missing:
            # yfinance พลาดตัวใหม่บางตัว — สำรองผ่าน SET API chart-quotation (ได้แค่
            # ~1Y ไม่ใช่ประวัติเต็ม แต่ดีกว่าไม่มีข้อมูลเลย)
            try:
                from sources.set_api import fetch_price_history_batch
                start_date = (pd.Timestamp.now() - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
                fb = fetch_price_history_batch(missing, start_date)
                if fb:
                    etf_store.upsert_bars(BASE_DIR, fb)
                    print(f"[ETF] backfill สำรอง SET API: {len(fb)}/{len(missing)} ตัว")
            except Exception as ex:
                print(f"[ETF] backfill สำรอง SET API ล้มเหลว: {ex}")
        last_dates = etf_store.get_last_dates(BASE_DIR)

    # 2) ตัวที่มีอยู่แล้วใน DB — เติมวันใหม่ผ่าน SET API เป็นหลัก (เร็ว <1s ต่างจาก
    # yfinance ที่ทั้งช้าและปล่อยแท่งปิดหุ้นไทยช้าเป็นวันๆ — ปัญหาที่รู้จักดี)
    existing = [t for t in tickers if t in last_dates]
    if not existing:
        return

    asof = prev_trading_date = None
    try:
        from sources.set_api import fetch_trading_calendar_tail
        cal = fetch_trading_calendar_tail()
        asof = cal[-1]
        prev_trading_date = cal[-2] if len(cal) >= 2 else None
    except Exception as ex:
        print(f"[ETF] เช็คปฏิทินวันเทรด SET API ล้มเหลว ({ex}) — fallback yfinance gap-fill ทั้งหมด")

    need_update = [t for t in existing if asof and last_dates[t] < asof]
    if asof and not need_update:
        print(f"[ETF] ราคาทุกตัวเป็นปัจจุบันแล้ว (asof={asof})")
        return
    if not asof:
        need_update = existing  # เช็คปฏิทินไม่ได้ — สมมติทุกตัวอาจขาด ให้ yfinance gap-fill ตัดสินเอง

    fast_done = set()
    if asof and prev_trading_date:
        # fast path: ตัวที่ขาดพอดี 1 วันเทรด (แท่งสุดท้ายตรงกับ "วันก่อนวันล่าสุด" จริง)
        # ใช้ fetch_quotes_batch ตรงๆ (snapshot วันเดียว เร็ว <1s) เหมือน TH Quick Update
        # fast path — ปลอดภัยเฉพาะกรณีขาดแค่ 1 วัน ไม่งั้นจะเติมข้าม gap หลายวันไปเงียบๆ
        fast_tickers = [t for t in need_update if last_dates[t] == prev_trading_date]
        if fast_tickers:
            try:
                from sources.set_api import fetch_quotes_batch
                quotes = fetch_quotes_batch(fast_tickers)
                idx = pd.DatetimeIndex([pd.Timestamp(asof)])
                fast_data = {}
                for t, q in quotes.items():
                    old = etf_store.get_closes_map(BASE_DIR, t, [last_dates[t]]).get(last_dates[t])
                    # sanity: prior ต้องใกล้เคียงราคาปิดเดิมใน DB — กัน SET API คืน
                    # ข้อมูลเพี้ยน/คนละตัวลงผิด ticker
                    if old and q["prior"] is not None and abs(q["prior"] - old) / old > 0.02:
                        continue
                    fast_data[t] = {
                        "open":   pd.Series([q["open"]],   index=idx),
                        "high":   pd.Series([q["high"]],   index=idx),
                        "low":    pd.Series([q["low"]],    index=idx),
                        "close":  pd.Series([q["close"]],  index=idx),
                        "volume": pd.Series([q["volume"]], index=idx),
                    }
                if fast_data:
                    etf_store.upsert_bars(BASE_DIR, fast_data)
                    fast_done = set(fast_data.keys())
                    print(f"[ETF] SET API fast path: {len(fast_data)}/{len(fast_tickers)} ตัว (asof={asof})")
            except Exception as ex:
                print(f"[ETF] SET API fast path ล้มเหลว: {ex}")

    remaining = [t for t in need_update if t not in fast_done]
    if remaining:
        # เหลือ (ขาดเกิน 1 วันเทรด/fast path พลาด/เช็คปฏิทินไม่ได้) — yfinance gap-fill
        # เต็ม OHLC ก่อน (เผื่อ ETF ตัวไหนหยุดพักเทรดมานาน ต้องดึงย้อนหลังหลายวัน)
        start = min(last_dates[t] for t in remaining)
        try:
            gap = fetch_gap_batch(remaining, start)
        except Exception as ex:
            print(f"[ETF] yfinance gap-fill ล้มเหลว: {ex}")
            gap = {}
        if gap:
            etf_store.upsert_bars(BASE_DIR, gap)
            print(f"[ETF] yfinance gap-fill: {len(gap)}/{len(remaining)} ตัว")

    # ปิดท้าย: เช็คซ้ำว่ายังมีตัวไหนล้าหลัง asof อยู่ไหม (yfinance อาจคืนข้อมูลมาบ้างแต่
    # ยังไม่ทันวันล่าสุดจริง — เจอได้กับ ETF เทรดเบาที่ Yahoo ปล่อยแท่งช้าหลายวันติด ไม่ใช่
    # แค่วันเดียว) เติมส่วนที่เหลือผ่าน SET API chart-quotation จนกว่าจะครบ/หมดหนทาง
    if asof:
        stuck = [t for t in need_update
                 if etf_store.get_last_dates(BASE_DIR).get(t, "") < asof]
        if stuck:
            try:
                from sources.set_api import fetch_price_history_batch
                last_now = etf_store.get_last_dates(BASE_DIR)
                fb = fetch_price_history_batch(stuck, min(last_now.get(t, "1900-01-01") for t in stuck))
                if fb:
                    etf_store.upsert_bars(BASE_DIR, fb)
                    print(f"[ETF] SET API chart-quotation เติมส่วนที่ยังล้าหลัง: {len(fb)}/{len(stuck)} ตัว")
            except Exception as ex:
                print(f"[ETF] SET API chart-quotation เติมส่วนที่เหลือล้มเหลว: {ex}")


def _etf_do_rebuild():
    import pandas as pd
    from datetime import datetime as _dt
    from core import etf_store

    try:
        etf_list = fetch_etf_list_live()
    except Exception as e:
        # SET API ล่ม — fallback ใช้ metadata รอบก่อนจาก cache (ราคาจะดึงใหม่ต่อไป)
        print(f"[ETF] ดึงรายชื่อ/metadata จาก SET API ไม่สำเร็จ (ใช้ของเก่า): {e}")
        prev = (_etf_cache.get("result") or {}).get("stocks", [])
        if not prev:
            raise ValueError(f"ดึงรายชื่อ ETF ไม่สำเร็จและไม่มี cache เก่า: {e}")
        etf_list = [{k: s.get(k) for k in (
            "symbol", "name_th", "name_en", "underlying", "underlying_class", "category",
            "issuer", "mgmt_fee", "investment_policy", "div_yield", "nav", "nav_date",
            "pnav_ratio", "mkt_cap", "aum", "value_traded", "dividend", "xd_date",
            "market_maker", "is_lna")} for s in prev]

    try:
        _etf_price_update([e["symbol"] for e in etf_list])
    except Exception as ex:
        print(f"[ETF] อัพเดทราคาลง etf_prices.db ล้มเหลว: {ex} — ใช้ข้อมูลเท่าที่มีใน DB เดิม")

    results = []
    price_failed = []
    for e in etf_list:
        yticker = f"{e['symbol']}.BK"
        try:
            series = etf_store.get_ohlc_series(BASE_DIR, yticker)
            if not series or len(series["closes"]) < 2:
                price_failed.append(e["symbol"])
                continue

            idx = pd.DatetimeIndex(pd.to_datetime(series["dates"]))
            close  = pd.Series(series["closes"],  index=idx, dtype=float)
            # แท่งที่มาจาก SET API chart-quotation ล้วน (fallback/backfill สำรอง) ไม่มี
            # OHLC แยก เก็บเป็น NULL ไว้ — เติม open/high/low ที่ขาดด้วย close วันนั้นแทน
            open_s = pd.Series(series["opens"], index=idx, dtype=float).fillna(close)
            high_s = pd.Series(series["highs"], index=idx, dtype=float).fillna(close)
            low_s  = pd.Series(series["lows"],  index=idx, dtype=float).fillna(close)
            vol_s  = pd.Series(series["volumes"], index=idx, dtype=float).fillna(0)

            if len(close) < 2:
                price_failed.append(e["symbol"])
                continue

            price = float(close.iloc[-1])
            prev  = float(close.iloc[-2])
            chg   = round((price - prev) / prev * 100, 2) if prev else None

            close100 = [round(float(x), 4) for x in close.tail(100).tolist()]
            dates_all  = [str(d)[:10] for d in close.index.tolist()]
            closes_all = [round(float(x), 6) for x in close.tolist()]

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
            # มูลค่าซื้อขายเฉลี่ย 20 วัน (บาท) — ตัวชี้สภาพคล่องจริงของ ETF ไทยหลายตัวที่
            # เทรดเบามาก (เช่น ABFTH/UBOT/UHERO) แต่ผลตอบแทนสวย ถ้าไม่โชว์ผู้ใช้จะไม่รู้ว่า
            # ซื้อขายจริงแทบไม่ได้ — ประมาณจากราคาปิดล่าสุด x ปริมาณเฉลี่ย (ไม่แม่นเป๊ะเท่า
            # value รายวันจริง แต่พอเพียงบอกระดับสภาพคล่อง)
            value_avg20 = round(vol_avg20 * price) if vol_avg20 is not None else None

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

            # ETF ไทยบางตัวเทรดไม่ครบทุกวันทำการ (เช่น UBOT/UHERO เบามาก) — นับ bar offset
            # ตรงๆ แบบ calc_return จะได้หน้าต่างเวลายาวกว่าตั้งใจอย่างเงียบๆ (เจอจริง: "1M"
            # กลายเป็นย้อนหลัง 1.5 เดือน) ใช้ calc_return_calendar (อิงวันปฏิทินจริงจาก index)
            # แทน ให้เทียบกับ ETF อื่นในหน้า RRG ได้ตรงช่วงเวลาเดียวกัน
            ret_1w = calc_return_calendar(close, 7)
            ret_1m = calc_return_calendar(close, 30)
            ret_3m = calc_return_calendar(close, 91)
            ret_6m = calc_return_calendar(close, 182)
            ret_1y = calc_return_calendar(close, 365)
            ret_3y = calc_return_calendar(close, 1095)
            ret_5y = calc_return_calendar(close, 1825)

            close_52w = close.iloc[-252:] if len(close) >= 252 else close
            high_52w = round(float(close_52w.max()), 4)
            low_52w  = round(float(close_52w.min()), 4)
            ath      = round(float(close.max()), 4)
            ath_pct  = round((price - ath) / ath * 100, 2) if ath else None

            try:
                cur_year  = _dt.now().year
                close_ytd = close[close.index >= pd.Timestamp(f"{cur_year}-01-01")]
                if len(close_ytd) > 0:
                    first_ytd = float(close_ytd.iloc[0])
                    ret_ytd = round((price - first_ytd) / first_ytd * 100, 2) if first_ytd else None
                else:
                    ret_ytd = None
            except Exception:
                ret_ytd = None

            rs_raw = calc_rs_raw(ret_1m, ret_3m, ret_6m, ret_1y)

            # Premium/Discount ต่อ NAV จริง — SET ให้แค่ pnavRatio ("P/NAV เท่า" ผลหาร
            # ไม่ใช่ % ส่วนต่าง เช่น TDEX pnavRatio=1.29 แต่ premium จริง = -0.34%) คำนวณเองจาก
            # ราคาปิดล่าสุด (etf_prices.db) เทียบ nav(SET ณ nav_date, ปกติ T-1) แทน ไม่ใช้ pnavRatio ดิบ
            nav_val = e.get("nav")
            premium_pct = round((price - nav_val) / nav_val * 100, 2) if nav_val else None

            results.append({
                "symbol":  e["symbol"],
                "name_th": e.get("name_th"),
                "name_en": e.get("name_en"),
                "underlying": e.get("underlying"),
                "category": e.get("category"),
                "is_lna":  bool(e.get("is_lna")),
                "issuer":  e.get("issuer"),
                "mgmt_fee": e.get("mgmt_fee"),
                "investment_policy": e.get("investment_policy"),
                "div_yield": e.get("div_yield"),
                "nav":      nav_val,
                "nav_date": e.get("nav_date"),
                "pnav_ratio": e.get("pnav_ratio"),  # P/NAV (เท่า) ดิบจาก SET — เก็บไว้อ้างอิง ไม่ใช่ % premium
                "premium_pct": premium_pct,  # % premium/discount จริง — ใช้ตัวนี้แสดงผล/เรียง/heatmap
                "mkt_cap":  e.get("mkt_cap"),  # หน่วยจดทะเบียน x ราคา — ไม่ใช่ขนาดกองทุนจริง ใช้ aum แทน
                "aum":      e.get("aum"),  # มูลค่าทรัพย์สินสุทธิกองทุนจริง (บาท)
                "value_traded": e.get("value_traded"),  # มูลค่าซื้อขายวันล่าสุด (บาท) จาก SET
                "dividend": e.get("dividend"),  # เงินปันผลล่าสุด (บาท/หน่วย)
                "xd_date":  e.get("xd_date"),
                "market_maker": e.get("market_maker"),
                "price":    round(price, 4),
                "chg":      chg,
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
                "close100": close100,
                "ohlc30":   ohlc30,
                "dates":    dates_all,
                "closes":   closes_all,
                "above_ema50":  above_ema50,
                "above_ema200": above_ema200,
                "price_history": price_history,
                "vol_history":   vol_history,
                "vol_today":     vol_today,
                "vol_avg20":     vol_avg20,
                "value_avg20":   value_avg20,
            })
        except Exception as ex:
            print(f"[ETF] {e['symbol']}: {ex}")
            price_failed.append(e["symbol"])

    # RS rank เฉพาะกลุ่ม non-L&I (Leveraged/Inverse มี beta ผิดธรรมชาติ เทียบกันไม่ได้)
    valid_rs = [r for r in results if r.get("rs_raw") is not None and not r["is_lna"]]
    valid_rs.sort(key=lambda x: x["rs_raw"])
    n_rs = len(valid_rs)
    for i, r in enumerate(valid_rs):
        r["rs_score"] = int(round(i / n_rs * 99)) if n_rs > 0 else None

    warnings = []
    if price_failed:
        warnings.append(f"ดึงราคาไม่สำเร็จ {len(price_failed)} ตัว: " + ", ".join(price_failed))

    result = {"stocks": results, "ts": _dt.now().isoformat(), "warnings": warnings}
    _etf_cache.update(result=result, ts=time.time())
    _save_etf_cache_to_file(result)
    return result


@app.route("/api/etf-history/<symbol>")
def get_etf_history(symbol):
    """คืน dates+closes เต็ม สำหรับกราฟ full-history ใน chart modal — เสิร์ฟจาก
    _etf_cache ก่อน (ไม่ fetch ซ้ำ) fallback etf_prices.db แล้วค่อย yfinance ตรง
    เป็นทางสุดท้าย (เช่น ETF ตัวใหม่ที่ยังไม่เคยรัน rebuild เข้า DB เลย)"""
    cached = _etf_cache.get("result")
    if cached:
        for s in cached.get("stocks", []):
            if s.get("symbol") == symbol:
                return jsonify({"dates": s.get("dates", []), "closes": s.get("closes", [])})
    from core import etf_store
    series = etf_store.get_ohlc_series(BASE_DIR, f"{symbol}.BK")
    if series and series.get("closes"):
        return jsonify({"dates": series["dates"], "closes": series["closes"]})
    try:
        import yfinance as yf
        hist = yf.Ticker(f"{symbol}.BK").history(period="max", auto_adjust=True)
        if hist.empty:
            return jsonify({"error": "ไม่มีข้อมูล"}), 404
        dates  = [str(d)[:10] for d in hist.index.tolist()]
        closes = [round(float(x), 6) for x in hist["Close"].tolist()]
        return jsonify({"dates": dates, "closes": closes})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/etf-full-refresh", methods=["POST"])
def etf_full_refresh():
    """บังคับให้ /api/etf ถือว่า cache หมดอายุแล้วดึงใหม่รอบถัดไป — ตั้ง ts=0 แทนการ
    clear() ทั้งก้อน (เดิม clear() ทำลาย `prev` ที่ _etf_do_rebuild ใช้เป็น fallback ตอน
    SET API ล่ม ทำให้ raise 'ไม่มี cache เก่า' แทนที่จะ fallback ได้ตามที่ตั้งใจไว้)"""
    if _etf_cache.get("result"):
        _etf_cache["ts"] = 0
    return jsonify({"ok": True})


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
    from sources.yahoo import _TimeoutSession

    with _dr_refresh_lock:
        if _dr_refresh_state["running"]:
            return jsonify({"status": "running"})
        _dr_refresh_state["running"] = True

    cached = _dr_cache.get("result")
    if not cached or not cached.get("stocks"):
        _dr_refresh_state["running"] = False
        return jsonify({"error": "ยังไม่มี DR cache — กรุณาโหลดหน้า DR ก่อน"}), 400
    # เก็บ ts ของ cache ตอนเริ่ม — ถ้า background full rebuild (_dr_do_rebuild, ดู
    # _kick_dr_rebuild) แทนที่ _dr_cache["result"] ด้วย object ใหม่เสร็จก่อนเราเขียนจบ
    # ต้องรู้ตัวและไม่ทับมันด้วย `cached` ที่เรามัวแต่ mutate อยู่ (ของเก่ากว่า) ทิ้งไป
    _ts_at_start = _dr_cache.get("ts")

    def _do_quick():
        # running ตั้งเป็น True แล้วตอนเช็ค TOCTOU ด้านบน (atomic ใต้ _dr_refresh_lock) —
        # ที่นี่แค่ reset field อื่นของรอบใหม่
        _dr_refresh_state.update(error=None, done=False, n_total=None, n_updated=None)
        try:
            _universe = load_dr_universe(BASE_DIR)
            yf_tickers = list({s["yf"] for s in _universe})

            # คำนวณ gap จาก last date ที่บันทึกไว้ในแต่ละ DR stock
            cached_stocks = (cached or {}).get("stocks", [])
            last_dates_dr = [s["dates"][-1] for s in cached_stocks if s.get("dates")]
            if last_dates_dr:
                # ไม่ +1 วัน — ต้อง include แท่งล่าสุดที่เก็บไว้แล้วซ้ำ (overlap) ด้วย ไม่งั้น
                # Yahoo คืนมาว่างเปล่า 0 แถวเสมอเวลา start=วันนี้ (ยืนยันแล้วว่า yf.download
                # กับ start=today ไม่คืนอะไรเลยแม้ตลาดกำลังเทรดอยู่) เดิม +1 วันทำให้ start
                # กลายเป็น "วันนี้" ทุกครั้ง (เพราะ min_last_dr ค้างที่ "เมื่อวาน" เสมอ — ไม่มี
                # แท่งวันนี้ให้ขยับต่อ) → raw ว่างตลอด → ทุก ticker เข้า len(close)<2 แล้ว skip
                # ทั้งหมด → ราคา DR ไม่อัพเดทเลยไม่ว่าจะกดกี่รอบ (นี่คือสาเหตุที่ผู้ใช้รายงานว่า
                # DR/DRx กดอัพเดทแล้วข้อมูลเดิม) ดู logic เดียวกันใน _run_index_gap_update (app.py)
                # ที่ทำถูกอยู่แล้วสำหรับ Heatmap US/HK/JP
                min_last_dr = min(last_dates_dr)
                start_dr = pd.to_datetime(min_last_dr).strftime("%Y-%m-%d")
                dl_kwargs = {"start": start_dr}
                print(f"[DR quick] gap fetch from {start_dr}")
            else:
                dl_kwargs = {"period": "30d"}
                print("[DR quick] no history, fallback to 30d")

            raw = yf.download(yf_tickers, auto_adjust=True, session=_TimeoutSession(),
                              progress=False, group_by="ticker", threads=True, **dl_kwargs)
            is_multi = len(yf_tickers) > 1

            def _series(yticker, field):
                try:
                    return (raw[yticker][field] if is_multi else raw[field]).dropna()
                except (KeyError, TypeError):
                    return pd.Series(dtype=float)

            # Build lookup จาก sym → stock entry เพื่ออัปเดต
            stock_map = {s["sym"]: s for s in cached["stocks"]}
            updated = 0   # นับตัวที่อัปเดตราคาสำเร็จจริง — ให้ frontend โชว์ "สำเร็จ N/M ตัว"
                          # แบบเดียวกับปุ่ม "⚡ ราคาล่าสุด" ของ Watchlist (ดู wlRefreshLivePrices)
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

                    entry = stock_map.get(sym)
                    if len(close) < 2:
                        # gap fetch ปกติได้แค่ 2 แท่ง (เมื่อวาน+วันนี้ — overlap ที่ตั้งใจ) พอ
                        # ตัดแท่งวันนี้ทิ้งเพราะยังไม่นิ่ง (ตลาดกำลังเทรดอยู่) จะเหลือแค่ 1 แท่ง
                        # ไม่พอคำนวณ chg/ต่อ history ใหม่ — เดิม continue ทิ้งทั้งตัวเลย ทำให้
                        # live_price ที่เพิ่งดึงมาได้ (ตัวแปร live_price ด้านบน) ถูกทิ้งไปด้วย
                        # ทั้งที่ใช้ได้ ผลคือ DR เกือบทั้งกระดาน (โดยเฉพาะหุ้น US ที่ตลาดเปิดอยู่
                        # ตอนกด quick update) ไม่ได้ราคาสดเลยสักตัว — แก้โดยอัปเดตแค่ live_price/
                        # live_chg (เทียบกับราคาปิดเดิมที่ entry มีอยู่แล้ว) แทนการทิ้งทั้งหมด
                        if entry and live_price is not None:
                            entry["live_price"] = round(live_price, 2)
                            base_price = entry.get("price")
                            entry["live_chg"] = (round((live_price - base_price) / base_price * 100, 2)
                                                  if base_price else None)
                            updated += 1
                        continue

                    price = float(close.iloc[-1])
                    prev  = float(close.iloc[-2])
                    chg   = round((price - prev) / prev * 100, 2) if prev else 0
                    live_chg = round((live_price - price) / price * 100, 2) if live_price and price else None

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
                        # อัปเดต full history (+ volume คู่กัน สำหรับ _bullish_vol ฝั่ง client)
                        # gap fetch ตั้งใจดึงทับแท่งสุดท้ายเดิมซ้ำ (overlap) — นับเฉพาะแท่งที่
                        # วันที่ใหม่จริงๆ (appended_count) ไว้ใช้ merge close100/ohlc30 ต่อ
                        # ไม่งั้นแท่งซ้ำจะถูกเบิ้ลเข้า close100 ทุกครั้งที่กดอัปเดต
                        old_dates  = entry.get("dates", [])
                        old_closes = entry.get("closes", [])
                        old_vols   = entry.get("_full_vols", [0] * len(old_dates))
                        appended_count = 0
                        for dt, cl, vv in zip(new_dates_raw, new_closes_raw, new_vols_raw):
                            if not old_dates or dt > old_dates[-1]:
                                old_dates.append(dt)
                                old_closes.append(cl)
                                old_vols.append(vv)
                                appended_count += 1
                        entry["dates"]  = old_dates
                        entry["closes"] = old_closes
                        entry["_full_vols"] = old_vols
                        # อัปเดต close100 จาก full history ที่ dedupe แล้ว (ตัดปัญหาแท่งซ้ำ)
                        # ปัด 4 ตำแหน่งให้ตรงกับ _dr_do_rebuild (old_closes/closes เก็บละเอียด
                        # 6 ตำแหน่งสำหรับ full history — close100 ใช้แค่วาด sparkline คร่าวๆ)
                        entry["close100"] = [round(c, 4) for c in old_closes[-100:]]
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
                        entry["ret_3y"] = _ret_q(old_closes, 756)
                        entry["ret_5y"] = _ret_q(old_closes, 1260)
                        entry["rs_raw"] = round(rs_raw, 4) if (rs_raw := calc_rs_raw(
                            entry["ret_1m"], entry["ret_3m"], entry["ret_6m"], entry["ret_1y"])) is not None else None
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
                        # ต่อ ohlc30 เฉพาะแท่งที่ใหม่จริง (appended_count เดียวกับ close100/dates
                        # ด้านบน) — เดิมต่อแท่งที่ fetch มาทั้งหมดรวม overlap ทำให้ตัดฐานเก่า
                        # ทิ้งเกิน 1 แท่งเสมอ (แท่งวันก่อนหน้าหายไปจากกราฟแท่งเทียน 30D)
                        try:
                            if appended_count > 0:
                                n = min(appended_count, len(close))
                                new_ohlc = []
                                for i in range(-n, 0):
                                    o = float(open_s.iloc[i]) if len(open_s) >= abs(i) else price
                                    h = float(high_s.iloc[i]) if len(high_s) >= abs(i) else price
                                    l = float(low_s.iloc[i])  if len(low_s)  >= abs(i) else price
                                    c2 = float(close.iloc[i])
                                    v  = float(vol_s.iloc[i]) if len(vol_s) >= abs(i) else 0
                                    new_ohlc.append([round(o,4), round(h,4), round(l,4), round(c2,4), int(v)])
                                old_ohlc = entry.get("ohlc30", [])
                                entry["ohlc30"] = (old_ohlc + new_ohlc)[-30:]
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
                        updated += 1
                except Exception as e:
                    print(f"[DR quick] {sym}: {e}")

            # RS rank ใหม่ทั้ง universe — rs_raw ของตัวที่เพิ่ง quick-update เปลี่ยนไปแล้ว
            # (ดู rs_raw ด้านบน) แต่อันดับเป็นค่าสัมพัทธ์เทียบกับทุกตัว ต้องคำนวณใหม่ทั้งชุด
            # เหมือน _dr_do_rebuild ไม่งั้น rs_score ค้างจนกว่าจะ full rebuild รอบถัดไป (4 ชม.)
            valid_rs = [s for s in cached["stocks"] if s.get("rs_raw") is not None]
            valid_rs.sort(key=lambda x: x["rs_raw"])
            n_rs = len(valid_rs)
            for i, s in enumerate(valid_rs):
                s["rs_score"] = int(round(i / n_rs * 99)) if n_rs > 0 else None

            # ถ้า background full rebuild แทนที่ _dr_cache ไปแล้วระหว่างที่เรารันอยู่
            # (ts ไม่ตรงกับตอนเริ่ม) ผลของมันสดกว่าและครบกว่า `cached` ที่เรา mutate
            # มาตลอด — ข้ามการเขียนทับไปเลย ไม่งั้นจะเอาผล quick-update (เก่ากว่า) ไปทับ
            if _dr_cache.get("ts") != _ts_at_start:
                print("[DR quick] ข้ามการบันทึก — background full rebuild เสร็จก่อนแล้ว")
            else:
                cached["ts"] = _dt.now().isoformat()
                _dr_cache.update(result=cached, ts=time.time())
                _save_dr_cache_to_file(cached)
            _dr_refresh_state["done"] = True
            _dr_refresh_state["n_total"] = len(_universe)
            _dr_refresh_state["n_updated"] = updated
            print(f"[DR quick] updated {updated}/{len(_universe)} ticker")
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
    ใช้ progress state ร่วมกับงานยาวอื่นๆ (_state/_update/_lock — ดู /api/refresh)
    เช็ค _HM_LIVE_STATE["US"] ด้วย — กัน race กับปุ่ม "⚡ อัพเดทราคา" ของ Heatmap US ที่ใช้ล็อก
    แยกต่างหาก (_hm_live_lock) เดิมสองปุ่มนี้เขียน us_prices.db/us_index_metrics.json พร้อมกัน
    ได้โดยไม่มีใครกันใคร ทำให้ข้อมูลที่เพิ่งคำนวณเสร็จหายเงียบๆ (lost update) หรือชน SQLite lock"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        if _HM_LIVE_STATE["US"]["running"]:
            return jsonify({"error": "กำลังอัพเดทราคาสด (Heatmap US) อยู่ โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา US Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_us_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_us_index_full_refresh():
    ok = False
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
        _update(current=len(tickers) - 1, total=len(tickers),
                message=f"กำลังบันทึก {len(data)} ตัวลง us_prices.db (หลายนาที อย่าเพิ่งปิด)...")
        us_store.init_db(BASE_DIR)
        us_store.upsert_bars(BASE_DIR, data)

        missing = len(tickers) - len(data)
        msg = f"เสร็จแล้ว! ดึงราคา US Index ย้อนหลังสูงสุด {len(data)}/{len(tickers)} ตัว"
        if missing:
            msg += f" (ขาด {missing} ตัว)"
        _update(done=True, message=msg)
        run_log.record_run(BASE_DIR, "us_index_full_refresh", True, msg)
        ok = True
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "us_index_full_refresh", False, str(e))
    finally:
        if not ok:
            _update(running=False)

    if not ok:
        return

    # คำนวณ RS/EMA/Stage/52W ใหม่จากราคาที่เพิ่งดึง — ทำนอก try/except หลักเพื่อให้รันแม้
    # ราคาบางตัวพลาด (best-effort, ไม่ทำให้ job หลักถูกมองว่า error) แต่ยัง "running=True"
    # อยู่ตลอดขั้นนี้ — กันปุ่ม Index Max/Heatmap live update ตัวอื่นเริ่มเขียน us_prices.db/
    # us_index_metrics.json ซ้อนกันก่อน build() รอบนี้เสร็จ (เดิมปล่อย running=False ก่อน
    # build() ทำให้กดปุ่มซ้ำหรือกด Heatmap live ระหว่างนี้ชิงเขียนไฟล์ชนกันได้ — lost update)
    try:
        from sources import us_index_metrics
        n_metrics = us_index_metrics.build(BASE_DIR)
        _us_breadth_cache.clear()
        _bump_cache_gen()
        print(f"[US Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[US Index] metrics build error: {e}")
    finally:
        _update(running=False)


def _run_index_gap_update(membership, store, region, label, progress_cb=None, sleep_s=0.3, index_key=None):
    """ดึงเฉพาะวันที่ขาดของสมาชิกดัชนี (gap-update, เร็ว) ต่างจาก full-refresh ที่ดึง
    ย้อนหลังสูงสุดทั้งประวัติ (ใช้เฉพาะกดมือ) ใช้จาก Quick Update ประจำวัน — คืน
    (จำนวน ticker ที่อัพเดทสำเร็จ, live_map, scope ticker ทั้งหมดที่ขอรอบนี้)

    scope ticker (ค่าที่ 3) ใช้บอกขอบเขตที่ "เช็คแล้ว" รอบนี้ — index_metrics_common.
    update_live_prices ใช้แยกว่า ticker ไหนหลุดจาก live_map เพราะราคานิ่งแล้ว/ตลาดปิด (ต้อง
    เคลียร์ live_price เก่าทิ้ง) กับ ticker ที่ไม่ได้อยู่ใน scope เลย (ไม่ต้องแตะ)

    เดิมมี _run_us_index_gap_update/_run_hk_index_gap_update แยกกัน 2 ฟังก์ชัน
    เหมือนกันทุกบรรทัดยกเว้น module/region code — รวมเป็นฟังก์ชันเดียวรับ membership/
    store module + region code ("US"/"HK") + label ไว้ขึ้น log แทน

    sleep_s — เวลาพักระหว่าง batch ที่ยิง Yahoo (ส่งต่อให้ fetch_gap_batch/fetch_all_batch)
    default 0.3 วิ (เหมือนเดิม) ตัวเรียกที่ universe ใหญ่และอาจถูกกดถี่ (เช่นปุ่ม Live ของ
    Heatmap US ~518 ticker) ควรส่งค่าสูงกว่านี้กัน Yahoo rate-limit/แบน — ดู
    _run_heatmap_live_update

    index_key — ถ้าระบุ (เช่น "DOW") จะดึงเฉพาะสมาชิกของดัชนีย่อยนั้น แทนที่จะเป็น union
    ทั้งหมดของ membership (all_tickers) — ใช้ตอนผู้ใช้กดปุ่ม Live ของ Heatmap ขณะดูแค่แท็บ
    ดัชนีย่อยเดียว (เช่น Dow 30 ตัว) ไม่ต้องรอไล่ยิง Yahoo ทั้ง ~518 ตัวของ US ทุกครั้ง"""
    from sources.yahoo import fetch_all_batch, fetch_gap_batch
    from sources.dr_universe import is_latest_bar_stable, region_today_date, region_expected_trading_date
    from services.refresh import detect_ca_mismatch, _repair_ca_tickers

    if index_key:
        tickers = sorted(set(membership.load_local(BASE_DIR).get(index_key, [])))
    else:
        tickers = membership.all_tickers(BASE_DIR)
    if not tickers:
        return 0, {}, tickers

    # last_dates อาจมี ticker ที่ถูกถอดจากดัชนีไปแล้วค้างอยู่ (ไม่อัพเดทต่อ) — ถ้าเอา
    # ไปหา min ทั้งก้อนจะยิ่งลากวันเริ่มดึง (start) ให้เก่าขึ้นเรื่อยๆ ทุกวันที่ผ่านไป
    # ต้อง filter เหลือเฉพาะ ticker ในดัชนีปัจจุบันก่อน (ตัวที่ถูกถอดไม่ต้องอัพเดทอยู่แล้ว)
    last_dates_all = store.get_last_dates(BASE_DIR)
    if not last_dates_all:
        # DB ยังว่าง (เครื่องใหม่/ยังไม่เคยดึง) — ถ้าปล่อยต่อ ทุกตัวจะกลายเป็น "หุ้นใหม่"
        # แล้ว Quick Update ประจำวันแอบกลายเป็น full backfill period='max' หลายร้อยตัว
        # (งานหนักที่ตั้งใจให้กดปุ่ม Index Max เองเท่านั้น)
        print(f"[{label}] {store.DB_FILE} ยังว่าง — ข้าม gap-update (กดปุ่ม Index Max ก่อน)")
        return 0, {}, tickers
    last_dates = {t: d for t, d in last_dates_all.items() if t in set(tickers)}
    new_tickers = [t for t in tickers if t not in last_dates]

    if last_dates:
        import pandas as pd
        min_last = min(last_dates.values())
        # ไม่ +1 วัน — ต้องดึงแท่งล่าสุดที่เก็บไว้แล้วซ้ำ (overlap) ด้วย ไม่งั้นไม่มี
        # แท่งให้เทียบตรวจ split (ดู detect_ca_mismatch) เหมือน dr_quick_update
        start = pd.to_datetime(min_last).strftime("%Y-%m-%d")
        data = fetch_gap_batch(list(last_dates.keys()), start, callback=progress_cb, sleep_s=sleep_s)
    else:
        data = {}   # ไม่มี ticker เก่าเลย (DB ว่าง/รอบแรก) — new_tickers ด้านล่างครอบคลุมหมด

    # หุ้นเข้าดัชนีใหม่ (ไม่มีราคาเก่าเลย) — ต้อง backfill เต็มประวัติแยกต่างหาก ไม่งั้น
    # gap-update ปกติจะดึงแค่ไม่กี่วันล่าสุด ทำให้ RS/EMA200/52W คำนวณไม่ได้อีกนาน
    if new_tickers:
        print(f"[{label}] หุ้นเข้าดัชนีใหม่ {len(new_tickers)} ตัว — backfill เต็มประวัติ: {new_tickers}")
        data.update(fetch_all_batch(new_tickers, callback=progress_cb, period="max", sleep_s=sleep_s))

    # Split detector: เทียบแท่ง overlap ก่อนบันทึก — ถ้า Yahoo เพิ่งปรับราคาย้อนหลัง
    # (แตกพาร์ ฯลฯ) ต้อง refetch เต็มเฉพาะตัว ไม่งั้น series จะเป็นฐานเก่าต่อฐานใหม่
    # (ดู detect_ca_mismatch ใน services/refresh.py — ใช้ตัวเดียวกับหุ้นไทย)
    replace_tickers = set()
    suspects = detect_ca_mismatch(BASE_DIR, data, store=store)
    if suspects:
        print(f"[{label} CA] พบ overlap mismatch: {suspects}")
        repaired = _repair_ca_tickers(BASE_DIR, data, suspects, progress_cb or (lambda *a: None))
        # ลบของเก่าทิ้งพร้อม insert ใหม่ในทรานแซกชันเดียว (ผ่าน upsert_bars ด้านล่าง)
        # แทนที่จะลบทันทีตรงนี้แล้วค่อย upsert ทีหลัง — เดิมถ้า upsert_bars ล้มเหลว
        # กลางทาง (ข้อมูลเสีย/exception) ราคาของ ticker เหล่านี้จะหายถาวรเพราะลบไปแล้ว
        replace_tickers = set(repaired)

    # ตัดแท่งล่าสุดทิ้งถ้ายังไม่นิ่ง (ตลาดกำลังเปิด/pre-market/after-hours) — เหตุผล
    # เดียวกับ dr_quick_update (ดูคอมเมนต์ยาวตรงนั้น) timezone ต่างกันตาม region
    # ก่อนตัดทิ้ง เก็บราคาที่ยังไม่นิ่งไว้แยกเป็น live_price/live_chg (เทียบกับแท่งก่อนหน้าที่
    # กำลังจะกลายเป็นแท่งปิดล่าสุดหลังตัด) — แบบเดียวกับ dr_quick_update ให้ Heatmap US/HK/JP
    # โชว์ราคา Live ระหว่างวันได้เหมือน DR/DRx (ดู index_metrics_common.build ที่ merge
    # live_map นี้เข้า stock rows)
    live_map = {}
    if not is_latest_bar_stable(region):
        today = region_today_date(region)
        if today is not None:
            for t in list(data.keys()):
                close = data[t].get("close")
                if close is None or len(close) == 0 or close.index[-1].date() != today:
                    continue
                if len(close) >= 2:
                    live_price = float(close.iloc[-1])
                    prev_price = float(close.iloc[-2])
                    if prev_price:
                        live_map[t] = {
                            "live_price": round(live_price, 4),
                            "live_chg": round((live_price - prev_price) / prev_price * 100, 2),
                        }
                for k in ("open", "high", "low", "close", "adj_close", "volume"):
                    s = data[t].get(k)
                    if s is not None and len(s):
                        data[t][k] = s.iloc[:-1]
                if len(data[t]["close"]) == 0:
                    del data[t]
    else:
        # ตลาดปิดไปแล้วจริง (พ้นช่วง buffer) แต่บางครั้ง Yahoo ยังไม่เติมช่อง Close ของแท่ง
        # รายวันล่าสุดให้ (เจอจริง 28 ก.ค. 2026 — หุ้นญี่ปุ่นทั้งกระดาน 225 ตัว มี Open/High/
        # Low/Volume ครบ แต่ Close เป็น NaN ทั้งวันแม้ผ่านมา 10+ ชม. — fetch_gap_batch/
        # _extract_ohlcav (sources/yahoo.py) dropna() ทิ้งแท่งนั้นไปทั้งแท่ง) ราคาปิดทางการ
        # ยังไม่มา แต่ Yahoo มีข้อมูลเทรดจริงรายนาที (ยืนยันด้วย yf.Ticker().history(interval=
        # '1m') ตอน debug) — กู้ราคาล่าสุดจาก intraday 1m มาโชว์เป็น live_price แทน (ไม่บันทึก
        # ลง DB ถาวรเพราะอาจไม่ตรงเป๊ะกับตัวเลขทางการที่ Yahoo จะเติมย้อนหลังทีหลัง)
        expected = region_expected_trading_date(region)
        if expected is not None:
            stale = []
            for t in data:
                close = data[t].get("close")
                last_date = close.index[-1].date() if close is not None and len(close) else None
                if last_date is None or last_date < expected:
                    stale.append(t)
            if stale:
                print(f"[{label}] {len(stale)} ticker ราคาปิดล่าสุด stale (Yahoo ยังไม่เติม Close) "
                      f"— ลอง fallback ดึง intraday 1m")
                import yfinance as yf
                from concurrent.futures import ThreadPoolExecutor, as_completed

                def _intraday_last_close(tick):
                    try:
                        h = yf.Ticker(tick).history(period="5d", interval="1m")
                        if h is None or not len(h):
                            return tick, None
                        h = h.dropna(subset=["Close"])
                        return (tick, float(h["Close"].iloc[-1])) if len(h) else (tick, None)
                    except Exception:
                        return tick, None

                with ThreadPoolExecutor(max_workers=8) as ex:
                    futures = [ex.submit(_intraday_last_close, t) for t in stale]
                    for f in as_completed(futures):
                        tick, live_price = f.result()
                        if live_price is None:
                            continue
                        prev_close = data.get(tick, {}).get("close")
                        prev_price = float(prev_close.iloc[-1]) if prev_close is not None and len(prev_close) else None
                        if prev_price:
                            live_map[tick] = {
                                "live_price": round(live_price, 4),
                                "live_chg": round((live_price - prev_price) / prev_price * 100, 2),
                            }

    # advanced — True ถ้ามีอย่างน้อย 1 ticker ได้แท่งปิดของวันใหม่จริง (วันที่มากกว่า
    # last_dates เดิมก่อนรอบนี้) ต่างจาก n (จำนวน ticker ที่ "เช็ค" รอบนี้ ซึ่ง fetch_gap_batch
    # จะคืนแท่ง overlap เดิมกลับมาเสมอแม้ไม่มีอะไรใหม่) ใช้บอก caller (heatmap live-update
    # ปุ่ม) ว่าควร build() ใหม่ทั้งไฟล์ไหม — เดิมพึ่ง live_map อย่างเดียว (ว่างเปล่าตอนตลาดปิด)
    # ทำให้ราคาปิดวันใหม่ที่เพิ่ง upsert ลง DB ไม่เคยถูกคำนวณเข้า <mkt>_index_metrics.json
    # เลยจนกว่าจะถึง Quick Update ของวันถัดไป (ดู _run_heatmap_live_update)
    advanced = bool(new_tickers)
    if not advanced:
        for t, d in data.items():
            if t not in last_dates:
                continue
            close = d.get("close")
            if close is None or not len(close):
                continue
            if close.index[-1].date() > pd.to_datetime(last_dates[t]).date():
                advanced = True
                break

    if data:
        store.upsert_bars(BASE_DIR, data, replace_tickers=replace_tickers)
    return len(data), live_map, tickers, advanced


def _run_us_index_gap_update(progress_cb=None, sleep_s=0.3, index_key=None):
    from sources import us_index_membership
    from core import us_store
    return _run_index_gap_update(us_index_membership, us_store, "US", "US Index", progress_cb, sleep_s=sleep_s, index_key=index_key)


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
    ใช้ progress state ร่วมกับงานยาวอื่นๆ (_state/_update/_lock — ดู /api/refresh)
    เช็ค _HM_LIVE_STATE["HK"] ด้วย — กัน race กับปุ่ม "⚡ อัพเดทราคา" ของ Heatmap HK
    (ดูเหตุผลเต็มที่ us_index_full_refresh)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        if _HM_LIVE_STATE["HK"]["running"]:
            return jsonify({"error": "กำลังอัพเดทราคาสด (Heatmap HK) อยู่ โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา HK Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_hk_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_hk_index_full_refresh():
    ok = False
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
        _update(current=len(tickers) - 1, total=len(tickers),
                message=f"กำลังบันทึก {len(data)} ตัวลง hk_prices.db (หลายนาที อย่าเพิ่งปิด)...")
        hk_store.init_db(BASE_DIR)
        hk_store.upsert_bars(BASE_DIR, data)

        missing = len(tickers) - len(data)
        msg = f"เสร็จแล้ว! ดึงราคา HK Index ย้อนหลังสูงสุด {len(data)}/{len(tickers)} ตัว"
        if missing:
            msg += f" (ขาด {missing} ตัว)"
        _update(done=True, message=msg)
        run_log.record_run(BASE_DIR, "hk_index_full_refresh", True, msg)
        ok = True
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hk_index_full_refresh", False, str(e))
    finally:
        if not ok:
            _update(running=False)

    if not ok:
        return

    # คำนวณ RS/EMA/Stage/52W ใหม่จากราคาที่เพิ่งดึง — ทำนอก try/except หลักเพื่อให้รันแม้
    # ราคาบางตัวพลาด (best-effort, ไม่ทำให้ job หลักถูกมองว่า error) แต่ยัง "running=True"
    # อยู่ตลอดขั้นนี้ — กันปุ่ม Index Max/Heatmap live update ตัวอื่นเริ่มเขียน hk_prices.db/
    # hk_index_metrics.json ซ้อนกันก่อน build() รอบนี้เสร็จ
    try:
        from sources import hk_index_metrics
        n_metrics = hk_index_metrics.build(BASE_DIR)
        _hk_breadth_cache.clear()
        _bump_cache_gen()
        print(f"[HK Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[HK Index] metrics build error: {e}")
    finally:
        _update(running=False)


def _run_hk_index_gap_update(progress_cb=None, index_key=None):
    from sources import hk_index_membership
    from core import hk_store
    return _run_index_gap_update(hk_index_membership, hk_store, "HK", "HK Index", progress_cb, index_key=index_key)


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
    query param index=HSI|HSCEI|HSTECH (default HSI) — ค่าที่ไม่รู้จักตอบ 400 เหมือน
    /api/hk-index-heatmap (เดิม endpoint นี้ fallback เป็น HSI เงียบๆ ทำให้พฤติกรรมไม่ตรงกัน)"""
    from sources import hk_index_metrics
    from core.metrics import summarize_groups
    idx = (request.args.get("index") or "HSI").upper()
    flag = {"HSI": "in_hsi", "HSCEI": "in_hscei", "HSTECH": "in_hstech"}.get(idx)
    if not flag:
        return jsonify({"error": "index ต้องเป็น HSI, HSCEI หรือ HSTECH เท่านั้น"}), 400
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
    ลง jp_prices.db — ปุ่มแยกต่างหาก ใช้เฉพาะกดมือ (ดู hk_index_full_refresh)
    เช็ค _HM_LIVE_STATE["JP"] ด้วย — กัน race กับปุ่ม "⚡ อัพเดทราคา" ของ Heatmap JP"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        if _HM_LIVE_STATE["JP"]["running"]:
            return jsonify({"error": "กำลังอัพเดทราคาสด (Heatmap JP) อยู่ โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message="กำลังเริ่มดึงราคา JP Index ย้อนหลังสูงสุด...")
    threading.Thread(target=_run_jp_index_full_refresh, daemon=True).start()
    return jsonify({"ok": True})


def _run_jp_index_full_refresh():
    ok = False
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
        _update(done=True, message=msg)
        run_log.record_run(BASE_DIR, "jp_index_full_refresh", True, msg)
        ok = True
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "jp_index_full_refresh", False, str(e))
    finally:
        if not ok:
            _update(running=False)

    if not ok:
        return

    # ยัง "running=True" อยู่ตลอด build() นี้ — กันปุ่ม Index Max/Heatmap live update ตัวอื่น
    # เริ่มเขียน jp_prices.db/jp_index_metrics.json ซ้อนกันก่อน build() รอบนี้เสร็จ
    try:
        from sources import jp_index_metrics
        n_metrics = jp_index_metrics.build(BASE_DIR)
        _jp_breadth_cache.clear()
        _bump_cache_gen()
        print(f"[JP Index] rebuilt metrics: {n_metrics} ticker")
    except Exception as e:
        print(f"[JP Index] metrics build error: {e}")
    finally:
        _update(running=False)


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


@app.route("/api/jp-sector-ranks")
def jp_sector_ranks():
    """จัดอันดับ Sector ของสมาชิก Nikkei 225 ตาม RS/return เฉลี่ย — reuse
    core.metrics.summarize_groups() ตัวเดียวกับหน้า Sectors ของหุ้นไทย/US/HK (ห้ามเขียนสูตรซ้ำ)
    ไม่มี query param ?index= เหมือน US/HK เพราะ JP มีดัชนีเดียว (Nikkei 225)"""
    from sources import jp_index_metrics
    from core.metrics import summarize_groups
    stocks = [s for s in jp_index_metrics.load_local(BASE_DIR).get("stocks", []) if s.get("in_nikkei225")]
    return jsonify({"sectors": summarize_groups(stocks, "sector")})


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
        _update(done=True, message=msg)
        run_log.record_run(BASE_DIR, "hedge_refresh", True, msg)
    except Exception as e:  # noqa: BLE001
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hedge_refresh", False, str(e))
    finally:
        _update(running=False)


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
        con.execute("PRAGMA busy_timeout=5000")
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
            _update(done=True, current=0, total=0,
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
        _update(done=True, message=msg)
        run_log.record_run(BASE_DIR, "hedge_fetch_missing", True, msg)
    except Exception as e:  # noqa: BLE001
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "hedge_fetch_missing", False, str(e))
    finally:
        _update(running=False)


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
    """ราคาล่าสุด ใช้คำนวณ PE/PBV band แบบสด ("มูลค่าเทียบอดีตตัวเอง") ในหน้างบการเงิน —
    งวด Finnomena ล่าสุดอาจเป็นราคา ณ สิ้นไตรมาสที่ผ่านมาแล้ว ไม่ใช่ราคาวันนี้ · ยังใช้เป็น
    "⚡ อัพเดทราคาล่าสุด" ของ ETF/DR/Watchlist ด้วย

    หุ้น/ETF ที่จดทะเบียนบน SET ตรง (ไม่ใช่ DR/mirror ต่างประเทศ) ลอง SET API ก่อนเสมอ —
    เร็วกว่า Yahoo มาก (~1s vs Yahoo ที่บางช่วงช้าถึง 30-45s ตอนโดน rate limit จนปุ่มกดแล้ว
    timeout เงียบๆ ราคาเลยค้างเป็นของเมื่อวาน) fallback yfinance เฉพาะตอน SET API พลาด"""
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

    if not is_dr and not yf_override:
        try:
            from sources.set_api import fetch_quotes_batch
            q = fetch_quotes_batch([yf_ticker], min_ratio=0)
            row = q.get(yf_ticker)
            if row and row.get("close") is not None:
                return jsonify({"sym": sym, "yf": yf_ticker, "price": float(row["close"]),
                                "currency": "THB"})
        except Exception:
            pass  # SET API พลาด (ล่ม/หุ้นพักเทรด) — ตกไปใช้ yfinance ด้านล่างตามเดิม

    import yfinance as yf
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
            _update(done=True,
                    message=f"เสร็จแล้ว! ดึงใหม่ {result['ok']} · ข้าม {result['skipped']} (มีอยู่แล้ว ไม่เก่า)"
                            + (f" · ล้มเหลว {result['fail']}" if result["fail"] else ""))
        except Exception as e:
            _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        finally:
            _update(running=False)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/dr-description-sync-index", methods=["POST"])
def start_dr_description_sync_index():
    """ดึงคำอธิบายบริษัท (Yahoo Finance + แปลไทย) ของหุ้นสมาชิกดัชนีหลัก US
    (S&P500/Dow/Nasdaq100) + HK (HSI/HSCEI/HSTECH) + JP (Nikkei225) ทั้งชุด — เสริมปุ่ม
    DR sync ที่ครอบคลุมแค่ DR universe 318 ตัว ให้ครอบคลุมหุ้น mirror ที่คนเปิดดูบ่อยด้วย
    (local-only ปุ่มกด — ผลลัพธ์ dr_descriptions.json ค่อย bake ขึ้น GitHub ทีหลังตอน push ปกติ)"""
    force = False
    if request.is_json:
        body = request.json or {}
        force = bool(body.get("force"))
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=0,
                      message="กำลังเริ่มดึงคำอธิบายบริษัทดัชนีหลัก US/HK/JP...")

    def _run():
        try:
            def cb(current, total, msg):
                _update(current=current, total=total, message=msg)
            result = dr_descriptions.sync_index_universe(BASE_DIR, force=force, callback=cb)
            _update(done=True,
                    message=f"เสร็จแล้ว! ดึงใหม่ {result['ok']} · ข้าม {result['skipped']} (มีอยู่แล้ว ไม่เก่า)"
                            + (f" · ล้มเหลว {result['fail']}" if result["fail"] else ""))
        except Exception as e:
            _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        finally:
            _update(running=False)

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

    data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr, market=market)
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
    data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr, market=market)
    return jsonify(_with_quality(data))


@app.route("/api/financials-qpl-report/<symbol>")
def get_financials_qpl_report(symbol):
    """ตารางงบกำไรขาดทุนรายไตรมาสสไตล์ broker research (รายได้/ต้นทุนขาย/กำไรขั้นต้น/
    SG&A แยกขาย-บริหาร/กำไรดำเนินงาน/ต้นทุนการเงิน/กำไรก่อนภาษี/ภาษี/กำไรสุทธิ) — ผสาน 3 แหล่ง
    Finnomena (ยาว ~16 ปี, บรรทัดหยาบ) + Yahoo (ละเอียดครบ, สั้นแค่ไม่กี่ไตรมาสล่าสุด) + SET
    official (ทางการ, แม่นสุด — chart 5 ปีหยาบ + งบละเอียดล่าสุด/อนุพันธ์ ~3-4 ไตรมาส เฉพาะหุ้นไทย)
    ดู compute_qpl_report() ใน financials_store.py สำหรับตรรกะการผสาน

    ทั้ง 3 แหล่งอ่านจาก DB local ก่อนเสมอ (pattern เดียวกันหมด ไม่มีแหล่งไหนยิงสดทุกครั้งที่เปิดหน้า)
    ยิงสด (แล้วเก็บลง DB ให้ครั้งต่อไปอ่านได้เลย) เฉพาะตอน DB ยังไม่มีข้อมูลหุ้นตัวนั้นเลย หรือผู้ใช้
    ขอ ?refresh=1 เอง — SET official (source 'set_qpl') สำคัญที่ต้อง sync สะสมไว้ทุกครั้งที่มีโอกาส
    เพราะ periods endpoint ของ SET มีแค่ปีปัจจุบัน+ปีก่อนหน้า พองวดเลื่อนหลุดออกจาก periods list
    จะดึงข้อมูลละเอียด (COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี) ของงวดนั้นซ้ำไม่ได้อีกเลย"""
    sym = symbol.upper().strip()
    is_dr = request.args.get("is_dr") == "1"
    market = request.args.get("market")
    refresh = request.args.get("refresh") == "1"

    # financials_store.get() เปิด connection ใหม่ (busy_timeout=5000) — ถ้า background
    # job (sync-all/mirror) กำลังเขียน financials.db นานเกิน 5s พอดี อาจได้
    # sqlite3.OperationalError: database is locked ต้องกันไว้ไม่ให้ endpoint นี้ 500
    # ทั้งที่ควร fallback ไปยิงสดแทนได้เหมือนกรณี "ไม่มีข้อมูลใน DB เลย"
    try:
        finn = None if refresh else financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr, market=market)
    except Exception:
        finn = None
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym, is_dr=is_dr)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh, is_dr=is_dr)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr, market=market)
        except Exception:
            finn = None

    try:
        yq = None if refresh else financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, market=market)
    except Exception:
        yq = None
    if yq is None:
        try:
            fresh = financials_store.fetch_yahoo_quarterly(sym, is_dr=is_dr, market=market)
            financials_store.upsert(BASE_DIR, sym, "yahoo_q", fresh, is_dr=is_dr)
            yq = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, market=market)
        except Exception:
            yq = None

    if not finn and not yq:
        return jsonify({"error": f"ไม่พบข้อมูลงบการเงินของ {sym} จากทั้ง Finnomena และ Yahoo"}), 404

    set_series = None
    if not is_dr:   # SET official มีแค่หุ้นไทย — DR ข้าม ไม่ยิง set.or.th เปล่าๆ
        if not refresh:
            try:
                set_series = financials_store.get_set_qpl_series(BASE_DIR, sym)
            except Exception:
                set_series = None
        if set_series is None:
            try:
                set_series = financials_store.sync_set_qpl_series(BASE_DIR, sym)
            except Exception:
                set_series = None

    try:
        report = financials_store.compute_qpl_report(finn, yq, set_series=set_series)
    except Exception as e:
        return jsonify({"error": f"ประมวลผลงบ {sym} ล้มเหลว: {e}"}), 500
    report["sym"] = sym
    report["name"] = (yq or finn or {}).get("name") or sym
    return jsonify(report)


@app.route("/api/financials-merged-report/<symbol>")
def get_financials_merged_report(symbol):
    """แท็บ 'งบรวมทุกแหล่ง' — ผสาน P&L (3 แหล่งเหมือน /api/financials-qpl-report, SET เป็นชั้น
    ท้ายสุด/แม่นสุด) + งบดุล/กระแสเงินสด รายไตรมาส (Finnomena+Yahoo เท่านั้น — SET.or.th ไม่มี
    รายไตรมาสจริงของ 2 งบนี้ ดู compute_full_report) ส่วนรายปี หุ้นไทยจะ override
    total_assets/total_equity/cfo/cfi/cff ด้วยตัวเลขทางการจาก SET company-highlight ปีที่ปิดงบ
    แล้วทับผลรวม Finnomena+Yahoo อีกที (ดู set_bscf_annual_layer) คืนทั้งรายไตรมาส ("quarters")
    และรายปี ("years") ในเรสปอนส์เดียว ให้ frontend toggle รายปี/รายไตรมาสได้โดยไม่ต้องยิงซ้ำ

    ดึงจาก cache local ก่อนเสมอ (pattern เดียวกับ /api/financials-qpl-report) ยิงสดเฉพาะ DB
    ว่างหรือ ?refresh=1"""
    sym = symbol.upper().strip()
    is_dr = request.args.get("is_dr") == "1"
    market = request.args.get("market")
    refresh = request.args.get("refresh") == "1"

    # financials_store.get() เปิด connection ใหม่ (busy_timeout=5000) — ถ้า background
    # job (sync-all/mirror) กำลังเขียน financials.db นานเกิน 5s พอดี อาจได้
    # sqlite3.OperationalError: database is locked ต้องกันไว้ไม่ให้ endpoint นี้ 500
    # ทั้งที่ควร fallback ไปยิงสดแทนได้เหมือนกรณี "ไม่มีข้อมูลใน DB เลย"
    try:
        finn = None if refresh else financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr, market=market)
    except Exception:
        finn = None
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym, is_dr=is_dr)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh, is_dr=is_dr)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr, market=market)
        except Exception:
            finn = None

    try:
        yq = None if refresh else financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, market=market)
    except Exception:
        yq = None
    if yq is None:
        try:
            fresh = financials_store.fetch_yahoo_quarterly(sym, is_dr=is_dr, market=market)
            financials_store.upsert(BASE_DIR, sym, "yahoo_q", fresh, is_dr=is_dr)
            yq = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, market=market)
        except Exception:
            yq = None

    if not finn and not yq:
        return jsonify({"error": f"ไม่พบข้อมูลงบการเงินของ {sym} จากทั้ง Finnomena และ Yahoo"}), 404

    set_series = None
    set_hl = None
    if not is_dr:   # SET official มีแค่หุ้นไทย — DR ข้าม ไม่ยิง set.or.th เปล่าๆ
        if not refresh:
            try:
                set_series = financials_store.get_set_qpl_series(BASE_DIR, sym)
            except Exception:
                set_series = None
        if set_series is None:
            try:
                set_series = financials_store.sync_set_qpl_series(BASE_DIR, sym)
            except Exception:
                set_series = None
        # company-highlight (source 'set') — ใช้ override total_assets/total_equity/cfo/cfi/cff
        # รายปีทับผลรวมจาก Finnomena+Yahoo (ดู set_bscf_annual_layer) หุ้นไทยเน้นข้อมูลจาก
        # SET ก่อนเพราะเป็นแหล่งทางการ ตรงตามที่บริษัทยื่นจริง
        try:
            set_hl = None if refresh else financials_store.get(BASE_DIR, sym, "set")
        except Exception:
            set_hl = None
        if set_hl is None:
            try:
                set_hl = financials_store.fetch_set_full(sym)
                financials_store.upsert(BASE_DIR, sym, "set", set_hl)
            except Exception:
                set_hl = None

    try:
        report = financials_store.compute_full_report(finn, yq, set_series=set_series)
        set_annual = financials_store.set_bscf_annual_layer(set_hl) if set_hl else None
        report["years"] = financials_store.rollup_full_report_annual(report["quarters"], set_annual=set_annual)["years"]
    except Exception as e:
        return jsonify({"error": f"ประมวลผลงบ {sym} ล้มเหลว: {e}"}), 500
    report["sym"] = sym
    report["name"] = (yq or finn or {}).get("name") or sym
    # ธนาคาร/ประกัน/เงินทุนฯ — บอก frontend ให้ซ่อน Income Sankey Diagram เพราะ COGS/กำไรขั้นต้น
    # ที่ผสานมาได้ (มักมาจาก Yahoo/Finnomena ที่ตีความดอกเบี้ยจ่ายเป็น 'Cost Of Revenue') ไม่ใช่
    # แนวคิดต้นทุนขายแบบธุรกิจทั่วไป ทำให้ Sankey เข้าใจผิดได้ (เช็คแล้วทั้ง KBANK และ JPM: cogs
    # ไม่ null เสมอไปสำหรับหุ้นกลุ่มนี้ พึ่งแค่ null-heuristic ฝั่ง frontend ไม่พอ) — reuse ตัวเช็ค
    # กลุ่มการเงินเดียวกับที่ใช้ยกเว้น Z-Score ทั่วทั้งแอป (factor_snapshot.py) แทนที่จะเขียนใหม่:
    # หุ้นไทยเช็คจาก set_data.json (_financial_sector_symbols, แคชไว้แล้ว) ส่วนหุ้นต่างประเทศ
    # (US/HK/JP ผ่าน DR) เช็ค GICS sector จาก {market}_index_metrics.json เทียบ FINANCIAL_SECTOR_NAMES
    # (ครอบ 'Financials'/'Finance'/'Financial Services' — ชื่อ sector สะกดไม่ตรงกันข้ามไฟล์ ดู
    # comment ที่ FINANCIAL_SECTOR_NAMES) เฉพาะสมาชิกดัชนีหลักที่มี sector เก็บไว้ล่วงหน้า — หุ้น
    # mirror นอกดัชนีหลักไม่มีข้อมูลนี้ frontend จะ fallback ไปใช้ null-heuristic เอง
    if not is_dr:
        report["is_financial_sector"] = sym in factor_snapshot._financial_sector_symbols(BASE_DIR)
    elif market in ("US", "HK", "JP"):
        try:
            entry = _tearsheet_universe_map(market).get(sym)
            if entry:
                report["is_financial_sector"] = entry.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES
        except Exception:
            pass
    return jsonify(report)


@app.route("/api/financials-qpl-set/<symbol>")
def get_financials_qpl_set(symbol):
    """ตารางกำไรขาดทุนรายไตรมาสจาก SET.or.th ล้วนๆ (source 'set_qpl' อย่างเดียว ไม่ผสาน
    Finnomena/Yahoo เหมือน /api/financials-qpl-report) — ใช้แสดงเสริมในแท็บ 🇹🇭 SET.or.th
    คู่กับตาราง Company Highlight รายปีที่มีอยู่เดิม เฉพาะหุ้นไทย (DR ไม่มีข้อมูล SET.or.th)
    อ่าน DB ก่อนเสมอ ยิงสดเฉพาะ DB ว่างหรือ ?refresh=1 (pattern เดียวกับ endpoint งบอื่นๆ)"""
    sym = symbol.upper().strip()
    refresh = request.args.get("refresh") == "1"

    set_series = None
    if not refresh:
        try:
            set_series = financials_store.get_set_qpl_series(BASE_DIR, sym)
        except Exception:
            set_series = None
    if set_series is None:
        try:
            set_series = financials_store.sync_set_qpl_series(BASE_DIR, sym)
        except Exception:
            set_series = None
    if not set_series:
        return jsonify({"error": f"ไม่พบข้อมูลงบไตรมาสจาก SET.or.th ของ {sym}"}), 404

    try:
        report = financials_store.compute_qpl_report(None, None, set_series=set_series)
        report["sym"] = sym
        payload = financials_store.get(BASE_DIR, sym, "set_qpl")
        report["synced_at"] = (payload or {}).get("synced_at")
    except Exception as e:
        return jsonify({"error": f"ประมวลผลงบ {sym} ล้มเหลว: {e}"}), 500
    return jsonify(report)


@app.route("/api/financials-compare/<symbol>")
def get_financials_compare(symbol):
    """ข้อมูลสำหรับแท็บ '⚖️ เทียบหุ้น 2 ตัว' ของหุ้นไทย (ไม่รองรับ DR — ฝั่ง frontend fallback
    ไป finnomena_q/yahoo_q ปกติสำหรับ DR เพราะ SET.or.th ไม่มีข้อมูลหุ้นต่างประเทศ) ใช้ SET.or.th
    เป็นแหล่งหลักตามคำขอ user: P&L รายไตรมาส (ผสาน Finnomena+Yahoo+SET ผ่าน compute_qpl_report —
    เหมือน /api/financials-qpl-report ทุกประการ เพื่อได้ประวัติลึก ~16-20 ปีแทนที่จะมีแค่ SET-only
    ~4-5 ปี ตามคำขอ user "ทำทั้งสองอย่าง" [เพิ่มความลึก + เพิ่มบรรทัด]) + company-highlight
    (ROE/ROA/D-E รายปีล่าสุด) เสริมด้วย Finnomena เฉพาะ 4 แถว valuation ที่ SET ไม่มีให้ (Market
    Cap/P-E ผ่าน Basic EPS+Close/P-BV/Dividend Yield) คืนโครงเดียวกับ /api/financials-full
    (income/ratios/valuation) ให้ _renderFinCompare ใช้ต่อได้ตรงๆ + คีย์ใหม่ 'qpl' ({date: row})
    เก็บ raw row เต็มของทุกไตรมาส (cogs/selling_exp/admin_exp/sga_total/operating_profit/
    financial_cost/pretax_profit/tax_expense + %) ให้ตารางเทียบรายไตรมาสฝั่ง frontend ใช้วาด
    บรรทัดเพิ่มได้ — field ไหนไม่มี (เช่น Finnomena พัง) จะเป็น '—' ในตารางแทน ไม่ error ทั้งแถว
    ไม่มีข้อมูลจากทั้ง 3 แหล่งเลย -> 404 ให้ frontend fallback ไป finnomena_q เอง"""
    sym = symbol.upper().strip()

    # Finnomena รายไตรมาส — ยาว ~16-20 ปีแต่หยาบ (ไม่มี COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี)
    # ดึงไว้ก่อนเพราะใช้ทั้งเสริมประวัติ P&L (compute_qpl_report) และดึง valuation ด้านล่าง
    finn = financials_store.get(BASE_DIR, sym, "finnomena_q")
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q")
        except Exception:
            finn = None

    # Yahoo รายไตรมาส — ครบทุกบรรทัดตรงตัวแต่สั้นแค่ไม่กี่ไตรมาสล่าสุด ใช้เสริมรายละเอียด
    # COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี ให้ช่วงที่ SET detail ยังไปไม่ถึง
    yq = financials_store.get(BASE_DIR, sym, "yahoo_q")
    if yq is None:
        try:
            fresh = financials_store.fetch_yahoo_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "yahoo_q", fresh)
            yq = financials_store.get(BASE_DIR, sym, "yahoo_q")
        except Exception:
            yq = None

    try:
        set_series = financials_store.get_set_qpl_series(BASE_DIR, sym)
    except Exception:
        set_series = None
    if set_series is None:
        try:
            set_series = financials_store.sync_set_qpl_series(BASE_DIR, sym)
        except Exception:
            set_series = None

    if not finn and not yq and not set_series:
        return jsonify({"error": f"ไม่พบข้อมูลงบการเงินของ {sym}"}), 404

    quarters = financials_store.compute_qpl_report(finn, yq, set_series=set_series)["quarters"]
    if not quarters:
        return jsonify({"error": f"ไม่พบข้อมูลงบการเงินของ {sym}"}), 404

    q_end = {1: "-03-31", 2: "-06-30", 3: "-09-30", 4: "-12-31"}
    income = {"Total Revenue": {}, "Net Income": {}}
    ratios = {"Net Margin": {}, "Gross Margin": {}}
    qpl = {}   # {date: raw row เต็ม} — ให้ตารางเทียบรายไตรมาสวาดบรรทัด COGS/SG&A/Operating/ฯลฯ เพิ่มได้
    for row in quarters:
        q, y = row.get("q"), row.get("year_ad")
        if q not in q_end or y is None:
            continue
        d = f"{y}{q_end[q]}"
        if row.get("revenue") is not None: income["Total Revenue"][d] = row["revenue"]
        if row.get("net_profit") is not None: income["Net Income"][d] = row["net_profit"]
        if row.get("npm") is not None: ratios["Net Margin"][d] = row["npm"]
        if row.get("gpm") is not None: ratios["Gross Margin"][d] = row["gpm"]
        qpl[d] = row

    # company-highlight -> ROE/ROA/D-E รายปีล่าสุด (entries เรียงเก่า->ใหม่ วนแล้วเก็บค่าตัวท้ายสุด
    # ที่ไม่ null ก็ได้ค่าล่าสุดจริงเสมอ ไม่ต้อง sort เพิ่ม) — กรองเฉพาะ quarter=='Q9' (งวดรายปี
    # ตามธรรมเนียมเดียวกับจุดอื่นในโค้ดเบส) ไม่งั้นถ้าปีปัจจุบันยังไม่จบปี entries ตัวท้ายสุดจะเป็น
    # ไตรมาสกลางปี (Q1-Q3) ได้ค่า ROE/ROA/D-E รายไตรมาสมาแทนที่จะเป็นรายปีตามที่ตั้งใจ
    hl = financials_store.get(BASE_DIR, sym, "set")
    if hl is None:
        try:
            fresh = financials_store.fetch_set_full(sym)
            financials_store.upsert(BASE_DIR, sym, "set", fresh)
            hl = financials_store.get(BASE_DIR, sym, "set")
        except Exception:
            hl = None
    hl_q_end = {"Q1": "-03-31", "Q2": "-06-30", "Q3": "-09-30", "Q9": "-12-31"}
    for src_key, dest in (("roe", "ROE"), ("roa", "ROA"), ("deRatio", "Debt To Equity")):
        val, dt = None, None
        for e in (hl or {}).get("entries", []):
            if e.get("quarter") != "Q9":
                continue
            v = e.get(src_key)
            if v is not None:
                val, dt = v, f"{e.get('year')}{hl_q_end.get(e.get('quarter'), '-12-31')}"
        if val is not None:
            ratios[dest] = {dt: val}

    # Finnomena — เสริมเฉพาะ valuation (Market Cap/P-BV/Dividend Yield) + Basic EPS/Close (คำนวณ P/E
    # TTM ฝั่ง frontend เหมือน path Finnomena ปกติ) ตามที่ user เลือก ไม่ใช่แหล่งหลักอีกต่อไป
    valuation = {}
    name = sym
    if finn:
        fval = finn.get("valuation") or {}
        for key in ("Market Cap", "PBV", "Dividend Yield", "Close"):
            if fval.get(key):
                valuation[key] = fval[key]
        feps = (finn.get("income") or {}).get("Basic EPS")
        if feps:
            income["Basic EPS"] = feps
        name = finn.get("name") or sym

    return jsonify({"sym": sym, "name": name, "income": income, "ratios": ratios,
                     "valuation": valuation, "qpl": qpl})


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
_CALENDAR_LOOKBACK_DAYS = 365   # ย้อนหลังไว้ให้โหมด "⏪ ย้อนหลัง" ฝั่ง frontend (ปุ่ม 7/30/
                                  # ทั้งหมด วันย้อนหลัง) มีข้อมูลให้ดูจริง รวมถึง earnings ที่ 'วัน
                                  # คาดการณ์' เลยมาแล้วให้โผล่พร้อมลิงก์ไปดูงบจริงที่ SET


@app.route("/api/calendar-events-all")
def get_all_calendar_events_endpoint():
    """ปฏิทินรวมทุกหุ้นที่เคย sync ไว้ใน local DB (ไม่ใช่แค่ watchlist) — ใช้กับตัวกรอง
    "ทั้งหมดที่มีข้อมูล"/"ตามตลาด" หน้า 📅 ปฏิทิน อ่านจาก cache เท่านั้น ไม่ fetch สด (เบา ไม่มี
    rate-limit risk) ?market=TH|US|HK|DR กรองเฉพาะตลาดนั้น ไม่ส่ง = ทุกตลาด"""
    from datetime import date as _date, timedelta as _td
    market = request.args.get("market")
    from_date = (_date.today() - _td(days=_CALENDAR_LOOKBACK_DAYS)).isoformat()
    events = financials_store.get_all_calendar_events(BASE_DIR, from_date=from_date, market=market)
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
    lookback_iso = (_date.today() - _td(days=_CALENDAR_LOOKBACK_DAYS)).isoformat()

    rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=lookback_iso)
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
            rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=lookback_iso)
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


# cache universe ตาม (mtime ของ set_prices.db, mtime ของ dr_universe_auto.json, วันที่วันนี้)
# — เดิมคำนวณใหม่ทุกครั้งที่เปิดหน้า Data Health/ตรวจความครบถ้วน โดยแค่
# price_store.get_last_dates() ก็กิน ~0.5 วิแล้ว (pattern เดียวกับ _dh_th_universe_cache
# ด้านล่างและ _load_short_data) ใส่วันที่ในคีย์ด้วยเพราะเกณฑ์ "ราคาไม่ขยับเกิน 180 วัน"
# อิง now() — ข้ามวันแล้วต้องคำนวณใหม่แม้ไฟล์ไม่เปลี่ยน
_fin_universe_cache = {"key": None, "result": None}


def _fin_universe_cache_key():
    def _m(p):
        try:
            return os.path.getmtime(p)
        except OSError:
            return None
    return (_m(os.path.join(BASE_DIR, price_store.DB_FILE)),
            _m(os.path.join(BASE_DIR, "dr_universe_auto.json")),
            time.strftime("%Y-%m-%d"))


def _financials_universe():
    ck = _fin_universe_cache_key()
    if _fin_universe_cache["key"] != ck:
        _fin_universe_cache.update(key=ck, result=_financials_universe_uncached())
    # คืนสำเนาเสมอ — caller บางตัวส่งต่อเป็น target ให้ sync_all/get_coverage
    # ที่อาจ sort/แก้ list ได้ ไม่ให้กระทบก้อนที่ cache ไว้
    return list(_fin_universe_cache["result"])


def _financials_universe_uncached():
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
    stale = []
    for s in syms:
        if s in dr_series or dw_pat.match(s) or s.startswith("!"):
            continue
        if _is_stale(s):
            # บันทึกไว้ให้ backtest รุ่นถัดไปรู้ว่าหุ้นนี้ "ยังอยู่จริง" ถึงวันไหน
            # (upsert — เก็บวันที่ตรวจพบครั้งแรก ไม่ทับทุกรอบที่ universe กรองซ้ำ)
            stale.append((s, "TH", f"ราคาไม่ขยับเกิน {_FIN_UNIVERSE_STALE_DAYS} วัน (แขวน SP/เพิกถอน)",
                          last_dates.get(s + ".BK") or last_dates.get(s)))
            continue
        out.append(s)
    # เขียน log รอบเดียวหลังจบ loop (และไม่เขียนเลยถ้าไม่มีตัวไหนเปลี่ยน) — เดิมเรียก
    # record_delisted() ในลูปทีละตัว = อ่าน+เขียนทั้งไฟล์ ~276 รอบต่อการเรียก 1 ครั้ง
    # กิน ~3 วินาทีของ /api/data-health ทั้งที่ปกติไม่มีอะไรเปลี่ยนสักตัว
    delisted_log.record_delisted_bulk(BASE_DIR, stale)
    return out


def _dr_financials_universe():
    """หุ้นต่างประเทศ (underlying ของ DR/DRx) — มีข้อมูลจาก Yahoo Finance เท่านั้น
    ไม่มี SET.or.th เพราะเป็นหุ้นต่างประเทศ (ไม่ใช่หุ้นไทย)
    ตัด ETF ออก — กองทุน/ETF ไม่มีงบการเงินแบบบริษัท (โชว์ note แยกในหน้า UI แทน)"""
    return sorted(s["sym"] for s in load_dr_universe(BASE_DIR) if not s.get("etf"))


def _index_group_quarter_coverage():
    """เทียบสมาชิก 'ทั้งหมด' ของแต่ละดัชนีหลัก (SP500/DOW/NDX/HSI/HSCEI/HSTECH/NIKKEI225)
    กับงบรายไตรมาสที่มีจริงใน DB — ต้องเช็ค 3 namespace แยกกันเพราะหุ้นตัวเดียวอาจถูกเก็บคนละที่:
    1. 'DR:{sym}' (sym = mnemonic ในพอร์ต DR ของ SET เช่น 'AIA'/'TEL') — sync yahoo_q+
       finnomena_q เต็มอยู่แล้วเป็นประจำ
    2. 'DR:{code}' (code = ticker ดิบ ไม่ใช่ mnemonic) — ปุ่ม "ดึงเฉพาะที่ขาด/เก่า" ของหน้า
       งบการเงิน → แท็บ US/HK/JP ยิง sync_all(sources=..., is_dr=True, market=ex,
       skip_up_to_date=True) ให้สมาชิกดัชนีทั้งหมดที่ยังไม่อยู่ในพอร์ต DR (2026-08-20 แก้เป็น
       ongoing refresh แล้ว ไม่ใช่ one-shot backfill แบบเดิม — ดู _run_us_index_sync/
       _run_hk_index_sync/_run_jp_index_sync) US/HK sync ทั้ง yahoo_q+yahoo คู่กัน ส่วน JP
       sync yahoo_q อย่างเดียว (yahoo รายปีของ JP ยังคงอยู่ใต้ namespace (3) แยกจากนี้ตามเดิม
       เพราะ sync_mirror_yahoo_index เดิมเขียนไว้อยู่แล้ว ไม่ได้ย้ายมารวม)
    3. 'FINN:{ex}:{code}' — เฉพาะ finnomena_q จากปุ่ม "📥 Mirror ทั้งตลาด" (ครอบ TH+HK+US
       ทั้งตลาดของ Finnomena ไม่ใช่แค่ดัชนีหลัก) ไม่มี yahoo_q ที่นี่เลย (sync_mirror_yahoo_index
       ดึงให้แค่งบรายปี 'yahoo') — namespace นี้ไม่มีของ JP เลย เพราะ Finnomena ไม่รองรับตลาด
       ญี่ปุ่น (_finn_resolve รองรับแค่ TH/US/HK) ดังนั้น finnomena_q ของ NIKKEI225 นอกพอร์ต DR
       จะเป็น 'not_tracked' เสมอ (ช่องว่างถาวรจากข้อจำกัดของ Finnomena เอง ไม่ใช่บั๊ก) — yahoo_q
       ของ NIKKEI225 ไม่มีข้อจำกัดแบบนี้แล้วหลังแก้ (2) ด้านบน เช็ค (2) ได้ปกติเหมือน US/HK"""
    from sources import us_index_membership as _uim, hk_index_membership as _him, jp_index_membership as _jim

    def _norm_hk(code):
        code = (code or "").upper().strip()
        if code.endswith(".HK"):
            code = code[:-3]
        return code.zfill(4) if code.isdigit() else code

    def _norm_jp(code):
        code = (code or "").upper().strip()
        return code[:-2] if code.endswith(".T") else code

    us = _uim.load_local(BASE_DIR)
    hk = _him.load_local(BASE_DIR)
    jp = _jim.load_local(BASE_DIR)
    groups_raw = {
        "SP500": ("US", [t.upper().strip() for t in (us.get("SP500") or [])]),
        "DOW": ("US", [t.upper().strip() for t in (us.get("DOW") or [])]),
        "NDX": ("US", [t.upper().strip() for t in (us.get("NDX") or [])]),
        "HSI": ("HK", [_norm_hk(t) for t in (hk.get("HSI") or [])]),
        "HSCEI": ("HK", [_norm_hk(t) for t in (hk.get("HSCEI") or [])]),
        "HSTECH": ("HK", [_norm_hk(t) for t in (hk.get("HSTECH") or [])]),
        "NIKKEI225": ("JP", [_norm_jp(t) for t in (jp.get("NIKKEI225") or [])]),
    }

    yf_to_dr = {}
    for e in load_dr_universe(BASE_DIR):
        if e.get("etf"):
            continue
        yf = (e.get("yf") or "").upper().strip()
        if yf.endswith(".HK"):
            code = _norm_hk(yf)
        elif yf.endswith(".T"):
            code = _norm_jp(yf)
        else:
            code = yf
        yf_to_dr[code] = e["sym"]

    latest_map = financials_store.get_latest_period_map_raw(BASE_DIR, sources=("yahoo_q", "finnomena_q"))
    target_yq = financials_store._target_period("yahoo_q")
    target_fq = financials_store._target_period("finnomena_q")

    def _bucket(latest, target):
        if latest is None:
            return "not_tracked"
        return "fresh" if latest >= target else "stale"

    out = {}
    for gk, (ex, codes) in groups_raw.items():
        res = {"index_total": len(codes),
               "yahoo_q": {"target": target_yq.isoformat(), "total": len(codes),
                           "fresh": 0, "stale": 0, "not_tracked": 0},
               "finnomena_q": {"target": target_fq.isoformat(), "total": len(codes),
                                "fresh": 0, "stale": 0, "not_tracked": 0}}
        for code in codes:
            dr_sym = yf_to_dr.get(code)
            yq_latest = latest_map.get((f"DR:{dr_sym}", "yahoo_q")) if dr_sym else None
            if yq_latest is None:
                # JP รวมด้วย — ต่างจาก finnomena_q ด้านล่าง เพราะ _run_jp_index_sync (2026-08-20)
                # เพิ่ม sync_all(sources=('yahoo_q',), is_dr=True, market='JP') ให้ตัวนอกพอร์ต DR
                # แล้ว เขียนลง 'DR:{code}' เหมือน US/HK ทุกประการ (ก่อนหน้านี้ JP ไม่มี yahoo_q
                # นอกพอร์ต DR เลยจริงๆ เลยเคยกันไว้ไม่เช็ค — ถ้าลบส่วนนี้ทิ้งจะมองไม่เห็นข้อมูลที่
                # เพิ่ง sync มาใหม่)
                yq_latest = latest_map.get((f"DR:{code}", "yahoo_q"))
            res["yahoo_q"][_bucket(yq_latest, target_yq)] += 1

            fq_latest = latest_map.get((f"DR:{dr_sym}", "finnomena_q")) if dr_sym else None
            if fq_latest is None and ex != "JP":
                fq_latest = latest_map.get((f"DR:{code}", "finnomena_q"))
            if fq_latest is None and ex != "JP":
                fq_latest = latest_map.get((f"FINN:{ex}:{code}", "finnomena_q"))
            res["finnomena_q"][_bucket(fq_latest, target_fq)] += 1
        out[gk] = res
    return out


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


@app.route("/api/financials-quarter-coverage")
def financials_quarter_coverage():
    """เทียบ universe กับ 'ไตรมาสล่าสุดที่ควรจะมีข้อมูลแล้ว ณ วันนี้' (_target_period) ต่างจาก
    /api/financials-coverage ที่เช็คแค่ 'มีข้อมูลหรือยัง' (เก่าแค่ไหนก็นับว่ามี) ตัวนี้เช็ค
    'ข้อมูลที่มีเป็นงวดล่าสุดจริงหรือยัง' — ใช้ตอบคำถาม 'ไตรมาสล่าสุดยังขาดกี่ตัว'
    ?universe=dr เช็คเฉพาะหุ้นต่างประเทศ (DR/DRx, ไม่มี set/set_qpl เพราะ SET.or.th ไม่มีข้อมูลหุ้นต่างประเทศ)"""
    if request.args.get("universe") == "dr":
        symbols = _dr_financials_universe()
        coverage = financials_store.get_quarter_coverage(
            BASE_DIR, symbols, sources=("yahoo_q", "finnomena_q"), is_dr=True)
    else:
        symbols = _financials_universe()
        coverage = financials_store.get_quarter_coverage(
            BASE_DIR, symbols, sources=("set", "set_qpl", "yahoo_q", "finnomena_q"), is_dr=False)
    return jsonify(coverage)


@app.route("/api/financials-quarter-coverage-by-index")
def financials_quarter_coverage_by_index():
    """เหมือน /api/financials-quarter-coverage?universe=dr แต่แจกแจงแยกตามดัชนีหลัก
    (SP500/DOW/NDX/HSI/HSCEI/HSTECH/NIKKEI225) เทียบกับ 'สมาชิกทั้งหมด' ของแต่ละดัชนีจริง
    (ไม่ใช่แค่ subset ในพอร์ต DR) — แต่ละ source (yahoo_q/finnomena_q) แบ่ง fresh/stale/
    not_tracked แยกกัน เพราะ yahoo_q มีแค่ตัวในพอร์ต DR เท่านั้น ส่วน finnomena_q ครอบคลุม
    กว้างกว่านั้น (เช็ค 'FINN:{ex}:' namespace จากปุ่ม 'Mirror ทั้งตลาด' เพิ่มด้วย)
    ดู docstring _index_group_quarter_coverage"""
    return jsonify(_index_group_quarter_coverage())


def _warmup_fin_dependent_caches():
    """เรียกซ้ำ endpoint ที่พึ่ง _fin_analytics_cache/_sector_compare_cache/_market_trend_cache
    ทันทีหลัง sync เคลียร์ cache พวกนี้ทิ้ง (แทนที่จะปล่อยให้ user คนแรกที่เปิดหน้าเจอ cold
    path เอง ~20-30 วิ) เรียกจาก thread เดิมของ sync ได้เลยเพราะ sync ก็รันใน background
    thread อยู่แล้ว — ไม่บล็อก request อื่น"""
    tc = app.test_client()
    for ep in ("/api/financials-analytics", "/api/sector-compare", "/api/market-trend"):
        try:
            t0 = time.time()
            tc.get(ep)
            print(f"[Warmup] {ep} พร้อม ({time.time() - t0:.0f} วิ)", flush=True)
        except Exception as e:
            print(f"[Warmup] {ep} ล้มเหลว (ไม่กระทบการใช้งาน): {e}", flush=True)


def _clear_fin_analytics_and_warm():
    """ล้าง _fin_analytics_cache แล้วอุ่นใหม่เบื้องหลัง — ใช้ท้ายงาน sync ที่แตะงบ
    (index sync US/HK/JP, mirror sync, full refresh) ซึ่งเดิมเรียก .clear() เปล่าๆ 8 จุด
    โดยไม่มีใครอุ่นซ้ำ ผลคือคนแรกที่เปิด "ดัชนีกลุ่มอุตสาหกรรม"/"แนวโน้มตลาด"/Screener
    หลัง sync ต้องรอ cold path ~12-13 วิ (วัดจริง)

    อุ่นใน daemon thread ไม่ใช่ thread ของ job — job จะได้ขึ้น "เสร็จแล้ว" ทันที
    ไม่ต้องค้างรออุ่นอีก 13 วิ (ต่างจาก _run_financials_sync/financials-update-all
    ที่เรียก _warmup_fin_dependent_caches() ตรงๆ แบบ synchronous มาแต่เดิม)

    งาน sync กลุ่มนี้แตะข้อมูล mirror US/HK/JP เป็นหลัก ไม่กระทบ sector/แนวโน้มตลาด
    ฝั่งหุ้นไทย จึงไม่ล้าง _sector_compare_cache/_market_trend_cache เหมือนเดิม —
    การอุ่นจะไป hit cache เดิมของ 2 ตัวนั้นทันที ไม่มี cost เพิ่ม"""
    _fin_analytics_cache.clear()
    threading.Thread(target=_warmup_fin_dependent_caches, daemon=True).start()


def _run_financials_sync(symbols=None, sources=None, is_dr=False, skip_up_to_date=False):
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
        srcs = tuple(sources) if sources else ("yahoo", "set", "set_qpl", "yahoo_q", "finnomena_q")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        result = financials_store.sync_all(BASE_DIR, target, sources=srcs, callback=cb,
                                           is_dr=is_dr, skip_up_to_date=skip_up_to_date)
        _fin_analytics_cache.clear()   # ข้อมูลเปลี่ยน — บังคับคำนวณ growth/PEG/FCF ใหม่รอบถัดไป
        _sector_compare_cache.clear()
        _market_trend_cache.clear()
        _sector_trend_cache.clear()
        _warmup_fin_dependent_caches()
        skipped = result.get("skipped", 0)
        _update(done=True,
                message=f"เสร็จแล้ว! สำเร็จ {result['ok']}/{result['total']}"
                        + (f" · ข้าม {skipped} คู่ (มีงวดล่าสุดอยู่แล้ว)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']} — อาจโดนบล็อคชั่วคราวหรือแหล่งข้อมูลไม่มีจริง ลองอีกครั้งได้)" if result["fail"] else ""))
    except Exception as e:
        _update(done=True, error=str(e),
                message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


@app.route("/api/financials/sync-all", methods=["POST"])
def start_financials_sync():
    symbols = None
    sources = None
    is_dr = False
    skip_up_to_date = False
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
        # incremental sync: ข้ามคู่ (หุ้น, แหล่ง) ที่มีข้อมูลงวดล่าสุดที่ควรจะมีอยู่แล้ว — ใช้กับปุ่ม
        # "ดึงเฉพาะที่ขาด/เก่า" แทนปุ่มดึงเต็มที่ยิงทุกตัวซ้ำทุกครั้ง (ดู sync_all skip_up_to_date)
        skip_up_to_date = bool(body.get("skip_up_to_date"))
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        label = ("กำลังเริ่ม sync งบการเงิน" + (" (หุ้นต่างประเทศ DR)" if is_dr else " (เฉพาะที่ขาด)" if symbols else "")
                + (" — ข้ามของที่มีงวดล่าสุดอยู่แล้ว" if skip_up_to_date else "") + "...")
        _state.update(running=True, done=False, error=None,
                      current=0, total=0, message=label)
    threading.Thread(target=_run_financials_sync, args=(symbols, sources, is_dr, skip_up_to_date), daemon=True).start()
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
    # connection เดียวใช้ร่วมกันตลอด loop (reuse ผ่าน con= ที่เพิ่งเพิ่มใน financials_store.get)
    # — เดิม get() เปิด-ปิด sqlite connection ใหม่ทุกครั้งที่เรียก วัดจริงตอน cache miss:
    # ~2.6 ครั้ง/หุ้น × ~1,300 หุ้น (TH+DR) = 3,419 connection กิน connect+close+PRAGMA
    # รวม ~5 วิ จาก 13 วิทั้งหมดของ /api/financials-analytics เปิดครั้งเดียวปิดท้าย loop
    # ปลอดภัย เพราะฟังก์ชันนี้รันในเธรดเดียว ไม่มีการแชร์ connection ข้าม thread
    con = financials_store._connect(BASE_DIR) if financials_store.db_exists(BASE_DIR) else None
    try:
        for sym in symbols:
            # กัน 1 หุ้นที่ payload/คำนวณพัง (เช่น field รูปทรงแปลกจากแหล่งข้อมูลเก่า) ทำให้
            # /api/financials-analytics ทั้ง endpoint ตอบ 500 — ข้ามหุ้นนั้นไปแทนที่จะล้มทั้งตลาด
            try:
                payload = financials_store.get(BASE_DIR, sym, "yahoo", is_dr=is_dr, con=con)
                if not payload:
                    continue
                gs = financials_store.compute_growth_score(payload)
                fcf = financials_store.compute_fcf_metrics(payload, mktcap_map.get(sym))
                streaks = financials_store.compute_growth_streaks(payload)
                ratios = financials_store.compute_ratio_trends(payload)
                q_finn = None
                # SET official รายไตรมาส (set_qpl) มักมีงวดล่าสุดเร็วกว่า Yahoo/Finnomena ~1 ไตรมาส
                # (หุ้นไทยเท่านั้น — DR ไม่มีข้อมูล SET.or.th) — เติมงวดล่าสุดให้ QoQ/YoY-Q ไม่ค้าง
                # ดู _set_qpl_latest_extra (เจอบั๊กจริงกับ BA: QoQ ผ่านเกณฑ์ 50% เพราะเทียบงวดเก่า)
                sqpl = financials_store.get(BASE_DIR, sym, "set_qpl", is_dr=is_dr, con=con) if not is_dr else None
                if yahoo_only:
                    q_used = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, con=con)
                    qg = financials_store.compute_quarterly_growth(q_used, set_qpl_payload=sqpl)
                else:
                    # การเติบโตรายไตรมาส (QoQ / YoY-Q / streak / เร่งตัว) จากงบ quarterly — Finnomena
                    # ลึกกว่า Yahoo เสมอเมื่อมี (ตรวจแล้วทุกกรณี ~60-80 ไตรมาส vs ~5-6) จึงเช็คแค่ตัวเดียว
                    # ก่อน ไม่ต้อง get() ทั้งคู่ทุกครั้ง
                    q_finn = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr, con=con)
                    q_used = q_finn if q_finn else financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, con=con)
                    qg = financials_store.compute_quarterly_growth(q_used, set_qpl_payload=sqpl)
                    # Finnomena สั้นผิดปกติ (<8 ไตรมาส เช่นหุ้นที่ Finnomena เพิ่งเริ่มเก็บ) — เช็ค yahoo_q
                    # เผื่อลึกกว่า ให้เลือกแหล่งแบบเดียวกับ factor_snapshot (ไม่งั้นสองเมนูต่างกันได้)
                    if q_finn is not None and qg["quarters_available"] < 8:
                        q_yah = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=is_dr, con=con)
                        qg_y = financials_store.compute_quarterly_growth(q_yah, set_qpl_payload=sqpl)
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
            except Exception as e:
                print(f"[fin-analytics] ข้าม {sym}: {e}")
                continue
    finally:
        if con is not None:
            con.close()

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

    # setdefault ต้องอยู่ใน lock — ถ้า .clear() แทรกระหว่างอ่าน cache_key กับสร้าง slot
    # ที่ได้จะเป็น dict เก่าที่หลุดจาก _fin_analytics_cache แล้ว เขียนผลไปก็สูญเปล่า
    with _fin_analytics_lock:
        slot = _fin_analytics_cache.setdefault(cache_key, {})

    def _compute():
        pe_map, mktcap_map = {}, {}
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    for s in json.load(f).get("stocks", []):
                        pe_map[s["symbol"]] = s.get("pe")
                        mktcap_map[s["symbol"]] = s.get("mkt_cap")
            except Exception:
                pass

        # mkt_cap หุ้น DR ไม่ได้อยู่ใน set_data.json (นั่นมีแต่หุ้นไทย) — ต้องอ่านจาก
        # dr_cache.json แยก (คีย์ 'sym') ไม่งั้น fcf_yield ของ DR ทุกตัวจะเป็น None เสมอ
        # (FCF Yield = FCF ÷ mkt_cap ต้องมี mkt_cap) — ใช้ pattern เดียวกับ /api/factor-screener
        dr_mktcap_map = {}
        try:
            with open(DR_CACHE_FILE, encoding="utf-8") as f:
                for s in json.load(f).get("stocks", []):
                    dr_mktcap_map[s["sym"]] = s.get("mkt_cap")
        except Exception:
            pass

        set_symbols = financials_store.get_synced_symbols(BASE_DIR, "yahoo", is_dr=False)
        dr_symbols = financials_store.get_synced_symbols(BASE_DIR, "yahoo", is_dr=True)
        fin_sector_syms = factor_snapshot._financial_sector_symbols(BASE_DIR)
        return {
            "set": _compute_fin_analytics_for(set_symbols, False, pe_map, mktcap_map, yahoo_only=yahoo_only, fin_sector_syms=fin_sector_syms),
            "dr": _compute_fin_analytics_for(dr_symbols, True, pe_map, dr_mktcap_map, yahoo_only=yahoo_only),
        }

    # lock กันหลายแท็บ/request ที่มาชนตอน cache หมดอายุพร้อมกันคำนวณซ้ำซ้อนกัน (ดูคอมเมนต์
    # ที่ _fin_analytics_lock) — stale-while-revalidate (ดู _swr_get_or_refresh): ถ้ามีค่าเก่า
    # อยู่แล้วแค่หมดอายุ ตอบค่าเก่าทันทีแล้วคำนวณใหม่เบื้องหลัง ไม่บล็อก request คำนวณสด
    # แบบ synchronous เฉพาะตอนไม่เคยมีค่าเลย (cold หลัง restart/clear)
    result = _swr_get_or_refresh(slot, _fin_analytics_lock, _FIN_ANALYTICS_CACHE_TTL, _compute)
    return jsonify(result)


@app.route("/api/sector-compare")
def sector_compare():
    """มุมมอง "⚖ เปรียบเทียบ Sector" ในหน้า "ดัชนีกลุ่มอุตสาหกรรม SET & mai" — รวมรายได้/กำไรสุทธิ
    รายไตรมาส (SET official, source 'set_qpl') กลุ่มตาม Sector (SET) + Industry MAI (หุ้น mai ไม่มี
    sector ย่อยแบบ SET set_data.json เลยใส่ field 'sector' เท่ากับ 'industry' ให้แล้วตอน sync
    เช่น 'Services -mai' — จับกลุ่มด้วย field เดียวกันได้เลย ไม่ต้อง merge ปนกับ sector ฝั่ง SET
    เพราะชื่อมีคำต่อท้าย '-mai' แยกกันชัดเจนอยู่แล้ว) ดู financials_store.get_sector_qpl_compare
    สำหรับ logic การรวม/YoY/เลือก quarter

    ROE ต่อ sector: reuse ผลลัพธ์ ratios['roe'] ต่อหุ้นจาก /api/financials-analytics ตรงๆ (ไม่เปิด
    sqlite ทีละหุ้นซ้ำเอง — endpoint นั้นแคชอยู่แล้ว คำนวณสดครั้งแรกช้า ~15-23 วิเหมือนกัน)"""
    def _compute():
        sector_by_symbol = {}
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    for s in json.load(f).get("stocks", []):
                        if s.get("market") in ("SET", "mai") and s.get("sector"):
                            sector_by_symbol[s["symbol"]] = s["sector"]
            except Exception:
                pass

        compare = financials_store.get_sector_qpl_compare(BASE_DIR, sector_by_symbol)

        fin_data = financials_analytics().get_json()
        roe_by_sector = {}
        for sym, sector in sector_by_symbol.items():
            roe = fin_data.get("set", {}).get(sym, {}).get("roe")
            if roe is not None:
                roe_by_sector.setdefault(sector, []).append(roe)
        set_fin = fin_data.get("set", {})
        for row in compare["sectors"]:
            vals = roe_by_sector.get(row["sector"])
            row["roe"] = round(sum(vals) / len(vals), 1) if vals else None
            for stock in row.get("stocks", []):
                stock["de_ratio"] = set_fin.get(stock["symbol"], {}).get("de_ratio")
        return compare

    # try/except ครอบทั้งก้อนคำนวณ — เดิมไม่มี ต่างจาก /api/indices-quick-update ฯลฯ ที่ครอบไว้
    # exception ที่ไม่คาดคิด (เช่น payload เพี้ยนใน DB) จะหลุดไปเป็นหน้า error 500 แบบ HTML ของ
    # Flask ตรงๆ ไม่ใช่ JSON — ฝั่ง frontend (renderSectorCompare/loadMarketTrendPage) parse
    # ไม่ได้ ขึ้นข้อความงงๆ "Unexpected token '<'..." ให้ผู้ใช้เห็นแทนที่จะเป็นข้อความไทยที่เข้าใจง่าย
    # (endpoint นี้ใช้ร่วมกันทั้งมุมมอง "เปรียบเทียบ Sector" ในหน้านี้ และตาราง Top 10
    # เติบโตโดดเด่นในหน้า Market Trend เลยกระทบ 2 หน้าพร้อมกันถ้าพัง) — ครอบเฉพาะ cold path
    # เท่านั้น (compute จริงครั้งแรก/หลัง clear) ถ้าพังตอน background revalidate (ดู
    # _swr_get_or_refresh) จะ log แล้วใช้ค่าเก่าต่อแทน ไม่ทำให้ endpoint ตอบ 500
    try:
        compare = _swr_get_or_refresh(_sector_compare_cache, _sector_compare_lock,
                                       _SECTOR_COMPARE_CACHE_TTL, _compute)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"รวมข้อมูล sector ล้มเหลว: {e}"}), 500
    return jsonify(compare)


@app.route("/api/market-trend")
def market_trend():
    """หน้า "📈 แนวโน้มตลาด" — แนวโน้มพื้นฐานตลาด SET ทั้งตลาดย้อนหลังสูงสุด 20 ไตรมาส (ไม่แยก
    sector ต่างจาก /api/sector-compare) ดู financials_store.get_market_trend สำหรับ logic เต็ม

    รายได้/กำไร/NPM ตัดหุ้นกลุ่มการเงิน (factor_snapshot._financial_sector_symbols, ใช้ตัวเดียวกับ
    ที่กัน Z-Score ไม่ได้) และ Property Fund & REITs ออก — นิยาม 'รายได้' ธุรกิจสองกลุ่มนี้ไม่เทียบเท่า
    บริษัททั่วไป ส่วน ROE/Cash Quality/Breadth ใช้ทั้งตลาดตามปกติ"""
    def _compute():
        symbols = set()
        reit_symbols = set()
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    for s in json.load(f).get("stocks", []):
                        if s.get("market") == "SET":
                            symbols.add(s["symbol"])
                            if s.get("sector") == "Property Fund & REITs":
                                reit_symbols.add(s["symbol"])
            except Exception:
                pass

        excluded = factor_snapshot._financial_sector_symbols(BASE_DIR) | reit_symbols
        return financials_store.get_market_trend(BASE_DIR, symbols, excluded)

    # try/except ครอบทั้งก้อนคำนวณ — เหมือน /api/sector-compare (ดู comment ที่นั่น) exception ที่
    # ไม่คาดคิด (payload เพี้ยนใน DB, DB ล็อกจาก sync พร้อมกัน, ฯลฯ) จะได้ตอบ JSON error กลับแทน
    # หน้า 500 HTML ของ Flask ที่ frontend (loadMarketTrendPage) parse ไม่ได้ — ครอบเฉพาะ cold
    # path เท่านั้น พังตอน background revalidate จะ log แล้วใช้ค่าเก่าต่อแทน (ดู _swr_get_or_refresh)
    try:
        trend = _swr_get_or_refresh(_market_trend_cache, _market_trend_lock,
                                     _MARKET_TREND_CACHE_TTL, _compute)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"รวมข้อมูลแนวโน้มตลาดล้มเหลว: {e}"}), 500
    return jsonify(trend)


@app.route("/api/sector-trend")
def sector_trend():
    """เทรนด์ย้อนหลัง 20 ไตรมาสของ sector เดียว (รายได้/กำไร YoY, NPM, ROE) — เปิดจาก modal
    รายละเอียด sector ในมุมมอง "⚖ เปรียบเทียบ Sector" (ปุ่ม "📈 เทรนด์ย้อนหลัง" ใน
    openSectorCompareModal ฝั่ง frontend) ต่างจาก /api/market-trend ที่เป็นภาพรวมทั้งตลาดก้อนเดียว
    — endpoint นี้ scope symbols เหลือเฉพาะหุ้นใน sector ที่ระบุ แล้ว reuse
    financials_store.get_market_trend ตัวเดียวกัน ไม่ต้อง exclude การเงิน/REIT ซ้ำเพราะ scope
    ด้วย sector อยู่แล้ว (ถ้า sector ที่เลือกเป็นกลุ่มการเงินเอง รายได้จะเป็น None ตามธรรมชาติ
    เพราะ set_qpl parse จากบัญชี 'รายได้จากการขายและให้บริการ' ที่ผังบัญชีธนาคาร/ประกันไม่มี — ดู
    คอมเมนต์ financials_store.get_sector_qpl_compare)"""
    sector = (request.args.get("sector") or "").strip()
    if not sector:
        return jsonify({"error": "ต้องระบุ sector"}), 400

    cached = _sector_trend_cache.get(sector)
    if cached and (time.time() - cached.get("ts", 0) < _SECTOR_TREND_CACHE_TTL):
        return jsonify(cached["result"])

    with _get_sector_trend_lock(sector):
        cached = _sector_trend_cache.get(sector)
        if cached and (time.time() - cached.get("ts", 0) < _SECTOR_TREND_CACHE_TTL):
            return jsonify(cached["result"])

        try:
            symbols = set()
            if os.path.exists(DATA_FILE):
                try:
                    with open(DATA_FILE, encoding="utf-8") as f:
                        for s in json.load(f).get("stocks", []):
                            if s.get("market") in ("SET", "mai") and s.get("sector") == sector:
                                symbols.add(s["symbol"])
                except Exception:
                    pass

            trend = financials_store.get_market_trend(BASE_DIR, symbols, set())
        except Exception as e:
            traceback.print_exc()
            return jsonify({"error": f"รวมข้อมูลเทรนด์ sector ล้มเหลว: {e}"}), 500

        _sector_trend_cache[sector] = {"result": trend, "ts": time.time()}
        return jsonify(trend)


_MIRROR_SYM_NAME_CACHE = {}   # {ex: (ts, [{symbol,market,name}])}
_MIRROR_SYM_NAME_TTL = 3600    # เปลี่ยนน้อย (sync รายวัน) แคชไว้กันเปิด DB ซ้ำทุกครั้งที่มี
                                # symbol ใน Watchlist ไม่รู้จักตลาด (ดู renderWatchlist ฝั่ง frontend)


@app.route("/api/mirror-symbol-names")
def mirror_symbol_names():
    """symbol+name+market ของหุ้น mirror US/HK ทั้ง universe (เรียก factor_snapshot.
    get_mirror_symbols — endpoint เบาตัวเดิมที่ /api/mirror-symbols ใช้อยู่แล้วสำหรับหน้า
    งบการเงิน แค่ query symbol/market ไม่ parse factors) แล้วเติมชื่อบริษัทให้ ใช้เติม
    datalist ค้นหา/เพิ่มหุ้นใน Watchlist ให้ครอบคลุมหุ้นนอกดัชนีหลักด้วย (เดิมมีแค่สมาชิก
    S&P500+Dow+NDX/HSI+HSCEI+HSTECH ~623 ตัว จาก us/hk_index_metrics.json ทำให้หุ้นอย่าง
    ZS/OKTA/S ไม่โผล่ในช่องค้นหาเลย — ดู feedback ผู้ใช้ 2026-08-10) แยก endpoint ต่างหากจาก
    /api/mirror-symbols เดิม (คนละ response shape — ตัวเดิมคืน {US:[sym,...],HK:[sym,...]}
    เฉยๆ ให้หน้างบการเงิน เปลี่ยนรูปแบบตรงนั้นจะกระทบของเดิมโดยไม่จำเป็น)

    name มาจาก financials_store.get_names_bulk (payload Yahoo ที่เคย sync แล้วเท่านั้น — ตัวที่
    ยังไม่เคยถูกเปิดดู/sync จะไม่มีชื่อ คืน null ให้ frontend fallback โชว์แค่ symbol) ไม่ใช่จาก
    factor_snapshot ตรงๆ (field 'name' ในนั้น default = symbol เฉยๆ ไม่ใช่ชื่อบริษัทจริง)
    HK คืน symbol แบบมี suffix ".HK" ให้ตรง convention เดียวกับ hk_index_metrics.json/
    _wlFetchMirror ฝั่ง frontend (mirror namespace เก็บรหัสดิบไม่มี suffix ต้องต่อเอง)"""
    out = []
    mirror_syms = factor_snapshot.get_mirror_symbols(BASE_DIR)
    for ex in ("US", "HK"):
        cached = _MIRROR_SYM_NAME_CACHE.get(ex)
        if cached and (time.time() - cached[0] < _MIRROR_SYM_NAME_TTL):
            out.extend(cached[1])
            continue
        names = financials_store.get_names_bulk(BASE_DIR, f"FINN:{ex}:")
        suffix = ".HK" if ex == "HK" else ""
        entries = [{"symbol": sym + suffix, "market": ex, "name": names.get(sym)}
                   for sym in mirror_syms.get(ex, [])]
        _MIRROR_SYM_NAME_CACHE[ex] = (time.time(), entries)
        out.extend(entries)
    return jsonify({"stocks": out})


_screener_mirror_cache: dict = {}
_screener_mirror_lock = threading.Lock()


def _screener_mirror_cache_key(uni):
    def _m(p):
        try:
            return os.path.getmtime(p)
        except OSError:
            return None
    metrics_file = "us_index_metrics.json" if uni == "us" else "hk_index_metrics.json"
    # factor_mirror_at เปลี่ยนทุกครั้งที่ build_mirror_snapshot รัน (ทั้ง US+HK ใช้ meta key
    # เดียวกัน — rebuild ตลาดหนึ่งจะ bust cache ของอีกตลาดไปด้วย ไม่เป็นไร แค่ recompute เกิน
    # ความจำเป็นนานๆ ครั้ง ไม่ใช่ทุก request) + mtime ของ index metrics json (overlay technical
    # เปลี่ยนตาม Index Sync/Quick Update ที่ไม่ได้ผูกกับ build_mirror_snapshot)
    return (financials_store._get_meta(BASE_DIR, "factor_mirror_at"),
            _m(os.path.join(BASE_DIR, "data", metrics_file)))


def _get_screener_mirror_rows(uni):
    """cache rows ของ Screener+ US/HK หลัง overlay technical แล้ว ต่อ universe — เดิมไม่มี
    cache เลย ทุก request (สลับตลาด/ติ๊ก filter/เรียงคอลัมน์) ยิง scan+overlay ใหม่ทั้งก้อน
    วัดจริง US ~0.3-1.7 วิ/ครั้ง ทั้งที่ผลลัพธ์เหมือนเดิมทุกครั้งจนกว่าข้อมูลต้นทางจะเปลี่ยนจริง
    (build_mirror_snapshot/Index Sync) — key ตาม _screener_mirror_cache_key() auto-bust ตรงจุด
    ไม่ต้องเดา TTL (ต่างจาก _sector_compare_cache ที่ไม่มีตัวบอกเวอร์ชันชัดเจนแบบนี้)

    return list เดียวกันข้ามหลาย request ได้อย่างปลอดภัย — filter/sort ท้าย factor_screener()
    อ่านอย่างเดียว ไม่เคยเขียนทับ field ของ row ที่ cache ไว้"""
    key = _screener_mirror_cache_key(uni)
    with _screener_mirror_lock:
        cached = _screener_mirror_cache.get(uni)
        if cached and cached[0] == key:
            return cached[1]
        rows = _build_screener_mirror_rows(uni)
        _screener_mirror_cache[uni] = (key, rows)
        return rows


def _build_screener_mirror_rows(uni):
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
            # fcf_yield ต้องใช้ mkt_cap สด — us_index_metrics.json ไม่เก็บ mkt_cap เลย
            # (ยืนยันแล้ว 0/518 ตัวมีค่า เหมือนที่ /api/tearsheet เจอ — ดูคอมเมนต์
            # "us/hk_index_metrics.json ไม่เก็บ mkt_cap" ในฟังก์ชัน tearsheet) คำนวณเองจาก
            # price สด × shares_out ล่าสุด (งบ Yahoo annual, มาจาก factor_snapshot อยู่แล้ว
            # ใน r) แทน — มีค่าเฉพาะหุ้นในดัชนีหลักที่มี price สด ตัวนอกดัชนียังเป็น None
            mc = s.get("mkt_cap") if s else None
            if mc is None and s and s.get("price") and r.get("shares_out"):
                mc = s["price"] * r["shares_out"]
            r["fcf_yield"] = (r["fcf"] / mc * 100) if (r.get("fcf") is not None and mc) else None
            # ซ่อน Z-Score ของกลุ่มการเงิน (แบงก์/ประกัน) เหมือนที่ /api/tearsheet และ
            # /api/peer-compare ทำไว้แล้ว (เจอตอนรีวิว 2026-08-01 ว่า Screener+ ยังไม่ซ่อน
            # ทำให้ JPM/BAC ฯลฯ ที่มีงบ Yahoo sync แล้วโชว์ Z-Score หลอกตาในตาราง ทั้งที่
            # สูตร Altman ใช้ไม่ได้กับงบดุลสถาบันการเงิน) — เช็คได้เฉพาะสมาชิกดัชนีหลักที่มี
            # sector จาก us_index_metrics.json เท่านั้น (ตัวนอกดัชนีไม่มี sector ให้เช็ค)
            if s and s.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES:
                r["z_score"], r["z_zone"] = None, None
                r["z_excluded_reason"] = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"
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
            mc = s.get("mkt_cap") if s else None
            if mc is None and s and s.get("price") and r.get("shares_out"):
                mc = s["price"] * r["shares_out"]
            r["fcf_yield"] = (r["fcf"] / mc * 100) if (r.get("fcf") is not None and mc) else None
            # ซ่อน Z-Score ของกลุ่มการเงิน — เหตุผลเดียวกับ branch US ด้านบน (HK มีทั้ง
            # "Financials"/"Finance" แยกกัน ดู FINANCIAL_SECTOR_NAMES — ครอบ HSBC/HKEX/
            # AIA/BOCHK ด้วย ไม่ใช่แค่แบงก์จีน)
            if s and s.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES:
                r["z_score"], r["z_zone"] = None, None
                r["z_excluded_reason"] = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"
    return rows


@app.route("/api/factor-screener")
def factor_screener():
    """Deep Screener — ตารางปัจจัยพื้นฐานต่อหุ้น (รวม Finnomena+Yahoo+SET) จาก factor_snapshot
    (local-only, precompute ด้วย build_snapshot.py) overlay pe/mkt_cap สดจาก set_data.json
    เพื่อคำนวณ peg/fcf_yield ที่อิงราคาปัจจุบัน — คืน {rows: [...], meta: {...}}

    ?universe=us / hk : คืนหุ้น mirror ทั้งตลาด (นอก universe หลัก, งบ Finnomena ล้วน)
    แทนชุดหลัก (ไทย+DR) — โหลดแยก เพราะชุดใหญ่ (US ~11k, HK ~2k) rows หลัง overlay cache ไว้
    (ดู _get_screener_mirror_rows) filter/sort/limit ด้านล่างคำนวณสดทุกครั้งเพราะเบามาก"""
    uni = (request.args.get("universe") or "").lower()
    if uni in ("us", "hk"):
        # ชุด mirror ใหญ่มาก (US ~17k) — กรองฝั่ง server ส่งเฉพาะผลลัพธ์ (≤ limit)
        rows = _get_screener_mirror_rows(uni)

        try:
            filters = json.loads(request.args.get("filters") or "[]")
            # ต้องเป็น list ของ dict {k,cmp,v} เท่านั้น — filters ที่ parse JSON ผ่านแต่ shape
            # ผิด (เช่น URL ถูกแก้มือ/บุ๊คมาร์คเก่าค้าง) จะทำ c.get(...) ใน _keep() ด้านล่าง
            # throw AttributeError ถ้าไม่กันตรงนี้ (ปกติ UI ส่ง shape ถูกเสมอ แต่กันไว้เผื่อ)
            if not isinstance(filters, list) or not all(isinstance(c, dict) for c in filters):
                filters = []
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
            # fcf_yield ของ DR หายไปเงียบๆ มาตลอด (เจอตอนรีวิว 2026-08-01) — mkt_cap ของ DR
            # ไม่ได้อยู่ใน r ตั้งแต่ factor_snapshot (นั่นมีแต่หุ้นไทย/mirror) ต้องอ่านจาก
            # dr_cache.json (คีย์ 'mkt_cap') เหมือน /api/fin-analytics ทำไว้แล้ว (ดูคอมเมนต์
            # "mkt_cap หุ้น DR ไม่ได้อยู่ใน set_data.json" ด้านบน) ไม่งั้น filter FCF Yield ≥
            # จะตัดหุ้น DR ทิ้งหมดทุกตัว (ไม่มีค่า = ไม่ผ่าน filter ปกติ)
            mc = e.get("mkt_cap")
            r["mkt_cap"] = mc
            r["fcf_yield"] = (r["fcf"] / mc * 100) if (r.get("fcf") is not None and mc) else None

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


def _widen_sector_group(candidates, level, group_key, base=None, sector_q=None):
    """คืน (members, level, group_key, widened) จาก candidates ({symbol: entry}) ตาม level/
    group_key ที่ให้มา — auto ขยับจาก sector เป็น industry เองถ้าสมาชิก < 4 ตัวและขยายแล้วได้
    กลุ่มใหญ่ขึ้นจริง ใช้ร่วมกันระหว่าง /api/peer-compare และ /api/financials-sankey-sector
    (เดิม copy โค้ดสองที่ก่อน drift กัน — รวมเป็นจุดเดียวแก้ทีเดียวจบ)

    caller กรอง self/หุ้นที่ไม่ต้องการนับออกจาก candidates ก่อนเรียกเองถ้าต้องการ (เช่น
    financials-sankey-sector กรองตัวเอง+หุ้นกลุ่มการเงินออกก่อน กันนับพองจน threshold <4
    ไม่ trigger ทั้งที่เหลือ peer จริงน้อยกว่านั้น) — peer_compare ส่ง th_map เต็มแบบเดิม"""
    members = [s for s, v in candidates.items() if v.get(level) == group_key]
    widened = False
    if level == "sector" and len(members) < 4:
        ind = (base or {}).get("industry") if base else None
        if not ind and sector_q:
            sample = candidates.get(members[0]) if members else None
            ind = sample.get("industry") if sample else None
        if ind:
            wide_members = [s for s, v in candidates.items() if v.get("industry") == ind]
            if len(wide_members) > len(members):
                members, level, group_key, widened = wide_members, "industry", ind, True
    return members, level, group_key, widened


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
        if level == "industry":
            # ปุ่ม "ขยับไปดู Industry" ตอนไม่มีหุ้นตั้งต้น (_peerWiden ส่ง sector เดิม + level=
            # industry ตรงๆ) — sector_q ยังเป็นชื่อ sector ต้องหา industry จากสมาชิกกลุ่มนั้น
            # ก่อน ไม่งั้นจะเอาไปเทียบกับ field 'industry' ตรงๆ แล้วไม่เจอสมาชิกเลย (ดู
            # auto-widen ด้านล่างที่ทำแบบเดียวกันสำหรับกรณี sector เริ่มต้นสมาชิกน้อยไป)
            sample = next((s for s in th_map.values() if s.get("sector") == sector_q), None)
            ind = sample.get("industry") if sample else None
            if ind:
                group_key = ind
            else:
                level = "sector"
    elif base:
        group_key = base.get(level) or base.get("sector")
        level = level if base.get(level) else "sector"
    elif symbol:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": f"ไม่พบหุ้น {symbol} ในตลาด {mkt}"}})
    else:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": "ต้องระบุ symbol หรือ sector"}})
    if not group_key:
        return jsonify({"rows": [], "median": None, "count": 0,
                        "meta": {"note": f"หุ้น {symbol} ไม่มีข้อมูล {level}"}})

    members, level, group_key, widened = _widen_sector_group(
        th_map, level, group_key, base=base, sector_q=sector_q)

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


def _peer_group_quarters(sym, con=None):
    """ดึงงบไตรมาสผสาน (Finnomena+Yahoo+SET) ของหุ้นไทย 1 ตัว คืน list ไตรมาส (เก่า->ใหม่) หรือ
    None ถ้าไม่มีข้อมูลเลย — logic เดียวกับ /api/financials-merged-report แต่ตัด SET
    company-highlight annual override ออก (ใช้แค่รายไตรมาสในเอนด์พอยท์นี้ ไม่มีมุมมองรายปี)

    con: ส่ง sqlite connection ที่เปิดไว้แล้วมาให้ get() reuse ได้ (ดู financials_store.get)
    — ใช้ตอน peer_group_detail() วนเรียกฟังก์ชันนี้สูงสุด 100 หุ้น/request เดิมเปิด-ปิด
    connection ใหม่ทุกครั้งที่ get() (อย่างน้อย 2 ครั้ง/หุ้น = สูงสุด 200 connection/request)
    เฉพาะ path อ่านข้อมูล (get) เท่านั้นที่ reuse — path ดึงสด/upsert ตอน cache miss ยังเปิด
    connection ของตัวเอง (เขียนไม่บ่อย ไม่ใช่คอขวด และแยก concern เขียน/อ่านชัดเจนกว่า)"""
    finn = financials_store.get(BASE_DIR, sym, "finnomena_q", con=con)
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q", con=con)
        except Exception:
            finn = None
    yq = financials_store.get(BASE_DIR, sym, "yahoo_q", con=con)
    if yq is None:
        try:
            fresh = financials_store.fetch_yahoo_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "yahoo_q", fresh)
            yq = financials_store.get(BASE_DIR, sym, "yahoo_q")
        except Exception:
            yq = None
    if not finn and not yq:
        return None
    try:
        set_series = financials_store.get_set_qpl_series(BASE_DIR, sym)
    except Exception:
        set_series = None
    if set_series is None:
        try:
            set_series = financials_store.sync_set_qpl_series(BASE_DIR, sym)
        except Exception:
            set_series = None
    return financials_store.compute_full_report(finn, yq, set_series=set_series)["quarters"]


def _ttm_sum(quarters, field):
    """ผลรวม field ของ 4 ไตรมาสล่าสุด — None ถ้าไม่ครบ 4 ไตรมาส หรือมีงวดไหนไม่มีค่า field นี้เลย
    (กันยอด TTM เพี้ยนจากการนับไม่ครบ)"""
    if len(quarters) < 4:
        return None
    vals = [q.get(field) for q in quarters[-4:]]
    if any(v is None for v in vals):
        return None
    return sum(vals)


def _peer_group_pct_change(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 2)


def _avg_last4(quarters, field):
    """ค่าเฉลี่ย field จาก 4 ไตรมาสล่าสุดที่มีค่าจริง (ไม่ต้องครบ 4 ไตรมาสเป๊ะเหมือน _ttm_sum —
    ใช้กับ field งบดุล ณ จุดเวลา เช่น total_assets/inventory/accounts_receivable ที่เอาไว้หา
    ค่าเฉลี่ยไตรมาสสำหรับอัตราส่วนหมุนเวียน) คืน None ถ้าไม่มีค่าเลยสักไตรมาส"""
    vals = [q.get(field) for q in quarters[-4:] if q.get(field) is not None]
    return (sum(vals) / len(vals)) if vals else None


def _latest_value(quarters, field, lookback=4):
    """ค่า field ล่าสุดที่ไม่ใช่ None ย้อนดูสูงสุด lookback ไตรมาสจากท้ายสุด — งบดุล/กระแสเงินสด
    รายไตรมาสจาก Yahoo มักตามหลัง P&L ที่มาจาก SET/Finnomena อยู่ 1 งวด (ไตรมาสล่าสุดสุดของ
    total_assets/total_debt/current_assets ฯลฯ อาจยังไม่มีค่าทั้งที่ revenue/net_profit มาแล้ว)
    ใช้แทน quarters[-1].get(field) ตรงๆ สำหรับอัตราส่วน ณ จุดเวลาที่ต้องการค่าล่าสุดเท่าที่มีจริง"""
    for q in reversed(quarters[-lookback:]):
        v = q.get(field)
        if v is not None:
            return v
    return None


@app.route("/api/peer-group-detail")
def peer_group_detail():
    """🆚 มุมมอง 'การ์ดเทียบกลุ่ม' ของหน้า Peer Compare — รายละเอียดงบ TTM + รายไตรมาสล่าสุดของ
    หุ้นทุกตัวในกลุ่ม sector/industry เดียวกันที่ frontend ส่งมา (ปกติทั้งกลุ่มจาก /api/peer-compare
    — ผู้ใช้ตัด/เพิ่มหุ้นเองได้ผ่าน checkbox) ต่างจาก /api/peer-compare ที่คืนตัวเลข factor รายปี
    จาก factor_snapshot — endpoint นี้คำนวณ TTM (ผลรวม 4 ไตรมาสล่าสุด) สดจากงบไตรมาสผสาน
    (Finnomena+Yahoo+SET) ต่อหุ้น ให้ตรงกับสเปกที่ผู้ใช้ขอ (Margins/ROE/Valuation "TTM 4Q")
    ส่วน Cash Cycle/DIO/DSO/DPO ยังอิง factor_snapshot (รายปีจากงบ Yahoo — ไม่มี Inventory/AR/AP
    รายไตรมาสในงบผสาน ดู compute_cash_cycle) เฉพาะหุ้นไทยเท่านั้นในเวอร์ชันนี้ — cap ที่ 100 ตัว
    กันคำขอผิดปกติ (sector ไทยใหญ่สุดจริงมีแค่ ~64 ตัว ดู set_data.json)"""
    mkt = (request.args.get("market") or "TH").upper()
    if mkt != "TH":
        return jsonify({"rows": [], "meta": {"note": "มุมมองการ์ดเทียบกลุ่มรองรับเฉพาะหุ้นไทยในเวอร์ชันนี้"}}), 501

    symbols = [s.strip().upper() for s in (request.args.get("symbols") or "").split(",") if s.strip()]
    if not symbols:
        return jsonify({"rows": [], "meta": {"note": "ต้องระบุ symbols"}})
    symbols = symbols[:100]
    base_symbol = (request.args.get("base") or "").upper().strip() or None

    th_map = _tearsheet_universe_map("TH")
    snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}

    # connection เดียวใช้ร่วมกันตลอด loop (reuse ผ่าน con= ใน financials_store.get) — เดิมเปิด-ปิด
    # ใหม่อย่างน้อย 2 ครั้ง/หุ้น (finnomena_q + yahoo_q) สูงสุด 200 connection/request (100 หุ้น)
    con = financials_store._connect(BASE_DIR) if financials_store.db_exists(BASE_DIR) else None
    try:
        rows = []
        for sym in symbols:
            entry = th_map.get(sym) or {}
            f = snap_rows.get(sym) or {}
            quarters = _peer_group_quarters(sym, con=con)
            if not quarters:
                rows.append({"symbol": sym, "name": entry.get("name") or sym, "no_data": True})
                continue

            last = quarters[-1]
            prior_q = quarters[-2] if len(quarters) >= 2 else None
            yoy_q = quarters[-5] if len(quarters) >= 5 else None

            rev_ttm = _ttm_sum(quarters, "revenue")
            np_ttm = _ttm_sum(quarters, "net_profit")
            gp_ttm = _ttm_sum(quarters, "gross_profit")
            op_ttm = _ttm_sum(quarters, "operating_profit")
            cogs_ttm = _ttm_sum(quarters, "cogs")
            cfo_ttm = _ttm_sum(quarters, "cfo")
            cfi_ttm = _ttm_sum(quarters, "cfi")
            capex_ttm = _ttm_sum(quarters, "capex")
            fcf_approx = (cfo_ttm + cfi_ttm) if (cfo_ttm is not None and cfi_ttm is not None) else None

            equity_avg = _avg_last4(quarters, "total_equity")
            assets_avg = _avg_last4(quarters, "total_assets")
            inventory_avg = _avg_last4(quarters, "inventory")
            ar_avg = _avg_last4(quarters, "accounts_receivable")

            mkt_cap = entry.get("mkt_cap")
            pe = (mkt_cap / np_ttm) if (mkt_cap and np_ttm and np_ttm > 0) else f.get("pe_value")
            # PBV จาก mkt_cap/equity งวดล่าสุด (สอดคล้องกับ P/E ที่ TTM ด้านบน) — fallback ไป
            # pbv_value ของ factor_snapshot (Finnomena) ถ้า total_equity รายไตรมาสไม่มี (ดู docstring
            # ของฟังก์ชันนี้ — งบดุลรายไตรมาสมีแค่บาง symbol) — ใช้ _latest_value แทน last.get() ตรงๆ
            # เพราะงบดุลจาก Yahoo มักตามหลัง P&L อยู่ 1 งวด (ไตรมาสล่าสุดสุดมี revenue/net_profit
            # แต่ total_equity/total_debt/current_assets ยังไม่ sync มา)
            equity_latest = _latest_value(quarters, "total_equity")
            pbv = (mkt_cap / equity_latest) if (mkt_cap and equity_latest and equity_latest > 0) else f.get("pbv_value")
            ibd_latest = _latest_value(quarters, "total_debt")
            current_assets_latest = _latest_value(quarters, "current_assets")
            current_liabilities_latest = _latest_value(quarters, "current_liabilities")
            cash_latest = _latest_value(quarters, "cash")
            # ROIC = NOPAT TTM (กำไรจากการดำเนินงาน หลังหักภาษี) ÷ Invested Capital (งวดล่าสุด) — สูตร
            # เดียวกับ ROIC (TTM) ใน "📌 อัตราส่วนหลัก" ของหน้างบรวมทุกแหล่ง (dashboard.js บรรทัด
            # ~17850) ทำตรงนี้ให้ตรงกันเป๊ะ (เดิมเคยลองใช้ flat 20% แทน แต่จะได้ตัวเลข ROIC ไม่ตรงกับ
            # ที่หน้าอื่นโชว์สำหรับหุ้นตัวเดียวกัน — clamp อัตราภาษี 0-60% กันไตรมาสขาดทุน/มีรายการพิเศษ
            # ทำให้ tax_expense/pretax_profit เพี้ยนสุดขั้ว)
            pretax_ttm = _ttm_sum(quarters, "pretax_profit")
            tax_ttm = _ttm_sum(quarters, "tax_expense")
            roic = None
            if (op_ttm is not None and pretax_ttm and tax_ttm is not None
                    and ibd_latest is not None and equity_latest is not None and cash_latest is not None):
                tax_rate = min(0.6, max(0, tax_ttm / abs(pretax_ttm)))
                invested_capital = ibd_latest + equity_latest - cash_latest
                if invested_capital > 0:
                    roic = round(op_ttm * (1 - tax_rate) / invested_capital * 100, 2)

            row = {
                "symbol": sym, "name": entry.get("name") or sym,
                "sector": entry.get("sector"), "industry": entry.get("industry"),
                "mkt_cap": mkt_cap,
                "revenue_ttm": rev_ttm, "net_profit_ttm": np_ttm,
                "gpm": round(gp_ttm / rev_ttm * 100, 2) if (gp_ttm is not None and rev_ttm) else None,
                "opm": round(op_ttm / rev_ttm * 100, 2) if (op_ttm is not None and rev_ttm) else None,
                "npm": round(np_ttm / rev_ttm * 100, 2) if (np_ttm is not None and rev_ttm) else None,
                "roe": round(np_ttm / equity_avg * 100, 2) if (np_ttm is not None and equity_avg) else f.get("roe"),
                "roa": f.get("roa"),
                "roic": roic,
                "de_ratio": f.get("de_ratio"),
                "pe": round(pe, 2) if pe is not None else None,
                "pbv": round(pbv, 2) if pbv is not None else None,
                "div_yield": entry.get("div_yield"),
                "peg": f.get("peg"),
                "interest_coverage": f.get("interest_coverage"),
                "f_score": f.get("f_score"), "f_score_max": f.get("f_score_max"),
                "z_score": f.get("z_score"), "z_zone": f.get("z_zone"), "z_variant": f.get("z_variant"),
                "z_excluded_reason": f.get("z_excluded_reason"),
                "rev_cagr": f.get("rev_cagr"), "profit_cagr": f.get("profit_cagr"),
                "cfo_ttm": cfo_ttm, "cfi_ttm": cfi_ttm, "fcf_approx": fcf_approx,
                "cfo_ni_ratio": round(cfo_ttm / np_ttm, 2) if (cfo_ttm is not None and np_ttm) else None,
                "fcf_margin": round(fcf_approx / rev_ttm * 100, 2) if (fcf_approx is not None and rev_ttm) else None,
                "cfo_margin": round(cfo_ttm / rev_ttm * 100, 2) if (cfo_ttm is not None and rev_ttm) else None,
                "cash_cycle": f.get("cash_cycle"), "dio": f.get("dio"), "dso": f.get("dso"), "dpo": f.get("dpo"),
                "q_revenue": last.get("revenue"), "q_cogs": last.get("cogs"), "q_sga": last.get("sga_total"),
                "q_ebit": last.get("operating_profit"), "q_net_profit": last.get("net_profit"),
                "q_cfo": last.get("cfo"), "total_assets": last.get("total_assets"),
                "total_equity": last.get("total_equity"), "ibd": last.get("total_debt"),
                "q_net_profit_qoq": _peer_group_pct_change(last.get("net_profit"), (prior_q or {}).get("net_profit")),
                "q_net_profit_yoy": _peer_group_pct_change(last.get("net_profit"), (yoy_q or {}).get("net_profit")),
                "q_revenue_qoq": _peer_group_pct_change(last.get("revenue"), (prior_q or {}).get("revenue")),
                "q_revenue_yoy": _peer_group_pct_change(last.get("revenue"), (yoy_q or {}).get("revenue")),
                "q_cfo_yoy": _peer_group_pct_change(last.get("cfo"), (yoy_q or {}).get("cfo")),
                "ttm_net_profit_yoy": None, "net_profit_ttm_prior": None,
                "rev_cagr_3y": None, "profit_cagr_3y": None,
                # อัตราส่วนสภาพคล่อง/หนี้/ประสิทธิภาพใช้สินทรัพย์ — คำนวณจาก field งบดุลรายไตรมาสที่
                # ผสานไว้แล้ว (ดู _BSCF_RAW_MAP ใน financials_store.py) ไม่ต้องดึงข้อมูลเพิ่ม
                "current_ratio": round(current_assets_latest / current_liabilities_latest, 2)
                    if (current_assets_latest is not None and current_liabilities_latest)
                    else None,
                "ibd_equity": round(ibd_latest / equity_latest, 2)
                    if (ibd_latest is not None and equity_latest) else None,
                "asset_turnover": round(rev_ttm / assets_avg, 2) if (rev_ttm is not None and assets_avg) else None,
                "inventory_turnover": round(cogs_ttm / inventory_avg, 2) if (cogs_ttm is not None and inventory_avg) else None,
                "receivable_turnover": round(rev_ttm / ar_avg, 2) if (rev_ttm is not None and ar_avg) else None,
                "capex_rev": round(abs(capex_ttm) / rev_ttm * 100, 2) if (capex_ttm is not None and rev_ttm) else None,
                "quarters_available": len(quarters), "ttm_partial": rev_ttm is None,
            }
            # YoY ของ TTM กำไรสุทธิ — เทียบ TTM ปัจจุบันกับ TTM ที่จบเมื่อ 4 ไตรมาสก่อน (ต้องมีอย่างน้อย
            # 8 ไตรมาสถึงจะมีข้อมูลพอทำ TTM สองช่วงที่ไม่ทับกัน)
            if len(quarters) >= 8 and np_ttm is not None:
                prior_ttm = _ttm_sum(quarters[:-4], "net_profit")
                row["ttm_net_profit_yoy"] = _peer_group_pct_change(np_ttm, prior_ttm)
                row["net_profit_ttm_prior"] = prior_ttm
            # CAGR 3 ปีเป๊ะ (ต่างจาก rev_cagr/profit_cagr ของ factor_snapshot ที่เป็นเต็มช่วงข้อมูล) —
            # เทียบ TTM ปัจจุบันกับ TTM ที่จบเมื่อ 12 ไตรมาสก่อน ต้องมีอย่างน้อย 16 ไตรมาสถึงจะมีข้อมูล
            # พอทำ TTM สองช่วงที่ไม่ทับกัน (เฉพาะกรณีทั้งคู่เป็นบวก — ฐานลบ/ศูนย์ทำ CAGR ไม่มีความหมาย)
            if len(quarters) >= 16:
                rev_ttm_3y_ago = _ttm_sum(quarters[:-12], "revenue")
                if rev_ttm and rev_ttm_3y_ago and rev_ttm > 0 and rev_ttm_3y_ago > 0:
                    row["rev_cagr_3y"] = round(((rev_ttm / rev_ttm_3y_ago) ** (1 / 3) - 1) * 100, 2)
                np_ttm_3y_ago = _ttm_sum(quarters[:-12], "net_profit")
                if np_ttm and np_ttm_3y_ago and np_ttm > 0 and np_ttm_3y_ago > 0:
                    row["profit_cagr_3y"] = round(((np_ttm / np_ttm_3y_ago) ** (1 / 3) - 1) * 100, 2)
            rows.append(row)
    finally:
        if con is not None:
            con.close()

    return jsonify({"rows": rows, "meta": {"base_symbol": base_symbol, "market": mkt}})


# field ที่ต้องใช้สร้างต้นไม้ Sankey ฝั่ง sector — ตรงกับ stages ที่ _renderFinSankeySvg (dashboard.js)
# วาด (Rev -> COGS/GP -> SG&A/EBIT -> FinCost/Pretax -> Tax/NetProfit) ไม่รวม 'revenue' เพราะ
# revenue ของฝั่ง sector ใช้ของหุ้นตั้งต้นเอง (ให้สองต้นไม้สเกลเท่ากัน เทียบด้วยตาง่ายกว่า)
_SANKEY_SECTOR_PCT_FIELDS = ['cogs', 'gross_profit', 'sga_total', 'operating_profit',
                             'financial_cost', 'pretax_profit', 'tax_expense', 'net_profit']


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


@app.route("/api/financials-sankey-sector/<symbol>")
def get_financials_sankey_sector(symbol):
    """ข้อมูลเปรียบเทียบสำหรับปุ่ม '🆚 เทียบกับกลุ่ม' บนการ์ด Income Sankey Diagram (แท็บ
    🧩 งบรวมทุกแหล่ง) — คำนวณ median ของแต่ละ % ต่อรายได้ (COGS%, GPM%, SG&A%, OPM%,
    ต้นทุนการเงิน%, กำไรก่อนภาษี%, ภาษี%, NPM%) ข้ามทุกหุ้นในกลุ่ม sector/industry เดียวกับ
    symbol — ใช้ median ของอัตราส่วน (ไม่ใช่ weighted-sum) ตามที่ผู้ใช้เลือก เพื่อไม่ให้หุ้นใหญ่
    ตัวเดียวครอบงำภาพรวม "โครงสร้างต้นทุนทั่วไปของกลุ่ม" — reuse การจัดกลุ่ม sector/widen เป็น
    industry เดียวกับ /api/peer-compare (ดู group_key/widened ด้านล่าง) และ TTM ต่อหุ้นเดียวกับ
    /api/peer-group-detail (_peer_group_quarters + _ttm_sum) เฉพาะหุ้นไทยเท่านั้น (เหมือน
    peer-group-detail — sector/industry grouping มีแค่ TH ใน set_data.json)

    ⚠ ข้อจำกัดของ median ratio: median ของแต่ละ % คำนวณแยกอิสระต่อ field ไม่ได้บังคับอัตลักษณ์
    ทางบัญชี (เช่น cogs%+gpm% อาจไม่รวมเป็น 100% เป๊ะ) เพราะเป็นค่ากลางของหุ้นคนละบริษัท ไม่ใช่
    งบของบริษัทเดียว — frontend ต้องอธิบายจุดนี้ให้ผู้ใช้เห็นชัด (tip/helper text) ไม่ใช่นำเสนอ
    เหมือนงบจริงบริษัทเดียว"""
    sym = symbol.upper().strip()
    th_map = _tearsheet_universe_map("TH")
    base = th_map.get(sym)
    if not base:
        return jsonify({"error": f"ไม่พบหุ้น {sym} ในข้อมูล sector ของหุ้นไทย"}), 404

    fin_syms = factor_snapshot._financial_sector_symbols(BASE_DIR)
    if sym in fin_syms:
        return jsonify({"error": "หุ้นกลุ่มการเงิน/ธนาคาร ไม่มีแนวคิด COGS/กำไรขั้นต้นแบบธุรกิจทั่วไป "
                                  "— เทียบกลุ่มแบบนี้ไม่มีความหมาย"}), 400

    level = "sector"
    group_key = base.get("sector")
    if not group_key:
        return jsonify({"error": f"หุ้น {sym} ไม่มีข้อมูล sector"}), 404

    # ตัดตัวเอง + หุ้นกลุ่มการเงินออกจาก "ผู้สมัคร" ก่อนนับ/ตัดสินใจ widen เสมอ (เทียบกับ "กลุ่ม
    # อื่นที่ไม่ใช่ตัวเอง") — เดิมนับก่อนตัดออกทำให้ sector ที่มีสมาชิกดิบพอดี 4 ตัว (ตัวเอง+peer
    # จริงแค่ 3) ไม่ trigger widen ทั้งที่เหลือ peer จริงน้อยกว่า threshold
    candidates = {s: v for s, v in th_map.items() if s not in fin_syms and s != sym}
    members, level, group_key, widened = _widen_sector_group(candidates, level, group_key, base=base)
    # cap กันคำขอผิดปกติ (sector ไทยใหญ่สุดจริงมีแค่ ~64 ตัว ดู set_data.json — เหมือน peer_group_detail)
    members = members[:80]
    if not members:
        return jsonify({"error": f"กลุ่ม {group_key} ไม่มีหุ้นอื่นให้เทียบ"}), 404

    pct_samples = {f: [] for f in _SANKEY_SECTOR_PCT_FIELDS}
    con = financials_store._connect(BASE_DIR) if financials_store.db_exists(BASE_DIR) else None
    try:
        for s in members:
            # ห่อ per-member กันหุ้นตัวเดียวพัง (DB lock ชั่วคราว/ข้อมูลรูปแบบแปลก)
            # ทำให้ทั้ง endpoint 500 แทนที่จะแค่ข้ามหุ้นตัวนั้นไปเหมือนสมาชิกอื่น
            try:
                quarters = _peer_group_quarters(s, con=con)
                if not quarters:
                    continue
                rev_ttm = _ttm_sum(quarters, "revenue")
                if not rev_ttm or rev_ttm <= 0:
                    continue
                for f in _SANKEY_SECTOR_PCT_FIELDS:
                    v = _ttm_sum(quarters, f)
                    if v is not None:
                        pct_samples[f].append(v / rev_ttm * 100)
            except Exception:
                continue
    finally:
        if con is not None:
            con.close()

    median_pct = {f: _median(pct_samples[f]) for f in _SANKEY_SECTOR_PCT_FIELDS}
    coverage = {f: len(pct_samples[f]) for f in _SANKEY_SECTOR_PCT_FIELDS}

    return jsonify({
        "sector_name": group_key, "level": level, "widened": widened,
        "member_count": len(members), "median_pct": median_pct, "coverage": coverage,
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
    # yf ticker เต็มอยู่แล้ว (มี .HK/.T ต่อท้ายให้ถ้าเป็น HK/JP) ส่วน US/HK/JP ที่เข้าตรงๆ
    # (ไม่ผ่าน DR) sym ก็เป็น yf ticker ที่ถูกต้องอยู่แล้วเหมือนกัน (มาจาก th_map ที่เก็บ
    # symbol พร้อม suffix ตาม convention เดียวกับ us/hk/jp_index_metrics.json ทุกจุดในแอป)
    # ห้ามเอาไปเข้า mirror_ondemand._yf_ticker() ซ้ำ — ฟังก์ชันนั้นออกแบบมาสำหรับ "รหัสดิบไม่มี
    # suffix" (namespace mirror) คนละบริบทกัน เดิมเรียกซ้ำแล้วได้ suffix HK ซ้อนสอง (เช่น
    # "0700.HK.HK") ทำให้ /api/live-price ของทุก Tearsheet หุ้น HK ได้ 500 กลับมาเงียบๆ
    # (frontend ดัก .catch(()=>{}) ไว้ ไม่มี error โชว์ แค่ราคาสด/PE/PBV/PS ไม่อัพเดท —
    # พบจากทดสอบจริงในเบราว์เซอร์ 2026-08-02)
    if lite:
        yf_symbol = lite_yf
    elif mkt == "TH":
        yf_symbol = f"{sym}.BK"
    else:
        yf_symbol = sym

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
    if not is_financial:
        # DCF Model (forecast เต็ม) — ดึงงบ Yahoo สดตรงนี้เอง (ไม่พึ่ง factor_snapshot ที่ cache
        # ไว้ล่วงหน้า) เพื่อให้ field ใหม่นี้ใช้ได้ทันทีไม่ต้องรอ rebuild snapshot ทั้ง universe —
        # key/is_dr ต้องตรงกับ namespace ที่ _factors_for ใช้จริงในแต่ละ path (ดู _mirror_sym
        # ด้านบน): lite -> 'DR:{lite_dr_sym}', TH -> รหัสดิบตรงๆ, mirror US/HK/JP -> 'FINN:{mkt}:{fkey}'
        if lite:
            yahoo_key, yahoo_is_dr = lite_dr_sym, True
        elif mkt == "TH":
            yahoo_key, yahoo_is_dr = sym, False
        else:
            yahoo_key, yahoo_is_dr = f"FINN:{mkt}:{fkey}", False
        y_payload = financials_store.get(BASE_DIR, yahoo_key, "yahoo", is_dr=yahoo_is_dr)
        if y_payload:
            dcf["forecast"] = financials_store.compute_dcf_forecast_inputs(y_payload)

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


@app.route("/api/dcf-screener")
def dcf_screener_endpoint():
    """🎯 DCF Screener — ผลลัพธ์ DCF Model (พยากรณ์เต็มรูปแบบ) ที่คำนวณไว้แล้วทั้งตลาดหุ้นไทย
    (ดู sources/dcf_screener.py) อ่านจาก cache ในตาราง dcf_screener ตรงๆ ไม่คำนวณสด —
    กดปุ่ม "⟳ คำนวณ DCF ใหม่ทั้งตลาด" (POST /api/dcf-screener/rebuild) ก่อนถึงจะมีข้อมูล"""
    return jsonify({"rows": dcf_screener.get_snapshot(BASE_DIR), "meta": dcf_screener.snapshot_meta(BASE_DIR)})


@app.route("/api/dcf-screener/rebuild", methods=["POST"])
def dcf_screener_rebuild():
    """คำนวณ DCF ใหม่ทั้งตลาด — ไม่ยิง network เพิ่ม (อ่านจากงบที่ sync ไว้แล้ว) จึงรันแบบ sync
    ในคำขอเดียวได้เลย ไม่ต้อง background thread/progress bar เหมือน sync งบชุดใหญ่

    body (ไม่ใส่ = ใช้ค่าเริ่มต้นทั้งหมด): {rf_pct, beta, erp_pct, terminal_growth_pct, years}
    — resolve_assumptions() clamp/กันค่าพังให้แล้วฝั่ง dcf_screener"""
    body = request.get_json(silent=True) or {}
    result = dcf_screener.build_snapshot(BASE_DIR, assumptions=body)
    return jsonify(result)


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
    """min_age_days: ไม่ใช้แล้ว (เก็บไว้แค่ backward-compat ของ /api/us-index-sync ที่ยังส่ง
    field นี้มา) — เดิมส่งต่อเป็น sync_all(min_age_days=...) ตรงๆ แต่ sync_all() ไม่มีพารามิเตอร์
    นี้จริง (บั๊กแฝง ไม่เคยเจอเพราะ 'extra' ว่างแทบทุกรอบหลัง sync ครั้งแรก — TypeError จะเกิดขึ้น
    เฉพาะตอนดัชนี reconstitute มีตัวใหม่เท่านั้น) แก้เป็น skip_up_to_date=True (เช็คเนื้อหาจริง
    เทียบ target quarter แทน) ซึ่งพลิกพฤติกรรมจากเดิมด้วย: เดิม sync แค่ตัวที่ 'ยังไม่เคยอยู่ใน
    mirror pool เลย' (one-shot backfill — ตัวที่เคย sync ไปแล้วครั้งเดียวจะไม่ถูกแตะอีกแม้ข้อมูล
    เก่าไปแล้ว) ตอนนี้ sync 'สมาชิกทั้งหมดของดัชนี' ทุกครั้งที่กด แต่ skip_up_to_date จะข้ามตัวที่
    สดอยู่แล้วให้เอง ทำให้กลายเป็น ongoing refresh จริง ไม่ใช่ backfill ครั้งเดียวทิ้ง"""
    try:
        diff, live = us_index_membership.sync_membership(BASE_DIR)
        _us_index_diff_cache.clear()   # ลิสต์เปลี่ยน — ผลเช็คหุ้นใหม่เดิมไม่ตรงแล้ว
        added_n = sum(len(v["new"]) for v in diff.values())
        removed_n = sum(len(v["removed"]) for v in diff.values())

        # ตัดตัวที่อยู่ในพอร์ต DR ที่ curate ไว้แล้วออก — ตัวนั้น sync yahoo_q/yahoo เป็นประจำ
        # อยู่แล้วผ่าน flow DR ปกติ (ดู _dr_financials_universe) กันสร้างแถวซ้ำซ้อนคนละ symbol
        # key สำหรับหุ้นตัวเดียวกัน (US: dr_sym เท่ากับ ticker ตรงๆ เทียบง่ายไม่ต้อง normalize)
        curated_us = {e["sym"] for e in load_dr_universe(BASE_DIR)
                      if not e.get("etf") and "." not in (e.get("yf") or "")}
        targets = sorted({s for lst in live.values() for s in lst} - curated_us)

        def cb(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · {msg}")

        if targets:
            result = financials_store.sync_all(BASE_DIR, targets, sources=("yahoo_q", "yahoo"),
                                               callback=cb, is_dr=True, market="US",
                                               skip_up_to_date=True)
            _clear_fin_analytics_and_warm()
        else:
            result = {"ok": 0, "fail": 0, "total": 0, "skipped": 0}

        # เติมชื่อบริษัทของตัวที่ยังไม่มีชื่อ (ไม่อยู่ใน mirror_names.json) จาก payload ที่เพิ่งดึงมา
        # ให้ปุ่มกรองดัชนีในหน้างบการเงินโชว์ชื่อได้ครบ ไม่ใช่แค่ symbol เฉยๆ
        local = us_index_membership.load_local(BASE_DIR)
        mirror_names_us = _load_mirror_names_us()
        extra_names = dict(local.get("extra_names") or {})
        for sym in targets:
            if sym in mirror_names_us or sym in extra_names:
                continue
            payload = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=True) \
                or financials_store.get(BASE_DIR, sym, "yahoo", is_dr=True)
            if payload and payload.get("name") and payload["name"] != sym:
                extra_names[sym] = payload["name"]
        local["extra_names"] = extra_names
        us_index_membership.save_local(BASE_DIR, local)

        skipped = result.get("skipped", 0)
        _update(done=True,
                message=f"เสร็จแล้ว! ดัชนีอัพเดท +{added_n}/-{removed_n} · งบการเงิน {result['ok']}/{result['total']} สำเร็จ"
                        + (f" · ข้าม {skipped} คู่ (มีงวดล่าสุดอยู่แล้ว)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']})" if result["fail"] else ""))
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


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
    """min_age_days: ไม่ใช้แล้ว ดูเหตุผลใน docstring _run_us_index_sync (bug เดียวกันเป๊ะ —
    sync_all() ไม่มีพารามิเตอร์นี้จริง) แก้เป็น skip_up_to_date=True แทน + เปลี่ยน scope จาก
    'เฉพาะตัวใหม่ที่ยังไม่เคยอยู่ใน mirror pool' เป็น 'สมาชิกทั้งหมดของดัชนี' ทุกครั้งที่กด
    (skip_up_to_date ข้ามตัวที่สดอยู่แล้วให้เอง กลายเป็น ongoing refresh ไม่ใช่ backfill ครั้งเดียว)"""
    try:
        def _sync_cb(current, total, msg):
            _update(current=current, total=total, message=msg)
        diff, live = hk_index_membership.sync_membership(BASE_DIR, progress_cb=_sync_cb)
        _hk_index_diff_cache.clear()   # ลิสต์เปลี่ยน — ผลเช็คหุ้นใหม่เดิมไม่ตรงแล้ว
        added_n = sum(len(v["new"]) for v in diff.values())
        removed_n = sum(len(v["removed"]) for v in diff.values())

        # ตัวในพอร์ต DR ใช้ mnemonic เป็น sym ('AIA'/'TEL') ไม่ใช่ ticker ตัวเลข — ต้อง normalize
        # ticker ดิบจาก field 'yf' (เลข 4 หลักเติม 0 นำหน้า) มาเทียบกับ membership ที่ format
        # เดียวกันอยู่แล้ว ('0700.HK') กันสร้างแถวซ้ำซ้อนคนละ namespace key สำหรับหุ้นตัวเดียวกัน
        def _hk_norm4(code):
            code = (code or "").upper().strip()
            if code.endswith(".HK"):
                code = code[:-3]
            return code.zfill(4) if code.isdigit() else code
        curated_hk = {_hk_norm4(e.get("yf")) for e in load_dr_universe(BASE_DIR)
                      if not e.get("etf") and (e.get("yf") or "").upper().endswith(".HK")}
        members = {s for k in ("HSI", "HSCEI", "HSTECH") for s in live.get(k, [])}
        targets = sorted({s[:-3] for s in members} - curated_hk)

        def cb(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · {msg}")

        if targets:
            result = financials_store.sync_all(BASE_DIR, targets, sources=("yahoo_q", "yahoo"),
                                               callback=cb, is_dr=True, market="HK",
                                               skip_up_to_date=True)
            _clear_fin_analytics_and_warm()
        else:
            result = {"ok": 0, "fail": 0, "total": 0, "skipped": 0}

        # เติมชื่อบริษัทของตัวที่ยังไม่มีชื่อ (ไม่อยู่ใน mirror_names.json) จาก payload ที่เพิ่งดึงมา
        # ให้ปุ่มกรองดัชนีในหน้างบการเงินโชว์ชื่อได้ครบ ไม่ใช่แค่ symbol เฉยๆ
        local = hk_index_membership.load_local(BASE_DIR)
        mirror_names_hk = _load_mirror_names_hk()
        extra_names = dict(local.get("extra_names") or {})
        for sym in targets:
            if sym in mirror_names_hk or sym in extra_names:
                continue
            payload = financials_store.get(BASE_DIR, sym, "yahoo_q", is_dr=True) \
                or financials_store.get(BASE_DIR, sym, "yahoo", is_dr=True)
            if payload and payload.get("name") and payload["name"] != sym:
                extra_names[sym] = payload["name"]
        local["extra_names"] = extra_names
        hk_index_membership.save_local(BASE_DIR, local)

        skipped = result.get("skipped", 0)
        _update(done=True,
                message=f"เสร็จแล้ว! ดัชนีอัพเดท +{added_n}/-{removed_n} · งบการเงิน {result['ok']}/{result['total']} สำเร็จ"
                        + (f" · ข้าม {skipped} คู่ (มีงวดล่าสุดอยู่แล้ว)" if skipped else "")
                        + (f" (ล้มเหลว {result['fail']})" if result["fail"] else ""))
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


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
    (ใช้ `sync_mirror_yahoo_index` — skip-if-exists) แล้ว rebuild factor_snapshot_mirror ให้
    Tearsheet/Peer Compare/F-Score-Z-Score เห็นข้อมูล — **ไม่ใช้ build_mirror_snapshot() ปกติ**
    (ผูกกับ finnomena_q ที่ไม่มีข้อมูล JP เลย) ใช้ build_mirror_snapshot_yahoo_only() แทน

    2026-08-20: เพิ่มขั้น sync yahoo_q (รายไตรมาส) ต่อท้าย — เดิมฟังก์ชันนี้ดึงให้แค่งบรายปี
    ('yahoo') เท่านั้น ทำให้สมาชิก Nikkei 225 นอกพอร์ต DR ไม่มีงบรายไตรมาสเลยสักตัว (ต่างจาก
    US/HK ที่มี sync_all(sources=('yahoo_q','yahoo')) ให้ตัวนอกพอร์ตอยู่แล้วบางส่วน) ใช้
    skip_up_to_date=True เหมือน US/HK ที่เพิ่งแก้ (ongoing refresh ไม่ใช่ backfill ครั้งเดียว)"""
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

        # ตัวในพอร์ต DR ใช้ mnemonic เป็น sym ('TEL'/'FANUC') ไม่ใช่ ticker ตัวเลข — กันสร้างแถว
        # ซ้ำซ้อนคนละ namespace key สำหรับหุ้นตัวเดียวกัน (เหมือน US/HK ด้านบน)
        curated_jp = {(e.get("yf") or "")[:-2].upper() for e in load_dr_universe(BASE_DIR)
                      if not e.get("etf") and (e.get("yf") or "").upper().endswith(".T")}
        q_targets = sorted(set(tickers) - curated_jp)

        def cb_q(current, total, msg):
            _update(current=current, total=total,
                    message=f"ดัชนีอัพเดทแล้ว (+{added_n}/-{removed_n}) · งบรายไตรมาส {msg}")

        if q_targets:
            result_q = financials_store.sync_all(BASE_DIR, q_targets, sources=("yahoo_q",),
                                                  callback=cb_q, is_dr=True, market="JP",
                                                  skip_up_to_date=True)
        else:
            result_q = {"ok": 0, "fail": 0, "total": 0, "skipped": 0}

        _update(message="กำลัง rebuild factor snapshot JP...")
        n_mirror = factor_snapshot.build_mirror_snapshot_yahoo_only(
            BASE_DIR, "JP", tickers, price_by_ticker=price_by_ticker)
        _clear_fin_analytics_and_warm()

        _update(done=True,
                message=f"เสร็จแล้ว! ดัชนี Nikkei 225 อัพเดท +{added_n}/-{removed_n} · "
                        f"งบรายปี {result['ok']} ตัว (ข้าม {result['skipped']} ที่มีอยู่แล้ว"
                        + (f", ล้มเหลว {result['fail']}" if result["fail"] else "") + f") · "
                        f"งบรายไตรมาส {result_q['ok']}/{result_q['total']} สำเร็จ"
                        + (f" (ล้มเหลว {result_q['fail']})" if result_q["fail"] else "") + f" · "
                        f"factor snapshot: {n_mirror} ตัว")
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


def _index_metrics_ts(data):
    """แปลง updated_at ("%Y-%m-%d %H:%M:%S", เวลาที่ build()/update_live_prices() เขียนไฟล์
    ล่าสุดจริง) เป็น epoch วินาที ให้ frontend คำนวณ "ข้อมูล ณ กี่นาทีที่แล้ว" ได้ถูกต้อง —
    เดิมส่ง time.time() (เวลาที่ request เข้ามา) ทำให้โชว์ "0 วิที่แล้ว" ตลอดแม้ไฟล์จะเก่า
    ข้ามวันก็ตาม ถ้าไม่มี updated_at (ไฟล์ยังไม่เคย build) fallback เป็นเวลาปัจจุบันแทน"""
    raw = data.get("updated_at")
    if raw:
        try:
            import datetime as _dt
            return _dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").timestamp()
        except ValueError:
            pass
    return time.time()


@app.route("/api/hk-index-heatmap")
def hk_index_heatmap():
    """Heatmap ของ HSI / HSCEI / HSTECH — ?index=HSI|HSCEI|HSTECH
    คืน {rows:[...stocks จาก hk_index_metrics.json...], ts, requested, missing}
    อ่านจาก hk_index_metrics.json ที่คำนวณไว้แล้วตอน Quick Update/HK Index Max โดยตรง
    (single source of truth เดียวกับหน้า "หุ้น HK") ไม่คำนวณ chg_1d/chg_1w เองอีกต่อไป —
    ได้ field ครบชุดเดียวกับหุ้นไทย (ret_1d..ret_1y, rs_score, vol_today/vol_avg20, high_52w,
    ath_pct ฯลฯ) มาฟรี ไม่ต้องยิง Yahoo หรือแตะ hk_prices.db เพิ่มเลย"""
    from sources import hk_index_metrics

    index_key = (request.args.get("index") or "HSI").upper()
    flag = {"HSI": "in_hsi", "HSCEI": "in_hscei", "HSTECH": "in_hstech"}.get(index_key)
    if not flag:
        return jsonify({"error": "index ต้องเป็น HSI, HSCEI หรือ HSTECH เท่านั้น"}), 400

    data = hk_index_metrics.load_local(BASE_DIR)
    rows = [s for s in data.get("stocks", []) if s.get(flag)]
    if not rows:
        return jsonify({"error": f"ยังไม่มีข้อมูล {index_key} ในเครื่อง — กด \"📈 HK Index Max\" ก่อน"}), 404

    requested = len(hk_index_membership.load_local(BASE_DIR).get(index_key) or [])
    return jsonify({"rows": rows, "ts": _index_metrics_ts(data),
                     "requested": max(requested, len(rows)), "missing": max(0, requested - len(rows))})


@app.route("/api/jp-index-heatmap")
def jp_index_heatmap():
    """Heatmap ของ Nikkei 225 — ดัชนีเดียว (ไม่มี ?index= ต่างจาก US/HK ที่มีหลายดัชนีให้เลือก)
    คืน {rows:[...stocks จาก jp_index_metrics.json...], ts, requested, missing}
    ก็อปโครงจาก hk_index_heatmap ทั้งหมด — อ่านจาก jp_index_metrics.json ตรงๆ เหมือนกัน"""
    from sources import jp_index_membership, jp_index_metrics

    data = jp_index_metrics.load_local(BASE_DIR)
    rows = [s for s in data.get("stocks", []) if s.get("in_nikkei225")]
    if not rows:
        return jsonify({"error": "ยังไม่มีข้อมูล NIKKEI225 ในเครื่อง — กด \"📈 JP Index Max\" ก่อน"}), 404

    requested = len(jp_index_membership.load_local(BASE_DIR).get("NIKKEI225") or [])
    return jsonify({"rows": rows, "ts": _index_metrics_ts(data),
                     "requested": max(requested, len(rows)), "missing": max(0, requested - len(rows))})


@app.route("/api/us-index-heatmap")
def us_index_heatmap():
    """Heatmap ของ S&P 500 / Dow Jones / Nasdaq 100 — ?index=SP500|DOW|NDX
    คืน {rows:[...stocks จาก us_index_metrics.json...], ts, requested, missing}
    อ่านจาก us_index_metrics.json ที่คำนวณไว้แล้วตอน Quick Update/US Index Max โดยตรง —
    ดู hk_index_heatmap สำหรับเหตุผลเดียวกัน (เดิมเคยยิง yf.download + fast_info ทีละ ticker
    ช้า ~30-60 วิ ตอน cache หมดอายุ ตอนนี้ไม่ยิง Yahoo จากหน้านี้เลย)"""
    from sources import us_index_metrics

    index_key = (request.args.get("index") or "SP500").upper()
    flag = {"SP500": "in_sp500", "DOW": "in_dow", "NDX": "in_ndx"}.get(index_key)
    if not flag:
        return jsonify({"error": "index ต้องเป็น SP500, DOW หรือ NDX เท่านั้น"}), 400

    data = us_index_metrics.load_local(BASE_DIR)
    rows = [s for s in data.get("stocks", []) if s.get(flag)]
    if not rows:
        return jsonify({"error": f"ยังไม่มีข้อมูล {index_key} ในเครื่อง — กด \"📈 US Index Max\" ก่อน"}), 404

    requested = len(us_index_membership.load_local(BASE_DIR).get(index_key) or [])
    return jsonify({"rows": rows, "ts": _index_metrics_ts(data),
                     "requested": max(requested, len(rows)), "missing": max(0, requested - len(rows))})


# ============================================================
# ปุ่ม "⚡ อัพเดทราคา" (Heatmap US/HK/JP) — ดึงราคาล่าสุด (gap-update) + คำนวณ live_price/
# live_chg แบบเดียวกับ dr_quick_update แล้ว merge เข้า <mkt>_index_metrics.json ที่มีอยู่แล้ว
# ตรงๆ (update_live_prices — ไม่ build() เต็มรูปแบบ) ให้ Heatmap อ่านราคา Live ได้ทันที
# RS/EMA/Stage เต็มรูปแบบคำนวณเฉพาะตอน Quick Update/Index Max วันละครั้งพอ (ดู
# _run_heatmap_live_update) ต่างจาก Quick Update ประจำวันที่ลากทำทุกอย่าง (insider/
# indices/short sales ฯลฯ) ตัวนี้ทำแค่ตลาดเดียวที่ผู้ใช้กำลังดูอยู่ — เร็วกว่ามาก
# ============================================================
_HM_LIVE_STATE = {
    "US": {"running": False, "error": None, "done": False},
    "HK": {"running": False, "error": None, "done": False},
    "JP": {"running": False, "error": None, "done": False},
}
# กัน race check-then-set ต่อ region (เดิมไม่มี lock — กดปุ่ม Live ซ้ำเร็วๆ/หลายแท็บ
# พร้อมกันในภูมิภาคเดียวกันชนกันยิง Yahoo ซ้อนได้ ก่อน running ถูกตั้งจริง)
_hm_live_lock = threading.Lock()

# ดัชนีย่อยที่ยอมให้กรอง (query param ?index=) ต่อ region — ใช้ตอนผู้ใช้กดปุ่ม Live ขณะ
# ดูอยู่แค่แท็บเดียว (เช่น Dow 30 ตัว) จะได้ไม่ต้องไล่ยิง Yahoo ทั้ง union ของ region นั้น
# ทุกครั้ง (US ~518 ตัว, HK ~105 ตัว) — JP มีดัชนีเดียว (Nikkei 225) ไม่ต้องกรอง
_HM_REGION_INDEXES = {"US": ("SP500", "DOW", "NDX"), "HK": ("HSI", "HSCEI", "HSTECH"), "JP": ()}


def _run_heatmap_live_update(region, index_key=None):
    state = _HM_LIVE_STATE[region]
    try:
        if region == "US":
            # S&P500+Dow+NDX รวมกัน ~518 ticker (union ไม่ซ้ำ) — universe ใหญ่กว่า HK/JP
            # หลายเท่า และปุ่มนี้ผู้ใช้กดเองได้ถี่กว่า Quick Update ประจำวันมาก จึงต้องพัก
            # ระหว่าง batch นานขึ้น (1.5 วิ แทน 0.3 วิ default) กัน Yahoo rate-limit/แบน —
            # HK (~105 ตัว) และ JP (~225 ตัว) ยังน้อยพอที่จะใช้ค่า default ปกติได้ ถ้าผู้ใช้
            # ระบุ index_key (เช่น ดูแค่แท็บ Dow) จะดึงเฉพาะ 30 ตัวนั้นแทนทั้ง union — เร็วขึ้นมาก
            n, live_map, scope, advanced = _run_us_index_gap_update(sleep_s=1.5, index_key=index_key)
            from sources import us_index_metrics as mod
            # แค่ merge live_price/live_chg เข้าไฟล์เดิม ไม่ build() เต็มรูปแบบ — เดิม build()
            # คำนวณ RS/EMA/Stage ใหม่ทั้ง ~518 ตัวของ union ทุกครั้งแม้ gap-update มาแค่ 30 ตัว
            # ของ Dow (RS percentile ต้อง rank เทียบทั้งกลุ่มถึงจะแม่น คำนวณแค่ scope ที่ขอไม่ได้)
            # ทำให้ปุ่มที่ควรเร็วกลับช้าเท่าดึงทั้งดัชนี — ตอนนี้ RS/EMA/Stage เต็มรูปแบบ
            # คำนวณเฉพาะตอน Quick Update/Index Max (วันละครั้ง) พอ ปุ่มนี้ใช้แค่โชว์ราคาสด — ยกเว้น
            # advanced=True (มีแท่งปิดของวันใหม่จริงมาจาก gap-update รอบนี้ เช่นกดตอนตลาดปิดไปแล้ว
            # และ Yahoo เพิ่งเติม Close ให้) ต้อง build() เต็มเสมอ ไม่งั้นราคา/ret_1d ที่ Heatmap
            # โชว์จะค้างของเมื่อวานจนกว่าจะถึง Quick Update รอบถัดไป (ดู docstring ของ advanced
            # ใน _run_index_gap_update)
            if advanced or not mod.update_live_prices(BASE_DIR, live_map, scope):
                mod.build(BASE_DIR, live_map=live_map)
            _us_breadth_cache.clear()
            _bump_cache_gen()
        elif region == "HK":
            n, live_map, scope, advanced = _run_hk_index_gap_update(index_key=index_key)
            from sources import hk_index_metrics as mod
            if advanced or not mod.update_live_prices(BASE_DIR, live_map, scope):
                mod.build(BASE_DIR, live_map=live_map)
            _hk_breadth_cache.clear()
            _bump_cache_gen()
        else:
            n, live_map, scope, advanced = _run_jp_index_gap_update()
            from sources import jp_index_metrics as mod
            if advanced or not mod.update_live_prices(BASE_DIR, live_map, scope):
                mod.build(BASE_DIR, live_map=live_map)
            _jp_breadth_cache.clear()
            _bump_cache_gen()
        state["done"] = True
        # n_fetched/n_live — ให้ frontend โชว์ข้อความ "⚡ ราคาสด ณ HH:MM:SS · สำเร็จ N/M ตัว"
        # แบบเดียวกับปุ่ม "⚡ ราคาล่าสุด" ของ Watchlist (ดู wlRefreshLivePrices ใน dashboard.js)
        # M = n (จำนวน ticker ที่ gap-update ได้จริงรอบนี้ — ตาม scope index_key ที่ขอ),
        # N = len(live_map) (ในนั้นกี่ตัวที่ได้ราคาสด ไม่ใช่แค่ราคาปิดเก่า)
        state["n_fetched"] = n
        state["n_live"] = len(live_map)
        idx_note = f" (index={index_key})" if index_key else ""
        print(f"[Heatmap live] {region}: gap-updated {n} ticker, {len(live_map)} live price{idx_note}")
        # ตลาดยังเปิดอยู่ (ควรได้ live_map) แต่ได้ราคาสดมาน้อยผิดปกติหรือ 0 ตัว — เกิดจาก Yahoo
        # rate-limit/บล็อกบางส่วนหรือทั้ง batch เงียบๆ (fetch_gap_batch/fetch_all_batch แค่ print
        # error แล้วข้ามไป ไม่ raise) เดิม build() จะ merge เท่าที่มีใน live_map เข้าไป ตัวที่ไม่ติด
        # จะไม่มี live_price/live_chg เลย หน้า Heatmap เลย fallback ไปโชว์ ret_1d เหมือนเดิมแบบ
        # เงียบๆ ทำให้ผู้ใช้เข้าใจผิดว่า "กดแล้วราคาไม่อัพเดท" ทั้งที่จริงคือ Yahoo ปฏิเสธไปบางส่วน/
        # ทั้งหมด — ต้อง surface เป็น error ให้ผู้ใช้เห็นแทนที่จะเงียบ (เกณฑ์ <20% ของที่ดึงได้จริง
        # กันเคส false-positive ตอนตลาดเพิ่งเปิดไม่กี่นาทีที่บาง ticker ยังไม่มีแท่งของวันนี้)
        if not is_latest_bar_stable(region):
            if n == 0:
                state["error"] = ("ตลาดยังเปิดอยู่แต่ดึงราคาจาก Yahoo ไม่ได้เลยสักตัว — Yahoo Finance "
                                   "อาจ rate-limit ชั่วคราว (กดถี่เกินไป) หรือเน็ตมีปัญหา "
                                   "ลองรอสัก 1-2 นาทีแล้วกดใหม่")
            elif len(live_map) < n * 0.2:
                state["error"] = (f"ตลาดยังเปิดอยู่แต่ได้ราคา Live แค่ {len(live_map)}/{n} ตัว — "
                                   "Yahoo Finance น่าจะ rate-limit บางส่วน หุ้นที่เหลือจะโชว์ % ปิด"
                                   "เมื่อวานแทนราคาสด ลองรอสัก 1-2 นาทีแล้วกดใหม่")
    except Exception as e:
        state["error"] = str(e)
        print(f"[Heatmap live] {region} ERROR: {e}")
    finally:
        state["running"] = False


@app.route("/api/heatmap-live-update/<region>", methods=["POST"])
def heatmap_live_update(region):
    region = region.upper()
    if region not in _HM_LIVE_STATE:
        return jsonify({"error": "region ต้องเป็น US, HK หรือ JP เท่านั้น"}), 400
    index_key = (request.args.get("index") or "").upper() or None
    if index_key not in _HM_REGION_INDEXES.get(region, ()):
        index_key = None   # ค่าที่ไม่รู้จัก/ไม่ส่งมา -> ดึงทั้ง union เหมือนเดิม
    state = _HM_LIVE_STATE[region]
    with _hm_live_lock:
        if state["running"]:
            return jsonify({"status": "running"})
        # เช็ค _state["running"] (ล็อกงานยาวหลัก เช่น Index Max/Quick Update) ด้วย — กัน
        # race ที่ปุ่มนี้แยกล็อกเอง (_hm_live_lock) เลยเริ่มงานซ้อนกับ full-refresh ของตลาด
        # เดียวกันได้ ทั้งสองเขียน {region}_prices.db/{region}_index_metrics.json ไฟล์เดียวกัน
        if _state["running"]:
            return jsonify({"status": "running"})
        state.update(running=True, error=None, done=False, n_fetched=None, n_live=None)
    threading.Thread(target=_run_heatmap_live_update, args=(region, index_key), daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/heatmap-live-status/<region>")
def heatmap_live_status(region):
    region = region.upper()
    if region not in _HM_LIVE_STATE:
        return jsonify({"error": "region ต้องเป็น US, HK หรือ JP เท่านั้น"}), 400
    return jsonify(_HM_LIVE_STATE[region])


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
            "ts": n.get("datetime") or "",   # ISO ครบพร้อม offset +07:00 — ห้ามตัดทิ้ง ไม่งั้น JS แปลผิดเป็น local time
            "source": "set",
            "publisher": "SET.or.th (ประกาศบริษัท)",
            "summary": "",
        })
    return rows


def _news_from_yahoo(yf_ticker):
    """ข่าวจาก Yahoo Finance ผ่าน yfinance — รองรับทั้ง payload รุ่นใหม่ (ห่อใน 'content')
    และรุ่นเก่า (field แบนราบ providerPublishTime เป็น epoch)"""
    import yfinance as yf
    from datetime import datetime, timezone
    rows = []
    for n in (yf.Ticker(yf_ticker).news or []):
        c = n.get("content") or n   # รุ่นใหม่ห่อใน content, รุ่นเก่าอยู่ชั้นนอกเลย
        title = c.get("title")
        if not title:
            continue
        url = (((c.get("canonicalUrl") or {}).get("url"))
               or ((c.get("clickThroughUrl") or {}).get("url"))
               or n.get("link") or "")
        # เก็บ 'Z'/offset ไว้เสมอ — ตัดทิ้งแล้ว JS ฝั่ง frontend จะแปล string เป็น local time แทน UTC
        # ทำให้เวลาข่าวคลาดเคลื่อนไปเท่า timezone offset ของเครื่องผู้ใช้ (ไทย +7 ชม.)
        ts = c.get("pubDate") or c.get("displayTime") or ""
        if not ts and n.get("providerPublishTime"):
            ts = datetime.fromtimestamp(n["providerPublishTime"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    import xml.etree.ElementTree as _ET
    from email.utils import parsedate_to_datetime
    from datetime import timezone as _dt_timezone
    loc = "hl=th&gl=TH&ceid=TH:th" if lang_th else "hl=en-US&gl=US&ceid=US:en"
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&{loc}"
    req = _ur.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"})
    ctx = ssl_context()
    with _ur.urlopen(req, context=ctx, timeout=20) as r:
        xml = r.read().decode("utf-8", "ignore")
    rows = []
    for it in _ET.fromstring(xml).findall(".//item"):
        title = it.findtext("title") or ""
        if not title:
            continue
        ts = ""
        try:
            # ต้องคง 'Z' ต่อท้ายไว้ — ไม่งั้น JS ฝั่ง frontend ตีความ UTC เป็น local time ผิด (คลาดเคลื่อน +7 ชม.)
            ts = parsedate_to_datetime(it.findtext("pubDate")).astimezone(_dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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

    # market อยู่ใน key ด้วย — หุ้นนอกตัวเดียวกันคนละ market ได้ yf ticker คนละตัว
    # (เช่น 0700 → 0700.HK vs 0700) ถ้าไม่แยกจะได้ผลของ market ก่อนหน้าติดมา
    cache_key = (sym, is_dr, market if is_dr else "")
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

    # ไม่ใช้ "with ThreadPoolExecutor(...) as ex:" — __exit__ ของมันเรียก shutdown(wait=True)
    # ซึ่งบล็อกรอทุก thread จบงานจริง แม้ f.result(timeout=30) ข้างล่างจะ timeout ไปแล้วก็ตาม
    # (เช่น yfinance ค้างเกิน 30 วิ) เท่ากับ timeout ที่ตั้งใจไว้ไม่มีผลจริง request thread
    # ค้างยาวเกิน 30 วิได้ — shutdown(wait=False) ให้ thread ที่ยังค้างทำงานต่อเบื้องหลังแทน
    # โดยไม่บล็อก request นี้
    rows, errors = [], {}
    ex = ThreadPoolExecutor(max_workers=3)
    try:
        futs = {name: ex.submit(fn) for name, fn in jobs.items()}
        for name, f in futs.items():
            try:
                rows.extend(f.result(timeout=30))
            except Exception as e:
                errors[name] = str(e)[:120]
    finally:
        ex.shutdown(wait=False)

    # dedupe หัวข้อซ้ำข้ามแหล่ง (Google มักเจอข่าวเดียวกับ Yahoo) — คงตัวที่เจอก่อนตามลำดับ
    # แหล่ง (yahoo มี summary ครบกว่า) · เทียบแบบตัดช่องว่าง/ตัวพิมพ์
    seen, deduped = set(), []
    for r in sorted(rows, key=lambda r: r["source"]!= "yahoo"):
        k = re.sub(r"\s+", "", r["title"].lower())[:80]
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    # เรียงตามเวลาจริง (แปลงเป็น UTC epoch ก่อนเทียบ) — เทียบ string ตรงๆ ไม่ได้เพราะ
    # SET ใช้ offset +07:00 ส่วน Yahoo/Google ใช้ 'Z' (UTC) รูปแบบ suffix ต่างกัน
    def _ts_key(r):
        from datetime import datetime as _dt
        try:
            return _dt.fromisoformat(r["ts"].replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0
    deduped.sort(key=_ts_key, reverse=True)

    result = {"rows": deduped[:80], "ts": time.time(), "symbol": sym, "errors": errors}
    # ไม่ cache "ก้อนว่างเพราะทุกแหล่งพัง" — เน็ตสะดุดครั้งเดียวแล้ว cache ไว้ 15 นาที
    # ทำให้ผู้ใช้กดค้นใหม่ก็ได้ผลว่างเดิมซ้ำๆ ทั้งที่แหล่งข้อมูลกลับมาปกติแล้ว
    # (มีข่าวอย่างน้อย 1 แถว = ถือว่าใช้ได้ cache ตามปกติ)
    if deduped or not errors:
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
    try:
        payload_yahoo = financials_store.get(BASE_DIR, sym, "yahoo")
        payload_set = financials_store.get(BASE_DIR, sym, "set")
        return jsonify(financials_store.compare_sources(payload_yahoo, payload_set))
    except Exception as e:
        return jsonify({"error": f"เทียบข้อมูลงบ {sym} ล้มเหลว: {e}"}), 500


INDICES_FILE = os.path.join(BASE_DIR, "indices_cache.json")

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
                # คำนวณ rs_raw ทุก 5 วันทำการ ย้อนหลัง 52 สัปดาห์ แล้ว rank เทียบ stock_raws
                # (percentile ของวันนี้) แบบเดียวกับ rs_val ด้านบน — เดิม normalize แบบ
                # min-max ภายใน range ของดัชนีตัวเอง (0-99 เทียบกับปีที่ผ่านมาของตัวมันเอง)
                # ทำให้สเกลของ backfill "rs" ไม่ตรงกับ rs_val ที่ append รายวัน (percentile
                # เทียบหุ้นทั้งตลาด) — RS Trend arrow เทียบข้ามสเกลกันจึงผิดเพี้ยนจนกว่าข้อมูล
                # สะสมจริงจะไล่ backfill ออกจากหน้าต่าง 4 สัปดาห์ที่ใช้เทียบ
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
                    if rr is not None and ns > 0:
                        rank = bisect.bisect_left(stock_raws, rr)
                        weekly.append({"date": dates[pos], "rs": int(round(rank / ns * 99))})
                if weekly:
                    weekly.reverse()  # เรียงตามเวลา oldest → newest
                    hist = weekly

            # เพิ่ม entry วันนี้ / เขียนทับถ้าเป็นวันเดียวกับที่มีอยู่แล้ว (closes ย้อนหลังที่
            # ใช้คำนวณ rs_raw อาจถูก TradingView แก้ไขระหว่างวัน — เดิม append-only เลยค้าง
            # ค่า rs ของวันนั้นไว้ถาวรตั้งแต่รอบแรกที่รัน)
            if rs_val is not None:
                if hist and hist[-1]["date"] == today_str:
                    hist[-1]["rs"] = rs_val
                else:
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
            # TradingView แก้ค่าปิดของวันที่ผ่านมาแล้วย้อนหลังได้ (พบว่าค้างได้หลายวัน
            # ทำการ ไม่ใช่แค่วันล่าสุด) เดิม merge แบบ append เฉพาะ d > last_d เลยไม่มี
            # ทางรับค่าที่แก้ไขนี้เข้ามาเลย — เปลี่ยนเป็นเขียนทับทุกวันที่ที่ TV ส่งมาใหม่
            # ในรอบนี้ (ทับของเดิมถ้าซ้ำวัน) แล้วค่อย append วันที่ใหม่จริงๆ ต่อท้าย
            date_map = dict(zip(entry["dates"], entry["closes"]))
            added = revised = 0
            for d, v in zip(new_dates, new_vals):
                if d in date_map:
                    if date_map[d] != v:
                        revised += 1
                else:
                    added += 1
                date_map[d] = v
            merged = sorted(date_map.items())
            old_dates = [d for d, _ in merged]
            old_vals  = [v for _, v in merged]
            print(f"[Indices] QU {sym} +{added}d revised={revised}d -> {(old_dates or ['?'])[-1]}")
        else:
            old_dates = new_dates
            old_vals  = new_vals
            print(f"[Indices] {'FR' if full_refresh else 'NEW'} {sym} {len(old_vals)} bars")

        v = old_vals
        def _ret(n, _v=v):
            # ป้องกันหารด้วยศูนย์ — ราคาปิดย้อนหลังเป็น 0 ได้จากข้อมูลเพี้ยนของ TradingView
            # (ไม่เคยเจอจริง แต่ถ้าเกิดจะทำให้ ZeroDivisionError หลุดออกจาก _merge_one ไป
            # ตัด loop ที่ประมวลผลดัชนีตัวอื่นทั้งหมดในรอบเดียวกันด้วย — ดู try/except รอบ
            # _merge_one() ด้านล่างที่กันอีกชั้นสำหรับข้อผิดพลาดที่ไม่คาดคิดอื่นๆ)
            if len(_v) <= n:
                return None
            base = _v[-(n+1)]
            return round((_v[-1] - base) / base * 100, 2) if base else None

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
    # _merge_one ห่อ try/except ทีละตัว — เดิมข้อผิดพลาดที่ไม่คาดคิดของดัชนีตัวเดียว (เช่น
    # ราคาปิดเป็น 0 ทำ ZeroDivisionError) จะหลุดออกจาก loop นี้ทั้งก้อน ทำให้ดัชนีตัวอื่นๆ ที่
    # fetch สำเร็จแล้วในรอบเดียวกันไม่ถูก merge/บันทึกไปด้วยทั้งที่ไม่เกี่ยวข้องกันเลย —
    # แยก error ต่อตัวให้เหมือนกับที่ทำกับ fetch failure (failed_syms) อยู่แล้ว
    def _merge_one_safe(sym, pairs):
        try:
            _merge_one(sym, pairs)
            return True
        except Exception:
            print(f"[Indices] merge ล้มเหลว {sym}: {traceback.format_exc()}")
            return False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, sym): sym for sym in all_syms}
        for future in as_completed(futures):
            sym, pairs = future.result()
            if pairs is None:
                failed_syms.append(sym)
                continue
            if _merge_one_safe(sym, pairs):
                fetched += 1
            else:
                failed_syms.append(sym)

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
            elif _merge_one_safe(sym, pairs):
                fetched += 1
            else:
                still_failed.append(sym)
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


# payload ~4.5MB ไม่บีบอัด (เต็มไปด้วย dates/closes รายวันของทุกดัชนี ซึ่ง frontend ใช้ทำกราฟ
# ใน openIdxChartModal จริง ตัดทิ้งแบบ _dr_light ไม่ได้) — cache gzip ไว้ต่อ id(data) ก้อนเดียว
# กัน compress ซ้ำทุก request (data ถูก reassign เป็น dict ใหม่ทุกครั้งที่เนื้อหาเปลี่ยนจริง)
_indices_gz_cache = {"id": None, "raw": None, "gz": None}


def _indices_response(data):
    import gzip as _gzip
    key = id(data)
    if _indices_gz_cache["id"] != key:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        _indices_gz_cache.update(id=key, raw=raw, gz=_gzip.compress(raw, compresslevel=6))
    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        return Response(_indices_gz_cache["gz"], mimetype="application/json",
                        headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"})
    return Response(_indices_gz_cache["raw"], mimetype="application/json")


@app.route("/api/indices")
def get_indices():
    """เสิร์ฟข้อมูลดัชนีจากไฟล์ หรือดึงใหม่ถ้าไม่มีไฟล์"""
    global _indices_cache
    data = _indices_cache.get("data")
    first = next(iter(data.values()), {}) if data else {}
    # ส่งจาก memory cache ถ้ามี rs_set และ rs_history ครบแล้ว
    if data and first.get("rs_set") is not None and len(first.get("rs_history", [])) >= 4:
        return _indices_response(data)
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
            return _indices_response(data)
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
    with _indices_job_lock:
        if _indices_job_state["running"]:
            return jsonify({"error": "กำลังอัปเดตดัชนีอยู่แล้ว โปรดรอสักครู่"}), 409
        _indices_job_state["running"] = True
    try:
        existing = _load_indices_existing()
        result, stats = _fetch_indices_tv(existing, full_refresh=False)
        _indices_cache["data"] = result
        return jsonify({"ok": stats["fetched"] > 0, "count": len(result), **stats,
                        "warning": (f"ดึงไม่สำเร็จ {stats['failed']}/{stats['total']} ดัชนี"
                                    if stats["failed"] else None),
                        "updated_at": time.strftime("%Y-%m-%d %H:%M")})
    except Exception as e:
        tb.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        _indices_job_state["running"] = False


@app.route("/api/indices-refresh", methods=["POST"])
def indices_refresh():
    """Full Refresh — ดึง 5000 bars (~20 ปี) จาก TradingView"""
    import traceback as tb
    global _indices_cache
    with _indices_job_lock:
        if _indices_job_state["running"]:
            return jsonify({"error": "กำลังอัปเดตดัชนีอยู่แล้ว โปรดรอสักครู่"}), 409
        _indices_job_state["running"] = True
    try:
        existing = _load_indices_existing()
        result, stats = _fetch_indices_tv(existing, full_refresh=True)
        _indices_cache["data"] = result
        return jsonify({"ok": stats["fetched"] > 0, "count": len(result), **stats,
                        "warning": (f"ดึงไม่สำเร็จ {stats['failed']}/{stats['total']} ดัชนี"
                                    if stats["failed"] else None)})
    except Exception as e:
        tb.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        _indices_job_state["running"] = False


def _any_job_running():
    """เช็คว่ามี background job (Refresh/Quick Update/Heatmap Live/Indices ฯลฯ) กำลังรันอยู่ไหม
    — ใช้ก่อน restart กัน os._exit(0) ตัดงานหนัก (Full Refresh ใช้เวลา 20 นาที - 1.5 ชม.)
    ทิ้งกลางคันโดยผู้ใช้ไม่รู้ตัว (เดิม /api/restart ไม่เช็คอะไรเลย)"""
    if _state["running"] or _dr_refresh_state["running"] or _indices_job_state["running"]:
        return True
    return any(s["running"] for s in _HM_LIVE_STATE.values())


# สถานะ restart ที่กำลังทำอยู่ — เก็บไว้ให้ /api/status รายงานได้ว่า restart ล้มเหลว
# หรือยัง (เดิมไม่มีเลย: /api/restart ตัด os._exit(0) ทันทีหลัง spawn โดยไม่เช็คว่า
# process ใหม่เปิดขึ้นมาสำเร็จจริงไหม ถ้าโค้ดพัง (syntax/import error จากการแก้ไฟล์
# ก่อนหน้า) process ใหม่จะตายทันทีตอนเปิด ในขณะที่ตัวเก่าถูกปิดไปแล้ว — เว็บล่มทั้งคู่
# ต้องไปรันเซิร์ฟเวอร์เองใหม่ด้วยมือ ไม่มีทางกู้จากหน้าเว็บได้เลย)
_restart_state = {"in_progress": False, "failed_error": None}


@app.route("/api/restart", methods=["POST"])
def restart_server():
    """Restart Flask process (Windows-safe: spawn new process then exit)
    ปฏิเสธถ้ามี job กำลังรันอยู่ เว้นแต่ส่ง {"force": true} มา (ผู้ใช้ยืนยันแล้วว่ายอมตัดทิ้ง)

    กันเว็บล่มทั้งคู่ (เก่าปิดไปแล้ว ใหม่ตายตั้งแต่เปิด) 2 ชั้น:
    1. pre-flight compile check ก่อนแตะอะไรเลย — จับ syntax error ของ app.py เอง
       (คุมได้แค่ syntax ของไฟล์นี้ ไม่ครอบคลุม import error จากไฟล์อื่นที่ import เข้ามา
       แต่ครอบคลุมสาเหตุที่พบบ่อยที่สุด: แก้โค้ดแล้วพิมพ์ผิด/วงเล็บไม่ครบ)
    2. หลัง spawn process ใหม่แล้ว รอสั้นๆ เช็คว่ามันไม่ตายทันที (import error ฯลฯ)
       ก่อนค่อยปิดตัวเอง — ถ้าตายเร็วผิดปกติ ไม่ os._exit ปล่อยตัวเก่าทำงานต่อ"""
    force = bool((request.get_json(silent=True) or {}).get("force"))
    if _any_job_running() and not force:
        return jsonify({"error": "มี job กำลังรันอยู่ (Refresh/Update ฯลฯ) — restart ตอนนี้จะตัดงานทิ้งกลางคัน",
                        "job_running": True}), 409

    script = os.path.abspath(__file__)
    try:
        import py_compile
        py_compile.compile(script, doraise=True)
    except Exception as e:
        return jsonify({"error": f"โค้ด app.py compile ไม่ผ่าน — ไม่ restart เพื่อกันเว็บล่มทั้งคู่: {e}"}), 400

    _restart_state.update(in_progress=True, failed_error=None)

    def _do_restart():
        time.sleep(0.8)
        try:
            proc = subprocess.Popen([sys.executable, script],
                                    cwd=os.path.dirname(script))
        except Exception as e:
            msg = f"เปิด process ใหม่ไม่สำเร็จ: {e}"
            print(f"[Restart] {msg}")
            run_log.record_run(BASE_DIR, "restart", False, msg)
            _restart_state.update(in_progress=False, failed_error=msg)
            return  # ไม่ os._exit — server เดิมทำงานต่อตามปกติ

        # เช็คว่า process ใหม่ไม่ปิดตัวเองทันที (syntax/import error ในไฟล์อื่นที่
        # py_compile ข้างบนตรวจไม่ถึง) ก่อนค่อยปิดตัวเก่า — ระหว่างนี้ port ยัง
        # เป็นของตัวเก่าอยู่ process ใหม่จะรอที่ _wait_port_free() เฉยๆ ไม่ error
        time.sleep(1.5)
        if proc.poll() is not None:
            msg = f"process ใหม่ปิดตัวเองทันที (exit code {proc.returncode}) — ไม่ restart"
            print(f"[Restart] {msg}")
            run_log.record_run(BASE_DIR, "restart", False, msg)
            _restart_state.update(in_progress=False, failed_error=msg)
            return  # ไม่ os._exit — server เดิมทำงานต่อตามปกติ

        os._exit(0)
    threading.Thread(target=_do_restart, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/kill-duplicate-servers", methods=["POST"])
def kill_duplicate_servers():
    """ปิด process อื่นที่ bind พอร์ตเดียวกัน (SERVER_PORT) ซ้อนอยู่ — Windows ยอมให้ bind
    ซ้อนกันได้ด้วย SO_REUSEADDR (ดูคอมเมนต์ _wait_port_free ท้ายไฟล์) ต่างจาก Linux ทำให้
    เผลอเปิด app.py ซ้ำ (เช่นรันจากหลาย terminal/agent พร้อมกัน) แล้วเกิด "server ผี" ค้างพอร์ต
    เดียวกันหลายตัว บาง request วิ่งเข้าตัวเก่าที่โค้ด/ข้อมูลไม่ตรงกับตัวที่กำลังดูอยู่ โดยผู้ใช้
    ไม่รู้ตัว — เดิมต้องเปิด Task Manager ไล่หา python.exe เอง

    หา PID ที่ LISTEN พอร์ต SERVER_PORT จาก `netstat -ano` (ไม่ใช้ psutil — ไม่ได้อยู่ใน
    requirements.txt, คำสั่งนี้มีในตัว Windows อยู่แล้ว) แล้ว taskkill ทิ้งทุกตัวยกเว้น PID
    ของ process ปัจจุบัน (os.getpid()) กันฆ่าตัวเอง"""
    if os.name != "nt":
        return jsonify({"error": "รองรับเฉพาะ Windows (netstat/taskkill)"}), 400

    my_pid = os.getpid()
    try:
        out = subprocess.run(["netstat", "-ano", "-p", "TCP"], capture_output=True,
                              text=True, timeout=10, check=True).stdout
    except Exception as e:
        return jsonify({"error": f"เรียก netstat ไม่สำเร็จ: {e}"}), 500

    port_suffix = f":{SERVER_PORT}"
    victim_pids = set()
    for line in out.splitlines():
        parts = line.split()
        # ตัวอย่างแถว: TCP    0.0.0.0:5001    0.0.0.0:0    LISTENING    12345
        if len(parts) < 5 or parts[0] != "TCP" or parts[3] != "LISTENING":
            continue
        if not parts[1].endswith(port_suffix):
            continue
        try:
            pid = int(parts[-1])
        except ValueError:
            continue
        if pid != my_pid:
            victim_pids.add(pid)

    killed, failed = [], []
    for pid in sorted(victim_pids):
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True,
                            text=True, timeout=10, check=True)
            killed.append(pid)
        except Exception as e:
            failed.append({"pid": pid, "error": str(e)})

    return jsonify({"ok": True, "my_pid": my_pid, "killed": killed, "failed": failed})


@app.route("/api/job-reset", methods=["POST"])
def job_reset():
    """ปลดล็อก job flag ที่ค้าง — ใช้เมื่อปุ่มงานหนัก (Refresh/Quick Update/Heatmap Live ฯลฯ)
    คืน 409 "กำลังทำงานอยู่" ตลอดแม้รอนานแล้ว เพราะ thread เดิมค้าง (เช่น socket ไปหา Yahoo
    ไม่ตอบไม่มีกำหนด — ก่อนหน้านี้ไม่มี timeout เลย) แต่ไม่มี exception มา trigger `finally:
    _update(running=False)` ให้

    ไม่สามารถ "ยกเลิก" thread ที่ค้างจริงๆ ได้ (Python ทำไม่ได้) แค่ reset flag ให้กดปุ่มใหม่ได้
    — ถ้า thread เดิมยังทำงานอยู่จริงและเขียนไฟล์สำเร็จช้าๆ ทีหลัง อาจชนกับรอบใหม่ที่เพิ่งกด
    แต่ดีกว่าต้องปิด server ทิ้งทั้งตัวเมื่อ job ค้าง"""
    with _lock:
        was_running = _state["running"]
        _state.update(running=False, done=True,
                      error="ยกเลิกโดยผู้ใช้ (job ค้างนานเกินไป)" if was_running else _state.get("error"))
    _dr_refresh_state["running"] = False
    for region_state in _HM_LIVE_STATE.values():
        region_state["running"] = False
    with _indices_job_lock:
        _indices_job_state["running"] = False
    # _restart_state["in_progress"] ตั้งเป็น True ก่อนเรียก subprocess.Popen (ดู
    # _do_restart) แล้วไม่มีทาง reset กลับถ้า Popen ค้างนานผิดปกติ (แอนตี้ไวรัสสแกน
    # python.exe/disk ช้า) — เพิ่มเข้า reset endpoint นี้ด้วยให้ครบทุก "running" flag
    # ที่มีในระบบ (ปลอดภัย: ถ้า restart จริงกำลังจะสำเร็จ process จะถูกแทนที่อยู่ดี
    # ค่า flag ที่ reset ผิดจังหวะไม่มีผลอะไรต่อ)
    _restart_state["in_progress"] = False
    # หมายเหตุ: _dr_rebuild_lock/_etf_rebuild_lock (threading.Lock ของ /api/dr,
    # /api/etf background rebuild) ตั้งใจไม่แตะที่นี่ — force-release lock ที่
    # thread อื่นอาจกำลังถือจริงและเขียนไฟล์ cache อยู่จะทำให้ 2 thread เขียนไฟล์
    # เดียวกันพร้อมกัน อันตรายกว่าปล่อยให้รอ (ตอนนี้ยิง yfinance มี timeout แล้ว
    # จึงไม่ควรค้างถาวรอีกต่อไป)
    return jsonify({"ok": True, "was_running": was_running})


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
        # process ใหม่ (ถ้า restart สำเร็จจริง) จะมี _restart_state สดใหม่เป็นค่า
        # default เสมอ (in_progress=False, failed_error=None) — client ใช้แยกแยะ
        # "restart สำเร็จ" ออกจาก "ยังเป็น process เก่าที่ restart ล้มเหลว" ได้
        "restart_in_progress": _restart_state["in_progress"],
        "restart_failed": _restart_state["failed_error"],
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


def _dh_gap_check_item(key, label, path, lookback_days=60, warn_n=4, red_n=10):
    """เช็คจำนวน "วันทำการ (จ-ศ)" ที่หายไปกลางช่วง lookback_days วันล่าสุดในไฟล์ rows-by-date
    (market/s50/bond flow) — เกณฑ์อื่นในหน้านี้เช็คแค่ mtime/จำนวนแถวรวม ไม่จับ "รูขาดกลางชุด"
    (เช่น TFEX/ThaiBMA ใช้ไม่ได้ติดกันหลายวันช่วงที่ไม่มีใครเปิดแอปพอดี) นับหยาบๆ ไม่รู้จักวันหยุด
    นักขัตฤกษ์ไทย (ปกติ ~15-16 วัน/ปี ≈ 2-3 วันใน 60 วัน) จึงตั้ง threshold หลวมพอให้วันหยุดยาว
    ไม่ขึ้นเตือนเอง ไม่นับ 2 วันทำการล่าสุด (ข้อมูลอาจยังไม่ post)"""
    try:
        with open(path, encoding="utf-8") as f:
            rows = json.load(f).get("rows") or []
    except Exception as e:
        return _dh_quality_item(key, label, "red", f"อ่านไฟล์ไม่ได้: {str(e)[:120]}")
    dates = {r.get("date") for r in rows if r.get("date")}
    if not dates:
        return _dh_quality_item(key, label, "red", "ไม่มีข้อมูลวันที่ในไฟล์เลย")

    today = _dh_dt.now().date()
    cutoff = today - timedelta(days=2)
    d = today - timedelta(days=lookback_days)
    missing = []
    while d <= cutoff:
        if d.weekday() < 5 and d.isoformat() not in dates:
            missing.append(d.isoformat())
        d += timedelta(days=1)

    n = len(missing)
    status = "red" if n >= red_n else ("warn" if n >= warn_n else "ok")
    if n == 0:
        note = f"ครบดี — ไม่พบวันทำการที่ขาดใน {lookback_days} วันล่าสุด"
    else:
        more = "…" if n > 6 else ""
        note = (f"ขาด {n} วันทำการใน {lookback_days} วันล่าสุด: "
                f"{', '.join(missing[:6])}{more} (เผื่อวันหยุดยาวไทยแล้ว ~{warn_n} วัน)")
    return _dh_quality_item(key, label, status, note, last_at=max(dates))


def _dh_drift_item(key, label, status_map):
    """รายการ "ตรวจ drift อัตโนมัติ" (หุ้นเข้าใหม่/ถูกถอดเทียบแหล่งสด) — อ่านผลจาก
    index_drift.read_status() (เขียนโดย _run_index_drift_checks ท้าย Quick Update
    สัปดาห์ละครั้ง — ดู PLAN_universe_data_health.txt) ไม่ยิง Wikipedia/SET.or.th สดตรงนี้
    (เบา อ่านไฟล์เฉยๆ) warn เมื่อเจอตัวเข้า/ออกที่ยัง sync มือไม่ตรง หรือผลเช็คเก่าเกิน
    TTL*2 (แปลว่า Quick Update ไม่ได้รันช่วงนี้) red เมื่อรอบล่าสุด error (เช่น Wikipedia
    เปลี่ยนโครงสร้างตาราง parse ไม่ออก)"""
    d = status_map.get(key)
    if not d:
        return {"key": f"drift_{key.lower()}", "label": label, "category": "หุ้นเข้าใหม่/ถูกถอด",
                "last_at": None, "age_hours": None, "status": "na",
                "note": "ยังไม่เคยตรวจอัตโนมัติ — รอ Quick Update รอบถัดไป หรือกดปุ่มเช็คมือได้เลย"}
    checked_at = _dh_parse(d.get("checked_at"))
    age_h = _dh_age_hours(checked_at)
    if d.get("error"):
        status, note = "red", f"⚠ เช็คล้มเหลว: {d['error']}"
    elif age_h is not None and age_h >= _INDEX_DRIFT_TTL_DAYS * 24 * 2:
        status = "warn"
        note = f"ผลเช็คเก่ากว่า {_INDEX_DRIFT_TTL_DAYS * 2} วัน — Quick Update อาจไม่ได้รันช่วงนี้"
    elif d.get("new_count") or d.get("removed_count"):
        parts = []
        if d.get("new_count"):
            new_sample = d.get("new_sample") or []
            sample = ', '.join(new_sample[:8])
            more = '…' if d["new_count"] > len(new_sample) else ''
            parts.append(f"ใหม่ {d['new_count']} ตัว ({sample}{more})")
        if d.get("removed_count"):
            rem_sample = d.get("removed_sample") or []
            sample = ', '.join(rem_sample[:8])
            more = '…' if d["removed_count"] > len(rem_sample) else ''
            parts.append(f"หายไป {d['removed_count']} ตัว ({sample}{more})")
        status, note = "warn", "พบการเปลี่ยนแปลง — " + " · ".join(parts) + " (ต้อง sync มือให้ตรง)"
    else:
        status, note = "ok", "ตรงกับแหล่งสดล่าสุด ไม่มีตัวเข้า/ออก"
    return {"key": f"drift_{key.lower()}", "label": label, "category": "หุ้นเข้าใหม่/ถูกถอด",
            "last_at": d.get("checked_at"), "age_hours": age_h, "status": status, "note": note}


def _dh_index_quality_item(key, label, market_code, membership_path, metrics_path, index_keys, bounds):
    """เช็คคุณภาพ (ไม่ใช่แค่ mtime) ของสมาชิกดัชนีต่างประเทศ + metrics (US/HK/JP):
    (1) จำนวนสมาชิกแต่ละดัชนีย่อยอยู่ในกรอบปกติไหม (bounds) — guard เดิมใน
        <mkt>_index_membership.py เช็คแค่ "ต่ำกว่า 10/20 ตัว = พังชัดเจน" ถ้า Wikipedia
        เปลี่ยน layout บางส่วนแล้ว parse ได้ เช่น 480/503 ตัว จะหลุดผ่าน guard เดิมไปเงียบๆ
    (2) สมาชิกใน membership.json ที่หายไปจาก metrics.json (ราคาดึงไม่ได้/เพิ่งเข้าดัชนียัง
        ไม่ backfill) — ปกติควรเป็น 0 หลัง Quick Update รอบถัดจาก sync
    (3) จำนวนหุ้นที่ sector เป็น "Unknown"/ว่างใน metrics — ปกติควรเป็น 0 (เคสจริงที่เจอ:
        FER ในดัชนี NDX sector เป็น Unknown เพราะ regex parse พลาดแถวที่ช่องว่างไม่สม่ำเสมอ
        ดู sources/us_index_membership.py::_parse_ndx_sectors — แก้แล้วแต่เคสแบบนี้เกิดซ้ำ
        กับตลาดอื่นได้ ควรมีระบบเฝ้าแทนที่จะรอเจอเอง)"""
    problems = []
    status = "ok"
    try:
        with open(membership_path, encoding="utf-8") as f:
            mem = json.load(f)
    except Exception as e:
        return _dh_quality_item(key, label, "red", f"อ่าน membership ไม่ได้: {str(e)[:120]}")
    if not isinstance(mem, dict):
        return _dh_quality_item(key, label, "red",
                                 f"membership.json รูปแบบผิดปกติ (ได้ {type(mem).__name__} ไม่ใช่ object)")

    union = set()
    for k in index_keys:
        members = mem.get(k) or []
        if not isinstance(members, list):
            status = "red"
            problems.append(f"{k} รูปแบบผิดปกติ ({type(members).__name__} ไม่ใช่ list)")
            continue
        n = len(members)
        union |= set(members)
        lo, hi = bounds.get(k, (0, 10 ** 9))
        if not (lo <= n <= hi):
            status = "warn"
            problems.append(f"{k} {n} ตัว (ปกติ {lo}-{hi})")

    try:
        with open(metrics_path, encoding="utf-8") as f:
            stocks = (json.load(f) or {}).get("stocks") or []
        if not isinstance(stocks, list):
            stocks = []
    except Exception:
        stocks = []
    stocks = [s for s in stocks if isinstance(s, dict)]
    metric_syms = {s.get("symbol") for s in stocks}
    missing = sorted(sym for sym in (union - metric_syms) if sym is not None)
    if missing:
        status = "warn"
        more = '…' if len(missing) > 8 else ''
        problems.append(f"หายจาก metrics {len(missing)} ตัว ({', '.join(missing[:8])}{more})")

    unknown = [s.get("symbol") for s in stocks if not s.get("sector") or s.get("sector") == "Unknown"]
    if unknown:
        status = "red" if len(unknown) > 3 else "warn"
        more = '…' if len(unknown) > 8 else ''
        problems.append(f"sector Unknown {len(unknown)} ตัว ({', '.join(unknown[:8])}{more})")

    note = " · ".join(problems) if problems else f"{len(union)} ตัว ({market_code}) ครบ ไม่มี sector Unknown"
    return _dh_quality_item(key, label, status, note)


_dh_th_universe_cache = {"mtime": None, "tickers": None}


def _dh_th_universe_tickers():
    """คืน set ของ ticker (.BK) ที่เป็นหุ้นสามัญจริงในดัชนี SET/mai หลัก (~930 ตัว) — อ่านจาก
    set_data.json (cache ตาม mtime กันอ่านซ้ำทุกครั้งที่เปิดหน้า Data Health) ไม่ใช้ทุก
    ticker ใน set_prices.db ตรงๆ เพราะ DB เก็บปนกับ derivative warrant/DR/กองทุนที่ดึงผ่าน
    ฟีเจอร์อื่น (เจอจริง: set_prices.db มี 1,814 ticker ทั้งที่หุ้นสามัญจริงมีแค่ ~930 ตัว —
    DW พวกนี้เทรดเบาบางมาก ราคานิ่งซ้ำกันหลายวันเป็นเรื่องปกติ ถ้าไม่กรองออกจะ false-positive
    ชนกับเช็ค 'ราคาซ้ำผิดปกติ' ด้านล่างทันที) คืน None ถ้าอ่านไม่ได้ (caller ควร skip filter)"""
    try:
        mtime = os.path.getmtime(DATA_FILE)
    except OSError:
        return None
    if _dh_th_universe_cache["mtime"] == mtime:
        return _dh_th_universe_cache["tickers"]
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            d = json.load(f)
        tickers = {s["ticker"] for s in d.get("stocks", []) if s.get("ticker")}
    except Exception:
        return None
    _dh_th_universe_cache.update(mtime=mtime, tickers=tickers)
    return tickers


def _dh_stale_close_check(key, label, db_path, dup_threshold=20, min_universe=30, universe=None):
    """เช็คว่า Quick Update 'ดึงราคาจริง' หรือแค่ mtime ขยับแต่ได้ราคาซ้ำแท่งก่อนหน้ามา —
    ต่างจาก _dh_item ด้านบนที่เช็คแค่ mtime (ไฟล์ถูกเขียนทับเวลาไหน) ไม่รู้เนื้อในว่าค่าจริง
    เปลี่ยนหรือเปล่า เจอเคสจริง 11 ส.ค. 2569: Yahoo ยังไม่ปล่อยแท่งปิดทางการ หุ้นส่วนใหญ่เลย
    ไม่มีแท่งใหม่เลย (เคสนี้ _dh_item จับได้อยู่แล้วเพราะ mtime ไม่ขยับ) แต่ถ้าวันไหน pipeline
    ดันดันแท่งซ้ำของเก่าเข้าไปเป็นวันใหม่ (บั๊กที่ยังไม่เจอแต่ป้องกันไว้ก่อน) mtime จะขยับปกติ
    ทำให้ _dh_item ไม่จับ ต้องเช็คเนื้อราคาจริงแทน

    เทียบ close ของแท่งล่าสุด vs แท่งก่อนหน้าต่อหุ้น (ไม่สนวันที่ label ตรงกันไหม) เฉพาะตัวที่
    volume ของแท่งล่าสุด > 0 (หุ้นหยุดพักเทรด/ราคาแช่แข็งไม่นับ เป็นเรื่องปกติอยู่แล้ว ไม่ใช่
    สัญญาณอัพเดทล้มเหลว) ถ้าจำนวนที่เหลือ >= dup_threshold ตัว ให้สงสัยว่าอัพเดทได้ข้อมูลซ้ำเดิมมา

    dup_threshold ต้อง calibrate แยกตามตลาด ห้ามใช้ค่าเดียวกันทุกตลาด — วัดจริงจาก 5 คู่วัน
    ล่าสุดของหุ้นไทย (929 ตัว ที่ยืนยันแล้วว่าอัพเดทถูกต้องทุกวัน) ได้ baseline ปกติ 251-306
    ตัว/วัน (หุ้นเพนนีสต็อกจำนวนมากในกระดาน mai ปิดราคาเท่าเดิมทั้งที่มีวอลุ่มเป็นเรื่องปกติ)
    ตัวเลข 20 ที่เหมาะกับ US/HK/JP (baseline จริงแค่ 1-4 ตัว) จะ false-positive ทุกวันถ้าใช้กับ
    หุ้นไทย จึงต้องส่ง dup_threshold สูงกว่านี้มากสำหรับ TH โดยเฉพาะ (ดูจุดเรียกใช้)

    universe — ถ้าระบุ (set ของ ticker) จะกรองเฉพาะ ticker กลุ่มนี้ก่อนนับ (ดู
    _dh_th_universe_tickers เหตุผลที่ต้องกรองฝั่งหุ้นไทย) US/HK/JP ไม่ต้องกรอง เพราะ DB
    ของ 3 ตลาดนั้นมีแต่สมาชิกดัชนีล้วนๆ อยู่แล้ว (ไม่ปนกับ DW/DR)

    เดิมเคยดึงด้วย ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) รอบเดียวทั้งตาราง
    แต่ prices เป็น WITHOUT ROWID จัดเรียงจริงตาม (ticker, date) ASC — ORDER BY date DESC ในแต่ละ
    partition ทำให้ SQLite ต้อง sort ทั้งตารางลง temp B-tree ก่อน วัดจริงบนเครื่อง 2569-08-13:
    set_prices.db (4.4M แถว) 4.3s, us_prices.db 4.6s รวม 4 ตลาด ~10.6s ทุกครั้งที่เปิดหน้า Data
    Health (คือสาเหตุหลักที่หน้านี้โหลดช้า) เปลี่ยนมา query ทีละ ticker ด้วย
    ORDER BY date DESC LIMIT 2 แทน — ใช้ primary key (ticker, date) seek ตรงๆ ไม่ต้อง sort
    ทั้งตาราง วัดใหม่รวม 4 ตลาดเหลือ <1.5s"""
    import sqlite3
    if not os.path.exists(db_path):
        return _dh_quality_item(key, label, "na", "ยังไม่มีไฟล์ราคา")
    try:
        con = sqlite3.connect(db_path)
        try:
            tickers = sorted(universe) if universe else [
                r[0] for r in con.execute("SELECT DISTINCT ticker FROM prices").fetchall()]
            dup_syms = []
            total = 0
            for tk in tickers:
                bars = con.execute(
                    "SELECT close, volume FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 2",
                    (tk,)).fetchall()
                if not bars:
                    continue   # อยู่ใน universe แต่ยังไม่มีราคาใน DB เลย (เพิ่งเข้าดัชนี) — ไม่นับ
                total += 1
                if len(bars) == 2:
                    (c1, v1), (c2, _v2) = bars
                    if c1 is not None and c1 == c2 and v1:   # v1>0 — วอลุ่มจริงแต่ราคานิ่งเป๊ะถึงนับ
                        dup_syms.append(tk)
                # len(bars) == 1: หุ้นมีแท่งเดียว (เพิ่ง backfill/เพิ่งเข้าดัชนี) — ไม่มีคู่ให้เทียบ
        finally:
            con.close()
    except Exception as e:
        return _dh_quality_item(key, label, "red", f"เช็คไม่ได้ (exception): {str(e)[:120]}")

    if total < min_universe:
        return _dh_quality_item(key, label, "na", f"หุ้นในฐานข้อมูลน้อยเกินไป ({total} ตัว) — ยังเช็คไม่ได้แม่นยำ")

    n = len(dup_syms)
    if n >= dup_threshold:
        status = "red" if n >= dup_threshold * 2 else "warn"
        more = "…" if n > 10 else ""
        note = (f"⚠ สงสัยอัพเดทได้ราคาซ้ำเดิม — {n}/{total} ตัว ราคาปิดเท่าเดิมเป๊ะกับแท่งก่อนหน้า "
                f"({', '.join(dup_syms[:10])}{more}) ผิดปกติทางสถิติถ้าไม่ใช่ตลาดหยุดทั้งกระดาน")
    else:
        status = "ok"
        note = f"ปกติ — {n}/{total} ตัวราคาซ้ำแท่งก่อนหน้า (ไม่ผิดปกติ)"
    return _dh_quality_item(key, label, status, note)


def _dh_error_item(key, label, category, err):
    """item แดงสำหรับตอนที่เรียก get_meta()/read_status() ของแหล่งข้อมูลนั้นแล้ว exception
    หลุดออกมาเอง (เช่น sqlite ล็อค/เสียหาย, ไฟล์ cache ถูกเขียนทับพอดีตอนอ่าน) — ใช้แทน
    _dh_item() ตรงจุดที่ยังไม่มี try/except ครอบ กัน exception ตัวเดียวทำให้ทั้ง endpoint
    /api/data-health ตอบ 500 (ไม่ใช่ JSON) จนหน้า Data Health ทั้งหน้าไม่ขึ้นอะไรเลย"""
    return {"key": key, "label": label, "category": category,
            "last_at": None, "age_hours": None, "status": "red",
            "note": f"⚠ เช็คสถานะไม่ได้ (exception): {err}"}


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


_data_health_cache = {"result": None, "ts": 0}
_data_health_lock  = threading.Lock()
_DATA_HEALTH_TTL   = 300   # 5 นาที — หน้านี้วัด "ความสดของข้อมูล" หน่วยชั่วโมง/วัน
                            # ไม่ต้อง real-time ระดับวินาที (ปุ่มรีเฟรชใช้ ?fresh=1 ข้าม cache ได้)


@app.route("/api/data-health")
def data_health():
    """สถานะความสดของไฟล์/DB หลักทุกตัวที่ dashboard ใช้ — เกณฑ์ ok/warn/red
    ต่อรายการ (ดู PLAN_universe_data_health.txt ส่วนที่ 6 งาน 1)

    cache 5 นาที (double-checked locking เหมือน /api/sector-compare) — เดิมไม่มี cache เลย
    คำนวณใหม่ทั้งหน้าทุกครั้งที่เปิด (วัดจริง ~3.6 วิ: สแกน 4 price DB + coverage งบ 4 รอบ)"""
    fresh = request.args.get("fresh") == "1"
    if not fresh:
        cached = _data_health_cache.get("result")
        if cached and (time.time() - _data_health_cache.get("ts", 0) < _DATA_HEALTH_TTL):
            return jsonify(cached)

    with _data_health_lock:
        if not fresh:
            cached = _data_health_cache.get("result")
            if cached and (time.time() - _data_health_cache.get("ts", 0) < _DATA_HEALTH_TTL):
                return jsonify(cached)
        result = _compute_data_health()
        _data_health_cache["result"] = result
        _data_health_cache["ts"] = time.time()
        return jsonify(result)


def _compute_data_health():
    items = []

    # ราคา/เทคนิค — auto 3 รอบ/วัน (จ-ศ), gap วันหยุดสุดสัปดาห์ ~59.5 ชม.
    try:
        items.append(_dh_item(
            "prices", "ราคา/RS/เทคนิค (หุ้นไทย)", "ราคา/เทคนิค",
            _dh_parse(price_store.get_meta(BASE_DIR, "updated_at")), 30, 72))
    except Exception as e:
        items.append(_dh_error_item("prices", "ราคา/RS/เทคนิค (หุ้นไทย)", "ราคา/เทคนิค", str(e)[:160]))

    # dup_threshold=600 (~65% ของจักรวาล 929 ตัว) ไม่ใช่ 20 แบบ US/HK/JP — วัด baseline จริง
    # จาก 5 คู่วันล่าสุดที่ยืนยันแล้วว่าอัพเดทถูกต้อง ได้ 251-306 ตัว/วัน (เพนนีสต็อกฝั่ง mai
    # ปิดราคาเท่าเดิมทั้งที่มีวอลุ่มเป็นเรื่องปกติของตลาดนี้) ตั้ง 600 ให้เหลือ margin ~2 เท่า
    # จาก baseline สูงสุด กันไม่ให้เตือนพร่ำเพรื่อทุกวัน แต่ยังจับ "อัพเดทได้ราคาซ้ำเกือบทั้ง
    # กระดาน" ได้จริงถ้าเกิดขึ้น (ดู docstring _dh_stale_close_check)
    items.append(_dh_stale_close_check(
        "prices_stale_dup", "ราคาซ้ำแท่งก่อนหน้าผิดปกติ (หุ้นไทย)",
        os.path.join(BASE_DIR, price_store.DB_FILE),
        dup_threshold=600,
        universe=_dh_th_universe_tickers()))

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

    try:
        _insider_at = _dh_parse(sec_store._get_meta(BASE_DIR, "insider_last_synced_at"))
        items.append(_dh_item(
            "insider", "Insider / ผู้ถือหุ้นใหญ่", "Flow/เจ้าของ",
            _insider_at, 48, 168))
    except Exception as e:
        items.append(_dh_error_item("insider", "Insider / ผู้ถือหุ้นใหญ่", "Flow/เจ้าของ", str(e)[:160]))

    # Hedge Holdings (13F superinvestors จาก Dataroma) — ยื่นรายไตรมาส ดีเลย์ ~45 วัน
    # เกณฑ์เตือนจึงหลวมกว่า flow อื่น (แนะนำกดรีเฟรชเดือนละครั้งพอ — ดู sources/dataroma.py)
    from sources import dataroma as _dataroma_dh
    try:
        _hedge_cache = _dataroma_dh.load_cache(BASE_DIR)
        items.append(_dh_item(
            "hedge_holdings", "Hedge Holdings 13F (Dataroma)", "Flow/เจ้าของ",
            _dh_parse(_hedge_cache.get("generated_at")) if _hedge_cache else None,
            45 * 24, 100 * 24,
            missing_note="ยังไม่เคยดึง Hedge Holdings ในเครื่องนี้ — กดปุ่ม '⟳ อัพเดท Hedge Holdings' "
                          "ในหน้า 🐋 Hedge Holdings", optional=True))
    except Exception as e:
        items.append(_dh_error_item("hedge_holdings", "Hedge Holdings 13F (Dataroma)", "Flow/เจ้าของ", str(e)[:160]))

    # หุ้นเข้าใหม่/ถูกถอด — ไฟล์อังกฤษเป็นตัวหลัก (symbol/market/industry/sector ทั้งระบบ
    # อ่านจากไฟล์นี้) ส่วนไฟล์ไทยให้แค่ "ชื่อบริษัทภาษาไทย" เสริม ไม่มีก็ไม่พัง — จึงไม่เอา
    # มารวมตัดสินสถานะ แค่ต่อท้ายหมายเหตุถ้าหาย/ค้างเกิน 90 วัน (ดู set_data_fetcher.py)
    _set_uni = _dh_item(
        "set_universe", "รายชื่อหุ้น SET (listedCompanies xls)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "listedCompanies_en_US.xls")), 45 * 24, 90 * 24)
    _th_xls_at = _dh_mtime(os.path.join(BASE_DIR, "listedCompanies_th_TH.xls"))
    _th_age = _dh_age_hours(_th_xls_at)
    _th_warn = None
    if _th_age is None:
        _th_warn = "⚠ ยังไม่มีไฟล์ชื่อไทย (_th_TH) — ตารางหุ้นไทยจะโชว์ชื่ออังกฤษอย่างเดียว"
    elif _th_age >= 90 * 24:
        _th_warn = f"⚠ ไฟล์ชื่อไทย (_th_TH) เก่า {_th_age/24:.0f} วัน — ชื่อไทยของหุ้นเข้าใหม่อาจยังไม่มี"
    if _th_warn:
        _prev = _set_uni.get("note")
        _set_uni["note"] = f"{_prev} · {_th_warn}" if _prev else _th_warn
    items.append(_set_uni)

    items.append(_dh_item(
        "us_index_membership", "สมาชิกดัชนี US (S&P500/DOW/NDX)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "us_index_membership.json")), 45 * 24, 90 * 24))

    items.append(_dh_item(
        "hk_index_membership", "สมาชิกดัชนี HK (HSI/HSCEI/HSTECH)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "hk_index_membership.json")), 45 * 24, 90 * 24))

    items.append(_dh_item(
        "jp_index_membership", "สมาชิกดัชนี JP (Nikkei 225)", "หุ้นเข้าใหม่/ถูกถอด",
        _dh_mtime(os.path.join(BASE_DIR, "data", "jp_index_membership.json")), 45 * 24, 90 * 24))

    # ตรวจ drift อัตโนมัติ (report-only, สัปดาห์ละครั้ง — piggyback ท้าย Quick Update ดู
    # _run_index_drift_checks) ต่างจาก 4 item ด้านบนที่เช็คแค่ "mtime ของไฟล์" — รายการนี้
    # เปรียบเทียบจริงกับแหล่งสด (SET.or.th/Wikipedia) แล้วเตือนถ้ามีตัวเข้า/ออกที่ sync
    # มือยังไม่ตรง กันเคสไฟล์ local ค้างเงียบๆ นานเป็นเดือนโดยไม่มีใครสังเกต (ดู
    # PLAN_universe_data_health.txt)
    try:
        _drift_status = index_drift.read_status(BASE_DIR)
        if not isinstance(_drift_status, dict):
            _drift_status = {}
    except Exception:
        _drift_status = {}
    items.append(_dh_drift_item("TH", "ตรวจ drift หุ้นไทย (auto, เทียบ SET.or.th)", _drift_status))
    items.append(_dh_drift_item("US", "ตรวจ drift ดัชนี US (auto, เทียบ Wikipedia)", _drift_status))
    items.append(_dh_drift_item("HK", "ตรวจ drift ดัชนี HK (auto, เทียบ Wikipedia)", _drift_status))
    items.append(_dh_drift_item("JP", "ตรวจ drift ดัชนี JP (auto, เทียบ Wikipedia)", _drift_status))

    # ราคา/metrics หุ้นดัชนี US/HK/JP — auto ผ่าน Quick Update/Index Max, เกณฑ์เดียวกับ
    # ราคาหุ้นไทย (30/72 ชม.) เพราะ upsert_bars() stamp 'updated_at' รอบเดียวกัน
    from core import us_store as _us_store, hk_store as _hk_store, jp_store as _jp_store
    try:
        items.append(_dh_item(
            "us_prices", "ราคาหุ้นดัชนี US (us_prices.db)", "ราคา/เทคนิค",
            _dh_parse(_us_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
            missing_note="ยังไม่เคยอัพเดทหุ้น US ในเครื่องนี้", optional=True))
    except Exception as e:
        items.append(_dh_error_item("us_prices", "ราคาหุ้นดัชนี US (us_prices.db)", "ราคา/เทคนิค", str(e)[:160]))

    items.append(_dh_stale_close_check(
        "us_prices_stale_dup", "ราคาซ้ำแท่งก่อนหน้าผิดปกติ (หุ้น US)",
        os.path.join(BASE_DIR, _us_store.DB_FILE)))

    try:
        items.append(_dh_item(
            "hk_prices", "ราคาหุ้นดัชนี HK (hk_prices.db)", "ราคา/เทคนิค",
            _dh_parse(_hk_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
            missing_note="ยังไม่เคยอัพเดทหุ้น HK ในเครื่องนี้", optional=True))
    except Exception as e:
        items.append(_dh_error_item("hk_prices", "ราคาหุ้นดัชนี HK (hk_prices.db)", "ราคา/เทคนิค", str(e)[:160]))

    items.append(_dh_stale_close_check(
        "hk_prices_stale_dup", "ราคาซ้ำแท่งก่อนหน้าผิดปกติ (หุ้น HK)",
        os.path.join(BASE_DIR, _hk_store.DB_FILE)))

    try:
        items.append(_dh_item(
            "jp_prices", "ราคาหุ้นดัชนี JP (jp_prices.db)", "ราคา/เทคนิค",
            _dh_parse(_jp_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
            missing_note="ยังไม่เคยอัพเดทหุ้น JP ในเครื่องนี้", optional=True))
    except Exception as e:
        items.append(_dh_error_item("jp_prices", "ราคาหุ้นดัชนี JP (jp_prices.db)", "ราคา/เทคนิค", str(e)[:160]))

    items.append(_dh_stale_close_check(
        "jp_prices_stale_dup", "ราคาซ้ำแท่งก่อนหน้าผิดปกติ (หุ้น JP)",
        os.path.join(BASE_DIR, _jp_store.DB_FILE)))

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
    try:
        _fin_summary = financials_store.get_meta_summary(BASE_DIR)
        items.append(_dh_item(
            "financials", "งบการเงิน หุ้นไทย+DR (financials.db)", "งบการเงิน",
            _dh_parse(_fin_summary.get("last_synced_at")), 100 * 24, 150 * 24,
            missing_note="ยังไม่เคยรัน update_financials.py"))
    except Exception as e:
        items.append(_dh_error_item("financials", "งบการเงิน หุ้นไทย+DR (financials.db)", "งบการเงิน", str(e)[:160]))

    # แจกแจง coverage ต่อแหล่งข้อมูลงบการเงิน (Yahoo/Finnomena/SET company-highlight/SET P&L
    # รายไตรมาส) — ต่างจาก item "financials" ด้านบนที่เช็คแค่ mtime ของ sync ล่าสุด รายการนี้
    # เทียบ universe หุ้นไทยจริง (_financials_universe, ตัวเดียวกับ /api/financials-coverage)
    # กับที่มีข้อมูลจริงต่อแหล่งใน DB ให้เห็นว่าแหล่งไหน sync ยังไม่ครบทั้งตลาดจริงๆ (เช่น
    # set_qpl เพิ่งเพิ่มเข้า bulk sync — ยิงหลาย request/หุ้น sync ทั้งตลาดใช้เวลา ~1 ชม.)
    try:
        _fin_cov_srcs = [("yahoo", "Yahoo Finance"), ("finnomena_q", "Finnomena"),
                          ("set", "SET company-highlight"), ("set_qpl", "SET P&L รายไตรมาส")]
        _fin_cov = financials_store.get_coverage(BASE_DIR, _financials_universe(),
                                                  sources=[k for k, _ in _fin_cov_srcs])
        _fin_cov_rows = []
        _fin_cov_worst_pct = 100.0
        for _k, _label in _fin_cov_srcs:
            _c = _fin_cov.get(_k, {"covered": 0, "total": 0})
            _total = _c["total"]
            _pct = (_c["covered"] / _total * 100) if _total else 0.0
            _fin_cov_worst_pct = min(_fin_cov_worst_pct, _pct)
            _color = "var(--green)" if _pct >= 99.5 else ("var(--yellow)" if _pct >= 90 else "var(--red)")
            _fin_cov_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_label}</td>'
                f'<td style="text-align:right;color:{_color};font-weight:600">'
                f'{_c["covered"]}/{_total} ({_pct:.1f}%)</td></tr>')
        _fin_cov_status = ("ok" if _fin_cov_worst_pct >= 99.5
                            else "warn" if _fin_cov_worst_pct >= 90 else "red")
        _fin_cov_extra = ""
        if _fin_cov_worst_pct < 90:
            _fin_cov_extra = ('<div style="margin-top:6px">ยังไม่ sync ทั้งตลาด — กดปุ่ม '
                               '"🔄 อัพเดทงบการเงินทั้งหมด" ด้านล่างเพื่อ sync ให้ครบ '
                               '(set_qpl ยิงหลาย request/หุ้น sync ทั้งตลาดใช้เวลานานสุด ~1 ชม.)</div>')
        items.append({
            "key": "financials_coverage_by_source",
            "label": "งบการเงิน — แจกแจงตามแหล่ง (financials.db)",
            "category": "งบการเงิน",
            "last_at": None, "age_hours": None,
            "status": _fin_cov_status,
            "note": (f'<table style="border-collapse:collapse">{"".join(_fin_cov_rows)}</table>'
                     f'{_fin_cov_extra}'),
        })
    except Exception as e:
        items.append(_dh_error_item("financials_coverage_by_source",
                                     "งบการเงิน — แจกแจงตามแหล่ง (financials.db)", "งบการเงิน", str(e)[:160]))

    # เช็คว่าไตรมาสล่าสุดที่ 'ควรจะมีข้อมูลแล้ว' (_target_period) ยังขาดกี่ตัวต่อแหล่ง — ต่างจาก
    # item ด้านบนที่นับว่า 'มีข้อมูล' แม้จะเก่าแค่ไหนก็ตาม (เช่น sync ไปตั้งแต่ Q4 ปีก่อนก็นับ
    # covered) รายการนี้ตอบคำถาม 'ตัวไหนยังไม่ได้ Q ล่าสุดจริง' ตามคำขอ user 2026-08-20
    try:
        _finq_cov = financials_store.get_quarter_coverage(
            BASE_DIR, _financials_universe(),
            sources=("set", "set_qpl", "yahoo_q", "finnomena_q"))
        _finq_labels = {"set": "SET company-highlight", "set_qpl": "SET P&L รายไตรมาส",
                         "yahoo_q": "Yahoo Finance", "finnomena_q": "Finnomena"}
        _finq_rows = []
        _finq_worst_pct = 100.0
        _finq_missing_total = 0
        for _k in ("set", "set_qpl", "yahoo_q", "finnomena_q"):
            _c = _finq_cov.get(_k, {"fresh": 0, "total": 0, "stale": 0})
            _total = _c["total"]
            _pct = (_c["fresh"] / _total * 100) if _total else 0.0
            _finq_worst_pct = min(_finq_worst_pct, _pct)
            _finq_missing_total += _c["stale"]
            _color = "var(--green)" if _pct >= 99.5 else ("var(--yellow)" if _pct >= 90 else "var(--red)")
            _finq_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_finq_labels[_k]}</td>'
                f'<td style="text-align:right;color:{_color};font-weight:600">'
                f'{_c["fresh"]}/{_total} ({_pct:.1f}%)</td>'
                f'<td style="padding-left:10px;color:var(--text2)">เป้าหมาย {_c["target"]}</td></tr>')
        # threshold หลวมกว่า item 'มีข้อมูล' ด้านบนมาก (99.5/90) เพราะบริษัทมีสิทธิ์ยื่นงบช้าได้
        # ถึง 45-60 วันหลังปิดไตรมาส (_target_period ไม่รอ deadline ถือว่า 'ควรมีแล้ว' ทันทีที่
        # ไตรมาสปฏิทินปิด) — % ต่ำตอนไตรมาสเพิ่งปิดใหม่ๆ เป็นเรื่องปกติ ไม่ใช่ sync ล้มเหลว
        _finq_status = ("ok" if _finq_worst_pct >= 95
                         else "warn" if _finq_worst_pct >= 50 else "red")
        items.append({
            "key": "financials_quarter_freshness",
            "label": "งบการเงิน — เช็คงวดล่าสุด (financials.db)",
            "category": "งบการเงิน",
            "last_at": None, "age_hours": None,
            "status": _finq_status,
            "note": (f'<table style="border-collapse:collapse">{"".join(_finq_rows)}</table>'
                     f'<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                     f'เป้าหมาย = ไตรมาสปฏิทินล่าสุดที่ปิดไปแล้ว (ไม่รอ deadline ยื่นงบ 45-60 วัน — '
                     f'% ต่ำตอนไตรมาสเพิ่งปิดใหม่ๆ เป็นเรื่องปกติ ไม่ใช่ sync ล้มเหลว)</div>'
                     + (f'<div style="margin-top:4px">ยังไม่ได้งวดล่าสุด {_finq_missing_total} คู่ (หุ้น×แหล่ง) — '
                        'กดปุ่ม "🔄 อัพเดทงบการเงินทั้งหมด" ด้านล่าง (ใช้ skip_up_to_date '
                        'ยิงเฉพาะที่ขาดอัตโนมัติอยู่แล้ว) หรือดูรายละเอียดที่ปุ่ม '
                        '"🗓️ เช็คงวดล่าสุด" หน้า "งบการเงิน"</div>' if _finq_missing_total else '')),
        })
    except Exception as e:
        items.append(_dh_error_item("financials_quarter_freshness",
                                     "งบการเงิน — เช็คงวดล่าสุด (financials.db)", "งบการเงิน", str(e)[:160]))

    # coverage ของ mirror งบดัชนีหลัก US/HK/JP (source 'yahoo' namespace FINN:/DR:) — แยก
    # "ไม่เคย sync จริง" ออกจาก "เก่าเกิน 1 ปี" ให้เห็นในแอปเลย ไม่ต้องไล่เช็คมือแบบ 2026-08-15
    # (วันนั้นเจอ 104 ตัวที่เข้าใจผิดว่า 'ไม่เคย sync' ทั้งที่จริงมีอยู่แล้วใต้ namespace 'DR:' —
    # get_mirror_index_coverage เช็คถูก namespace แล้ว ไม่เกิด false positive แบบนั้นอีก)
    #
    # โหลด 'have' (ทุกแถว source='yahoo') ครั้งเดียว ใช้ร่วมกับ 4 การเช็ค coverage ด้านล่าง
    # (ก้อนรวม US/HK/JP + แยกย่อย SP500/Dow/Nasdaq100 อีก 3 รอบ) — เดิมแต่ละรอบ query DB
    # ใหม่ทุกครั้ง (วัดจริง ~0.9 วิรวม) ถ้าโหลดพลาดตรงนี้ปล่อยเป็น None ให้แต่ละ try ด้านล่าง
    # (ที่ยังมี except คลุมแยกอิสระเหมือนเดิม) โหลดซ้ำเองแทน
    try:
        _mim_have = financials_store.load_mirror_yahoo_have(BASE_DIR)
    except Exception:
        _mim_have = None

    try:
        from sources import us_index_metrics as _mim_us, hk_index_metrics as _mim_hk, jp_index_metrics as _mim_jp
        _mim_us_syms = [s["symbol"] for s in _mim_us.load_local(BASE_DIR).get("stocks", [])]
        _mim_hk_syms = [s["symbol"].replace(".HK", "") for s in _mim_hk.load_local(BASE_DIR).get("stocks", [])]
        _mim_jp_stocks = _mim_jp.load_local(BASE_DIR).get("stocks", [])
        _mim_jp_syms = [s["symbol"][:-2] for s in _mim_jp_stocks if s["symbol"].endswith(".T")]
        _have = _mim_have if _mim_have is not None else financials_store.load_mirror_yahoo_have(BASE_DIR)
        _mim_cov = financials_store.mirror_index_coverage_from_have(
            _have, {"US": _mim_us_syms, "HK": _mim_hk_syms, "JP": _mim_jp_syms}, stale_days=365)
        _mim_total = _mim_cov["total"]
        _mim_missing = len(_mim_cov["missing"])
        _mim_stale = len(_mim_cov["stale"])
        _mim_status = "red" if _mim_missing else ("warn" if _mim_stale else "ok")
        _mim_stale_rows = "".join(
            f'<tr><td style="padding:2px 10px 2px 0">{ex}:{name}</td>'
            f'<td style="text-align:right;color:var(--text2)">{latest} ({age}d)</td></tr>'
            for ex, name, latest, age in _mim_cov["stale"][:15])
        items.append({
            "key": "mirror_index_coverage",
            "label": "งบดัชนีหลัก US/HK/JP — mirror coverage (financials.db)",
            "category": "งบการเงิน",
            "last_at": None, "age_hours": None,
            "status": _mim_status,
            "note": (f'สด {_mim_cov["fresh"]}/{_mim_total} · เก่าเกิน 1 ปี {_mim_stale} · '
                     f'ไม่เคย sync {_mim_missing}'
                     + (f'<div style="margin-top:6px">ยังไม่เคย sync {_mim_missing} ตัว — กดปุ่ม '
                        '"🔄 อัพเดทงบการเงินทั้งหมด" ด้านล่างเพื่อ sync ให้ครบ</div>' if _mim_missing else "")
                     + (f'<details style="margin-top:6px"><summary style="cursor:pointer">'
                        f'ตัวที่เก่าเกิน 1 ปี ({_mim_stale})</summary>'
                        f'<table style="border-collapse:collapse;margin-top:4px">{_mim_stale_rows}</table>'
                        f'{"" if _mim_stale <= 15 else f"<div>...และอีก {_mim_stale-15} ตัว</div>"}'
                        f'</details>' if _mim_stale else "")),
        })
    except Exception as e:
        items.append(_dh_error_item("mirror_index_coverage",
                                     "งบดัชนีหลัก US/HK/JP — mirror coverage (financials.db)", "งบการเงิน", str(e)[:160]))

    # แจกแจง coverage งบ US แยกตาม 3 ดัชนีหลัก (S&P500/Dow/Nasdaq100) — item ด้านบนรวม
    # US ทั้งก้อนเป็นตัวเลขเดียว มองไม่ออกว่าดัชนีไหน sync ไม่ครบไปกี่ตัว รูปแบบ "covered/total"
    # เหมือน financials_coverage_by_source ด้านบน (นับว่า "มีข้อมูล" ไม่ว่าจะเก่าแค่ไหน
    # ต่างจาก mirror_index_coverage ที่แยกสด/เก่า/ไม่เคย sync)
    try:
        from sources import us_index_membership as _uim
        _uim_local = _uim.load_local(BASE_DIR)
        _uim_groups = [("SP500", "S&P 500"), ("DOW", "Dow Jones"), ("NDX", "Nasdaq 100")]
        _uim_rows = []
        _uim_worst_pct = 100.0
        _uim_missing_by_group = {}
        _have = _mim_have if _mim_have is not None else financials_store.load_mirror_yahoo_have(BASE_DIR)
        for _gk, _glabel in _uim_groups:
            _gsyms = [s.upper().strip() for s in (_uim_local.get(_gk) or [])]
            _gcov = financials_store.mirror_index_coverage_from_have(_have, {"US": _gsyms}, stale_days=365)
            _gtotal = _gcov["total"]
            _gmissing = _gcov["missing"]
            _gcovered = _gtotal - len(_gmissing)
            _gpct = (_gcovered / _gtotal * 100) if _gtotal else 0.0
            _uim_worst_pct = min(_uim_worst_pct, _gpct)
            _gcolor = "var(--green)" if _gpct >= 99.5 else ("var(--yellow)" if _gpct >= 90 else "var(--red)")
            _uim_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_glabel}</td>'
                f'<td style="text-align:right;color:{_gcolor};font-weight:600">'
                f'{_gcovered}/{_gtotal} ({_gpct:.1f}%)</td></tr>')
            if _gmissing:
                _uim_missing_by_group[_glabel] = [name for _, name in _gmissing]
        _uim_status = ("ok" if _uim_worst_pct >= 99.5
                        else "warn" if _uim_worst_pct >= 90 else "red")
        _uim_extra = ""
        if _uim_missing_by_group:
            _uim_missing_html = "".join(
                f'<div style="margin-top:4px"><b>{lbl}</b> ({len(syms)}): {", ".join(syms)}</div>'
                for lbl, syms in _uim_missing_by_group.items())
            _uim_extra = (f'<details style="margin-top:6px"><summary style="cursor:pointer">'
                           f'ตัวที่ยังไม่มีงบ</summary>{_uim_missing_html}</details>')
        items.append({
            "key": "us_index_coverage_by_index",
            "label": "งบการเงิน US — แจกแจงตามดัชนี (S&P500/Dow/Nasdaq100)",
            "category": "งบการเงิน",
            "last_at": None, "age_hours": None,
            "status": _uim_status,
            "note": f'<table style="border-collapse:collapse">{"".join(_uim_rows)}</table>{_uim_extra}',
        })
    except Exception as e:
        items.append(_dh_error_item("us_index_coverage_by_index",
                                     "งบการเงิน US — แจกแจงตามดัชนี (S&P500/Dow/Nasdaq100)", "งบการเงิน", str(e)[:160]))

    # เก็บเป็น JSON string {"at": "YYYY-MM-DD HH:MM", "ok":.., "empty":.., "fail":.., "total":.., "force":..}
    # ไม่ใช่ plain datetime string เหมือน meta อื่น — ต้องแกะก่อน parse
    try:
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
    except Exception as e:
        items.append(_dh_error_item("mirror", "Mirror งบ US/HK ทั้งตลาด (Finnomena)", "งบการเงิน", str(e)[:160]))

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

    try:
        _idx_covered, _idx_total = dr_descriptions.index_universe_coverage(BASE_DIR)
        _idx_pct = (_idx_covered / _idx_total * 100) if _idx_total else 0
        _idx_status = "ok" if _idx_pct >= 90 else ("warn" if _idx_pct >= 50 else "red")
        items.append(_dh_quality_item(
            "dr_descriptions_index", "คำอธิบายบริษัทดัชนีหลัก US/HK/JP (Nikkei/HSI/S&P500 ฯลฯ)",
            _idx_status, f"{_idx_covered}/{_idx_total} ตัว ({_idx_pct:.0f}%) มีคำแปลไทยแล้ว"))
    except Exception as e:
        items.append(_dh_quality_item(
            "dr_descriptions_index", "คำอธิบายบริษัทดัชนีหลัก US/HK/JP (Nikkei/HSI/S&P500 ฯลฯ)",
            "red", f"เช็คไม่ได้: {str(e)[:120]}"))

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
        # แต่ถ้าต่ำกว่า 1MB = แทบแน่นอนว่าเป็น payload ว่าง/พร่อง ไม่ใช่แค่ slim — เช็ค mb
        # ก่อนแยกเป็น red เดี่ยวๆ ไม่งั้นเคส mb<1 แต่ bn>=500 (ไม่เกิดจริงในทางปฏิบัติ แต่ถ้าเกิด
        # ก็ควรแดง) จะหลุดไปเป็นแค่ warn ทั้งที่ข้อความข้างล่างบอกว่า "<1 MB = payload ว่าง"
        if _bake_mb < 1.0 or _bn < 500:
            _q2 = "red"
        elif _bn < 800:
            _q2 = "warn"
        else:
            _q2 = "ok"
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

    # ความครบ/คุณภาพดัชนี US/HK/JP (membership vs metrics, sector Unknown) — เพิ่มหลังเจอเคส
    # FER (NDX) sector Unknown จาก regex parse พลาด 2026-08-02 (ดู _dh_index_quality_item)
    items.append(_dh_index_quality_item(
        "q_us_index", "ความครบ/คุณภาพดัชนี US (membership + metrics)", "US",
        os.path.join(_DATA_DIR, "us_index_membership.json"),
        os.path.join(_DATA_DIR, "us_index_metrics.json"),
        ("SP500", "DOW", "NDX"),
        {"SP500": (495, 515), "DOW": (28, 32), "NDX": (98, 108)}))

    items.append(_dh_index_quality_item(
        "q_hk_index", "ความครบ/คุณภาพดัชนี HK (membership + metrics)", "HK",
        os.path.join(_DATA_DIR, "hk_index_membership.json"),
        os.path.join(_DATA_DIR, "hk_index_metrics.json"),
        ("HSI", "HSCEI", "HSTECH"),
        {"HSI": (75, 90), "HSCEI": (45, 55), "HSTECH": (25, 35)}))

    items.append(_dh_index_quality_item(
        "q_jp_index", "ความครบ/คุณภาพดัชนี JP (membership + metrics)", "JP",
        os.path.join(_DATA_DIR, "jp_index_membership.json"),
        os.path.join(_DATA_DIR, "jp_index_metrics.json"),
        ("NIKKEI225",),
        {"NIKKEI225": (215, 232)}))

    # รูขาดข้อมูลกลางชุด (ไม่ใช่แค่ "สดแค่ไหน") — market/s50/bond flow เป็นไฟล์สะสมที่ผูกกับ
    # "วันไหนมีใครเปิดแอป/Actions รันติดพอดี" ถ้าขาดหลายวันติดกันจะไม่โผล่ในเกณฑ์ mtime ด้านบนเลย
    # (ไฟล์ยังถูกเขียนทุกวันจากวันที่มีข้อมูล แค่วันกลางๆ หาย) ดู _dh_gap_check_item
    items.append(_dh_gap_check_item(
        "q_market_flow_gap", "รูขาดข้อมูล Capital Flow (60 วันล่าสุด)", _MARKET_FLOW_FILE))
    items.append(_dh_gap_check_item(
        "q_s50_flow_gap", "รูขาดข้อมูล S50 Futures Flow (60 วันล่าสุด)", _S50_FLOW_FILE))
    items.append(_dh_gap_check_item(
        "q_bond_flow_gap", "รูขาดข้อมูล Bond Flow (60 วันล่าสุด)", _BOND_FLOW_FILE))

    summary = {"ok": 0, "warn": 0, "red": 0, "na": 0}
    for it in items:
        summary[it["status"]] += 1

    return {
        "checked_at": _dh_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        "items": items,
        "summary": summary,
    }


@app.route("/api/data-health-ping")
def data_health_ping():
    """ยิงทดสอบแหล่งข้อมูลภายนอกทีละ 1 request เบาๆ (timeout สั้น) — ใช้ตอบคำถาม
    'ตอนนี้ดึงจาก SET/Yahoo/Finnomena/TradingView ได้จริงไหม' ไม่ได้ผูกกับ mtime"""
    import urllib.request as _dh_ur
    from concurrent.futures import ThreadPoolExecutor

    def _ping(key, label, url, headers=None, timeout=6):
        t0 = time.time()
        try:
            req = _dh_ur.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
            # การดึงจริงทุกแหล่ง (รวม siamchart) verify cert ผ่าน core.net.ssl_context() แล้ว
            ctx = ssl_context()
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
          "Referer": "https://siamchart.com/"}, 8),
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
    ("us_prices.db", "ราคาหุ้นดัชนี US (us_prices.db)"),
    ("hk_prices.db", "ราคาหุ้นดัชนี HK (hk_prices.db)"),
    ("jp_prices.db", "ราคาหุ้นดัชนี JP (jp_prices.db)"),
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
    "restart":        "↺ Restart Server",
    "financials_sync": "🔄 อัพเดทงบการเงิน (update_financials.py)",
    "mirror_finnomena": "📥 Mirror US/HK ทั้งตลาด (mirror_finnomena.py)",
    "mirror_finnomena_normal": "📥 Mirror ทั้งตลาด (ปกติ — เฉพาะที่ขาด) (mirror_finnomena.py)",
    "build_mirror_names": "🏷️ ดึงชื่อหุ้น mirror ใหม่ (build_mirror_names.py)",
    "mirror_yahoo_index_sync": "🌐 Sync Mirror Yahoo US/HK ทั้ง universe",
    "us_index_full_refresh": "📈 ดึงราคา US Index ย้อนหลังสูงสุด",
    "hk_index_full_refresh": "📈 ดึงราคา HK Index ย้อนหลังสูงสุด",
    "jp_index_full_refresh": "📈 ดึงราคา JP Index ย้อนหลังสูงสุด",
    "offsite_backup":  "🛟 สำรองไฟล์สร้างใหม่ไม่ได้นอกเครื่อง (backup_financials_offsite.py)",
    "hedge_refresh":   "🐋 อัพเดท Hedge Holdings (Dataroma)",
    "hedge_fetch_missing": "⬇️ ดึงหุ้น Hedge Holdings ที่ยังไม่มีเข้าคลัง",
    "static_bake": "🧱 Bake ไฟล์ static ทั้งหมด (run_static_update.py)",
    "static_indices_refresh": "📊 รีเฟรช Indices (sub-step ใน run_static_update.py)",
    "static_short_sales": "📉 รีเฟรช Short Sales (sub-step ใน run_static_update.py)",
    "static_nvdr": "📥 รีเฟรช NVDR (sub-step ใน run_static_update.py)",
    "static_market_stats_daily": "📋 Import Market Stats รายวัน (Table_PE/PBV.xls)",
    "static_market_stats_monthly": "📋 Import Market Stats รายเดือน (Market_Statistics_Month)",
    "static_sec_sync": "🕵️ Sync sec_filings.db (insider/major-changes)",
    "static_snapshot_optional": "📦 Snapshot endpoint เสริม (ไม่ required) ใน run_static_update.py",
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


@app.route("/api/update-history")
def update_history():
    """ประวัติล้มเหลวสะสม (ไม่ใช่แค่รอบล่าสุด) รวมทุกกลไก เรียงเวลาล่าสุดก่อน —
    เขียนโดย core.run_log.record_run() ทุกครั้งที่มีการเรียก (เก็บ 50 รอบล่าสุด/กลไก)
    ใช้โชว์ตาราง "ประวัติล้มเหลวล่าสุด" ในหน้า Data Health เพื่อดูแนวโน้ม (ล้มติดกัน
    กี่รอบ/ล้มตอนไหนบ้าง) ต่างจาก /api/update-status ที่บอกได้แค่สถานะปัจจุบัน"""
    fails = run_log.read_recent_failures(BASE_DIR, limit=100)
    for f in fails:
        f["label"] = _UPDATE_STATUS_LABEL.get(f["source"], f["source"])
    return jsonify({"recent_failures": fails})


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
        _clear_fin_analytics_and_warm()
        _update(done=True,
                message=f"เสร็จแล้ว! ดึงงบได้ {result['ok']} ตัว · ไม่มีงบ {result['empty']} ตัว"
                        + (f" (ล้มเหลว {result['fail']} — ลองอีกครั้งได้)" if result["fail"] else ""))
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


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
        _clear_fin_analytics_and_warm()

        summary = (f"เสร็จแล้ว! ดึงงบ Yahoo ได้ {result['ok']} ตัว "
                   f"(ข้าม {result['skipped']} ที่มีอยู่แล้ว"
                   + (f", ล้มเหลว {result['fail']}" if result["fail"] else "") + ") · "
                   f"rebuild mirror snapshot: US {mirror_counts.get('US', 0)} / HK {mirror_counts.get('HK', 0)} ตัว")
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "mirror_yahoo_index_sync", True, summary)
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "mirror_yahoo_index_sync", False, str(e))
    finally:
        _update(running=False)


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
# หลังงบไตรมาสออก (ก.พ./พ.ค./ส.ค./พ.ย.) — เดิมต้องเปิด command line รัน
# update_financials.py / mirror_finnomena.py / build_snapshot.py / build_mirror_names.py
# เอง (คู่มือ-อัพเดทข้อมูล.txt) ย้ายมาเป็นปุ่มในหน้า Data Health แทน ใช้ job system
# เดียวกับ mirror-sync-new/mirror-yahoo-index-sync ด้านบน (progress bar + run_log)
# ============================================================

# เดิมข้ามคู่ (หุ้น, แหล่ง) ที่ synced_at ใหม่กว่า N วัน (min_age_days=7 → 2 → ...) กันกดซ้ำ/
# เผลอกดสองครั้งแล้วต้องรอ sync ทั้งตลาดใหม่ทั้งหมด — แต่เดาจาก "เวลา sync" ทำให้หุ้นที่เพิ่งมีข่าว
# งบไตรมาสใหม่ (แต่ SET.or.th ยังไม่ทันขึ้นข้อมูลตอนที่เรา sync ครั้งก่อน) ถูก skip ทิ้งผิดๆ จนกว่าจะ
# ครบ N วัน (2026-08-15) เปลี่ยนมาใช้ sync_all(skip_up_to_date=True) แทน — เช็คเนื้อหาจริงใน DB
# ว่ามีงวดล่าสุดที่ควรจะมีแล้วหรือยัง (ดู _target_period/_payload_latest_period ใน
# financials_store.py) ไม่ต้องเดาจำนวนวันอีกต่อไป กดกี่ครั้งก็ retry เฉพาะคู่ที่ยังไม่ครบจริงๆ


def _run_financials_update_all():
    """เทียบเท่า `python update_financials.py` (scope=all) — เช็ค DR ใหม่ → sync งบหุ้นไทย
    (Yahoo+SET+SET-QPL+Finnomena) → sync งบ DR (Yahoo+Finnomena) → sync งบสมาชิกดัชนีหลัก US
    (S&P500+Dow+NDX)/HK (HSI+HSCEI+HSTECH)/JP (Nikkei 225) ผ่าน Yahoo (ข้าม ticker ที่มีงบ
    อยู่แล้วจาก DR/DRx โดยอัตโนมัติ — ดู sync_mirror_yahoo_index) → refresh งบ mirror US/HK
    ที่ค้นบ่อยใน 90 วัน → build factor snapshot ใหม่ ปุ่มที่ใช้บ่อยสุด (ทุกครั้งหลังงบไตรมาสออก)

    SET-QPL (source 'set_qpl') = ข้อมูลตาราง P&L รายไตรมาสจาก SET official (ดู
    compute_qpl_report ใน financials_store.py) — เก็บสะสมใน DB เหมือนแหล่งอื่น กันข้อมูลละเอียด
    (COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี) หายเมื่อ SET periods list เลื่อนหลุด

    Incremental (skip_up_to_date=True, ดู sync_all ใน financials_store.py): เดิมยิงสดทุกคู่
    (หุ้น×แหล่ง) ทั้ง 929+ ตัวทุกครั้งที่กด แม้เพิ่ง sync ไปหมาดๆ — เปลืองเวลา/ยิง SET.or.th ซ้ำโดยไม่
    จำเป็น ตอนนี้ข้ามเฉพาะคู่ที่ "มีข้อมูลงวดล่าสุดที่ควรจะมีอยู่แล้วจริง" (เช็คเนื้อหา ไม่ใช่เดาจาก
    วันที่ sync) — หุ้นที่ยังไม่มีงวดล่าสุดจะถูก retry ทุกครั้งที่กดจนกว่าจะได้ ไม่ต้องรอครบ N วัน"""
    t0 = time.time()
    searched = []
    try:
        _update(current=1, total=100, message="เช็ค DR ใหม่จาก SET.or.th...")
        try:
            sync_dr_universe(BASE_DIR)
        except Exception as e:
            print(f"[FinancialsUpdateAll] DR-sync error (ข้าม ใช้ universe เดิม): {e}")

        syms_th = _financials_universe()
        _update(current=5, total=100, message=f"sync งบหุ้นไทย {len(syms_th)} ตัว...")

        def _th_cb(done, total, msg):
            pct = 5 + (done / max(total, 1) * 45)
            _update(current=round(pct), total=100, message=f"งบหุ้นไทย: {done}/{total}")
        r_th = financials_store.sync_all(BASE_DIR, syms_th,
                                          sources=("yahoo", "set", "set_qpl", "yahoo_q", "finnomena_q"),
                                          callback=_th_cb, is_dr=False,
                                          skip_up_to_date=True)

        syms_dr = _dr_financials_universe()
        _update(current=50, total=100, message=f"sync งบ DR {len(syms_dr)} ตัว...")

        def _dr_cb(done, total, msg):
            pct = 50 + (done / max(total, 1) * 20)
            _update(current=round(pct), total=100, message=f"งบ DR: {done}/{total}")
        r_dr = financials_store.sync_all(BASE_DIR, syms_dr,
                                          sources=("yahoo", "yahoo_q", "finnomena_q"),
                                          callback=_dr_cb, is_dr=True,
                                          skip_up_to_date=True)

        # sync งบสมาชิกดัชนีหลัก US (S&P500+Dow+NDX) / HK (HSI+HSCEI+HSTECH) / JP (Nikkei 225)
        # ผ่าน Yahoo — sync_mirror_yahoo_index สแกน namespace 'DR:{sym}'/'FINN:{ex}:{sym}' ที่มี
        # source='yahoo' อยู่แล้วก่อนเสมอ (ดู docstring ในฟังก์ชัน) จึงข้ามตัวที่ซ้ำกับ DR/DRx ที่
        # sync ไปแล้วข้างบนโดยอัตโนมัติ ไม่ต้องกรองมือซ้ำที่นี่
        from sources import us_index_metrics, hk_index_metrics, jp_index_metrics
        us_syms = [s["symbol"] for s in us_index_metrics.load_local(BASE_DIR).get("stocks", [])]
        hk_syms = [s["symbol"].replace(".HK", "") for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", [])]
        jp_stocks = jp_index_metrics.load_local(BASE_DIR).get("stocks", [])
        jp_syms = [s["symbol"][:-2] for s in jp_stocks if s["symbol"].endswith(".T")]
        jp_price_by_ticker = {s["symbol"][:-2]: s["price"] for s in jp_stocks
                               if s.get("price") and s["symbol"].endswith(".T")}

        _update(current=70, total=100,
                message=f"sync งบดัชนีหลัก US {len(us_syms)} · HK {len(hk_syms)} · JP {len(jp_syms)} ตัว...")

        def _idx_cb(done, total, msg):
            pct = 70 + (done / max(total, 1) * 15)
            _update(current=round(pct), total=100, message=f"งบดัชนีหลัก US/HK/JP: {done}/{total}")
        r_idx = financials_store.sync_mirror_yahoo_index(
            BASE_DIR, {"US": us_syms, "HK": hk_syms, "JP": jp_syms}, callback=_idx_cb)

        refreshed_mirror = 0
        port = set(syms_dr)
        searched = [s for s in financials_store.get_recent_searches(BASE_DIR, days=90) if s not in port]
        if searched:
            for i, s in enumerate(searched):
                _update(current=85 + round(i / len(searched) * 7), total=100,
                        message=f"refresh งบ mirror US/HK ที่ค้นบ่อย {i}/{len(searched)}...")
                try:
                    if financials_store.refresh_mirror_stock(BASE_DIR, s):
                        refreshed_mirror += 1
                except Exception:
                    pass
                time.sleep(0.3)   # throttle กัน Finnomena บล็อก

        _update(current=93, total=100, message="กำลังคำนวณ factor snapshot ใหม่...")
        factor_snapshot.build_snapshot(BASE_DIR)
        idx_changed = bool(r_idx["ok"] or r_idx["fail"])
        if refreshed_mirror or idx_changed:
            _update(current=97, total=100, message="กำลัง rebuild mirror snapshot US/HK...")
            factor_snapshot.build_mirror_snapshot(BASE_DIR, exchanges=("US", "HK"))
        if idx_changed:
            _update(current=99, total=100, message="กำลัง rebuild mirror snapshot JP...")
            factor_snapshot.build_mirror_snapshot_yahoo_only(
                BASE_DIR, "JP", jp_syms, price_by_ticker=jp_price_by_ticker)
        _fin_analytics_cache.clear()
        _sector_compare_cache.clear()
        _market_trend_cache.clear()
        _sector_trend_cache.clear()
        _warmup_fin_dependent_caches()

        elapsed_min = (time.time() - t0) / 60
        summary = (f"เสร็จแล้ว! หุ้นไทย {r_th['ok']}/{r_th['total']} (พลาด {r_th['fail']}, "
                   f"ข้าม {r_th['skipped']} คู่ที่มีงวดล่าสุดอยู่แล้ว) · "
                   f"DR {r_dr['ok']}/{r_dr['total']} (พลาด {r_dr['fail']}, ข้าม {r_dr['skipped']}) · "
                   f"ดัชนีหลัก US/HK/JP {r_idx['ok']}/{r_idx['total']} (ข้าม {r_idx['skipped']} ที่มีแล้ว)"
                   + (f" · mirror ค้นบ่อย {refreshed_mirror}/{len(searched)}" if searched else "")
                   + f" · ใช้เวลา {elapsed_min:.0f} นาที")
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "financials_sync", True, summary)
    except Exception as e:
        run_log.record_run(BASE_DIR, "financials_sync", False, str(e))
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


@app.route("/api/financials-update-all", methods=["POST"])
def financials_update_all():
    """ปุ่ม '🔄 อัพเดทงบการเงินทั้งหมด' (หน้า Data Health) — แทน `python update_financials.py`
    ครอบคลุมหุ้นไทย/DR/DRx + สมาชิกดัชนีหลัก US/HK/JP (สแกนข้าม ticker ที่มีงบซ้ำกับ DR/DRx
    อยู่แล้วอัตโนมัติ) ใช้เวลาไม่กี่นาทีถึงเกือบครึ่งชั่วโมง (ขึ้นกับจำนวนหุ้น/DR) ใช้บ่อยสุด
    (ทุกครั้งหลังงบไตรมาสออก)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=100, message="กำลังเริ่มอัพเดทงบการเงิน...")
    threading.Thread(target=_run_financials_update_all, daemon=True).start()
    return jsonify({"ok": True})


def _run_mirror_finnomena_force_full():
    """เทียบเท่า `python mirror_finnomena.py force` แล้วต่อด้วย `python build_snapshot.py`
    — ยิงซ้ำงบ Finnomena ทั้งตลาด TH+HK+US (ทุกตัวที่มีงบอยู่แล้ว) เพื่อดึงงวดใหม่มา merge
    ใช้เวลานานมาก (เป็นชั่วโมง) ควรรันไตรมาสละครั้งหลัง earnings season"""
    t0 = time.time()
    try:
        def cb(current, total, msg):
            pct = (current / max(total, 1) * 85)
            _update(current=round(pct), total=100, message=msg)
        result = financials_store.mirror_finnomena(BASE_DIR, exchanges=("TH", "HK", "US"),
                                                     callback=cb, force=True)
        _mirror_diff_cache.clear()

        _update(current=88, total=100, message="กำลังคำนวณ factor snapshot หลัก (ไทย+DR)...")
        factor_snapshot.build_snapshot(BASE_DIR)
        _update(current=94, total=100, message="กำลัง rebuild mirror snapshot US/HK...")
        factor_snapshot.build_mirror_snapshot(BASE_DIR)
        _clear_fin_analytics_and_warm()

        elapsed_min = (time.time() - t0) / 60
        summary = (f"เสร็จแล้ว! มีงบ {result['ok']} · ไม่มีงบ {result['empty']}"
                   + (f" · พลาด {result['fail']}" if result["fail"] else "")
                   + f" · ใช้เวลา {elapsed_min:.0f} นาที")
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "mirror_finnomena", True, summary)
    except Exception as e:
        run_log.record_run(BASE_DIR, "mirror_finnomena", False, str(e))
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


@app.route("/api/mirror-finnomena-force-full", methods=["POST"])
def mirror_finnomena_force_full():
    """ปุ่ม '📥 Mirror ทั้งตลาด (force) + rebuild snapshot' (หน้า Data Health) — แทน
    `python mirror_finnomena.py force` + `python build_snapshot.py` ใช้เวลานานมาก (เป็น
    ชั่วโมง) ปิดแท็บ/ปิดคอมได้ระหว่างรัน (สคริปต์ resume เองรอบหน้า) ควรรันไตรมาสละครั้ง"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=100, message="กำลังเริ่ม mirror งบ Finnomena ทั้งตลาด (force)...")
    threading.Thread(target=_run_mirror_finnomena_force_full, daemon=True).start()
    return jsonify({"ok": True})


def _run_mirror_finnomena_normal_full():
    """เทียบเท่า `python mirror_finnomena.py` (ไม่ใส่ force) แล้วต่อด้วย
    `python build_snapshot.py` — ต่างจาก force ตรงที่ข้ามตัวที่มีงบ schema ปัจจุบันอยู่แล้ว
    ยิงเฉพาะตัวที่ยังไม่มี/พลาดไปก่อนหน้า (schema เก่า/marker 'ไม่มีงบ' ก็ยิงซ้ำเพื่อเติม
    field ใหม่) เร็วกว่า force มาก เหมาะใช้ไล่เก็บตัวที่พลาดหลังรอบ force"""
    t0 = time.time()
    try:
        def cb(current, total, msg):
            pct = (current / max(total, 1) * 85)
            _update(current=round(pct), total=100, message=msg)
        result = financials_store.mirror_finnomena(BASE_DIR, exchanges=("TH", "HK", "US"),
                                                     callback=cb, force=False)
        _mirror_diff_cache.clear()

        _update(current=88, total=100, message="กำลังคำนวณ factor snapshot หลัก (ไทย+DR)...")
        factor_snapshot.build_snapshot(BASE_DIR)
        _update(current=94, total=100, message="กำลัง rebuild mirror snapshot US/HK...")
        factor_snapshot.build_mirror_snapshot(BASE_DIR)
        _clear_fin_analytics_and_warm()

        elapsed_min = (time.time() - t0) / 60
        summary = (f"เสร็จแล้ว! ดึงงบได้ {result['ok']} · ไม่มีงบ {result['empty']}"
                   + (f" · พลาด {result['fail']}" if result["fail"] else "")
                   + f" · ใช้เวลา {elapsed_min:.0f} นาที")
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "mirror_finnomena_normal", True, summary)
    except Exception as e:
        run_log.record_run(BASE_DIR, "mirror_finnomena_normal", False, str(e))
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


@app.route("/api/mirror-finnomena-normal-full", methods=["POST"])
def mirror_finnomena_normal_full():
    """ปุ่ม '📥 Mirror ทั้งตลาด (ปกติ — เฉพาะที่ขาด) + rebuild snapshot' (หน้า Data Health)
    — แทน `python mirror_finnomena.py` (ไม่ใส่ force) + `python build_snapshot.py`
    ครอบคลุม TH+HK+US เหมือนปุ่ม force แต่ข้ามตัวที่มีงบ schema ปัจจุบันอยู่แล้ว ยิงเฉพาะ
    ตัวที่ขาด/พลาดไปก่อนหน้า เร็วกว่า force มาก (นาที ไม่ใช่ชั่วโมง)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=100, message="กำลังเริ่ม mirror งบ Finnomena ทั้งตลาด (ปกติ — เฉพาะที่ขาด)...")
    threading.Thread(target=_run_mirror_finnomena_normal_full, daemon=True).start()
    return jsonify({"ok": True})


def _run_build_mirror_names():
    """เทียบเท่า `python build_mirror_names.py` — ดึงชื่อบริษัทของหุ้น US/HK ที่มีงบใน mirror
    เก็บ mirror_names.json เร็ว (ไม่กี่วินาที) รันหลัง mirror เจอหุ้นใหม่"""
    t0 = time.time()
    try:
        _update(current=10, total=100, message="ดึงรายชื่อ US จาก Finnomena...")
        out = {}
        con = financials_store._connect(BASE_DIR)
        try:
            for i, ex in enumerate(("US", "HK")):
                _update(current=10 + i * 40, total=100, message=f"ดึงรายชื่อ {ex} จาก Finnomena...")
                rows = (financials_store._finn_get(f"/stock/list?exchange={ex}", timeout=120) or {}).get("data") or []
                name_map = {(r.get("name") or "").upper(): (r.get("en_name") or "").strip()
                            for r in rows if r.get("name")}
                have = {r[0] for r in con.execute(
                    "SELECT symbol FROM financials WHERE source='finnomena_q' "
                    "AND symbol LIKE ? AND payload NOT LIKE '%\"empty\": true%'", (f"FINN:{ex}:%",))}
                have = {s.split(":", 2)[2] for s in have}
                out[ex] = {t: name_map[t] for t in have if name_map.get(t)}
        finally:
            con.close()

        _update(current=95, total=100, message="กำลังบันทึก mirror_names.json...")
        out_path = os.path.join(BASE_DIR, "mirror_names.json")
        _atomic_write_json(out_path, out)

        elapsed_s = time.time() - t0
        summary = f"เสร็จแล้ว! US {len(out['US'])} + HK {len(out['HK'])} ชื่อ ({elapsed_s:.0f} วิ)"
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "build_mirror_names", True, summary)
    except Exception as e:
        run_log.record_run(BASE_DIR, "build_mirror_names", False, str(e))
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


@app.route("/api/build-mirror-names", methods=["POST"])
def build_mirror_names_route():
    """ปุ่ม '🏷️ ดึงชื่อหุ้น mirror ใหม่' (หน้า Data Health) — แทน `python build_mirror_names.py`
    เร็ว (ไม่กี่วินาที) รันเฉพาะตอนมี mirror หุ้นใหม่ (ไม่รันก็ไม่พัง แค่ตัวใหม่โชว์ ticker เปล่า)"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=100, message="กำลังดึงชื่อหุ้น mirror ใหม่...")
    threading.Thread(target=_run_build_mirror_names, daemon=True).start()
    return jsonify({"ok": True})


# จำนวน endpoint ใน SNAPSHOTS ของ run_static_update.py — ใช้แค่ประมาณ % ความคืบหน้า
# เฟส 2 (bake) คร่าวๆ ถ้าไฟล์นั้นเพิ่ม/ลด endpoint แล้ว progress bar จะคลาดเคลื่อนนิดหน่อย
# (ไม่กระทบผลลัพธ์จริง แค่ตัวเลข % ระหว่างรอ)
_STATIC_BAKE_SNAPSHOT_TOTAL = 29


def _run_static_bake():
    """เทียบเท่า `python run_static_update.py` — รันเป็น subprocess แยก process กับ
    dev server นี้ทุกประการ (เหมือนที่ผู้ใช้รันเองใน terminal / ที่ GitHub Actions รัน)
    ทำ Quick/Full Refresh ราคา แล้ว bake ทุก /api/* endpoint ที่เว็บ static ใช้ลง
    data/*.json (รวม financials_analytics_yahoo.json, stock_valuation_stats.json ฯลฯ)
    แปลง log ของสคริปต์เป็น progress คร่าวๆ ให้ progress bar ในแอป: เฟส 1 (ราคา) อ่าน
    "[ NN%]" ที่สคริปต์พิมพ์เอง map ไป 0-35%, เฟส 2 (snapshot) นับบรรทัด ✅/⚠️/❌/⏭️
    เทียบ _STATIC_BAKE_SNAPSHOT_TOTAL map ไป 35-95% — สคริปต์บันทึก run_log ของตัวเอง
    (key "static_bake") ตอน atexit อยู่แล้วไม่ว่าใครเป็นคนรัน ไม่ต้องบันทึกซ้ำที่นี่
    ยกเว้นกรณีเปิด process ไม่ได้เลย (สคริปต์เองไม่มีโอกาสรัน atexit)"""
    script = os.path.join(BASE_DIR, "run_static_update.py")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    try:
        try:
            proc = subprocess.Popen(
                [sys.executable, script], cwd=BASE_DIR, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1)
        except Exception as e:
            run_log.record_run(BASE_DIR, "static_bake", False, f"เปิด process ไม่ได้: {e}")
            _update(done=True, error=str(e), message=f"เปิด process ไม่ได้: {e}")
            return

        snapshot_mode = False
        snapshot_done = 0
        last_line = "กำลังเริ่ม..."
        try:
            for raw in proc.stdout:
                line = raw.strip()
                if not line:
                    continue
                last_line = re.sub(r"^\[\d{2}:\d{2}:\d{2}\]\s*", "", line)
                if "Snapshot API endpoints" in line:
                    snapshot_mode = True
                if snapshot_mode and line.startswith(("✅", "⚠️", "❌", "⏭️")):
                    snapshot_done += 1
                    pct = 35 + min(snapshot_done / _STATIC_BAKE_SNAPSHOT_TOTAL, 1.0) * 60
                    _update(current=round(pct), total=100, message=last_line)
                else:
                    m = re.search(r"\[\s*(\d{1,3})%\]", line)
                    if m and not snapshot_mode:
                        _update(current=round(min(int(m.group(1)), 100) * 0.35), total=100, message=last_line)
                    else:
                        _update(message=last_line)
            proc.wait()
        except Exception as e:
            proc.kill()
            _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
            return

        if proc.returncode == 0:
            _update(done=True, error=None, current=100, total=100,
                    message="เสร็จแล้ว! bake ไฟล์ static ทั้งหมดสำเร็จ")
        else:
            _update(done=True, error=last_line,
                    message=f"เกิดข้อผิดพลาด (exit {proc.returncode}): {last_line}")
    finally:
        _update(running=False)


@app.route("/api/run-static-bake", methods=["POST"])
def run_static_bake_route():
    """ปุ่ม '🧱 Bake ไฟล์ static ทั้งหมด' (หน้า Data Health) — แทน `python run_static_update.py`
    ใช้เวลานาน (Quick Update ราคา ~ไม่กี่นาที ถึง Full Refresh ~ครึ่ง-1 ชม. ถ้ายังไม่มี
    set_prices.db) ปิดแท็บ/ปิดคอมได้ระหว่างรัน — ปกติไม่จำเป็นต้องกดเอง (GitHub Actions
    รันให้อัตโนมัติแล้วเว็บ static จะได้ของใหม่หลัง git pull) ใช้ตอนอยากได้ไฟล์ data/*.json
    สดในเครื่องทันทีโดยไม่ต้องรอรอบ Actions"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None,
                      current=0, total=100, message="กำลังเริ่ม bake ไฟล์ static (run_static_update.py)...")
    threading.Thread(target=_run_static_bake, daemon=True).start()
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
        except Exception as e:
            logging.warning(f"สำรองข้อมูลก่อน refresh ล้มเหลว: {e}")

    try:
        from services import refresh as _refresh_svc

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        _refresh_svc.run_with_progress(cb, BASE_DIR, period=period)
        _market_internals_cache.clear()
        _breadth_cache.clear()
        _bump_cache_gen()

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
                _clear_fin_analytics_and_warm()
        except Exception as e:
            print(f"[FullRefresh] Batch dividends error: {e}")
            warnings.append(f"Batch fetch ปันผลล้มเหลว: {e}")

        final_msg = "เสร็จแล้ว!" if not warnings else "เสร็จแล้ว (มีบางส่วนล้มเหลว: " + "; ".join(warnings) + ")"
        _update(done=True, message=final_msg)
        run_log.record_run(BASE_DIR, "full_refresh", True, final_msg)

    except Exception as e:
        # ดึงข้อมูลใหม่ล้มเหลว — คืนค่าข้อมูลสำรอง
        run_log.record_run(BASE_DIR, "full_refresh", False, str(e))
        if has_backup and os.path.exists(BACKUP_FILE):
            try:
                shutil.copy2(BACKUP_FILE, DATA_FILE)
                _update(done=True,
                        error=str(e),
                        message="ดึงข้อมูลใหม่ไม่สำเร็จ — ใช้ข้อมูลล่าสุดแทน")
            except Exception:
                _update(done=True, error=str(e),
                        message=f"เกิดข้อผิดพลาด: {e}")
        else:
            _update(done=True, error=str(e),
                    message=f"เกิดข้อผิดพลาด: {e}")
    finally:
        _update(running=False)


_MARKET_STATS_FILE = os.path.join(BASE_DIR, "set_market_stats.json")
# กัน race: /api/refresh-market-stats กับ /api/refresh-market-stats-monthly เขียนไฟล์
# เดียวกันพร้อมกัน (เปิดหลายแท็บ/กดสองปุ่มติดกัน) ไม่งั้นได้ JSON เสีย หรือ endpoint หนึ่ง
# อ่านค่าเก่าไปเขียนทับผลลัพธ์ล่าสุดของอีก endpoint (lost update) — ดู pattern เดียวกับ _dr_refresh_lock
_market_stats_lock = threading.Lock()


def _resolve_xls(name):
    """หาไฟล์ .xls ต้นทางจาก SET — โฟลเดอร์โปรเจกต์ก่อน แล้วค่อยโฟลเดอร์ที่ผู้ใช้ดาวน์โหลดไว้
    (ตั้ง env SET_XLS_DIR ได้ ไม่งั้น default ~/Downloads/dash) จะได้ไม่ต้องก๊อปไฟล์เข้ามาทุกเดือน
    คืน (path, exists) — path เป็นตัวแรกที่เจอ ถ้าไม่เจอเลยคืน path ในโฟลเดอร์โปรเจกต์"""
    cands = [os.path.join(BASE_DIR, name)]
    extra = os.environ.get("SET_XLS_DIR") or os.path.join(os.path.expanduser("~"), "Downloads", "dash")
    cands.append(os.path.join(extra, name))
    for p in cands:
        if os.path.exists(p):
            return p, True
    return cands[0], False


@app.route("/api/market-stats")
def market_stats():
    if not os.path.exists(_MARKET_STATS_FILE):
        return jsonify({"error": "ไม่พบ set_market_stats.json — รัน import_market_stats.py ก่อน"}), 404
    try:
        with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": f"set_market_stats.json เสีย อ่านไม่ได้: {e}"}), 500


@app.route("/api/market-stats-meta")
def market_stats_meta():
    """เดือนล่าสุดที่มีข้อมูล P/E & P/BV จริง (ตาม Table_PE.xls ที่ import ล่าสุด) —
    ใช้เช็คฝั่ง UI ว่าข้อมูลตกรุ่นหรือยัง เดิมใช้ mtime ของไฟล์ซึ่งผิด: บนเวอร์ชันเว็บ
    ไฟล์นี้ถูก regenerate ทุกรอบ GitHub Actions (ไม่ว่า Table_PE.xls จะมีข้อมูลใหม่จริง
    หรือไม่) เลย mtime รีเซ็ตเป็น "วันนี้" ตลอด ทำให้ข้อมูลค้างหลายเดือนก็ยังดูเหมือนสด"""
    if not os.path.exists(_MARKET_STATS_FILE):
        return jsonify({"updated_at": None})
    try:
        with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return jsonify({"updated_at": None})
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

    PE_FILE,  pe_ok  = _resolve_xls("Table_PE.xls")
    PBV_FILE, pbv_ok = _resolve_xls("Table_PBV.xls")

    missing = [f for f, ok in [(PE_FILE, pe_ok), (PBV_FILE, pbv_ok)] if not ok]
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

    # ป้องกันเขียนทับประวัติทั้งชุดด้วยข้อมูลว่าง — parse_ym() คืน None แบบเงียบๆ (ไม่ throw)
    # ถ้า SET เปลี่ยนรูปแบบคอลัมน์วันที่ใน .xls เล็กน้อย ทำให้ทุกแถวถูกข้ามหมด ก่อนหน้านี้โค้ด
    # จะเขียน output ว่างลงไฟล์ไปแล้วค่อยไป crash ตอนอ่าน pe_data['dates'][0] ด้านล่าง — ประวัติ
    # P/E, P/BV ย้อนหลังทั้งหมดหายถาวรไปแล้วก่อนที่ error จะโผล่ให้เห็นด้วยซ้ำ
    if not pe_data["dates"] or not pbv_data["dates"]:
        return jsonify({"ok": False, "error": "อ่านไฟล์ได้แต่ไม่พบแถวข้อมูลที่ parse วันที่ได้เลย "
                                              "(รูปแบบคอลัมน์ Month-Year ในไฟล์ .xls อาจเปลี่ยนไป) "
                                              "— ไม่เขียนทับไฟล์เดิมเพื่อกันข้อมูลย้อนหลังหาย"}), 400

    with _market_stats_lock:
        # check if newer than current
        old_latest = None
        old = None
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
        # ซีรีส์ที่มีเฉพาะใน Market_Statistics_Month_th_TH.xls (Table_PE/PBV ไม่มี) ต้องคงไว้ —
        # เดิม rebuild ทับทั้งไฟล์ ทำให้ประวัติปันผล/มูลค่าหลักทรัพย์/breadth ที่สะสมมาหายทั้งชุด
        for _k in ("div_yield", "mkt_cap", "breadth"):
            if old and _k in old:
                output[_k] = old[_k]

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


@app.route("/api/refresh-market-stats-monthly", methods=["POST"])
def refresh_market_stats_monthly():
    """อ่าน Market_Statistics_Month_th_TH.xls แล้ว merge (upsert รายเดือน) เข้า
    set_market_stats.json ที่มีอยู่ — ไม่ rebuild ทับประวัติทั้งชุดเหมือน
    /api/refresh-market-stats (Table_PE/PBV.xls) ดู sources/set_market_stats_monthly.py"""
    from datetime import datetime as _dt

    from sources.set_market_stats_monthly import merge_monthly, parse_annual_market_statistics

    SRC_FILE, src_ok = _resolve_xls("Market_Statistics_Month_th_TH.xls")
    if not src_ok:
        return jsonify({"ok": False, "error": "ไม่พบไฟล์ Market_Statistics_Month_th_TH.xls "
                                              f"(หาใน {BASE_DIR} และโฟลเดอร์ดาวน์โหลด)"}), 400

    try:
        records, year_ad = parse_annual_market_statistics(SRC_FILE)
    except Exception as e:
        return jsonify({"ok": False, "error": f"อ่านไฟล์ไม่สำเร็จ: {e}"}), 500
    if not records:
        return jsonify({"ok": False, "error": f"ไม่พบข้อมูลเดือนไหนเลยในไฟล์ (ปี {year_ad})"}), 400

    with _market_stats_lock:
        data = {}
        old_latest = None
        if os.path.exists(_MARKET_STATS_FILE):
            with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            old_latest = (data.get("pe", {}).get("dates") or [None])[-1]

        data = merge_monthly(data, records)
        data["updated_at"] = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
        _atomic_write_json(_MARKET_STATS_FILE, data)

    new_latest = data["pe"]["dates"][-1] if data["pe"]["dates"] else None
    pe_cur  = data["pe"]["stats"].get("SET", {})
    pbv_cur = data["pbv"]["stats"].get("SET", {})
    dy_cur  = data["div_yield"]["stats"].get("SET", {})
    mc_cur  = data["mkt_cap"]["stats"].get("SET", {})
    breadth = data["breadth"]["series"]
    return jsonify({
        "ok": True,
        "new_data": new_latest != old_latest,
        "months_in_file": len(records),
        "file_range": f"{min(records)} – {max(records)}",
        "src_file": SRC_FILE,
        "pe_current":  pe_cur.get("current"),
        "pbv_current": pbv_cur.get("current"),
        "pe_zscore":   pe_cur.get("zscore"),
        "pbv_zscore":  pbv_cur.get("zscore"),
        "div_yield_current": dy_cur.get("current"),
        "mkt_cap_current":   mc_cur.get("current"),
        "listed_current":    breadth["listed_SET"][-1] if breadth.get("listed_SET") else None,
        "new_listed_latest": breadth["new_listed_SET"][-1] if breadth.get("new_listed_SET") else None,
        "delisted_latest":   breadth["delisted_SET"][-1] if breadth.get("delisted_SET") else None,
        "updated_at": data["updated_at"],
        "old_latest": old_latest,
        "new_latest": new_latest,
    })


# cache กันยิง SET.or.th ซ้ำถ้ากดปุ่ม/รีเฟรชหน้าถี่ๆ (ดู pattern เดียวกับ _dr_diff_cache)
_delisted_check_cache = {"result": None, "ts": 0}
_DELISTED_CHECK_CACHE_TTL = 3600

_trading_halt_check_cache = {"result": None, "ts": 0}
_TRADING_HALT_CHECK_CACHE_TTL = 900


@app.route("/api/check-delisted-th", methods=["POST"])
def check_delisted_th():
    """เทียบรายชื่อหุ้นเพิกถอนที่ SET.or.th ยืนยันแล้วกับ delisted_log.json ในเครื่อง —
    รายงานเฉพาะตัวที่ (1) ยังไม่มีใน log และ (2) เราเคยเก็บราคาไว้จริงใน set_prices.db
    (ตัวที่ไม่เคย track ไม่ต้องทำอะไร ไม่โผล่มากวนใจ) ไม่แก้ delisted_log.json ให้อัตโนมัติ —
    ให้ผู้ใช้รัน mark_delisted.py เองถ้าต้องการบันทึกจริง

    แหล่งข้อมูล: ถ้ามีไฟล์ delisted-securities-th.xlsx (ดาวน์โหลดมือจากหน้า SET.or.th
    securities-list/delisted-list) วางไว้ในโฟลเดอร์โปรเจกต์/SET_XLS_DIR ใช้ไฟล์นั้นก่อน
    (เร็ว ไม่เสี่ยงโดน WAF บล็อก) ไม่มีไฟล์ค่อย fallback ไปดึงสดผ่าน
    sources/set_api.py::fetch_delisted_companies (ต้อง bootstrap cookie ผ่าน Incapsula)"""
    cached = _delisted_check_cache.get("result")
    if cached and (time.time() - _delisted_check_cache.get("ts", 0) < _DELISTED_CHECK_CACHE_TTL):
        return jsonify(cached)
    try:
        from core.delisted_log import read_log
        from core.store import get_last_dates
        from sources.set_api import fetch_delisted_companies, parse_delisted_companies_xlsx

        xlsx_path, xlsx_ok = _resolve_xls("delisted-securities-th.xlsx")
        if xlsx_ok:
            official = parse_delisted_companies_xlsx(xlsx_path)
            source = "file"
        else:
            official = fetch_delisted_companies()
            source = "live"
        our_log = read_log(BASE_DIR)
        our_th_symbols = {v["symbol"] for v in our_log.values() if v.get("market") == "TH"}
        last_dates = get_last_dates(BASE_DIR)

        actionable = []
        for c in official["companies"]:
            sym = c["symbol"]
            if sym in our_th_symbols:
                continue
            last_seen = last_dates.get(f"{sym}.BK")
            if not last_seen:
                continue  # ไม่เคย track ราคาไว้เลย — ไม่มีอะไรต้องแก้
            actionable.append({**c, "last_price_date": last_seen})
        actionable.sort(key=lambda c: c["delist_date"], reverse=True)

        result = {
            "ok": True,
            "as_of_date": official["as_of_date"],
            "official_count": len(official["companies"]),
            "actionable": actionable,
            "source": source,
        }
        _delisted_check_cache["result"] = result
        _delisted_check_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/check-trading-halt-th", methods=["POST"])
def check_trading_halt_th():
    """ดึงรายชื่อหลักทรัพย์ที่หยุดพักการซื้อขายชั่วคราว (SP/H/P) ปัจจุบันจาก SET.or.th
    (sources/set_api.py::fetch_trading_halts) มาเทียบกับ Watchlist ในเครื่อง (data/watchlist.json)
    เฉพาะสัญลักษณ์หุ้นไทยล้วน (ไม่มี prefix DR:/US:/HK:) — คืนทั้งลิสต์เต็ม (หุ้นสามัญ) และ
    ตัวที่ตรงกับ watchlist แยกไว้ให้ frontend เน้นแสดง ไม่แก้ไฟล์ใดๆ ให้อัตโนมัติ"""
    cached = _trading_halt_check_cache.get("result")
    if cached and (time.time() - _trading_halt_check_cache.get("ts", 0) < _TRADING_HALT_CHECK_CACHE_TTL):
        return jsonify(cached)
    try:
        from sources.set_api import fetch_trading_halts

        data = fetch_trading_halts()
        common = [x for x in data["items"] if x["security_type"] == "S"]
        common.sort(key=lambda x: x["symbol"])

        try:
            with open(WATCHLIST_FILE, encoding="utf-8") as f:
                wl_all = json.load(f)
        except Exception:
            wl_all = []
        wl_syms = {s for s in wl_all if isinstance(s, str) and ":" not in s}
        watchlist_hits = [x for x in common if x["symbol"] in wl_syms]

        result = {
            "ok": True,
            "as_of": data["as_of"],
            "total_count": len(data["items"]),
            "common_count": len(common),
            "watchlist_hits": watchlist_hits,
            "items": common,
        }
        _trading_halt_check_cache["result"] = result
        _trading_halt_check_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
    from services.rotation import load_state, pending_of, CONFIRM_DAYS, DEAD_ZONE_PCT, FAST_DEAD_ZONE_PCT
    state = load_state(BASE_DIR)
    today = state.get("last_processed")

    return jsonify({
        "transitions":      state.get("transitions", [])[:20],
        "pending":          pending_of(state.get("groups", {}), today),
        "transitions_fast": state.get("transitions_fast", [])[:20],
        "pending_fast":     pending_of(state.get("groups_fast", {}), today),
        "last_processed":   today,
        "rules": {"confirm_days": CONFIRM_DAYS, "dead_zone_pct": DEAD_ZONE_PCT,
                  "axes": "x=ret_3m, y=ret_1m"},
        "rules_fast": {"confirm_days": CONFIRM_DAYS, "dead_zone_pct": FAST_DEAD_ZONE_PCT,
                       "axes": "x=ret_1m, y=ret_1w"},
    })


# _cache_gen bump คู่กับทุก .clear() ของ breadth/market-internals cache ด้านล่าง — กัน
# request ที่ compute_breadth() ค้างอยู่ก่อน refresh มาเขียนผลลัพธ์เก่าทับ cache ที่เพิ่ง
# clear ไป (เช็ค gen ก่อน-หลัง compute แล้วข้าม write ถ้ามี refresh คั่นกลางระหว่างนั้น)
_cache_gen = {"n": 0}

def _bump_cache_gen():
    _cache_gen["n"] += 1


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
    cached = _breadth_cache.get(rng)
    if cached:
        return jsonify(cached)
    gen0 = _cache_gen["n"]
    try:
        from services.breadth import compute_breadth
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng])
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณา Full Refresh ก่อน"}), 404
        if _cache_gen["n"] == gen0:
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
    cached = _us_breadth_cache.get(rng)
    if cached:
        return jsonify(cached)
    gen0 = _cache_gen["n"]
    try:
        from services.breadth import compute_breadth
        from core import us_store
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng], store=us_store)
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณากด 📈 US Index Max ก่อน"}), 404
        if _cache_gen["n"] == gen0:
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
    cached = _hk_breadth_cache.get(rng)
    if cached:
        return jsonify(cached)
    gen0 = _cache_gen["n"]
    try:
        from services.breadth import compute_breadth
        from core import hk_store
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng], store=hk_store)
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณากด 📈 HK Index Max ก่อน"}), 404
        if _cache_gen["n"] == gen0:
            _hk_breadth_cache[rng] = data
        return jsonify(data)
    except Exception as e:
        print(f"[HKBreadth] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


_jp_breadth_cache: dict = {}

@app.route("/api/jp-breadth")
def jp_market_breadth():
    """Market Breadth รายวันของหุ้น JP (% above EMA50/200) จาก jp_prices.db —
    reuse services.breadth.compute_breadth ตัวเดียวกับหุ้นไทย/US/HK, แค่สลับ store module
    รับ query param ?range=1y|3y|5y|all (default 1y) — cache แยกต่อ range ใน memory,
    clear ทั้งหมดหลัง JP Index refresh/gap-update"""
    from services.breadth import RANGE_DAYS
    rng = request.args.get("range", "1y")
    if rng not in RANGE_DAYS:
        rng = "1y"
    cached = _jp_breadth_cache.get(rng)
    if cached:
        return jsonify(cached)
    gen0 = _cache_gen["n"]
    try:
        from services.breadth import compute_breadth
        from core import jp_store
        data = compute_breadth(BASE_DIR, days=RANGE_DAYS[rng], store=jp_store)
        if not data:
            return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณากด 📈 JP Index Max ก่อน"}), 404
        if _cache_gen["n"] == gen0:
            _jp_breadth_cache[rng] = data
        return jsonify(data)
    except Exception as e:
        print(f"[JPBreadth] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


_market_internals_cache: dict = {}

@app.route("/api/market-internals")
def market_internals():
    """
    คำนวณ 52W New High / New Low count ต่อวัน ย้อนหลัง 63 วันทำการ (~3 เดือน)
    จาก SQLite price store — cache ผลลัพธ์ใน memory, expire เมื่อ Quick Update เสร็จ
    (result-cache ใช้ event invalidation: ถูก clear หลัง refresh — ไม่พึ่ง mtime)
    """
    cached = _market_internals_cache.get("data")
    if cached:
        return jsonify(cached)

    if not (price_store.db_exists(BASE_DIR)
            or os.path.exists(os.path.join(BASE_DIR, "set_history.json"))):
        return jsonify({"error": "ไม่พบข้อมูลราคา — กรุณา Full Refresh ก่อน"}), 404

    gen0 = _cache_gen["n"]
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
        if _cache_gen["n"] == gen0:
            _market_internals_cache["data"] = result
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


_INDEX_DRIFT_TTL_DAYS = 6   # เช็คสัปดาห์ละครั้งพอ — Wikipedia/SET.or.th ไม่เปลี่ยน
                             # constituents บ่อยกว่านั้น และ Quick Update รันวันละหลายรอบ
                             # (มือ + GitHub Actions) ไม่อยากยิงซ้ำทุกรอบโดยไม่จำเป็น


def _run_index_drift_checks():
    """เช็ค "หุ้นเข้าใหม่/ถูกถอด" ของทุก universe (TH/US/HK/JP) แบบ report-only เบาๆ
    (ไม่แก้ไฟล์ local ให้ — เหมือนปุ่มเช็คมือที่มีอยู่แล้ว) แล้วบันทึกผลลง
    index_drift_status.json ผ่าน core/index_drift.py ให้ /api/data-health อ่านต่อแบบเบา
    โดยไม่ต้องยิง Wikipedia/SET.or.th สดทุกครั้งที่เปิดแอป (ดู PLAN_universe_data_health.txt)

    เช็คเฉพาะ universe ที่ผลล่าสุดเก่ากว่า _INDEX_DRIFT_TTL_DAYS วัน (หรือยังไม่เคยเช็ค)
    แต่ละ universe ห่อ try/except แยกกัน — ตัวหนึ่งพัง (เช่น Wikipedia เปลี่ยนโครงสร้างตาราง)
    ไม่บล็อกตัวอื่น และไม่ทำให้ Quick Update ทั้งรอบ error (เรียกผ่าน _sub_step อยู่แล้ว)"""
    status = index_drift.read_status(BASE_DIR)

    def _stale(key):
        prev = status.get(key)
        if not prev or not prev.get("checked_at"):
            return True
        try:
            last = _dh_dt.strptime(prev["checked_at"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return True
        return (_dh_dt.now() - last).days >= _INDEX_DRIFT_TTL_DAYS

    if _stale("TH"):
        try:
            from set_data_fetcher import load_set_symbols
            live_syms = {s["symbol"] for s in load_set_symbols(BASE_DIR)}
            local_syms = set(_financials_universe())
            new_syms = sorted(live_syms - local_syms)
            removed_syms = sorted(local_syms - live_syms)
            index_drift.record_check(BASE_DIR, "TH", len(new_syms), len(removed_syms), new_syms, removed_syms)
        except Exception as e:
            index_drift.record_check(BASE_DIR, "TH", 0, 0, error=str(e)[:200])

    def _check_wiki_index(key, membership_mod, mirror_market):
        try:
            mirror_syms = factor_snapshot.get_mirror_symbols(BASE_DIR).get(mirror_market, [])
            result, _live = membership_mod.diff_membership(BASE_DIR, **{f"mirror_{mirror_market.lower()}": mirror_syms})
            new_n = sum(len(v) for v in result["new"].values())
            rem_n = sum(len(v) for v in result["removed"].values())
            new_sample = sorted({s for lst in result["new"].values() for s in lst})
            rem_sample = sorted({s for lst in result["removed"].values() for s in lst})
            index_drift.record_check(BASE_DIR, key, new_n, rem_n, new_sample, rem_sample)
        except Exception as e:
            index_drift.record_check(BASE_DIR, key, 0, 0, error=str(e)[:200])

    if _stale("US"):
        _check_wiki_index("US", us_index_membership, "US")
    if _stale("HK"):
        _check_wiki_index("HK", hk_index_membership, "HK")
    if _stale("JP"):
        try:
            from sources import jp_index_membership
            result, _live = jp_index_membership.diff_membership(BASE_DIR)
            new_n = sum(len(v) for v in result["new"].values())
            rem_n = sum(len(v) for v in result["removed"].values())
            new_sample = sorted({s for lst in result["new"].values() for s in lst})
            rem_sample = sorted({s for lst in result["removed"].values() for s in lst})
            index_drift.record_check(BASE_DIR, "JP", new_n, rem_n, new_sample, rem_sample)
        except Exception as e:
            index_drift.record_check(BASE_DIR, "JP", 0, 0, error=str(e)[:200])


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
        from services import refresh as _refresh_svc

        # run_quick_update ใช้ตั้งแต่ 0-90% ของ progress bar เอง — ขั้นตอนเสริม
        # ด้านล่างไล่ 90-99% ต่อ ไม่งั้นแถบวิ่งถอยหลัง (99% -> 93% -> ...)
        def cb(current, total, msg):
            pct = (current / total * 90) if total > 0 else 0
            _update(current=round(pct), total=100, message=msg)

        _refresh_svc.run_quick_update(cb, BASE_DIR)
        _market_internals_cache.clear()
        _breadth_cache.clear()
        _bump_cache_gen()

        def _insider():
            from sources import sec_store as _sec_store
            _sec_store.sync_insider_trades(BASE_DIR)
            _sec_store.sync_major_changes(BASE_DIR)
            _sec_store.bake_backup(BASE_DIR)   # no-op นอก CI — ดู sec_store.bake_backup
            _invalidate_flow_signals()   # insider เป็น 1 ใน 3 ชั้นของสัญญาณรวม
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
            n_us, live_us, _scope_us, _adv_us = _run_us_index_gap_update(progress_cb=_us_cb)
            from sources import us_index_metrics
            us_index_metrics.build(BASE_DIR, live_map=live_us)
            _us_breadth_cache.clear()
            _bump_cache_gen()
            print(f"[QuickUpdate] US Index: gap-updated {n_us} ticker, metrics rebuilt")
        _sub_step("US Index", 95, "อัพเดทราคา US Index...", _us_index)

        # อัพเดทราคา HK Index (HSI/HSCEI/HSTECH gap-update) + คำนวณ RS/EMA/Stage/52W ใหม่
        def _hk_index():
            def _hk_cb(current, total, msg):
                _update(message=f"HK Index: {msg}")
            n_hk, live_hk, _scope_hk, _adv_hk = _run_hk_index_gap_update(progress_cb=_hk_cb)
            from sources import hk_index_metrics
            hk_index_metrics.build(BASE_DIR, live_map=live_hk)
            _hk_breadth_cache.clear()
            _bump_cache_gen()
            print(f"[QuickUpdate] HK Index: gap-updated {n_hk} ticker, metrics rebuilt")
        _sub_step("HK Index", 96, "อัพเดทราคา HK Index...", _hk_index)

        # อัพเดทราคา JP Index (Nikkei 225 gap-update) + คำนวณ RS/EMA/Stage/52W ใหม่
        def _jp_index():
            def _jp_cb(current, total, msg):
                _update(message=f"JP Index: {msg}")
            n_jp, live_jp, _scope_jp, _adv_jp = _run_jp_index_gap_update(progress_cb=_jp_cb)
            from sources import jp_index_metrics
            jp_index_metrics.build(BASE_DIR, live_map=live_jp)
            _jp_breadth_cache.clear()
            _bump_cache_gen()
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

        # เช็ค drift หุ้นเข้าใหม่/ถูกถอดสัปดาห์ละครั้ง (TH/US/HK/JP) — report-only, บันทึกผล
        # ให้ Data Health อ่านต่อ (ดู _run_index_drift_checks ด้านบน + index_drift.py)
        _sub_step("ตรวจ drift หุ้นเข้า/ออก", 99, "ตรวจหุ้นเข้าใหม่/ถูกถอด (TH/US/HK/JP)...",
                  _run_index_drift_checks)

        if failed_steps:
            summary = "Quick Update เสร็จแล้ว (⚠️ ล้มเหลว: " + ", ".join(failed_steps) + ")"
        else:
            summary = "Quick Update เสร็จแล้ว!"
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "quick_update", not failed_steps, summary)

    except Exception as e:
        _update(done=True, error=str(e),
                message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "quick_update", False, str(e))
    finally:
        _update(running=False)


# ============================================================
# SEC Insider / Major-Holder endpoints — เก็บสะสมใน sec_filings.db
# (sources/sec_store.py) sync ตอน Quick Update / auto-update cron
# ============================================================

# fetchInsiderData() ฝั่ง frontend ยิง /api/insider-trades + /api/major-changes
# พร้อมกันเสมอ (Promise.all) — ถ้าเป็นการเปิดแอปครั้งแรกที่ยังไม่มี sec_filings.db เลย
# ทั้งสอง request จะเห็น db_exists()==False พร้อมกันแล้วแยกกัน sync/เขียน DB เดียวกัน
# พร้อมกัน (มี busy_timeout กันพังอยู่แล้วแต่ไม่กันแข่งกันเปล่าๆ) — lock นี้บังคับให้
# sync ครั้งแรกเกิดครั้งเดียว รวมทั้ง insider+major เข้าด้วยกัน กัน request ที่มาทีหลัง
# เห็น db_exists()==True (ไฟล์ถูกสร้างแล้วจาก request แรก) แล้วข้าม sync ของตัวเองไปเฉยๆ
_sec_first_sync_lock = threading.Lock()


def _ensure_sec_db_ready():
    if sec_store.db_exists(BASE_DIR):
        return
    with _sec_first_sync_lock:
        if sec_store.db_exists(BASE_DIR):   # เช็คซ้ำหลังได้ lock — อีก request อาจ sync เสร็จไปแล้วระหว่างรอ
            return
        sec_store.sync_insider_trades(BASE_DIR)
        sec_store.sync_major_changes(BASE_DIR)


@app.route("/api/insider-trades")
def insider_trades():
    """ผู้บริหารซื้อขายหุ้น (แบบ 59) — อ่านจากฐานข้อมูลสะสม (sec_filings.db)
    ไม่ยิง SEC สดทุกครั้งอีกต่อไป ข้อมูลใหม่เข้ามาจาก sync_insider_trades()
    (เรียกตอน Quick Update / auto-update cron) เท่านั้น"""
    from datetime import datetime as _dt

    try:
        days = int(request.args.get("days", 30))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))

    if not sec_store.db_exists(BASE_DIR):
        # ยังไม่เคย sync เลย -> sync ครั้งแรก (ช้า ~2-3 นาที ดึงย้อนหลัง 180 วัน)
        try:
            _ensure_sec_db_ready()
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

    try:
        days = int(request.args.get("days", 30))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))

    if not sec_store.db_exists(BASE_DIR):
        try:
            _ensure_sec_db_ready()
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
        _invalidate_flow_signals()   # insider เป็น 1 ใน 3 ชั้นของสัญญาณรวม
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


def _merge_daily_tail(daily_list, tail_rows, field_names):
    """แทรก tail_rows ([date, v1, v2, ...]) เข้า daily_list เฉพาะวันที่ยังไม่มี (ไม่ทับของเดิม)
    แล้ว sort ตาม date — field_names กำหนดชื่อ key ของ v1, v2, ... ตามลำดับ"""
    existing = {d.get("date") for d in daily_list}
    for row in tail_rows:
        date = row[0]
        if not date or date in existing:
            continue
        daily_list.append({"date": date, **dict(zip(field_names, row[1:]))})
        existing.add(date)
    daily_list.sort(key=lambda d: d["date"])
    return daily_list


# True เมื่อรันบน GitHub Actions เอง (run_static_update.py) — ใช้ 2 ที่: (1) ให้
# _fetch_github_raw_json อ่านไฟล์จาก checkout แทนยิง network (2) กันไม่ให้เครื่อง local
# เขียนทับ market/s50/bond_flow_data.json ที่ Actions commit อยู่แล้ว — เดิม local เขียน+
# auto-push ชน commit ของ Actions เป็นระยะ (2026-08 เจอ conflict จริงบน bond/s50 flow)
_IS_GITHUB_ACTIONS = bool(os.environ.get("GITHUB_ACTIONS"))
_GITHUB_RAW_BASE = "https://raw.githubusercontent.com/crazyass669/SET-IPAD/main"


def _fetch_github_raw_json(rel_path, timeout=8):
    """อ่านไฟล์ JSON "เวอร์ชันที่อยู่บน GitHub" มาเติมช่องว่างข้อมูล — บนเครื่อง local ยิง
    raw.githubusercontent, บน CI อ่านไฟล์เดียวกันจาก checkout ตรงๆ (เนื้อหาชุดเดียวกันเพราะ
    checkout มาจาก GitHub อยู่แล้ว แต่เร็วกว่าและไม่พึ่ง network)

    *** ห้ามเปลี่ยนสาขา CI ให้ return None เฉยๆ *** — ดูเผินๆ เหมือน no-op เพราะ market/s50/
    bond_flow_data.json ถูก commit ไว้และ caller อ่านไฟล์นั้นเป็นขั้นแรกอยู่แล้ว แต่ short_sales/
    nvdr ตรงข้าม: ไฟล์สะสม short_sales_data.json/nvdr_data.json อยู่ใน .gitignore ฝั่ง CI จึง
    พึ่ง actions/cache อย่างเดียว ถ้า cache miss (eviction 7 วัน/เกิน 10GB) ไฟล์ bake ที่ commit
    ไว้ (data/short_sales.json ~21 วัน/หุ้น) คือทางเดียวที่กู้ประวัติกลับมาได้ ไม่งั้นรอบนั้นจะ
    สะสมได้วันเดียวแล้ว bake ทับของเดิม = ประวัติหายทั้ง repo (บั๊ก 2026-07-17 เวอร์ชันใหม่)

    ผู้เรียกต้อง handle ทั้ง None (ไม่มีไฟล์) และ exception (network ล้ม/ไฟล์ JSON เสีย)"""
    if _IS_GITHUB_ACTIONS:
        try:
            with open(os.path.join(BASE_DIR, *rel_path.split("/")), encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
    import urllib.request as _ur
    ctx = ssl_context()
    req = _ur.Request(f"{_GITHUB_RAW_BASE}/{rel_path}", headers={"User-Agent": "Mozilla/5.0"})
    with _ur.urlopen(req, context=ctx, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


# ~1 ปีเทรดดิ้ง — เพดานความลึกของไฟล์สำรอง short_sales/nvdr ที่ commit ขึ้น GitHub (ดู
# _bake_history_backup) ตั้ง bound ไว้ ไม่ปล่อยยาวไม่จำกัดตามไฟล์สะสม local เพราะจะบวม repo
# ซ้ำปัญหาเดิมที่เพิ่งย้าย indices_cache.json/short_sales_data.json/nvdr_data.json ออกจาก git
# ไปใช้ actions/cache แทนแล้ว (ดู .gitignore comment) — 260 วันยังให้ backup ยาวกว่า 21 วันเดิม
# ของ /api/short-sales, /api/nvdr ถึง 12 เท่า ในขนาดไฟล์ที่ยัง commit รายวันได้โดยไม่หนักเกินไป
_HISTORY_BACKUP_DEPTH = 260


def _bake_history_backup(accumulator_path, backup_rel_path, field_names, label):
    """เขียนไฟล์สำรอง "ประวัติยาว" (สูงสุด _HISTORY_BACKUP_DEPTH วันล่าสุด/หุ้น) ไปที่ data/ จาก
    ไฟล์สะสม local (accumulator_path — ไม่ถูกตัดเหมือน /api/short-sales, /api/nvdr ที่ตัดแค่ 21
    วันไว้ให้หน้าเว็บ/iPad โหลดเร็ว) แยกไฟล์จาก data/short_sales.json, data/nvdr_data.json (ตัวที่
    bake จาก endpoint สดสำหรับหน้าเว็บ) โดยเจตนา — ไฟล์นี้มีไว้กู้คืนกรณี actions/cache ของไฟล์สะสม
    หาย ไม่ใช่ตัวที่หน้าเว็บ/iPad อ่านจริง เขียนเฉพาะตอนรันบน GitHub Actions เท่านั้น (เหมือน 3
    ไฟล์ flow — กันเครื่อง local เขียนชน commit ของ Actions ดู _IS_GITHUB_ACTIONS)"""
    if not _IS_GITHUB_ACTIONS:
        return
    try:
        with open(accumulator_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[{label}] อ่านไฟล์สะสมไม่ได้ ({e}) — ข้าม bake backup")
        return
    out = {}
    for sym, v in (data.get("stocks") or {}).items():
        daily = v.get("daily") or []
        out[sym] = [[d.get("date")] + [d.get(fn) for fn in field_names]
                    for d in daily[-_HISTORY_BACKUP_DEPTH:]]
    backup_path = os.path.join(BASE_DIR, "data", backup_rel_path)
    _atomic_write_json(backup_path, {
        "updated_at": time.strftime("%Y-%m-%d %H:%M"),
        "depth_days": _HISTORY_BACKUP_DEPTH,
        "fields": field_names,
        "stocks": out,
    })
    print(f"[{label}] bake backup: {len(out)} stocks -> data/{backup_rel_path}")


def _fetch_short_sales_github_fallback():
    """ดึง data/short_sales_backup.json ที่ GitHub Actions bake+commit ไว้ (สูงสุด
    _HISTORY_BACKUP_DEPTH ~260 วันล่าสุดต่อหุ้น — ดู _bake_history_backup) มาเติมช่องว่าง
    บนเครื่อง local — ลึกกว่า data/short_sales.json เดิม (21 วัน) ถึง 12 เท่า ไม่มี
    pct_vol/pct_value (backup ไม่เก็บ 2 field นี้เหมือนไฟล์เดิม) วันที่เติมจาก fallback จะไม่มี
    2 field นี้ ไม่กระทบวันอื่นที่ได้จากดึงสดจริง — ถ้ายังไม่เคยมีไฟล์นี้ (deploy ใหม่ ยังไม่ผ่าน
    Actions รอบแรก) คืน {} เฉยๆ ไม่ error"""
    data = _fetch_github_raw_json("data/short_sales_backup.json")
    return (data or {}).get("stocks") or {}


def short_sales_daily_update():
    """ดึงข้อมูล short sales วันนี้จาก API แล้ว append ลง short_sales_data.json"""
    import urllib.request as _ur
    from datetime import datetime as _dt

    try:
        ctx = ssl_context()
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

        if os.path.exists(_SHORT_DATA_FILE):
            with open(_SHORT_DATA_FILE, encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"stocks": {}}

        stocks = data.setdefault("stocks", {})

        # เติมช่องว่างจาก GitHub fallback (~21 วันล่าสุด) ก่อนอัพเดทวันนี้ — ไม่ทับของเดิม
        try:
            for sym, tail in _fetch_short_sales_github_fallback().items():
                s = stocks.setdefault(sym, {
                    "period_vol": 0, "period_local_vol": 0, "period_nvdr_vol": 0,
                    "period_value": 0, "period_pct_value": 0, "short_pos": 0,
                    "short_pos_local": 0, "short_pos_nvdr": 0, "short_pos_pct": 0, "daily": [],
                })
                _merge_daily_tail(s.setdefault("daily", []), tail, ["short_pos", "short_pos_pct"])
        except Exception as e:
            print(f"[short-sales] GitHub fallback ไม่ได้ ({e})")

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
            # append snapshot วันใหม่ / เขียนทับถ้าเป็นวันเดียวกับที่มีอยู่แล้ว (SET อาจ
            # แก้ตัวเลขระหว่างวันถ้ารันซ้ำหลายรอบ — เดิม append-only เลยค้างค่าแรกไว้ถาวร)
            daily = stocks[sym].setdefault("daily", [])
            if daily and daily[-1].get("date") == trade_date:
                daily[-1] = snap
            else:
                daily.append(snap)  # สะสมไปเรื่อยๆ ไม่ตัดทิ้ง (เหมือนราคาหุ้น)
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
        _bake_history_backup(_SHORT_DATA_FILE, "short_sales_backup.json",
                              ["short_pos", "short_pos_pct"], "short-sales")

        global _short_data_cache, _short_data_ts
        _short_data_cache = None  # invalidate cache
        _invalidate_flow_signals()   # short เป็น 1 ใน 3 ชั้นของสัญญาณรวม
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
    import urllib.request as _ur, re as _re, ast as _ast
    ctx = ssl_context()
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


def _fetch_flow_market_github_fallback():
    """ดึง market_flow_data.json ที่ GitHub Actions commit ไว้ (cron รันวันละ 4 รอบ) มา
    เติมวันที่ขาดบนเครื่อง local — เผื่อกรณี siamchart โดนบล็อค/ล่มพร้อมกับ SET API ไม่ผ่าน
    วันนั้นพอดีตอนเครื่อง local รัน (เหมือน s50: _fetch_flow_s50_github_fallback)"""
    data = _fetch_github_raw_json("market_flow_data.json")
    return (data or {}).get("rows") or []


def _fetch_flow_data():
    """รวมข้อมูล Capital Flow จากไฟล์สะสม + GitHub fallback + siamchart + SET API —
    คำนวณ chg แล้วอัพเดท _flow_cache เสมอ · บันทึกไฟล์สะสม (atomic) เฉพาะตอนรันบน GitHub
    Actions เท่านั้น — เครื่อง local ไม่เขียนไฟล์ 3 ไฟล์ flow (market/s50/bond) นี้อีกแล้ว
    กัน auto-push ยิงทับ/ชน commit ของ Actions (ดู _IS_GITHUB_ACTIONS) เครื่อง local ยังได้
    ข้อมูลสดครบเหมือนเดิมผ่าน _flow_cache ในหน่วยความจำ แค่ไม่ persist ลงไฟล์ที่ push ขึ้น repo
    ล้มเฉพาะเมื่อไม่มีข้อมูลเลยจริงๆ"""
    rows_by_date = {}
    try:
        with open(_MARKET_FLOW_FILE, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = {k: r0.get(k) for k in
                                                ("date", "fund", "foreign", "retail", "set")}
    except Exception:
        pass

    # เติมเฉพาะวันที่ขาดบนเครื่อง (ไม่ทับของเดิม — ไฟล์ local อาจถูกแก้ไขมือไว้แล้ว)
    try:
        for r0 in _fetch_flow_market_github_fallback():
            if r0.get("date") and r0["date"] not in rows_by_date:
                rows_by_date[r0["date"]] = {k: r0.get(k) for k in
                                            ("date", "fund", "foreign", "retail", "set")}
    except Exception as e:
        print(f"[Flow] GitHub fallback ไม่ได้ ({e})")

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
    if sources and _IS_GITHUB_ACTIONS:   # ได้ของใหม่จริง + รันบน Actions ค่อยเขียนไฟล์ (เจ้าของไฟล์เดียว)
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
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



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
    import urllib.request as _ur, re as _re
    ctx = ssl_context()
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


def _fetch_flow_s50_github_fallback():
    """ดึง s50_flow_data.json ที่ GitHub Actions commit ไว้ (cron รันวันละ 3 รอบ:
    06:00/13:10/18:30 ICT) มาใช้เติมวันที่ขาดบนเครื่อง — TFEX ให้แค่ "วันล่าสุดวันเดียว"
    ถ้าเครื่อง local ไม่ได้เปิดแอป/กด Quick Update พอดีตอน TFEX ยังโชว์วันนั้นอยู่ วันนั้น
    จะหายถาวร แต่ GitHub Actions มีโอกาสจับติดมากกว่าเพราะรันถี่กว่า 3 เท่า"""
    data = _fetch_github_raw_json("s50_flow_data.json")
    return (data or {}).get("rows") or []


def _fetch_flow_s50_data():
    """รวมข้อมูล S50 Futures flow จากไฟล์สะสม + GitHub fallback + TFEX (วันล่าสุด) — เหมือน SET flow
    (เขียนไฟล์สะสมเฉพาะตอนรันบน Actions เท่านั้น — ดู _fetch_flow_data)"""
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
    if sources and _IS_GITHUB_ACTIONS:
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
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ── Thai Bond flow (ThaiBMA) ──────────────────────────────────────────────
# แหล่ง: thaibma.or.th/nrdaily/GetNR/ — JSON เปิด ไม่ต้อง auth คืนประวัติเต็ม
# (ต่างจาก SET/S50 ที่ต้องสะสมเอง) หน่วย: ล้านบาท
_flow_bond_cache: dict = {}
_BOND_FLOW_FILE = os.path.join(BASE_DIR, "bond_flow_data.json")


def _fetch_flow_bond_github_fallback():
    """ดึง bond_flow_data.json ที่ GitHub Actions commit ไว้ (cron รันวันละ 4 รอบ) มา
    เติมวันที่ขาดบนเครื่อง local — เผื่อกรณี ThaiBMA ล่ม/เปลี่ยนหน้าตอนเครื่อง local รันพอดี
    (เหมือน s50: _fetch_flow_s50_github_fallback)"""
    data = _fetch_github_raw_json("bond_flow_data.json")
    return (data or {}).get("rows") or []


def _fetch_flow_bond_data():
    """รวมข้อมูล Thai Bond flow จากไฟล์สะสม + GitHub fallback + ThaiBMA (merge by date
    เหมือน SET/S50 flow — เดิมเขียนทับไฟล์ทั้งไฟล์ด้วยผลลัพธ์ ThaiBMA ตรงๆ ถ้าวันไหน ThaiBMA
    ส่งประวัติสั้นลง (เช่นเหลือแค่ YTD) ข้อมูลย้อนหลังที่สะสมไว้จะหายถาวร — ตอนนี้ merge เข้าไฟล์เดิมแทน
    เขียนไฟล์สะสมเฉพาะตอนรันบน Actions เท่านั้น — ดู _fetch_flow_data)"""
    import urllib.request as _ur
    ctx = ssl_context()
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

    # เติมเฉพาะวันที่ขาดบนเครื่อง (ไม่ทับของเดิม)
    try:
        for r0 in _fetch_flow_bond_github_fallback():
            if r0.get("date") and r0["date"] not in rows_by_date:
                rows_by_date[r0["date"]] = r0
    except Exception as e:
        print(f"[Bond Flow] GitHub fallback ไม่ได้ ({e})")

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
    if sources and _IS_GITHUB_ACTIONS:
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
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


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
    import urllib.request as _ur
    ctx = ssl_context()
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


def _fetch_nvdr_github_fallback():
    """ดึง data/nvdr_backup.json ที่ GitHub Actions bake+commit ไว้ (สูงสุด
    _HISTORY_BACKUP_DEPTH ~260 วันล่าสุดต่อหุ้น — ดู _bake_history_backup) มาเติมช่องว่างบน
    เครื่อง local — ลึกกว่า data/nvdr_data.json เดิม (21 วัน) ถึง 12 เท่า ถ้ายังไม่เคยมีไฟล์นี้
    (deploy ใหม่ ยังไม่ผ่าน Actions รอบแรก) คืน {} เฉยๆ ไม่ error"""
    data = _fetch_github_raw_json("data/nvdr_backup.json")
    return (data or {}).get("stocks") or {}


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

        stocks = data.setdefault("stocks", {})

        # เติมช่องว่างจาก GitHub fallback (~21 วันล่าสุด) ก่อนอัพเดทวันนี้ — ไม่ทับของเดิม
        try:
            for sym, tail in _fetch_nvdr_github_fallback().items():
                s = stocks.setdefault(sym, {"nvdr_pct": 0, "nvdr_shares": 0,
                                             "paid_up_shares": 0, "daily": []})
                _merge_daily_tail(s.setdefault("daily", []), tail, ["nvdr_pct", "nvdr_shares"])
        except Exception as e:
            print(f"[nvdr] GitHub fallback ไม่ได้ ({e})")

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

            # เขียนทับถ้าเป็นวันเดียวกับที่มีอยู่แล้ว (SET อาจแก้ตัวเลขระหว่างวันถ้ารันซ้ำ
            # หลายรอบ — เดิม append-only เลยค้างค่าแรกไว้ถาวร)
            daily = stocks[sym].setdefault("daily", [])
            if daily and daily[-1].get("date") == trade_date:
                daily[-1] = snap
            else:
                daily.append(snap)  # สะสมไปเรื่อยๆ ไม่ตัดทิ้ง (เหมือนราคาหุ้น)
            updated += 1

        data["stocks"]     = stocks
        data["updated_at"] = trade_date
        _atomic_write_json(_NVDR_DATA_FILE, data)
        _bake_history_backup(_NVDR_DATA_FILE, "nvdr_backup.json",
                              ["nvdr_pct", "nvdr_shares"], "nvdr")
        global _nvdr_data_cache
        _nvdr_data_cache = None
        _invalidate_flow_signals()   # NVDR เป็น 1 ใน 3 ชั้นของสัญญาณรวม
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


def _invalidate_flow_signals():
    """ล้าง cache สัญญาณรวม — ต้องเรียกทุกครั้งที่แหล่งต้นทาง (short / NVDR / insider)
    ถูกอัพเดต ไม่งั้นหน้า "🎯 สัญญาณรวม" ค้างข้อมูลชุดเก่าได้อีกเกือบชั่วโมงเต็ม TTL
    ทั้งที่เพิ่งกด Quick Update เสร็จ (เจอจริง: กดอัพเดตแล้ว score ไม่ขยับจนกว่าจะครบ 1 ชม.)"""
    _flow_signals_cache["result"] = None
    _flow_signals_cache["ts"] = 0


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
# Watchlist sync ข้ามเครื่อง — เก็บเป็นไฟล์ data/watchlist.json ให้ git push/pull
# พาไปด้วยได้ (แยกจาก localStorage ที่ผูกกับเบราว์เซอร์/เครื่องเดียว) frontend เรียก
# GET ตอนโหลดหน้าเพื่อ merge เข้า localStorage แล้วยิง POST กลับทุกครั้งที่ watchlist
# เปลี่ยน (ดู _wlSave() ใน dashboard.js) — merge แบบ union เท่านั้น ไม่มีการลบข้ามเครื่อง
# ============================================================
@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    if not os.path.exists(WATCHLIST_FILE):
        return jsonify([])
    try:
        with open(WATCHLIST_FILE, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify([])


@app.route("/api/watchlist", methods=["POST"])
def save_watchlist():
    body = request.get_json(silent=True) or {}
    syms = body.get("symbols")
    if not isinstance(syms, list) or len(syms) > 500 or not all(isinstance(s, str) for s in syms):
        return jsonify({"error": "invalid symbols"}), 400
    os.makedirs(os.path.dirname(WATCHLIST_FILE), exist_ok=True)
    _atomic_write_json(WATCHLIST_FILE, syms)
    return jsonify({"ok": True, "count": len(syms)})


# ============================================================
# Price alert sync ข้ามเครื่อง — เก็บเป็นไฟล์ data/price_alerts.json ให้ git push/pull
# พาไปด้วยได้เหมือน /api/watchlist ด้านบน (alert เดิมอยู่แค่ localStorage ต่อเครื่อง จึง
# ไม่เคยติดไปตอนย้ายเครื่อง) frontend merge แบบ union ด้วย "id" เอง (ดู _alertsSyncFromServer
# ใน dashboard.js) ฝั่ง backend แค่เก็บ/คืนทั้งก้อนตามที่ frontend ส่งมาเหมือน watchlist
# ============================================================
@app.route("/api/price-alerts", methods=["GET"])
def get_price_alerts():
    if not os.path.exists(PRICE_ALERTS_FILE):
        return jsonify([])
    try:
        with open(PRICE_ALERTS_FILE, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception:
        return jsonify([])


def _valid_price_alert(a):
    """เช็คโครงสร้างขั้นต่ำของ 1 alert — กัน record เพี้ยน (targetPrice หาย/ไม่ใช่ตัวเลข)
    หลุดเข้าไปสะสมใน price_alerts.json แล้ว sync กระจายไปทุกเครื่อง ทำให้ a.targetPrice.toFixed()
    ฝั่ง frontend throw ตอน render จนตาราง Watchlist/แผงแจ้งเตือนค้างทั้งหน้า (ดู _wlAlertCell)"""
    if not (isinstance(a, dict) and isinstance(a.get("id"), str)):
        return False
    tp = a.get("targetPrice")
    if not (isinstance(tp, (int, float)) and not isinstance(tp, bool) and math.isfinite(tp)):
        return False
    if a.get("condition") not in ("above", "below"):
        return False
    if not isinstance(a.get("symbol"), str):
        return False
    return True


@app.route("/api/price-alerts", methods=["POST"])
def save_price_alerts():
    body = request.get_json(silent=True) or {}
    alerts = body.get("alerts")
    if (not isinstance(alerts, list) or len(alerts) > 500
            or not all(_valid_price_alert(a) for a in alerts)):
        return jsonify({"error": "invalid alerts"}), 400
    os.makedirs(os.path.dirname(PRICE_ALERTS_FILE), exist_ok=True)
    _atomic_write_json(PRICE_ALERTS_FILE, alerts)
    return jsonify({"ok": True, "count": len(alerts)})


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
    port = SERVER_PORT

    if not _wait_port_free(port):
        print(f"[!] พอร์ต {port} มี server อื่นรันอยู่แล้ว — ไม่ start ซ้อน")
        print(f"    ปิดตัวเก่าก่อน (Task Manager -> python) หรือใช้ตัวที่รันอยู่ได้เลย")
        sys.exit(1)

    local_ip = get_local_ip()

    # โหลด DR/ETF cache จากไฟล์ก่อนเริ่ม server
    _load_dr_cache_from_file()
    _load_etf_cache_from_file()

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
        # เรียงตาม "เมนูที่ผู้ใช้เปิดแล้วรอจริง" ก่อน — เดิม financials-analytics (หนักสุด
        # ~13 วิ และเป็นตัวที่ sector-compare พึ่งอยู่) ถูกอุ่นเป็นลำดับที่ 4 ต่อจาก
        # breadth ~6 วิ + market-internals ~10 วิ ทำให้หน้า "ดัชนีกลุ่มอุตสาหกรรม SET & mai"
        # กับ "แนวโน้มตลาด" ยังเย็นอยู่ ~40 วิแรกหลังเปิดแอป (ตอนนี้พร้อมภายใน ~17 วิ)
        for ep in ("/api/financials-analytics", "/api/sector-compare", "/api/market-trend",
                   "/api/data-health",
                   "/api/market-flow", "/api/market-internals", "/api/breadth?range=1y",
                   "/api/us-breadth?range=1y", "/api/hk-breadth?range=1y", "/api/jp-breadth?range=1y"):
            try:
                t0 = _t.time()
                tc.get(ep)
                print(f"[Warmup] {ep} พร้อม ({_t.time() - t0:.0f} วิ)", flush=True)
            except Exception as e:
                print(f"[Warmup] {ep} ล้มเหลว (ไม่กระทบการใช้งาน): {e}", flush=True)

    threading.Thread(target=_warmup_caches, daemon=True).start()

    try:
        from waitress import serve
        # 24 threads (เดิม 12) — endpoint หนักบางตัว (financials-analytics ~15-23 วิ,
        # /api/progress SSE ที่เปิดค้างได้นานถึง 20 นาทีต่อแท็บที่เปิดดูระหว่าง Full
        # Refresh) กิน thread ค้างได้นาน เปิดหลายแท็บ/หลายเครื่องพร้อมกันชนโควตาเดิม
        # จนทั้งเว็บค้างได้ทั้งที่ process ยังไม่ตาย — waitress thread เบา ไม่ใช่ process
        # เพิ่มแล้วไม่มี cost อะไรตอนใช้งานปกติ
        THREADS = 24
        print(f"  Server: waitress (production WSGI, {THREADS} threads)\n")
        logging.getLogger("waitress").setLevel(logging.INFO)
        # channel_timeout สูงเพื่อ SSE /api/progress ที่เปิดค้างระหว่าง
        # Full Refresh (10+ นาที)
        serve(app, host="0.0.0.0", port=port, threads=THREADS, channel_timeout=1200)
    except ImportError:
        print("  Server: Flask dev (ติดตั้ง waitress เพื่อใช้ production server)\n")
        app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
