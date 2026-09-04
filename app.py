"""
SET Dashboard — Flask Web Server
รัน: python app.py
หรือดับเบิ้ลคลิก start.bat
"""

import html
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
    เพราะ entry ใหม่มาเรื่อย ๆ เร็วกว่าของเก่าหมดอายุ

    Flask รันแบบ multi-threaded — ล็อกด้วย threading.Lock กัน race บน
    move_to_end()/popitem() เวลาหลาย thread เขียน cache พร้อมกันตอนใกล้เต็ม maxsize
    (ไม่งั้น popitem ของ thread หนึ่งอาจไปชนคีย์ที่อีก thread กำลัง move_to_end อยู่
    จน raise KeyError)"""

    def __init__(self, maxsize):
        super().__init__()
        self.maxsize = maxsize
        self._lock = threading.Lock()

    def __setitem__(self, key, value):
        with self._lock:
            if super().__contains__(key):
                self.move_to_end(key)
            super().__setitem__(key, value)
            if len(self) > self.maxsize:
                self.popitem(last=False)

    def __getitem__(self, key):
        with self._lock:
            return super().__getitem__(key)

    def __contains__(self, key):
        with self._lock:
            return super().__contains__(key)

    def get(self, key, default=None):
        with self._lock:
            return super().get(key, default)


# Band cache — เก็บผล mrlikestock.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_band_cache = _LRUCache(2000)
_BAND_CACHE_TTL = 6 * 3600

# NP data cache — เก็บผลกำไรสุทธิรายไตรมาสจาก 9hoon.com ไว้ 6 ชั่วโมง เพื่อลด latency ค้นซ้ำ
_npdata_cache = _LRUCache(2000)
_NPDATA_CACHE_TTL = 6 * 3600

# Research reports cache — บทวิเคราะห์ (PDF) รายโบรกจาก 9hoon.com ไว้ 3 ชั่วโมง
# (บทใหม่เข้ามาระหว่างวันได้ ไม่ควร cache นานเท่า npdata)
_rr_cache = _LRUCache(2000)
_RR_CACHE_TTL = 3 * 3600

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
# in-flight guard — diff_membership ยิง urlopen Wikipedia 3 หน้าเรียงกัน (timeout 30s ต่อหน้า =
# บล็อก worker ได้ถึง ~90s) frontend abort ที่ 30s แล้ว re-enable ปุ่มให้กดซ้ำ — ถ้าไม่กันจะเกิด
# worker ค้างซ้อนกันหลายตัวจน waitress thread pool หมด ตอน cache เย็น (restart / หลัง sync)
_us_index_diff_lock = threading.Lock()

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

# Quarterly growth screener cache — เก็บผลโหลดดิบ financials_store._load_set_qpl_all (sqlite+JSON
# parse ~930 หุ้น) แยกจาก _sector_compare_cache แม้อ่านตารางเดียวกัน เพราะเมนู "🚀 หุ้นโตแรงรายไตรมาส"
# ต้องคำนวณซ้ำได้ทุกไตรมาสที่ผู้ใช้กด ◀▶/dropdown เลือก (ไม่ใช่แค่ไตรมาสล่าสุดตายตัวแบบ sector-compare)
# แคชแค่ชั้นโหลด ส่วนคำนวณ QoQ/YoY ต่อไตรมาส (get_qpl_growth_screener) รันสดทุก request เพราะเร็ว
# (loop ในหน่วยความจำ ไม่มี I/O) — สลับไตรมาสในหน้าเว็บจึงไม่ต้องอ่าน DB ใหม่ทุกครั้ง
_qpl_parsed_cache: dict = {}
_QPL_PARSED_CACHE_TTL = 24 * 3600
_qpl_parsed_lock = threading.Lock()

# Quarterly growth screener — เวอร์ชันหุ้นต่างประเทศ (?universe=dr|us|hk|jp) อ่าน finnomena_q/
# yahoo_q ดิบผ่าน financials_store._load_mirror_qpl_all (US mirror ~3.9k payload — cold load
# ช้ากว่าหุ้นไทยมาก) cache แยกต่อ universe แต่ TTL/SWR เดียวกับหุ้นไทย · เคลียร์พร้อม
# _qpl_parsed_cache ทุกจุด + ใน _clear_fin_analytics_and_warm (งาน sync mirror US/HK/JP)
_qpl_mirror_caches: dict = {}     # uni -> {"result":.., "ts":..}  (persistent per uni)
_qpl_mirror_locks: dict = {}      # uni -> threading.Lock
_qpl_mirror_guard = threading.Lock()


def _qpl_mirror_slot(uni):
    """คืน (cache_dict, lock) ต่อ universe — lazily init, persistent (ห้าม .clear() ทั้ง dict
    เพราะ _swr_get_or_refresh ต้องได้ dict เดิมทุกครั้ง — เคลียร์ด้วย _clear_qpl_mirror_caches
    ที่ pop เฉพาะ result/ts ออก)"""
    slot = _qpl_mirror_caches.get(uni)
    if slot is None:
        with _qpl_mirror_guard:
            slot = _qpl_mirror_caches.get(uni)
            if slot is None:
                slot = {}
                _qpl_mirror_locks[uni] = threading.Lock()
                _qpl_mirror_caches[uni] = slot
    return slot, _qpl_mirror_locks[uni]


def _clear_qpl_mirror_caches(unis=None):
    """ล้าง result/ts ของ growth-screener mirror cache — unis=None ล้างทุก universe,
    unis=("dr",) ล้างเฉพาะที่ระบุ (งาน sync ที่แตะแค่บาง namespace เช่น DR sync ไม่แตะ
    FINN:US/HK/JP จึงไม่ควร invalidate ตัวอื่นให้ recompute cold โดยเปล่าประโยชน์)"""
    for uni, slot in list(_qpl_mirror_caches.items()):
        if unis is not None and uni not in unis:
            continue
        slot.pop("result", None)
        slot.pop("ts", None)

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
      ค่า None ไป ต่างจากเคส "เก่า" ที่มีของเก่าให้คืนก่อนได้

    _bg() เขียนผลกลับต้องเช็ค cache["ts"] เทียบกับเวลาที่เริ่มคำนวณ (t_start) ก่อนเขียน —
    กันเคส event อื่น (เช่น sync งบเสร็จ) ล้าง cache แล้วคำนวณ synchronous ใหม่แทรกระหว่างที่
    _bg() นี้กำลังคำนวณค้างอยู่ (ช้า ~6-13 วิ) ถ้าไม่เช็คจะเอาผลเก่าของ _bg() มาเขียนทับ
    ผลสดที่เพิ่ง warmup ไปได้ (ts ใหม่กว่าด้วยซ้ำ ดูเหมือนสดทั้งที่เป็นข้อมูลก่อน sync)"""
    result = cache.get("result")
    if result is not None:
        if time.time() - cache.get("ts", 0) < ttl:
            return result
        with lock:
            if not cache.get("_revalidating"):
                cache["_revalidating"] = True

                def _bg():
                    t_start = time.time()
                    try:
                        new_result = compute_fn()
                        with lock:
                            if cache.get("ts", 0) < t_start:
                                cache["result"] = new_result
                                cache["ts"] = time.time()
                    except Exception as e:
                        print(f"[SWR] คำนวณใหม่เบื้องหลังล้มเหลว (ใช้ค่าเก่าต่อ): {e}")
                    finally:
                        with lock:
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
from sources import set_company
from sources import set_daily_val
from sources import analyst_consensus
from sources import dcf_screener
from sources import pbv_pe_screener
from sources import dr_descriptions
from sources import us_index_membership
from sources import hk_index_membership


BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
# พอร์ตที่ server ตัวนี้ bind (ดู __main__ ท้ายไฟล์) — ยกมาเป็น module-level constant
# ให้ /api/kill-duplicate-servers อ้างอิงพอร์ตเดียวกันได้โดยไม่ต้อง hardcode ซ้ำ 2 จุด
SERVER_PORT  = int(os.environ.get("SET_DASH_PORT", "5001"))
DATA_FILE    = os.path.join(BASE_DIR, "set_data.json")
BACKUP_FILE  = os.path.join(BASE_DIR, "set_data_backup.json")
HTML_FILE    = os.path.join(BASE_DIR, "set_dashboard.html")
HISTORY_FILE = os.path.join(BASE_DIR, "set_history.json")
DR_CACHE_FILE = os.path.join(BASE_DIR, "dr_cache.json")
ETF_CACHE_FILE = os.path.join(BASE_DIR, "etf_cache.json")
ETF_NEW_LISTINGS_FILE = os.path.join(BASE_DIR, "etf_known_symbols.json")
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

_dr_refresh_state = {"running": False, "error": None, "done": False,
                     "n_total": None, "n_updated": None}
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

# read-modify-write etf_known_symbols.json เกิดจาก 2 path พร้อมกันได้: (1) background
# rebuild thread เรียก _check_etf_new_listings (2) request thread เรียก
# /api/etf-new-listings/ack — ถ้าไม่ล็อก last-writer-wins อาจทำ known set หด/badge เด้ง
# กลับหลัง ack ไปแล้ว
_etf_new_listings_lock = threading.Lock()


def _check_etf_new_listings(stocks):
    """เทียบ symbol ที่เพิ่งดึงมา (ETF ไม่มี static list ให้ curate แบบ DR — ดึงสดจาก
    SET ทุกรอบ rebuild อยู่แล้ว) กับ known set ที่บันทึกไว้ในไฟล์ — ตัวใหม่ที่ไม่เคยเห็น
    มาก่อนจะถูกเติมเข้า pending_new (ไว้เติม badge เตือนบนปุ่ม nav ของหน้า ETF) ค้างจน
    กว่าผู้ใช้จะเปิดหน้า ETF (เรียก /api/etf-new-listings/ack เคลียร์) — persist ลงไฟล์
    กันหายตอน restart server ครั้งแรกที่ไม่มีไฟล์เลย (เพิ่ง deploy ฟีเจอร์นี้) ไม่ถือว่า
    ทุกตัวเป็น "ใหม่" ทั้งหมด (กัน badge ระเบิดเทียบกับ known set ว่าง)"""
    current = {s["symbol"] for s in stocks if s.get("symbol")}
    from datetime import datetime as _dt
    with _etf_new_listings_lock:
        file_exists = os.path.exists(ETF_NEW_LISTINGS_FILE)
        state = None
        if file_exists:
            try:
                with open(ETF_NEW_LISTINGS_FILE, encoding="utf-8") as f:
                    state = json.load(f)
            except Exception as e:
                # ไฟล์มีอยู่แต่อ่านไม่ได้ (เขียนค้าง/ไฟดับ/เสีย) — ข้ามรอบนี้ ห้ามเขียนทับ
                # ไม่งั้น known set + pending_new badge ที่สะสมไว้หายเกลี้ยงเหมือน first-run
                print(f"[ETF] known-symbols state อ่านไม่ได้ ({e}) — ข้ามรอบนี้ ไม่เขียนทับ")
                return

        if not file_exists:
            _atomic_write_json(ETF_NEW_LISTINGS_FILE, {
                "known": sorted(current), "pending_new": [], "updated_at": _dt.now().isoformat(),
            })
            return
        if not isinstance(state, dict):
            print("[ETF] known-symbols state ไม่ใช่ dict — ข้ามรอบนี้")
            return

        known = set(state.get("known") or [])
        pending = set(state.get("pending_new") or [])
        new_syms = current - known
        if new_syms:
            pending |= new_syms
            print(f"[ETF] พบ ETF ใหม่ {len(new_syms)} ตัว: {', '.join(sorted(new_syms))}")
        known |= current
        _atomic_write_json(ETF_NEW_LISTINGS_FILE, {
            "known": sorted(known), "pending_new": sorted(pending), "updated_at": _dt.now().isoformat(),
        })

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
_index_html_lock = threading.Lock()


@app.route("/")
def index():
    mtime = os.path.getmtime(HTML_FILE)
    with _index_html_lock:
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
        with _data_gz_lock:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if _data_gz_cache["path"] == path and _data_gz_cache["mtime"] == mtime:
                return _data_gz_cache["raw"], _data_gz_cache["gz"], f'"{path == BACKUP_FILE}-{mtime}"'
            try:
                with open(path, "rb") as f:
                    raw = f.read()
                json.loads(raw)  # validate ครั้งเดียวต่อ mtime
                # เช็ค mtime ซ้ำหลังอ่าน — กัน race: ถ้าไฟล์ถูกเขียนทับระหว่างอ่าน raw จะไม่ตรง
                # กับ mtime ที่ cache ไว้แล้ว ต้องอ่านใหม่แทนที่จะ cache bytes ใหม่คู่กับ mtime เก่า
                mtime2 = os.path.getmtime(path)
                if mtime2 != mtime:
                    raw = None
            except Exception:
                raw = None
            if raw is None:
                # ลองอ่านซ้ำอีกครั้งด้วย mtime ล่าสุด แทนที่จะข้ามไป backup ทั้งที่ไฟล์หลักไม่ได้เสีย
                try:
                    mtime = os.path.getmtime(path)
                    with open(path, "rb") as f:
                        raw = f.read()
                    json.loads(raw)
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
        # Quick Update มีขั้นตอนย่อยอัพเดทราคา US/HK/JP Index (เขียน *_prices.db /
        # *_index_metrics.json) — กันชนกับปุ่ม "⚡ อัพเดทราคา" ของ Heatmap ที่เขียนไฟล์
        # เดียวกันภายใต้ _hm_live lock คนละตัว (heatmap_live_update เช็ค _state['running']
        # ฝั่งตรงข้ามอยู่แล้ว ต้องเช็คสองทางถึงกัน race ได้จริง — เหมือน us_index_full_refresh)
        if any(s["running"] for s in _HM_LIVE_STATE.values()):
            return jsonify({"error": "กำลังอัพเดทราคาสด (Heatmap) อยู่ โปรดรอสักครู่"}), 409
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
        # 9hoon เปลี่ยนป้ายจาก "Div. Yield (12M)" เป็น "Dividend Yield" — รองรับทั้งคู่
        div_yield  = _stat_value('Dividend Yield') or _stat_value('Div. Yield (12M)')

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

        # ตารางกำไรสุทธิรายปี/ไตรมาส (พร้อม EPS + YoY ต่อไตรมาส)
        def _cell(td):
            eps_divs = td.xpath('.//div[@class="eps-sub"]')
            eps_text = _txt1(eps_divs)
            eps = eps_text.strip('[] ').strip() if eps_text else None
            # ค่าหลัก = text node ตรงๆ ของ <td> (ไม่รวม eps-sub / yoy-tip-bubble)
            val_text = ' '.join(t.strip() for t in td.xpath('./text()') if t.strip())
            val = None if (not val_text or val_text == '-') else val_text
            yoy_pct = _txt1(td.xpath('.//span[contains(@class,"tip-pct")]'))
            return {"val": val, "eps": eps, "yoy": yoy_pct}

        # หน้า 9hoon มี 2 ตาราง: (1) "เทียบ YoY" คอลัมน์ Q1..Q4 ตามปฏิทิน
        # (2) "ไล่ตามเวลา" คอลัมน์เรียงตามงวดที่รายงานจริง + คอลัมน์ "รวม" = TTM
        # (4 ไตรมาสต่อเนื่องกันจริง ไม่ใช่ปีปฏิทิน) — เดิม parser วน //table//tbody/tr
        # รวมทั้งสองตารางทำให้แถวปนกันและคอลัมน์ Q เพี้ยน
        def _parse_np_table(tbl):
            heads = [th.text_content().strip() for th in tbl.xpath('./thead//th')]
            q_labels = heads[1:-1] if len(heads) >= 3 else []
            rows = []
            for tr in tbl.xpath('./tbody/tr'):
                tds = tr.xpath('./td')
                if len(tds) < len(q_labels) + 2:
                    continue
                cells = [_cell(td) for td in tds[1:1 + len(q_labels)]]
                rows.append({
                    "year": tds[0].text_content().strip(),
                    "quarters": [dict(c, q=q) for q, c in zip(q_labels, cells)],
                    "total": _cell(tds[-1]),
                })
            return {"quarter_order": q_labels, "rows": rows}

        np_tables = []
        for tbl in tree.xpath('//table'):
            ct = tbl.xpath('./preceding::div[contains(@class,"chart-title")][1]')
            np_tables.append(((ct[0].text_content() if ct else ''), _parse_np_table(tbl)))

        def _find_tbl(kw, default_idx):
            for title, parsed in np_tables:
                if kw in title:
                    return parsed
            return np_tables[default_idx][1] if len(np_tables) > default_idx else None

        yoy_parsed = _find_tbl('เทียบ YoY', 0)
        seq_parsed = _find_tbl('ไล่ตามเวลา', 1)

        # ตารางเทียบ YoY — คงรูปเดิม q1..q4 ไว้ให้ frontend เดิมใช้ได้ (map ตามชื่อไตรมาสจริง)
        table = []
        for row in (yoy_parsed["rows"] if yoy_parsed else []):
            qmap = {qc["q"]: qc for qc in row["quarters"]}
            def _pick(qq, _qmap=qmap):
                c = _qmap.get(qq)
                return {"val": c["val"], "eps": c["eps"], "yoy": c["yoy"]} if c \
                    else {"val": None, "eps": None, "yoy": None}
            table.append({
                "year": row["year"],
                "q1": _pick("Q1"), "q2": _pick("Q2"),
                "q3": _pick("Q3"), "q4": _pick("Q4"),
                "total": row["total"],
            })

        # ตารางไล่ตามเวลา + TTM (ของใหม่)
        table_seq = seq_parsed
        ttm = None
        if seq_parsed and seq_parsed["rows"]:
            r0 = seq_parsed["rows"][0]
            qo = seq_parsed["quarter_order"]
            ttm = {
                "period_label": (f"{qo[-1]}/{r0['year']}" if qo else r0["year"]),
                "year": r0["year"],
                "value": r0["total"]["val"],
                "eps": r0["total"]["eps"],
                "yoy": r0["total"]["yoy"],
            }

        disclaimer = _txt1(tree.xpath('//div[contains(@class,"footer-note")]'))

        # กราฟ: 9hoon เลิกใช้ google.visualization แล้ว ฝัง SVG inline แทน —
        # ดึงจาก data-tooltip ("Q1/2021: 32,587.60 ลบ.") ของแท่งใน svg แรก (เทียบ YoY)
        chart = {"years": [], "quarters": []}
        svgs = tree.xpath('//*[contains(@class,"np-svg-chart")]')
        if svgs:
            byq, years_seen = {}, []
            for tip in svgs[0].xpath('.//*[@data-tooltip]/@data-tooltip'):
                m = _re.match(r'\s*(Q\d)/(\d{4}):\s*([-\d,.]+)', tip)
                if not m:
                    continue
                q, yr, raw = m.group(1), m.group(2), m.group(3).replace(',', '')
                try:
                    v = float(raw)
                except ValueError:
                    v = None
                byq.setdefault(q, {})[yr] = v
                if yr not in years_seen:
                    years_seen.append(yr)
            years = sorted(years_seen)
            chart = {
                "years": years,
                "quarters": [{"q": q, "vals": [byq[q].get(y) for y in years]}
                             for q in ("Q1", "Q2", "Q3", "Q4") if q in byq],
            }

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
            "table_seq": table_seq,
            "ttm": ttm,
            "disclaimer": disclaimer,
            "chart": chart,
        }
        fetched_at = _dt.now().strftime("%H:%M น.")
        _npdata_cache[sym] = {"ts": time.time(), "fetched_at": fetched_at, "data": result}
        result["cached_at"] = None
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# settrade Incapsula bootstrap cookie — cache ต่อ process (TTL 15 นาที เหมือน
# set_api._HDR_TTL) · การเปิดหน้า /th/equities/quote/<sym>/analyst-consensus
# ครั้งเดียวได้ cookie visid_incap/incap_ses ที่ใช้กับ /api/set-fund/consensus/*
# ได้ทั้ง domain (ทุก symbol) — ไม่ใช่ต่อ request
_settrade_cookie_cache: dict = {}   # {"ts": float, "cookie": str}
_SETTRADE_COOKIE_TTL = 15 * 60
_settrade_cookie_lock = threading.Lock()


def _settrade_ac_cookie(force=False):
    import requests as req
    c = _settrade_cookie_cache.get("cookie")
    if not force and c and time.time() - _settrade_cookie_cache["ts"] < _SETTRADE_COOKIE_TTL:
        return c
    with _settrade_cookie_lock:
        c = _settrade_cookie_cache.get("cookie")
        if not force and c and time.time() - _settrade_cookie_cache["ts"] < _SETTRADE_COOKIE_TTL:
            return c
        s = req.Session()
        s.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36")
        s.get("https://www.settrade.com/th/equities/quote/PTTGC/analyst-consensus", timeout=25)
        cookie = "; ".join(f"{k}={v}" for k, v in s.cookies.get_dict().items())
        if "incap_ses" not in cookie:
            raise RuntimeError("settrade bootstrap: ไม่ได้ Incapsula cookie")
        _settrade_cookie_cache.update({"ts": time.time(), "cookie": cookie})
        return cookie


def _rr_company_name(sym):
    """ชื่อบริษัทภาษาไทยจาก set_data.json (cache ตาม mtime, ไม่มี network) —
    best-effort คืน None ถ้าไม่เจอ (settrade consensus API ไม่ส่งชื่อบริษัทมา)"""
    try:
        e = _tearsheet_universe_map("TH").get(sym) or {}
        return e.get("name_th") or e.get("name")
    except Exception:
        return None


def _rr_num(v, nd=2):
    """float → string ทศนิยม nd ตำแหน่ง (คั่นหลักพันสำหรับตัวใหญ่) — None ถ้าไม่มีค่า"""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f"{f:,.{nd}f}"


def _rr_from_settrade(sym):
    """บทวิเคราะห์ + ฉันทามติจาก settrade IAA consensus (แหล่งทางการ) — คืน dict
    รูปเดียวกับ _rr_from_9hoon หรือ raise · endpoint:
      /api/set-fund/consensus/stock/<sym>/consensus   (per-broker + avg/median/high/low)
      /api/set-fund/consensus/stock/overall?symbol=   (last price + buy/hold/sell)
    ต้องแนบ Incapsula cookie จาก _settrade_ac_cookie() ไม่งั้น 403"""
    import requests as req
    from datetime import datetime as _dt

    def _get(path, params):
        for attempt in range(2):
            ck = _settrade_ac_cookie(force=attempt > 0)
            r = req.get("https://www.settrade.com" + path, params=params, timeout=20,
                        headers={"Accept": "application/json", "Cookie": ck,
                                 "Referer": f"https://www.settrade.com/th/equities/quote/{sym}/analyst-consensus",
                                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"})
            if r.status_code == 403 and attempt == 0:
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError("settrade 403 (Incapsula)")

    con = _get(f"/api/set-fund/consensus/stock/{sym}/consensus", {"lang": "th"})
    rows = con.get("consensuses") or []
    if not rows:
        raise ValueError(f"settrade: ไม่มีฉันทามติสำหรับ {sym}")

    try:
        ov = (_get("/api/set-fund/consensus/stock/overall",
                   {"lang": "th", "symbol": sym}).get("overall") or [{}])[0]
    except Exception:
        ov = {}

    by1 = str(int(con["currentYear"]) + 543) + "F" if con.get("currentYear") else "F1"
    by2 = str(int(con["nextYear"]) + 543) + "F" if con.get("nextYear") else "F2"

    avg = con.get("average") or {}
    med = con.get("median") or {}
    last_price = ov.get("lastPrice")
    tgt_med = ov.get("medianTargetPrice") or med.get("targetPrice")
    upside = None
    if tgt_med and last_price:
        u = (tgt_med / last_price - 1) * 100
        upside = f"{u:+.1f}%"
    rt = ov.get("recommendType")

    summary = {
        "market":        "SET",
        "last_price":    _rr_num(last_price),
        "coverage":      str(ov["totalCoverage"]) if ov.get("totalCoverage") is not None else str(len(rows)),
        "buy":  str(ov["buy"])  if ov.get("buy")  is not None else None,
        "hold": str(ov["hold"]) if ov.get("hold") is not None else None,
        "sell": str(ov["sell"]) if ov.get("sell") is not None else None,
        "rating":        rt.capitalize() if rt else None,
        "target_avg":    _rr_num(ov.get("averageTargetPrice") or avg.get("targetPrice")),
        "target_median": _rr_num(tgt_med),
        "upside":        upside,
        "eps_y1": _rr_num(avg.get("currentYearEps")), "eps_y2": _rr_num(avg.get("nextYearEps")),
        "np_y1":  _rr_num(avg.get("currentYearNetProfit")), "np_y2": _rr_num(avg.get("nextYearNetProfit")),
        "pe_y1":  _rr_num(avg.get("currentYearPe")), "pe_y2": _rr_num(avg.get("nextYearPe")),
    }

    def _fmt_date(iso):
        if not iso:
            return None
        try:
            return _dt.fromisoformat(iso).strftime("%d %b %Y")
        except ValueError:
            return None

    reports = []
    for c in rows:
        pdf = (c.get("lastResearchURL") or "").strip()
        if not pdf.lower().startswith("https://"):
            continue   # บางโบรกส่งตัวเลขแต่ไม่มีไฟล์ PDF
        reports.append({
            "broker":   c.get("brokerName"),
            "analyst":  c.get("analystName"),
            "date":     _fmt_date(c.get("lastUpdateDate")),
            "rating":   c.get("recommend"),
            "target":   _rr_num(c.get("targetPrice")),
            "year":     f"{by1}/{by2}",
            "rec":      c.get("recommend"),
            "pdf_url":  pdf,
            "_ts":      c.get("lastUpdateDate") or "",
        })
    reports.sort(key=lambda x: x["_ts"], reverse=True)
    for c in reports:
        c.pop("_ts", None)

    if summary["last_price"] is None and not reports:
        raise ValueError(f"settrade: ข้อมูล {sym} ไม่พอ")

    return {
        "symbol": sym,
        "company_name": _rr_company_name(sym),
        "summary": summary,
        "year_labels": [by1, by2],
        "reports": reports,
        "source": "settrade",
    }


def _rr_from_9hoon(sym):
    """fallback: scrape 9hoon.com/aset/view_research_reports.php — ใช้เมื่อ settrade
    ล่ม/Incapsula เข้มขึ้น · 9hoon รวมลิงก์ PDF portal.settrade.com ให้ + สร้าง
    แถวสรุปด้วย AI (อาจคลาดเคลื่อน) คืน dict รูปเดียวกับ _rr_from_settrade หรือ raise"""
    import requests as req
    import lxml.html as _lh

    r = req.get("https://9hoon.com/aset/view_research_reports.php", params={"symbol": sym},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=25)
    tree = _lh.fromstring(r.text)

    def _t1(els):
        return els[0].text_content().strip() if els else None

    company_name = None
    hdr = tree.xpath('//a[contains(@href,"analyst-consensus")]')
    if hdr:
        spans = hdr[0].getparent().xpath('./span')
        if spans:
            company_name = spans[0].text_content().strip()

    summary = None
    rows = tree.xpath('//table//tbody/tr')
    if rows:
        tds = rows[0].xpath('./td')
        if len(tds) >= 12:
            def _c(i):
                return (tds[i].text_content().strip() or None) if i < len(tds) else None
            bhs = tds[3].xpath('.//span')
            summary = {
                "market": _c(0), "last_price": _c(1), "coverage": _c(2),
                "buy":  bhs[0].text_content().strip() if len(bhs) > 0 else None,
                "hold": bhs[1].text_content().strip() if len(bhs) > 1 else None,
                "sell": bhs[2].text_content().strip() if len(bhs) > 2 else None,
                "rating": _c(4), "target_avg": _c(5), "target_median": _c(6), "upside": _c(7),
                "eps_y1": _c(8), "eps_y2": _c(9), "np_y1": _c(10), "np_y2": _c(11),
                "pe_y1": _c(12), "pe_y2": _c(13),
            }

    yr = tree.xpath('//table//thead/tr[2]/th')
    year_labels = [x.text_content().strip() for x in yr if x.text_content().strip()]

    reports = []
    for ifr in tree.xpath('//iframe[contains(@class,"pdf-frame")]'):
        card = ifr.getparent()
        pdf = (ifr.get("src") or "").strip()
        if pdf.startswith("//"):
            pdf = "https:" + pdf
        if not pdf.lower().startswith("https://"):
            continue
        rating = _t1(card.xpath('.//span[contains(@class,"rounded-full")]'))
        dd = card.xpath('.//div[contains(@class,"text-xs") and contains(@class,"text-gray-400")]')
        target = year = rec = None
        meta = card.xpath('.//div[contains(@class,"bg-gray-50")][1]')
        if meta:
            mono = meta[0].xpath('.//span[contains(@class,"font-mono")]')
            if mono:
                target = mono[0].text_content().strip()
            ital = meta[0].xpath('.//div[contains(@class,"italic")]')
            if ital:
                rec = ital[0].text_content().strip().strip('"')
            for d in meta[0].xpath('./div'):
                if 'ปี' in d.text_content():
                    year = d.text_content().replace('ปี', '').strip()
        reports.append({
            "broker": _t1(card.xpath('.//div[contains(@class,"font-semibold")]')),
            "analyst": None,
            "date": dd[0].text_content().strip() if dd else None,
            "rating": rating, "target": target, "year": year, "rec": rec, "pdf_url": pdf,
        })

    if summary is None and not reports:
        raise ValueError(f"9hoon: ไม่พบบทวิเคราะห์สำหรับ {sym}")

    return {
        "symbol": sym, "company_name": company_name, "summary": summary,
        "year_labels": year_labels, "reports": reports, "source": "9hoon",
    }


@app.route("/api/research-reports/<symbol>")
def get_research_reports(symbol):
    """บทวิเคราะห์รายหุ้น (PDF รายโบรก) + สรุปฉันทามตินักวิเคราะห์

    แหล่งหลัก = settrade IAA consensus (ทางการ, ข้อมูลครบกว่า — EPS/NP/PE/ปันผล
    รายโบรก + ชื่อ analyst) · fallback = scrape 9hoon.com ถ้า settrade ล่ม/Incapsula
    เข้มขึ้น · cache 3 ชม. เหมือน /api/npdata · ไฟล์ PDF โฮสต์ที่
    portal.settrade.com — หน้า Valuation Band ฝัง <iframe> ชี้ pdf_url ตรงๆ"""
    from datetime import datetime as _dt

    sym = symbol.upper().strip()

    cached = _rr_cache.get(sym)
    if cached and (time.time() - cached["ts"] < _RR_CACHE_TTL):
        result = dict(cached["data"])
        result["cached_at"] = cached["fetched_at"]
        return jsonify(result)

    result = err = None
    for fn in (_rr_from_settrade, _rr_from_9hoon):
        try:
            result = fn(sym)
            break
        except Exception as e:
            err = e
            logging.info("[research-reports] %s %s: %s", sym, fn.__name__, e)

    if result is None:
        return jsonify({"error": f"ไม่พบบทวิเคราะห์สำหรับ {sym} ({err})"}), 404

    fetched_at = _dt.now().strftime("%H:%M น.")
    _rr_cache[sym] = {"ts": time.time(), "fetched_at": fetched_at, "data": result}
    result = dict(result)
    result["cached_at"] = None
    return jsonify(result)


def _strip_history_light(result, drop_keys, refreshing=False):
    """ตัด field ประวัติราคาเต็ม (dates/closes/... — ~98% ของ payload) ออกจาก response
    ของ /api/dr และ /api/etf — frontend ใช้แค่ close100 (sparkline) + ohlc30 (แท่งเทียนย่อ)
    กราฟ full history ดึงแยกรายตัวจาก /api/{dr,etf}-history ซึ่งอ่านจาก cache ฝั่ง server
    (cache ในหน่วยความจำ/ไฟล์ยังเก็บเต็มเหมือนเดิม — quick-update ก็ใช้ dates ต่อได้)"""
    out = {"stocks": [{k: v for k, v in s.items() if k not in drop_keys}
                      for s in result.get("stocks", [])],
           "ts": result.get("ts"),
           "warnings": result.get("warnings") or []}
    if refreshing:
        out["refreshing"] = True   # ให้ UI ติดป้าย 'กำลังอัพเดทเบื้องหลัง' แล้ว poll ซ้ำ
    return out


def _dr_light(result, refreshing=False):
    return _strip_history_light(result, ("dates", "closes", "_full_vols"), refreshing)


_dr_rebuild_lock = threading.Lock()


def _kick_dr_rebuild():
    """rebuild DR cache ใน background thread — ไม่ start ซ้อนถ้ามีตัวหนึ่งรันอยู่แล้ว

    ใช้ acquire(blocking=False) แทนการเช็ค .locked() เฉยๆ — เดิม 2 request ที่มาพร้อมกัน
    หลัง cache หมดอายุ อาจเห็น locked()==False พร้อมกันทั้งคู่ (TOCTOU) แล้วสปอน thread
    รีบิลด์ซ้อนกัน 2 ชุด ยิง yf.download()/market-cap batch ซ้ำเปล่าๆ — acquire แบบนี้
    การเช็ค+จองคิวเป็นจังหวะเดียวแบบ atomic เลยกันซ้อนได้จริง"""
    if not _dr_rebuild_lock.acquire(blocking=False):
        return

    def _bg():
        try:
            # เช็คซ้ำเผื่อมี rebuild อื่น (เช่น blocking path จาก request คนละตัว) เพิ่งเสร็จ
            if not (_dr_cache.get("result") and _dr_cache.get("ts") is not None
                    and time.time() - _dr_cache["ts"] < 120):
                _dr_do_rebuild()
            print("[DR] background refresh เสร็จ")
        except Exception as e:
            print(f"[DR] background refresh ล้มเหลว: {e}")
        finally:
            _dr_rebuild_lock.release()

    threading.Thread(target=_bg, daemon=True).start()


@app.route("/api/dr")
def get_dr_data():
    """ดึงราคา underlying foreign stocks ของ DR/DRx ทั้งหมด — cache 4 ชั่วโมง

    cache หมดอายุ: ตอบข้อมูลเก่าทันที (flag refreshing=true) แล้ว rebuild เบื้องหลัง
    (stale-while-revalidate) — ผู้ใช้ไม่ต้องรอ 1-2 นาทีเหมือนเดิมอีก
    ?fresh=1 = บังคับทำสดแบบ blocking (ใช้ตอน bake static site — ห้ามได้ข้อมูลเก่า)"""
    fresh = request.args.get("fresh") == "1"
    # snapshot ครั้งเดียวแทนอ่าน _dr_cache["result"]/["ts"] แยก 2 จังหวะ — เดิม background
    # thread อาจ .update() คั่นกลางพอดี ทำให้ได้ result เก่าคู่กับ ts ใหม่ (torn read)
    snap = dict(_dr_cache)
    cached = snap.get("result")
    if not fresh and cached and cached.get("stocks") and snap.get("ts") is not None:
        age = time.time() - snap["ts"]
        try:
            if age < _DR_CACHE_TTL:
                return jsonify(_dr_light(cached))
            _kick_dr_rebuild()
            return jsonify(_dr_light(cached, refreshing=True))
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ไม่มี cache เลย (รันครั้งแรกสุดของเครื่อง) หรือ fresh=1 — ทำสดแบบ blocking
    try:
        result = _rebuild_dr_cache()
        return jsonify(_dr_light(result))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _rebuild_dr_cache():
    """ดึงราคา+คำนวณ DR ทั้ง universe แล้วอัพเดท _dr_cache + dr_cache.json — คืน result เต็ม
    lock กันรันซ้อน (เรียกได้ทั้งจาก request ตรงและ background thread)"""
    with _dr_rebuild_lock:
        # อีก thread เพิ่ง rebuild เสร็จระหว่างเรารอ lock — ใช้ผลนั้นเลย ไม่ยิงซ้ำ
        if (_dr_cache.get("result") and _dr_cache.get("ts") is not None
                and time.time() - _dr_cache["ts"] < 120):
            return _dr_cache["result"]
        return _dr_do_rebuild()


def _compute_dr_metrics(closes, dates, vols, cur_year):
    """คำนวณ metrics ชุดเดียวกันจาก full price history (closes/dates/vols ascending,
    ความยาวเท่ากันทั้ง 3 list) — ใช้ร่วมกันทั้ง _dr_do_rebuild (full) และ dr_quick_update
    (_do_quick) กันปัญหา 2 path drift กัน (เคยเจอจริง: YTD tz-aware TypeError เฉพาะ full,
    vol_avg20 int() vs round() ปัดคนละแบบ) — เรียกได้ก็ต่อเมื่อ len(closes) >= 2 เท่านั้น
    (ผู้เรียกทั้งคู่ guard เงื่อนไขนี้ไว้ก่อนแล้ว)"""
    import pandas as pd

    price = closes[-1]

    def _ret(n):
        if len(closes) < n + 1:
            return None
        p = closes[-(n + 1)]
        return round((price - p) / p * 100, 2) if p else None

    close_52w = closes[-252:]
    ath = round(max(closes), 4)

    # เทียบ string ISO date ตรงๆ (เรียงตามลำดับตัวอักษร = เรียงตามวันที่จริงสำหรับ
    # รูปแบบ YYYY-MM-DD) แทนการเทียบ pd.Timestamp ที่เคยพัง TypeError ตอน index
    # tz-aware — ดู comment เดิมใน _dr_do_rebuild ก่อนรีวิว 2026-09-01
    ytd_idx = next((i for i, d in enumerate(dates) if d >= f"{cur_year}-01-01"), None)
    if ytd_idx is not None:
        first_ytd = closes[ytd_idx]
        ret_ytd = round((price - first_ytd) / first_ytd * 100, 2) if first_ytd else None
    else:
        ret_ytd = None

    ret_1m, ret_3m, ret_6m, ret_1y = _ret(21), _ret(63), _ret(126), _ret(250)
    rs_raw = calc_rs_raw(ret_1m, ret_3m, ret_6m, ret_1y)

    _close_series = pd.Series(closes)
    ema50  = calc_ema(_close_series, 50)
    ema200 = calc_ema(_close_series, 200)

    hist_bars = min(len(closes), 500)

    return {
        "ret_1w":  _ret(5),
        "ret_1m":  ret_1m,
        "ret_3m":  ret_3m,
        "ret_6m":  ret_6m,
        "ret_1y":  ret_1y,
        "ret_3y":  _ret(756),
        "ret_5y":  _ret(1260),
        "ret_ytd": ret_ytd,
        "high_52w": round(max(close_52w), 4),
        "low_52w":  round(min(close_52w), 4),
        "ath":      ath,
        "ath_pct":  round((price - ath) / ath * 100, 2) if ath else None,
        "rs_raw":   round(rs_raw, 4) if rs_raw is not None else None,
        "above_ema50":  bool(price > ema50)  if ema50  is not None else None,
        "above_ema200": bool(price > ema200) if ema200 is not None else None,
        "price_history": [
            [d, round(float(p), 4 if p < 1 else 2)]
            for d, p in zip(dates[-hist_bars:], closes[-hist_bars:])
        ],
        "vol_history": vols[-260:] if vols else [],
        "vol_today":   vols[-1] if vols else None,
        "vol_avg20":   round(sum(vols[-21:-1]) / 20) if len(vols) >= 21 else None,
        "close100":    [round(c, 4) for c in closes[-100:]],
    }


def _dr_refetch_full(yticker, region, cur_year):
    """refetch ประวัติเต็ม (period=max) ของ DR underlying ตัวเดียว — ใช้ตอน quick-update
    ตรวจพบ corporate action (แตกพาร์/รวมพาร์/หุ้นปันผล → Yahoo ปรับราคาย้อนหลังทั้งเส้น
    เพราะ auto_adjust=True) หรือช่วงข้อมูลขาด (underlying พักเทรดนานแล้วกลับมา) — delta-merge
    ปกติจะต่อแท่งใหม่ (คนละ basis) ทับฐานเก่า หรือทิ้งรูโหว่ไว้ ต้องดึงใหม่ทั้งเส้นแทน
    คู่ขนานกับ _repair_ca_tickers (หุ้นไทย) + detect_ca_mismatch ใน _run_index_gap_update
    (Heatmap US/HK/JP) — DR เป็น incremental-price pipeline เดียวที่ยังไม่มี split detector
    ก่อนรีวิว 2026-09-04 (จุดที่ 8). คืน dict สำหรับ entry.update(...) หรือ None ถ้าดึงไม่ได้/
    ข้อมูลสั้นผิดปกติ (caller ต้องคงข้อมูลเดิมไว้ ห้าม merge ต่อด้วย basis ที่ไม่ตรง)"""
    import yfinance as yf
    import pandas as pd
    from sources.yahoo import _TimeoutSession

    raw = yf.download([yticker], auto_adjust=True, session=_TimeoutSession(),
                      progress=False, threads=False, group_by="ticker", period="max")
    if raw is None or getattr(raw, "empty", True):
        return None

    def _s(field):
        try:
            return raw[yticker][field]
        except (KeyError, TypeError):
            try:
                return raw[field]
            except Exception:
                return pd.Series(dtype=float)

    close  = _s("Close").dropna()
    if len(close) < 100:
        return None
    open_s = _s("Open").reindex(close.index).fillna(close)
    high_s = _s("High").reindex(close.index).fillna(close)
    low_s  = _s("Low").reindex(close.index).fillna(close)
    vol_s  = _s("Volume").reindex(close.index).fillna(0)

    # ตัดแท่งวันนี้ที่ยังไม่นิ่งออก แบบเดียวกับ path หลัก (_do_quick / _dr_do_rebuild)
    if not is_latest_bar_stable(region) and len(close):
        tdy = region_today_date(region)
        if tdy is not None and close.index[-1].date() == tdy:
            close  = close.iloc[:-1]
            open_s, high_s, low_s, vol_s = (open_s.iloc[:-1], high_s.iloc[:-1],
                                            low_s.iloc[:-1], vol_s.iloc[:-1])
    if len(close) < 100:
        return None

    dates  = [str(d)[:10] for d in close.index.tolist()]
    closes = [round(float(c), 6) for c in close.tolist()]
    vols   = ([int(v) for v in vol_s.tolist()] if len(vol_s) == len(close)
              else [0] * len(close))
    price, prev = closes[-1], closes[-2]

    n = min(30, len(close))
    ohlc30 = []
    for i in range(-n, 0):
        try:
            o = float(open_s.iloc[i]) if len(open_s) >= abs(i) else price
            h = float(high_s.iloc[i]) if len(high_s) >= abs(i) else price
            l = float(low_s.iloc[i])  if len(low_s)  >= abs(i) else price
            v = float(vol_s.iloc[i])  if len(vol_s)  >= abs(i) else 0
            ohlc30.append([round(o, 4), round(h, 4), round(l, 4),
                           round(float(close.iloc[i]), 4), int(v)])
        except Exception:
            pass

    out = {
        "price": round(price, 2),
        "chg":   round((price - prev) / prev * 100, 2) if prev else None,
        "live_price": None, "live_chg": None,
        "dates": dates, "closes": closes, "_full_vols": vols, "ohlc30": ohlc30,
    }
    out.update(_compute_dr_metrics(closes, dates, vols, cur_year))
    return out


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
            return s
        except (KeyError, TypeError):
            return pd.Series(dtype=float)

    results = []
    price_failed = []   # sym ที่ดึงราคาไม่สำเร็จ/ข้อมูลไม่พอ — เก็บไว้แจ้งใน result["warnings"]
    cur_year = _dt.now().year   # ใช้ตัด YTD — ปีเดียวกันตลอดทั้งรอบ ไม่ต้องคำนวณใหม่ทุกตัว (~283 ครั้ง)
    for stock in _dr_universe:
        try:
            yticker = stock["yf"]
            # เดิม .dropna() แยกราย field ใน _series() ทำให้ open/high/low/volume อาจสั้น/
            # ไม่ align กับ close ถ้า NaN ไม่ตรงกันระหว่าง field (เจอได้กับหุ้นเทรดเบามาก) —
            # โค้ดด้านล่างใช้ .iloc[i] แบบ positional ล้วน จะจับคู่วันผิดกันเงียบๆ ถ้าความยาว
            # ไม่เท่ากัน แก้โดย reindex ทุก field ให้ตรงกับ index ของ close เสมอ (เหมือนที่
            # ETF pipeline ทำใน _etf_do_rebuild)
            close  = _series(yticker, "Close").dropna()
            open_s = _series(yticker, "Open").reindex(close.index).fillna(close)
            high_s = _series(yticker, "High").reindex(close.index).fillna(close)
            low_s  = _series(yticker, "Low").reindex(close.index).fillna(close)
            vol_s  = _series(yticker, "Volume").reindex(close.index).fillna(0)
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
            chg   = (price - prev) / prev * 100 if prev else None
            live_chg = round((live_price - price) / price * 100, 2) if live_price and price else None

            # เก็บ full price history สำหรับ chart popup (date + price)
            dates_all  = [str(d)[:10] for d in close.index.tolist()]
            closes_all = [round(float(x), 6) for x in close.tolist()]
            # เก็บคู่กับ dates/closes เต็มประวัติ (ไม่ใช่แค่ tail 260) — dr_quick_update
            # ใช้ต่อความยาวทุกครั้งที่อัพเดท (entry.get("_full_vols", ...)) เดิมไม่เคยเซ็ต
            # ค่านี้เลยในนี้ ทำให้รอบ quick-update แรกหลัง full rebuild ทุกครั้งเข้า default
            # [0]*len(old_dates) (เป็นพันแท่ง) เจือจาง vol_avg20/vol_history จนกลายเป็น 0
            # ผิดๆ — align กับความยาว close เหมือน dr_quick_update ทำ (ดู new_vols_raw)
            # ไม่ align (เช่น NaN ถูก dropna ไม่เท่ากัน) ก็ fallback เป็น 0 ทั้งเส้น
            full_vols = ([int(v) for v in vol_s.tolist()] if len(vol_s) == len(close)
                         else [0] * len(close))

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

            # metrics (returns/52W/ATH/YTD/RS/EMA/price+vol history/close100) มาจาก
            # helper กลาง _compute_dr_metrics เดียวกับ dr_quick_update._do_quick — กัน
            # 2 path drift กัน (ดู comment ในนิยาม _compute_dr_metrics)
            metrics = _compute_dr_metrics(closes_all, dates_all, full_vols, cur_year)

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
                "chg":      round(chg, 2) if chg is not None else None,
                "live_price": round(live_price, 2) if live_price is not None else None,
                "live_chg":   live_chg,
                "ret_1w":   metrics["ret_1w"],
                "ret_1m":   metrics["ret_1m"],
                "ret_3m":   metrics["ret_3m"],
                "ret_6m":   metrics["ret_6m"],
                "ret_1y":   metrics["ret_1y"],
                "ret_3y":   metrics["ret_3y"],
                "ret_5y":   metrics["ret_5y"],
                "ret_ytd":  metrics["ret_ytd"],
                "high_52w": metrics["high_52w"],
                "low_52w":  metrics["low_52w"],
                "ath":      metrics["ath"],
                "ath_pct":  metrics["ath_pct"],
                "rs_raw":   metrics["rs_raw"],
                "rs_score": None,
                "mkt_cap":  mkt_cap,
                "drs":      stock["drs"],
                "etf":      stock.get("etf", False),
                "close100": metrics["close100"],
                "ohlc30":   ohlc30,
                "dates":    dates_all,
                "closes":   closes_all,
                "_full_vols":    full_vols,
                "above_ema50":   metrics["above_ema50"],
                "above_ema200":  metrics["above_ema200"],
                "price_history": metrics["price_history"],
                "vol_history":   metrics["vol_history"],
                "vol_today":     metrics["vol_today"],
                "vol_avg20":     metrics["vol_avg20"],
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
    return _strip_history_light(result, ("dates", "closes"), refreshing)


_etf_rebuild_lock = threading.Lock()


def _kick_etf_rebuild():
    """rebuild ETF cache ใน background — acquire(blocking=False) กัน TOCTOU เหมือน
    _kick_dr_rebuild (เดิมเช็ค .locked() เฉยๆ 2 request ที่มาพร้อมกันหลัง cache หมดอายุ
    อาจเห็น locked()==False ทั้งคู่ แล้วสปอน thread รีบิลด์ซ้อนกัน 2 ชุด yfinance ซ้ำเปล่าๆ
    ตัวที่ 2 ยังไปบล็อกรอ lock ใน _rebuild_etf_cache กิน worker thread ทิ้งด้วย)"""
    if not _etf_rebuild_lock.acquire(blocking=False):
        return

    def _bg():
        try:
            # เช็คซ้ำเผื่อมี rebuild อื่น (blocking path จาก request คนละตัว) เพิ่งเสร็จ
            if not (_etf_cache.get("result") and _etf_cache.get("ts") is not None
                    and time.time() - _etf_cache["ts"] < 120):
                _etf_do_rebuild()
            print("[ETF] background refresh เสร็จ")
        except Exception as e:
            print(f"[ETF] background refresh ล้มเหลว: {e}")
        finally:
            _etf_rebuild_lock.release()

    threading.Thread(target=_bg, daemon=True).start()


@app.route("/api/etf")
def get_etf_data():
    """ดึงราคา+metadata ETF ที่จดทะเบียนบน SET ทั้งหมด — cache 2 ชั่วโมง
    stale-while-revalidate เหมือน /api/dr — ?fresh=1 บังคับทำสดแบบ blocking"""
    fresh = request.args.get("fresh") == "1"
    cached = _etf_cache.get("result")
    if not fresh and cached and cached.get("stocks") and _etf_cache.get("ts") is not None:
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
        if (_etf_cache.get("result") and _etf_cache.get("ts") is not None
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
    # เปิด connection เดียวอ่านทุก ETF (iter_all_series) แทน get_ohlc_series ทีละตัว —
    # เดิมแต่ละตัว connect+PRAGMA+close ใหม่ (~150-190 รอบ/รีบิลด์) ทั้งที่มี bulk reader
    # แบบนี้อยู่แล้ว (ใช้ pattern เดียวกับ sources/index_metrics_common.py::_compute_all_rows)
    all_series = dict(etf_store.iter_all_series(BASE_DIR))
    cur_year = _dt.now().year   # ใช้ตัด YTD — ปีเดียวกันตลอดทั้งรอบ ไม่ต้องคำนวณใหม่ทุก ETF
    for e in etf_list:
        yticker = f"{e['symbol']}.BK"
        try:
            series = all_series.get(yticker)
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

            # close เองก็เป็น NaN ได้ (เช่น SET API fast path คืน close:None ให้ ETF เทรด
            # เบามากที่ไม่มีเทรดวันนั้น แล้วถูกเขียนลง etf_prices.db ตรงๆ) ถ้าหลุดเข้า
            # jsonify() ทีหลัง Python จะ emit literal `NaN` token ซึ่งไม่ใช่ JSON ที่ถูกสเปก
            # ทำให้ response.json() ฝั่ง frontend throw SyntaxError พังทั้งแท็บ ETF ไม่ใช่แค่
            # ตัวที่มีปัญหา — ตัดแถวที่ close เป็น NaN ทิ้งก่อน ด้วย mask เดียวกันทุก field
            # กันไม่ให้ open/high/low/volume เพี้ยนตำแหน่งจาก close (ไม่ใช้ .dropna() แยกราย
            # field แบบเดิมของ DR pipeline ที่เจอบั๊ก misalignment)
            valid_mask = close.notna()
            if not valid_mask.all():
                close  = close[valid_mask]
                open_s = open_s[valid_mask]
                high_s = high_s[valid_mask]
                low_s  = low_s[valid_mask]
                vol_s  = vol_s[valid_mask]

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
    try:
        _check_etf_new_listings(results)
    except Exception as e:
        print(f"[ETF] เช็ค ETF ใหม่ล้มเหลว (ไม่กระทบ rebuild หลัก): {e}")
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


@app.route("/api/etf-new-listings")
def etf_new_listings():
    """คืนจำนวน/รายชื่อ ETF ใหม่ที่ยังไม่ ack — ใช้เติม badge บนปุ่ม nav ตอนเปิดแอป
    (เบาๆ อ่านไฟล์ตรง ไม่ต้อง trigger rebuild) — ไม่มีไฟล์ = ยังไม่เคยรัน rebuild เลย
    หลังติดตั้งฟีเจอร์นี้ ถือว่ายังไม่มีอะไรใหม่"""
    if not os.path.exists(ETF_NEW_LISTINGS_FILE):
        return jsonify({"count": 0, "symbols": []})
    try:
        with open(ETF_NEW_LISTINGS_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return jsonify({"count": 0, "symbols": []})
    pending = sorted(state.get("pending_new") or [])
    return jsonify({"count": len(pending), "symbols": pending})


@app.route("/api/etf-new-listings/ack", methods=["POST"])
def etf_new_listings_ack():
    """ผู้ใช้เปิดหน้า ETF แล้ว (เห็น badge/แจ้งเตือนแล้ว) — เคลียร์ pending_new ล้าง
    badge โดยไม่แตะ known set (ไม่งั้นตัวเดิมจะโดนนับเป็น "ใหม่" ซ้ำรอบ rebuild ถัดไป)"""
    if not os.path.exists(ETF_NEW_LISTINGS_FILE):
        return jsonify({"ok": True})
    try:
        with _etf_new_listings_lock:
            with open(ETF_NEW_LISTINGS_FILE, encoding="utf-8") as f:
                state = json.load(f)
            state["pending_new"] = []
            _atomic_write_json(ETF_NEW_LISTINGS_FILE, state)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
        # ไม่ cache ผลที่เป็น error (เช่น SET API ไม่ตอบ) — ไม่งั้นค้าง 6 ชม. ทั้งที่รอบหน้า
        # อาจเช็คได้ปกติ
        if not result.get("error"):
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

    # snapshot ครั้งเดียว — เดิมอ่าน _dr_cache["result"] กับ ["ts"] แยก 2 จังหวะ ถ้า
    # background full rebuild (_dr_do_rebuild, ดู _kick_dr_rebuild) .update() คั่นกลางพอดี
    # จะได้ `cached` เป็น object เก่า คู่กับ `_ts_at_start` เป็น ts ใหม่ → ท้ายฟังก์ชัน
    # เช็ค `_dr_cache["ts"] != _ts_at_start` เป็น False (ts ตรงกัน) แล้วเขียนทับผล rebuild
    # สดด้วยผล quick-update ที่ mutate มาจาก `cached` เก่า → ข้อมูลชุดใหม่หายยาว 4 ชม.
    # (torn read — เหมือนที่แก้ใน get_dr_data ด้วย snap = dict(_dr_cache))
    _snap = dict(_dr_cache)
    cached = _snap.get("result")
    if not cached or not cached.get("stocks"):
        # เดิมรีเซ็ตแค่ running — เหลือ done=True + n_updated/n_total ของรอบก่อนค้างอยู่
        # frontend poll เห็น !running && !error เลยขึ้น "✓ สำเร็จ N/M" หลอกทั้งที่ request นี้
        # error 400 → เคลียร์ทั้ง state + ใส่ error ให้ตรงกับ HTTP body
        _dr_refresh_state.update(running=False, done=False,
                                 error="ยังไม่มี DR cache — กรุณาโหลดหน้า DR ก่อน")
        return jsonify({"error": "ยังไม่มี DR cache — กรุณาโหลดหน้า DR ก่อน"}), 400
    _ts_at_start = _snap.get("ts")

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
                # ตัดตัวที่ค้างเก่ากว่าแท่งใหม่สุดมาก (หุ้นพักเทรด/เพิกถอน/ETF VN เทรดเบา)
                # ออกจากการหา start — เดิม min() ตรงๆ ทำให้ 1 ตัวที่ค้าง 3 เดือนบังคับให้
                # quick-update ดาวน์โหลดย้อนหลังทั้ง universe (~283 ตัว) หลายเดือนทุกครั้งที่กด
                # (ตัวที่ค้างจริงจะได้ข้อมูลกลับตอน full rebuild period=max รอบ 4 ชม. ถัดไป)
                _newest_dt = pd.to_datetime(max(last_dates_dr))
                _recent = [d for d in last_dates_dr
                           if (_newest_dt - pd.to_datetime(d)).days <= 10]
                min_last_dr = min(_recent) if _recent else max(last_dates_dr)
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
                    return raw[yticker][field] if is_multi else raw[field]
                except (KeyError, TypeError):
                    return pd.Series(dtype=float)

            # Build lookup จาก sym → stock entry เพื่ออัปเดต
            stock_map = {s["sym"]: s for s in cached["stocks"]}
            updated = 0   # นับตัวที่อัปเดตราคาสำเร็จจริง — ให้ frontend โชว์ "สำเร็จ N/M ตัว"
                          # แบบเดียวกับปุ่ม "⚡ ราคาล่าสุด" ของ Watchlist (ดู wlRefreshLivePrices)
            ca_refetch = 0   # เพดานกัน refetch period=max ทั้งกระดานถ้า overlap เพี้ยนทุกตัว
                             # (แหล่งเปลี่ยน basis หมด) — เหมือน MAX_CA_REPAIR ของ services.refresh
            cur_year = _dt.now().year   # ใช้ตัด YTD — ปีเดียวกันตลอดทั้งรอบ ไม่ต้องคำนวณใหม่ทุกตัว
            for st in _universe:
                sym, yticker = st["sym"], st["yf"]
                try:
                    # reindex ทุก field ให้ align กับ index ของ close เสมอ — กัน .iloc[i]
                    # ด้านล่างจับคู่วันผิดกันถ้า field ไหน dropna แล้วสั้นกว่า close (ดู
                    # comment เดียวกันใน _dr_do_rebuild)
                    close  = _series(yticker, "Close").dropna()
                    open_s = _series(yticker, "Open").reindex(close.index).fillna(close)
                    high_s = _series(yticker, "High").reindex(close.index).fillna(close)
                    low_s  = _series(yticker, "Low").reindex(close.index).fillna(close)
                    vol_s  = _series(yticker, "Volume").reindex(close.index).fillna(0)
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
                    chg   = round((price - prev) / prev * 100, 2) if prev else None

                    if entry:
                        # ปัด 6 ตำแหน่งให้ตรงกับ _dr_do_rebuild (closes_all) — เดิมปัด 4
                        # ตำแหน่งตรงนี้ ทำให้ความละเอียดของ entry["closes"] ต่างกันไปตามว่า
                        # แท่งนั้นถูกเติมเข้ามาตอน full rebuild หรือ quick update
                        new_closes_raw = [round(float(c), 6) for c in close.tolist()]
                        new_dates_raw  = [str(d)[:10] for d in close.index.tolist()]
                        new_vols_raw   = [int(v) for v in vol_s.tolist()] if len(vol_s) == len(close) else [0] * len(close)
                        old_dates  = entry.get("dates", [])
                        old_closes = entry.get("closes", [])
                        old_vols   = entry.get("_full_vols", [0] * len(old_dates))
                        old_idx    = {d: i for i, d in enumerate(old_dates)}

                        # ── split / gap detector (จุดที่ 8, รีวิว 2026-09-04) ──────────────
                        # gap fetch ดึงแท่ง overlap (แท่งที่เก็บไว้แล้ว) มาด้วยเสมอ (ดู "ไม่ +1
                        # วัน") — เทียบกับที่เก็บไว้: เพี้ยน >= 2 แท่ง = Yahoo ปรับราคาย้อนหลัง
                        # (auto_adjust=True → แตกพาร์/รวมพาร์/หุ้นปันผลของ underlying) delta-merge
                        # ปกติจะต่อแท่งใหม่คนละ basis ทับฐานเก่า → ret/EMA/ATH/RS/sparkline เพี้ยน
                        # ยาวถึง full rebuild รอบถัดไป (4 ชม.) · ไม่มี overlap เลย (underlying
                        # พักเทรดนานกว่าช่วง gap) = จะเกิดรูโหว่ในเส้นราคา — สองเคสนี้ต้อง
                        # refetch period=max ทั้งเส้น (คู่ขนานกับ detect_ca_mismatch/
                        # _repair_ca_tickers หุ้นไทย + _run_index_gap_update ของ Heatmap US/HK/JP
                        # ที่มี split detector อยู่แล้ว — DR เป็น pipeline เดียวที่ยังไม่มี)
                        mism = n_overlap = 0
                        for d, cl in zip(new_dates_raw, new_closes_raw):
                            j = old_idx.get(d)
                            if j is None or j >= len(old_closes):
                                continue
                            oc = old_closes[j]
                            if oc and oc > 0:
                                n_overlap += 1
                                if abs(cl - oc) / oc > 0.005:
                                    mism += 1
                        gap_hole = (n_overlap == 0 and old_dates and new_dates_raw
                                    and new_dates_raw[0] > old_dates[-1])
                        if old_closes and (mism >= 2 or gap_hole) and ca_refetch < 30:
                            ca_refetch += 1
                            why = "split/CA" if mism >= 2 else "gap"
                            print(f"[DR quick] {sym}: {why} — refetch period=max")
                            patch = _dr_refetch_full(yticker, st["region"], cur_year)
                            if patch:
                                entry.update(patch)
                                updated += 1
                            else:
                                print(f"[DR quick] {sym}: refetch ไม่สำเร็จ — คงข้อมูลเดิม (รอ full rebuild)")
                            continue

                        # merge: overlap → update ทับในที่ (Yahoo revised ค่า ex-div เล็กน้อย
                        # < 0.5% — ไม่ใช่ split เพราะผ่าน detector แล้ว) · แท่งใหม่ → append
                        # นับ appended_count (แท่งใหม่จริง) ไว้ merge ohlc30 ต่อ เดิมแท่ง overlap
                        # ไม่ถูกแตะเลย ทำให้ entry["price"] (จากแท่ง fetch) กับ metrics (จาก
                        # old_closes[-1]) ขัดกันเองตอนไม่มีแท่งใหม่
                        appended_count = 0
                        for dt, cl, vv in zip(new_dates_raw, new_closes_raw, new_vols_raw):
                            j = old_idx.get(dt)
                            if j is not None:
                                if j < len(old_closes): old_closes[j] = cl
                                if j < len(old_vols):   old_vols[j]   = vv
                            elif not old_dates or dt > old_dates[-1]:
                                old_dates.append(dt)
                                old_closes.append(cl)
                                old_vols.append(vv)
                                appended_count += 1
                        entry["dates"]  = old_dates
                        entry["closes"] = old_closes
                        entry["_full_vols"] = old_vols

                        # ราคา/chg ยึดจาก history หลัง merge (closes[-1]/[-2]) ให้ตรงกับ anchor
                        # ที่ _compute_dr_metrics ใช้
                        if len(old_closes) >= 2:
                            price = old_closes[-1]
                            _pv   = old_closes[-2]
                            chg   = round((price - _pv) / _pv * 100, 2) if _pv else None
                        entry["price"] = round(price, 2)
                        entry["chg"]   = chg
                        if live_price is not None:
                            entry["live_price"] = round(live_price, 2)
                            entry["live_chg"]   = (round((live_price - price) / price * 100, 2)
                                                   if price else None)
                        else:
                            # เดิม pop() คีย์ทิ้ง → ขนาด dict เปลี่ยนขณะ GET /api/dr iterate
                            # s.items() อยู่ → RuntimeError: dictionary changed size -> 500
                            # เซ็ต None แทน (คีย์มีอยู่แล้วเสมอจาก _dr_do_rebuild)
                            entry["live_price"] = None
                            entry["live_chg"] = None
                        # metrics (returns/52W/ATH/YTD/RS/EMA/price+vol history/close100)
                        # มาจาก helper กลางเดียวกับ _dr_do_rebuild — กัน 2 path drift กัน
                        entry.update(_compute_dr_metrics(old_closes, old_dates, old_vols, cur_year))
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
            #
            # check-then-write ต้องอยู่ใต้ _dr_rebuild_lock เดียวกับ _dr_do_rebuild
            # (เขียน _dr_cache ใต้ lock นี้เสมอ ผ่าน _rebuild_dr_cache/_kick_dr_rebuild)
            # เดิมสองบรรทัดนี้ไม่ atomic ต่อกัน — background rebuild หรือ
            # /api/dr-full-refresh (ตั้ง ts=0) อาจ commit คั่นระหว่าง if กับ update()
            # แล้วผลสดถูก quick-update ทับเงียบๆ ยาว 4 ชม. (torn read ครึ่งท้ายที่
            # ยังเหลือจากรีวิวลำดับ 21)
            with _dr_rebuild_lock:
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

    try:
        threading.Thread(target=_do_quick, daemon=True).start()
    except Exception as e:
        # thread/resource exhaustion — _do_quick ไม่เคยรัน จึงไม่มี finally มา clear
        # running=True ที่ตั้งไว้ด้านบน → ทุก request ถัดไปติด {"status":"running"} ถาวร
        # จนกว่าจะ restart / job-reset
        _dr_refresh_state.update(running=False, error=str(e))
        return jsonify({"error": f"เริ่มงานอัปเดตไม่ได้: {e}"}), 500
    return jsonify({"status": "started"})


@app.route("/api/dr-quick-status")
def dr_quick_status():
    return jsonify(_dr_refresh_state)


@app.route("/api/dr-full-refresh", methods=["POST"])
def dr_full_refresh():
    """บังคับให้ /api/dr ถือว่า cache หมดอายุแล้วดึงใหม่รอบถัดไป — ตั้ง ts=0 แทนการ
    clear() ทั้งก้อน (เดิม clear() ทำให้ get_dr_data() แข่งกับ request ที่กำลังอ่าน
    _dr_cache["ts"] อยู่พอดี (TOCTOU) กลายเป็น KeyError -> 500 ดิบๆ ไม่มี JSON error
    body ให้ frontend อ่าน + ทำลาย _old_mc fallback ใน _dr_do_rebuild (อ่านจาก
    _dr_cache["result"]) ทำให้ market cap หายไปทั้งตัวแทนที่จะ fallback ใช้ค่าเก่า —
    เหมือนบั๊กที่แก้แล้วใน etf_full_refresh() ดู comment ที่นั่น"""
    if _dr_cache.get("result"):
        _dr_cache["ts"] = 0
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
        return 0, {}, tickers, False

    # last_dates อาจมี ticker ที่ถูกถอดจากดัชนีไปแล้วค้างอยู่ (ไม่อัพเดทต่อ) — ถ้าเอา
    # ไปหา min ทั้งก้อนจะยิ่งลากวันเริ่มดึง (start) ให้เก่าขึ้นเรื่อยๆ ทุกวันที่ผ่านไป
    # ต้อง filter เหลือเฉพาะ ticker ในดัชนีปัจจุบันก่อน (ตัวที่ถูกถอดไม่ต้องอัพเดทอยู่แล้ว)
    last_dates_all = store.get_last_dates(BASE_DIR)
    if not last_dates_all:
        # DB ยังว่าง (เครื่องใหม่/ยังไม่เคยดึง) — ถ้าปล่อยต่อ ทุกตัวจะกลายเป็น "หุ้นใหม่"
        # แล้ว Quick Update ประจำวันแอบกลายเป็น full backfill period='max' หลายร้อยตัว
        # (งานหนักที่ตั้งใจให้กดปุ่ม Index Max เองเท่านั้น)
        print(f"[{label}] {store.DB_FILE} ยังว่าง — ข้าม gap-update (กดปุ่ม Index Max ก่อน)")
        return 0, {}, tickers, False
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
        repaired = _repair_ca_tickers(BASE_DIR, data, suspects, progress_cb or (lambda *a: None),
                                      store=store)
        # ลบของเก่าทิ้งพร้อม insert ใหม่ในทรานแซกชันเดียว (ผ่าน upsert_bars ด้านล่าง)
        # แทนที่จะลบทันทีตรงนี้แล้วค่อย upsert ทีหลัง — เดิมถ้า upsert_bars ล้มเหลว
        # กลางทาง (ข้อมูลเสีย/exception) ราคาของ ticker เหล่านี้จะหายถาวรเพราะลบไปแล้ว
        replace_tickers = set(repaired)
        # ตัวที่ตรวจพบ CA แต่ refetch เต็มไม่สำเร็จ (Yahoo partial/history-floor/exception
        # หรือเกิน MAX_CA_REPAIR) — ห้าม upsert แท่ง overlap ฐานใหม่จาก fetch_gap_batch
        # ทับ archive ฐานเก่า (upsert_bars จะ INSERT OR REPLACE ทีละแท่ง ตัวที่ไม่อยู่ใน
        # replace_tickers ไม่ถูก DELETE ก่อน) ไม่งั้น series กลายเป็นฐานเก่าต่อฐานใหม่ +
        # รอบหน้า detector เทียบกับแท่งที่เพิ่งเขียนทับ (ฐานใหม่แล้ว) เลยไม่เจอ mismatch
        # อีก = seam ถาวรมองไม่เห็น — ข้ามรอบนี้ รอ retry รอบถัดไป (เหมือน run_quick_update/
        # run_with_progress ใน services/refresh.py)
        unrepaired = set(suspects) - repaired
        for t in unrepaired:
            data.pop(t, None)
        if unrepaired:
            print(f"[{label} CA] {len(unrepaired)} ตัวซ่อมไม่สำเร็จ — ข้ามการบันทึกรอบนี้ "
                  f"(รอรอบถัดไป retry): {sorted(unrepaired)}")

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

                def _prev_price(tick):
                    pc = data.get(tick, {}).get("close")
                    return float(pc.iloc[-1]) if pc is not None and len(pc) else None

                # โพรบก่อนยิงทั้ง batch: region_expected_trading_date ไม่รู้จักปฏิทินวันหยุด
                # ตลาด เลยคืน "วันนี้" (วันหยุดที่เป็นวันธรรมดา) ทำให้ทุก ticker ถูกมองว่า stale
                # ทั้งกระดาน จากนั้นเดิมยิง yf.Ticker().history(1m) ครบ ~500 req — ได้ราคาปิด
                # ของ session ก่อนวันหยุด (= ราคาปิดเดิมเป๊ะ chg~0) มาเขียนทับ live_price ทำให้
                # มุมมอง "👁 Live" โชว์ทั้งดัชนีที่ 0.00% เหมือนเป็นราคาสด ทั้งที่วันนั้นไม่มีเทรด
                # ถ้าหุ้นตัวอย่างชุดแรกได้ราคา intraday = ราคาปิดเดิมทั้งหมด = วันหยุด ข้ามทั้ง batch
                # (เคส Yahoo ค้าง Close ของ "วันเทรดจริง" — เช่น JP 28 ก.ค. 2026 — จะมีราคาขยับ
                # จาก session จริง โพรบไม่ผ่านเงื่อนไข flat ก็ยิงต่อทั้ง batch ตามเดิม)
                probe = stale[:8]
                probe_res = {}
                with ThreadPoolExecutor(max_workers=8) as ex:
                    for f in as_completed([ex.submit(_intraday_last_close, t) for t in probe]):
                        tick, lp = f.result()
                        if lp is not None:
                            probe_res[tick] = lp

                def _is_flat(tick, lp):
                    pp = _prev_price(tick)
                    return pp is not None and pp > 0 and abs(lp - pp) / pp < 0.0005

                if len(probe_res) >= 4 and all(_is_flat(t, lp) for t, lp in probe_res.items()):
                    print(f"[{label}] intraday โพรบ {len(probe_res)} ตัวได้ราคา = ราคาปิดเดิมทั้งหมด "
                          f"(chg~0) — วันนี้เป็นวันหยุดตลาด ข้าม intraday fallback ทั้ง batch")
                    stale = []

            if stale:
                fb_map = {}
                with ThreadPoolExecutor(max_workers=8) as ex:
                    futures = [ex.submit(_intraday_last_close, t) for t in stale]
                    for f in as_completed(futures):
                        tick, live_price = f.result()
                        if live_price is None:
                            continue
                        prev_price = _prev_price(tick)
                        if prev_price:
                            fb_map[tick] = {
                                "live_price": round(live_price, 4),
                                "live_chg": round((live_price - prev_price) / prev_price * 100, 2),
                            }
                # กันเคสที่โพรบข้างบนหลุด (ได้ผลโพรบ <4 ตัวเพราะ Yahoo 1m ก็ไม่ให้ข้อมูล) แล้วยิง
                # ทั้ง batch บนวันหยุด — ถ้าผลรวมแทบทุกตัว chg~0 = re-read session เดิม ทิ้งทั้งชุด
                if fb_map:
                    flat = sum(1 for v in fb_map.values() if abs(v["live_chg"]) < 0.05)
                    if len(fb_map) >= 10 and flat >= len(fb_map) * 0.9:
                        print(f"[{label}] intraday fallback: {flat}/{len(fb_map)} ตัว chg~0 "
                              f"— วันหยุดตลาด ทิ้งผล fallback ทั้งชุด")
                    else:
                        live_map.update(fb_map)

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
    try:
        return jsonify(us_index_metrics.load_local(BASE_DIR))
    except Exception as e:
        print(f"[USIndexMetrics] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/us-sector-ranks")
def us_sector_ranks():
    """จัดอันดับ Sector (GICS) ของสมาชิกดัชนี US ตาม RS/return เฉลี่ย — reuse
    core.metrics.summarize_groups() ตัวเดียวกับหน้า "Sectors" ของหุ้นไทย (ห้ามเขียนสูตรซ้ำ)
    query param index=SP500|DOW|NDX (default SP500)"""
    from sources import us_index_metrics
    from core.metrics import summarize_groups
    idx = (request.args.get("index") or "SP500").upper()
    flag = {"SP500": "in_sp500", "DOW": "in_dow", "NDX": "in_ndx"}.get(idx, "in_sp500")
    try:
        stocks = [s for s in us_index_metrics.load_local(BASE_DIR).get("stocks", [])
                  if isinstance(s, dict) and s.get(flag)]
        return jsonify({"sectors": summarize_groups(stocks, "sector")})
    except Exception as e:
        print(f"[USSectorRanks] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


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
    try:
        return jsonify(hk_index_metrics.load_local(BASE_DIR))
    except Exception as e:
        print(f"[HKIndexMetrics] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


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
    try:
        stocks = [s for s in hk_index_metrics.load_local(BASE_DIR).get("stocks", [])
                  if isinstance(s, dict) and s.get(flag)]
        return jsonify({"sectors": summarize_groups(stocks, "sector")})
    except Exception as e:
        print(f"[HKSectorRanks] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


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
    try:
        return jsonify(jp_index_metrics.load_local(BASE_DIR))
    except Exception as e:
        print(f"[JPIndexMetrics] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/jp-sector-ranks")
def jp_sector_ranks():
    """จัดอันดับ Sector ของสมาชิก Nikkei 225 ตาม RS/return เฉลี่ย — reuse
    core.metrics.summarize_groups() ตัวเดียวกับหน้า Sectors ของหุ้นไทย/US/HK (ห้ามเขียนสูตรซ้ำ)
    ไม่มี query param ?index= เหมือน US/HK เพราะ JP มีดัชนีเดียว (Nikkei 225)"""
    from sources import jp_index_metrics
    from core.metrics import summarize_groups
    try:
        stocks = [s for s in jp_index_metrics.load_local(BASE_DIR).get("stocks", [])
                  if isinstance(s, dict) and s.get("in_nikkei225")]
        return jsonify({"sectors": summarize_groups(stocks, "sector")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        body = request.json
        syms = body.get("symbols") or [] if isinstance(body, dict) else []
    syms = [_hedge_norm(s) for s in syms if isinstance(s, str) and s]
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

    # fallback: fetch จาก yfinance โดยตรง — ต้องมี session timeout เหมือนทุกจุดอื่นใน DR
    # pipeline (ดู _dr_do_rebuild) ไม่งั้น Yahoo ค้าง socket = thread ที่รับ request นี้
    # ค้างไม่มีกำหนด (เกิดง่ายเพราะ path นี้ทำงานพอดีตอน _dr_cache ว่าง/ไม่มี symbol —
    # เช่นหลัง dr_full_refresh() หรือก่อน rebuild ครั้งแรกของเครื่อง)
    yf_ticker = dr_entry["yf"]
    try:
        from sources.yahoo import _TimeoutSession
        t = yf.Ticker(yf_ticker, session=_TimeoutSession())
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
    # is_dr ต้องระบุชัดเจนจาก caller (pattern เดียวกับ /api/financials-full, /api/financials-qpl-report
    # ฯลฯ) — เดิมค้นใน DR universe ก่อนเสมอโดยไม่มีทางบอกเจตนา ทำให้ symbol ที่ชนกันระหว่างหุ้นไทย
    # กับ DR (เช่น 'META' มีทั้ง DR ของ Meta Platforms และหุ้นไทย mai 'META Corporation') resolve
    # เป็น DR เสมอ ผิดตัวแบบเงียบๆ แล้วแคชค้าง 24h (ดู _dr_key()/financials_store.get() ที่แก้
    # ปัญหานี้ไปแล้วสำหรับ endpoint อื่น)
    is_dr = request.args.get("is_dr") == "1"
    cache_key = f"{sym}:{'dr' if is_dr else 'set'}"

    cached = _fin_cache.get(cache_key)
    if cached and (time.time() - cached["ts"] < _FIN_CACHE_TTL):
        return jsonify(cached["data"])

    if is_dr:
        dr_entry = next((s for s in load_dr_universe(BASE_DIR) if s["sym"] == sym), None)
        if not dr_entry:
            return jsonify({"error": f"ไม่พบ {sym} ใน DR universe"}), 404
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
                # ต้องเทียบ TTM กับ 'ปีล่าสุด' ไม่ใช่ปีที่ยอดสูงสุด — บริษัทที่รายได้หด >95%
                # จากจุดพีคหลายปีก่อน จะโดน max(key=abs) หยิบยอดพีคมาเป็นตัวหาร ratio ต่ำเกิน
                # 0.05 แล้วล้าง TTM ที่ถูกต้องทิ้ง (ดู ttm_bad ด้านล่าง)
                items = [(d, v) for d, v in row.items() if v is not None]
                if items:
                    ann_rev = max(items, key=lambda x: str(x[0]))[1]
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
        _fin_cache[cache_key] = {"ts": time.time(), "data": data}
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
        # financials_store.get() เปิด connection ใหม่ (busy_timeout=5000) — ถ้า background
        # job (sync-all/mirror) กำลังเขียน financials.db นานเกิน 5s พอดี อาจได้
        # sqlite3.OperationalError: database is locked (pattern เดียวกับ /api/financials-qpl-report)
        try:
            q = financials_store.get(BASE_DIR, sym, "finnomena_q", is_dr=is_dr)
        except Exception:
            q = None
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
        # _finnomena_annual_status อ่าน yahoo/yahoo_q จาก DB เพิ่ม (unguarded get) + คณิต
        # median-diff แบบหาร — DB lock หรือค่า non-numeric ใน payload เก่าทำให้ throw ได้
        # ไม่ควรทำให้ทั้ง endpoint 500 ทั้งที่ annual (ตัวหลัก) พร้อมส่งแล้ว
        try:
            status, med = _finnomena_annual_status(annual, sym, is_dr)
        except Exception:
            status, med = "unverified", None
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

    try:
        data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr, market=market)
    except Exception:
        data = None
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
    try:
        data = financials_store.get(BASE_DIR, sym, source, is_dr=is_dr, market=market)
    except Exception:
        data = None
    if not data:
        # re-read หลัง upsert ล้มเหลว/ได้ค่าว่าง (เช่น DB lock แทรกทันทีหลังเราเพิ่งเขียนเอง) —
        # อย่าคืน body `null` HTTP 200 (frontend d.income/d.name -> TypeError หน้าเว็บค้าง)
        return jsonify({"error": f"{sym}: บันทึกงบการเงินแล้วแต่อ่านกลับไม่สำเร็จ ลองใหม่อีกครั้ง"}), 503
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
    # สกุลเงินของงบ — DR US/HK/JP งบเป็น USD/HKD/JPY (compute_qpl_report ไม่แปลงค่าเงิน)
    # frontend ใช้ตั้ง label หน่วย ไม่ให้เขียน "พันบาท" ทับตัวเลขสกุลอื่น
    report["currency"] = (yq or finn or {}).get("currency") or "—"
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
    # สกุลเงินของงบ — DR US/HK/JP งบเป็น USD/HKD/JPY (compute_full_report ไม่แปลงค่าเงิน)
    # frontend ใช้ตั้ง label หน่วย ไม่ให้เขียน "พันบาท"/"ล้านบาท" ทับตัวเลขสกุลอื่น
    report["currency"] = (yq or finn or {}).get("currency") or "—"
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
    # financials_store.get() เปิด connection ใหม่ (busy_timeout=5000) — ถ้า background job
    # (sync-all/mirror) กำลังเขียน financials.db นานเกิน 5s พอดี อาจได้ sqlite3.OperationalError:
    # database is locked ต้องกันไว้ไม่ให้ endpoint นี้ 500 (pattern เดียวกับ /api/financials-qpl-report)
    try:
        finn = financials_store.get(BASE_DIR, sym, "finnomena_q")
    except Exception:
        finn = None
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q")
        except Exception:
            finn = None

    # Yahoo รายไตรมาส — ครบทุกบรรทัดตรงตัวแต่สั้นแค่ไม่กี่ไตรมาสล่าสุด ใช้เสริมรายละเอียด
    # COGS/SG&A แยก/ต้นทุนการเงิน/ภาษี ให้ช่วงที่ SET detail ยังไปไม่ถึง
    try:
        yq = financials_store.get(BASE_DIR, sym, "yahoo_q")
    except Exception:
        yq = None
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

    try:
        quarters = financials_store.compute_qpl_report(finn, yq, set_series=set_series)["quarters"]
    except Exception:
        quarters = None
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
    try:
        hl = financials_store.get(BASE_DIR, sym, "set")
    except Exception:
        hl = None
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
    hk_prices.db, JP จาก jp_prices.db (เฉพาะสมาชิกดัชนีหลักที่มีราคาโหลดไว้แล้ว) DR ยังไม่รองรับ
    (ต้องมี price series ในเครื่องของ underlying ก่อน ยังไม่มี local store แยกให้)"""
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
    elif mkt == "JP":
        from core import jp_store
        # jp_prices.db เก็บ ticker แบบมี suffix ".T" ตรงกับ jp_index_membership.json (ตาราง
        # dividends เก็บรหัสดิบไม่มี suffix เหมือน HK — ดู resolve_yf_ticker ใน dr_descriptions.py)
        data = jp_store.get_ohlc_series(BASE_DIR, sym + ".T")
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
        elif mkt in ("US", "HK", "JP"):
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
    try:
        events = financials_store.get_all_calendar_events(BASE_DIR, from_date=from_date, market=market)
    except Exception as e:
        # เช่น sqlite locked ชั่วคราวตอนหุ้นอื่นกำลัง sync ปฏิทินพร้อมกัน — คืน events ว่างแทน 500
        # ทั้งหน้า (ดู get_calendar_events_endpoint ด้านล่างที่กันเคสเดียวกันต่อหุ้น)
        return jsonify({"events": [], "error": str(e)})
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

    try:
        rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=lookback_iso)
    except Exception:
        # DB lock ชั่วคราว (เช่น watchlist ยิง Promise.all พร้อมกันหลายหุ้น) — ถือว่ายังไม่มีข้อมูล
        # แล้วลองดึงสดด้านล่างแทน 500
        rows, synced_at = None, None
    stale = True
    if synced_at:
        try:
            stale = (_dt.now() - _dt.fromisoformat(synced_at)) > _td(days=_CALENDAR_STALE_DAYS)
        except (ValueError, TypeError):
            stale = True

    fetch_error = None
    if force or rows is None or stale:
        try:
            fresh = financials_store.fetch_calendar_events(sym, market=mkt)
            financials_store.save_calendar_events(BASE_DIR, sym, mkt, fresh)
        except Exception as e:
            fetch_error = str(e)
        else:
            # เขียนสำเร็จแล้ว ณ จุดนี้ — ไม่รายงาน fetch_error อีกต่อไปแม้ re-read ด้านล่างจะพลาด
            # (เดิมถ้า re-read ล้มเหลว จะกระโดดเข้า except ด้านนอกและรายงาน fetch_error ทั้งที่
            # ข้อมูลถูกเขียนลง DB สำเร็จแล้วจริง — ทำให้ผู้ใช้เข้าใจผิดว่า sync ล้มเหลว)
            stale = False
            try:
                rows, synced_at = financials_store.get_calendar_events(BASE_DIR, sym, mkt, from_date=lookback_iso)
            except Exception:
                pass   # เก็บ rows/synced_at เดิมไว้ก่อน (ดีกว่าไม่มีอะไรเลย) — เขียนลง DB สำเร็จแล้ว

    return jsonify({
        "symbol": sym, "market": mkt, "synced_at": synced_at, "stale": stale,
        "fetch_error": fetch_error, "events": rows or [],
    })


_set_val_check_cache: dict = {}
_SET_VAL_CHECK_TTL = 3600   # 1 ชม.


@app.route("/api/set-valuation-check/<symbol>")
def set_valuation_check(symbol):
    """PE/PBV ทางการจาก SET.or.th ใช้เป็นเส้นตรวจสอบ Valuation Band (ดู
    PLAN_set_api_expansion.txt งาน #4) ไม่ใช่แทนที่ pipeline หลัก (mrlikestock.com ที่ให้
    ประวัติหลายปี) — cache ต่อ symbol 1 ชม.

    งาน #4B: ทุกครั้งที่ยิงสด (~6 เดือนล่าสุด) จะ upsert สะสมลง set_daily_valuation ด้วย
    (ไม่ลบของเก่า) แล้วคำนวณสถิติ avg/min/max จาก "ประวัติสะสม" ที่อาจยาวกว่า 6 เดือน —
    ตัว current/date_to ยังอิงชุดสดล่าสุดเสมอ"""
    sym = symbol.upper().strip()
    cached = _set_val_check_cache.get(sym)
    if cached and (time.time() - cached["ts"] < _SET_VAL_CHECK_TTL):
        return jsonify(cached["data"])
    try:
        from sources.set_api import fetch_historical_trading
        rows = fetch_historical_trading(sym)
        if not rows:
            return jsonify({"error": f"ไม่มีข้อมูลราคา/PE/PBV ทางการของ {sym}"}), 404
        # สะสมลง DB (opportunistic — โตทุกครั้งที่มีคนเปิด Band ของหุ้นตัวนี้) ไม่ให้ error
        # การเขียนทำให้ response พัง — เป็นแค่ผลพลอยได้
        try:
            set_daily_val.upsert_rows(BASE_DIR, sym, rows)
        except Exception as e:
            # log ไว้ (ไม่ใช่ pass เงียบ) — ถ้าเขียนล้มเหลว acc ด้านล่างจะยังเป็นชุดเก่าที่
            # ไม่รวมวันนี้ ทั้งที่ date_to ของ response ยังอิง rows (ชุดสด) อยู่ดี ถ้าไม่ log
            # ไว้จะไม่มีร่องรอยว่าทำไม avg/min/max ถึงไม่ขยับตามที่คาด (code review 2026-08-27)
            logging.warning(f"[set-valuation-check] upsert_rows {sym} ล้มเหลว ({e}) — สถิติรอบนี้ใช้ชุดเก่า")
        acc = set_daily_val.get_series(BASE_DIR, sym)
        # ใช้ชุดสะสมถ้ายาวกว่าชุดสด (ปกติจะยาวกว่าเสมอหลัง sync ไปหลายรอบ) — fallback ชุดสด
        stat_rows = acc if len(acc) >= len(rows) else rows
        pes = [r["pe"] for r in stat_rows if r.get("pe") is not None]
        pbvs = [r["pbv"] for r in stat_rows if r.get("pbv") is not None]
        latest = rows[-1]
        accumulated = None
        if acc and len(acc) > len(rows):
            accumulated = {"date_from": acc[0]["date"], "date_to": acc[-1]["date"],
                           "count_days": len(acc)}
        result = {
            "symbol": sym, "date_from": stat_rows[0]["date"], "date_to": rows[-1]["date"],
            "count_days": len(stat_rows), "live_count_days": len(rows),
            "accumulated": accumulated,
            "pe": {"current": latest.get("pe"), "avg": round(sum(pes) / len(pes), 2) if pes else None,
                   "min": round(min(pes), 2) if pes else None, "max": round(max(pes), 2) if pes else None},
            "pbv": {"current": latest.get("pbv"), "avg": round(sum(pbvs) / len(pbvs), 2) if pbvs else None,
                    "min": round(min(pbvs), 2) if pbvs else None, "max": round(max(pbvs), 2) if pbvs else None},
        }
        _set_val_check_cache[sym] = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_set_daily_val_sync_running = {"on": False}
# ล็อกคุม _set_daily_val_sync_running แบบเดียวกับ _dr_refresh_lock — เดิมเช็ค flag แบบ
# ไม่ล็อกในเธรด request แล้วค่อยตั้ง True *ข้างใน* เธรดที่ spawn ไปแล้ว (TOCTOU: 2 คลิกรัว
# ๆ ผ่าน guard ได้ทั้งคู่ก่อนเธรดไหนทันตั้ง flag) ตั้ง flag ให้ atomic ใต้ lock นี้ในเธรด
# request เองก่อน spawn เสมอ (code review 2026-08-27) — ชั้นป้องกันจริงที่กันข้อมูลพัง
# คือ _sync_lock ใน sources/set_daily_val.py::sync_all() เอง lock นี้แค่ให้ตอบ 409 ที่
# ตรงไปตรงมาแทนการ silent no-op
_set_daily_val_sync_lock = threading.Lock()


@app.route("/api/set-daily-val/sync", methods=["POST"])
def set_daily_val_sync():
    """สะสม PE/PBV/BVPS/DivYield รายวันทางการจาก SET.or.th ทั้งกระดานลง financials.db
    (งาน #4B) — ปุ่มมือในหน้า Data Health · ปกติ run_quick_update() ยิงให้อัตโนมัติ
    (staleness gate: ข้ามตัวที่สะสมถึงวันล่าสุดแล้ว) ตัวนี้บังคับ sync รอบเต็ม

    รันใน background thread (ทั้งกระดาน ~1-2 นาที) แล้วคืนทันที — ดูผลที่ item
    'PE/PBV รายวันทางการ' ในหน้านี้รอบถัดไป (บันทึกลง run_log ด้วย)"""
    with _set_daily_val_sync_lock:
        if _set_daily_val_sync_running["on"]:
            return jsonify({"ok": False, "error": "กำลัง sync อยู่แล้ว — รอรอบนี้เสร็จก่อน"}), 409
        _set_daily_val_sync_running["on"] = True
    # force=True: ปุ่มมือ "บังคับ sync รอบเต็ม" ใน Data Health ต้องส่ง {force:true} มาจริง
    # (dashboard.js::startSetDailyValSync) — เดิมฝั่ง JS ไม่ส่ง body เลย ทำให้ force เป็น
    # None เสมอ ปุ่มที่ข้อความบอกว่า "บังคับรอบเต็ม" จริงๆ แล้วไม่ต่างจากรอ staleness gate
    # อัตโนมัติเลย (code review 2026-08-27)
    force = bool((request.get_json(silent=True) or {}).get("force"))

    def _job():
        try:
            n, total, rows = set_daily_val.sync_all(BASE_DIR, skip_up_to_date=not force)
            run_log.record_run(BASE_DIR, "set_daily_val_sync", True,
                               f"สะสม PE/PBV รายวัน {n}/{total} ตัว (+{rows} แถว)")
        except Exception as e:
            run_log.record_run(BASE_DIR, "set_daily_val_sync", False, str(e)[:200])
        finally:
            with _set_daily_val_sync_lock:
                _set_daily_val_sync_running["on"] = False

    threading.Thread(target=_job, daemon=True).start()
    return jsonify({"ok": True, "started": True})


@app.route("/api/financial-health/<symbol>")
def financial_health(symbol):
    """SET Financial Health Check ของหุ้น 1 ตัว (ดู PLAN_set_api_expansion.txt งาน #5) — **DB-first
    แล้ว** (source='set_health' ใน financials.db, ดู financials_store.get_set_health/
    sync_set_health) แทนที่ live-fetch + cache 24 ชม. ในหน่วยความจำแบบเดิม (หายตอน restart) — ใช้
    pattern เดียวกับ set_qpl เป๊ะ: อ่าน DB ก่อนเสมอ, ยิงสดเฉพาะตอน DB ว่างหรือ ?refresh=1
    ใช้กับแท็บ "🩺 สุขภาพการเงิน (SET)" ในเมนูงบการเงิน + การ์ดย่อใน Tearsheet"""
    sym = symbol.upper().strip()
    refresh = request.args.get("refresh") == "1"
    try:
        import urllib.error
        data = None if refresh else financials_store.get_set_health(BASE_DIR, sym)
        if not data:
            data = financials_store.sync_set_health(BASE_DIR, sym)
        if not data or not data.get("themes"):
            return jsonify({"error": f"ไม่มีข้อมูล Financial Health ของ {sym}"}), 404
        return jsonify(data)
    except urllib.error.HTTPError as e:
        # SET.or.th คืน 404 สำหรับหุ้นกลุ่มการเงิน (ธนาคาร/เงินทุน/ประกัน) — Financial
        # Health Check ไม่ครอบกลุ่มนี้ (เหมือน Z-Score ที่ยกเว้นกลุ่มเดียวกัน สูตรไม่ valid
        # กับงบดุลธนาคาร) ไม่ใช่ error จริง — คืน 404 พร้อมเหตุผลชัดๆ แทน 500 แต่ก่อนคืน error
        # เช็คของสะสมเก่าใน DB ก่อน (sync ล้มเหลวรอบนี้ไม่ควรทำของเก่าที่เคยดึงได้หายไป)
        if e.code == 404:
            cached = financials_store.get_set_health(BASE_DIR, sym)
            if cached and cached.get("themes"):
                return jsonify(cached)
            return jsonify({"error": f"ไม่มีข้อมูล Financial Health ของ {sym} "
                                      "(SET.or.th ไม่ครอบคลุมกลุ่มธนาคาร/เงินทุน/ประกัน)"}), 404
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/financial-factsheet/<symbol>")
def financial_factsheet(symbol):
    """SET.or.th factsheet ของหุ้น 1 ตัว (source='set_factsheet' ใน financials.db) — รวม 5
    sub-endpoint: วงจรเงินสด/อัตราส่วนการเงิน/อัตราการเติบโต/สถิติซื้อขายรายปี/ผลตอบแทนเทียบ
    sector-market เจอ endpoint ตระกูลนี้ตอนตรวจสอบตัวเลข "วงจรเงินสด" ที่หน้า factsheet จริงของ
    SET.or.th โชว์ (ดักฟัง network ด้วย Playwright — ตรงกับ /financial-health เดิมไม่ครบ)
    DB-first เหมือน /api/financial-health ใช้กับแท็บ "📄 Factsheet (SET)" ในเมนูงบการเงิน

    key 'มีข้อมูลไหม' ต้องเช็คแบบ any ไม่ใช่ key เดียว — cash_cycle เป็น None ปกติสำหรับหุ้น
    กลุ่มการเงิน/REIT (endpoint นั้น 404 เสมอ เหมือน financial-health) แต่อีก 4 key ยังมีค่า"""
    sym = symbol.upper().strip()
    refresh = request.args.get("refresh") == "1"
    keys = financials_store._FACTSHEET_KEYS
    try:
        import urllib.error
        data = None if refresh else financials_store.get_set_factsheet(BASE_DIR, sym)
        if not data:
            data = financials_store.sync_set_factsheet(BASE_DIR, sym)
        if not data or not any(data.get(k) for k in keys):
            return jsonify({"error": f"ไม่มีข้อมูล Factsheet ของ {sym}"}), 404
        return jsonify(data)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            cached = financials_store.get_set_factsheet(BASE_DIR, sym)
            if cached and any(cached.get(k) for k in keys):
                return jsonify(cached)
            return jsonify({"error": f"ไม่มีข้อมูล Factsheet ของ {sym}"}), 404
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/financial-cashflow-balance/<symbol>")
def financial_cashflow_balance(symbol):
    """กระแสเงินสด (OCF/CFI/CFF/CapEx) + งบดุลย่อ (เงินสด/ลูกหนี้/สต็อก/หนี้สินรวม/ส่วนผู้ถือหุ้น)
    รายไตรมาสจาก SET official (source 'set_cashflow'/'set_balance') — DB-first เหมือน
    /api/financial-health ด้านบน ใช้กับแท็บ "💵 กระแสเงินสด & งบดุล (SET)" ในเมนูงบการเงิน

    ทั้งสอง source คีย์ (year,q) แบบเดียวกับ set_qpl (จาก endDate จริง) จึง merge เป็นแถวเดียวต่อ
    ไตรมาสได้ตรงๆ — balance_sheet มักมีจุดข้อมูลมากกว่า cash_flow (ไม่ต้องรอคู่ subtract) ดังนั้น
    บางไตรมาสอาจมีแค่ฝั่งงบดุลไม่มีกระแสเงินสด (โชว์ '—' ที่ frontend ไม่ใช่ error)"""
    sym = symbol.upper().strip()
    refresh = request.args.get("refresh") == "1"
    try:
        cf = None if refresh else financials_store.get_set_cashflow_series(BASE_DIR, sym)
        if cf is None:
            cf = financials_store.sync_set_cashflow_series(BASE_DIR, sym)
        bs = None if refresh else financials_store.get_set_balance_series(BASE_DIR, sym)
        if bs is None:
            bs = financials_store.sync_set_balance_series(BASE_DIR, sym)
        if not cf and not bs:
            return jsonify({"error": f"ไม่มีข้อมูลกระแสเงินสด/งบดุลของ {sym} "
                                      "(SET.or.th ไม่ครอบคลุมกลุ่มธนาคาร/เงินทุน/ประกันสำหรับบางบัญชี)"}), 404
        # เรียงเก่า->ใหม่ (ซ้าย->ขวา) ให้ตรงกับธรรมเนียมตาราง SET Financial Health/P&L รายไตรมาส
        # เดิมในเมนูนี้ (periods ของ fetch_financial_health ก็มาเรียงแบบนี้เหมือนกัน)
        quarters = sorted(set(cf.keys()) | set(bs.keys()))
        rows = []
        for key in quarters:
            row = {"quarter": f"{key[0]}-{key[1]}"}
            row.update(cf.get(key, {}))
            row.update(bs.get(key, {}))
            rows.append(row)
        return jsonify({"symbol": sym, "quarters": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_index_info_cache: dict = {}
_INDEX_INFO_TTL = 300   # 5 นาที — real-time แต่ไม่ต้อง sub-minute


@app.route("/api/index-info-list")
def index_info_list():
    """ดัชนีทั้งหมด real-time จาก SET.or.th รวมตระกูล Free-Float adjusted (SET50FF
    ฯลฯ) ที่ TradingView (indices_cache.json) ไม่มี — ใช้เป็นการ์ดเสริมในหน้า
    ดัชนีกลุ่ม (ดู PLAN_set_api_expansion.txt งาน #7) ไม่มีประวัติย้อนหลัง"""
    cached = _index_info_cache.get("result")
    if cached and (time.time() - _index_info_cache.get("ts", 0) < _INDEX_INFO_TTL):
        return jsonify(cached)
    try:
        from sources.set_api import fetch_index_info_list
        items = fetch_index_info_list()
        result = {"ok": True, "items": items}
        _index_info_cache["result"] = result
        _index_info_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


_ipo_list_cache: dict = {}
_IPO_LIST_TTL = 1800   # 30 นาที — IPO list เปลี่ยนไม่บ่อย (ต่างจาก event ปฏิทินอื่น)


@app.route("/api/ipo-list")
def ipo_list():
    """หุ้น IPO ที่กำลังจะเข้า + เพิ่งเข้าตลาด จาก SET.or.th (ดึงสดทุกครั้งที่แคชหมดอายุ
    ไม่เก็บสะสมเหมือน calendar_events — ดู PLAN_set_api_expansion.txt งาน #6) ใช้กับ
    scope 🆕 IPO ในหน้าปฏิทิน"""
    cached = _ipo_list_cache.get("result")
    if cached and (time.time() - _ipo_list_cache.get("ts", 0) < _IPO_LIST_TTL):
        return jsonify(cached)
    try:
        from sources.set_api import fetch_ipo_upcoming, fetch_ipo_recently
        upcoming = fetch_ipo_upcoming()
        recently = fetch_ipo_recently()
        result = {"ok": True, "as_of_date": upcoming["as_of_date"],
                  "upcoming": upcoming["items"], "recently": recently}
        _ipo_list_cache["result"] = result
        _ipo_list_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/financials-meta")
def financials_meta():
    """วันที่ sync งบการเงินเต็มล่าสุด — ใช้เช็คฝั่ง UI ว่าถึงรอบเตือนอัพเดท (~2 เดือน) หรือยัง"""
    try:
        return jsonify(financials_store.get_meta_summary(BASE_DIR))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
_fin_universe_lock = threading.Lock()


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
        # ล็อคกัน check-then-act race — ไม่งั้น 2 request แข่งกันเรียก
        # _financials_universe_uncached() ซ้อนกัน ซึ่งข้างในเรียก record_delisted_bulk()
        # ที่อ่าน+เขียน delisted_log.json ทั้งไฟล์ (read-modify-write ไม่มี lock ของตัวเอง)
        # แข่งกันเขียนทับกันได้
        with _fin_universe_lock:
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
    try:
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
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/financials-quarter-coverage")
def financials_quarter_coverage():
    """เทียบ universe กับ 'ไตรมาสล่าสุดที่ควรจะมีข้อมูลแล้ว ณ วันนี้' (_target_period) ต่างจาก
    /api/financials-coverage ที่เช็คแค่ 'มีข้อมูลหรือยัง' (เก่าแค่ไหนก็นับว่ามี) ตัวนี้เช็ค
    'ข้อมูลที่มีเป็นงวดล่าสุดจริงหรือยัง' — ใช้ตอบคำถาม 'ไตรมาสล่าสุดยังขาดกี่ตัว'
    ?universe=dr เช็คเฉพาะหุ้นต่างประเทศ (DR/DRx, ไม่มี set/set_qpl เพราะ SET.or.th ไม่มีข้อมูลหุ้นต่างประเทศ)"""
    try:
        if request.args.get("universe") == "dr":
            symbols = _dr_financials_universe()
            coverage = financials_store.get_quarter_coverage(
                BASE_DIR, symbols, sources=("yahoo_q", "finnomena_q"), is_dr=True)
        else:
            symbols = _financials_universe()
            coverage = financials_store.get_quarter_coverage(
                BASE_DIR, symbols, sources=("set", "set_qpl", "yahoo_q", "finnomena_q"), is_dr=False)
        return jsonify(coverage)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/financials-quarter-coverage-by-index")
def financials_quarter_coverage_by_index():
    """เหมือน /api/financials-quarter-coverage?universe=dr แต่แจกแจงแยกตามดัชนีหลัก
    (SP500/DOW/NDX/HSI/HSCEI/HSTECH/NIKKEI225) เทียบกับ 'สมาชิกทั้งหมด' ของแต่ละดัชนีจริง
    (ไม่ใช่แค่ subset ในพอร์ต DR) — แต่ละ source (yahoo_q/finnomena_q) แบ่ง fresh/stale/
    not_tracked แยกกัน เพราะ yahoo_q มีแค่ตัวในพอร์ต DR เท่านั้น ส่วน finnomena_q ครอบคลุม
    กว้างกว่านั้น (เช็ค 'FINN:{ex}:' namespace จากปุ่ม 'Mirror ทั้งตลาด' เพิ่มด้วย)
    ดู docstring _index_group_quarter_coverage"""
    try:
        return jsonify(_index_group_quarter_coverage())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _warmup_fin_dependent_caches(mirror_universes=()):
    """เรียกซ้ำ endpoint ที่พึ่ง _fin_analytics_cache/_sector_compare_cache/_market_trend_cache
    ทันทีหลัง sync เคลียร์ cache พวกนี้ทิ้ง (แทนที่จะปล่อยให้ user คนแรกที่เปิดหน้าเจอ cold
    path เอง ~20-30 วิ) เรียกจาก thread เดิมของ sync ได้เลยเพราะ sync ก็รันใน background
    thread อยู่แล้ว — ไม่บล็อก request อื่น

    mirror_universes: ('dr'|'us'|'hk'|'jp', ...) — งานที่เคลียร์ _qpl_mirror_caches ด้วย
    (DR sync / index sync US/HK/JP / full refresh) ต้องระบุมา ไม่งั้น growth screener เวอร์ชัน
    ต่างประเทศจะค้าง cold ให้ user คนแรกเจอ _compute_qpl_mirror_parsed เต็ม ๆ (~15-60 วิ/ตลาด)"""
    tc = app.test_client()
    eps = ["/api/financials-analytics", "/api/sector-compare", "/api/market-trend",
           "/api/quarterly-growth-screener"]
    eps += [f"/api/quarterly-growth-screener?universe={u}" for u in mirror_universes]
    for ep in eps:
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
    _clear_qpl_mirror_caches()   # งานกลุ่มนี้ sync finnomena_q/yahoo_q ของ mirror US/HK/JP
    # อุ่น growth screener เวอร์ชัน us/hk/jp ด้วย (เพิ่ง _clear_qpl_mirror_caches ไป) — daemon thread
    # อยู่แล้ว ไม่บล็อกใคร · dr ไม่รวมเพราะงานกลุ่มนี้ไม่แตะ DR: keys (จะ recompute ตอนมีคนเปิดเอง)
    threading.Thread(target=lambda: _warmup_fin_dependent_caches(mirror_universes=("us", "hk", "jp")),
                     daemon=True).start()


def _quarterly_bake_universe():
    """หุ้นไทยที่เว็บมือถือแสดงจริง = symbol ใน data/set_data.json (ตรงกับที่ bake ไป
    แล้ว) — fallback ไป _financials_universe() ถ้าอ่านไฟล์ไม่ได้"""
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            return [s["symbol"] for s in json.load(f).get("stocks", [])]
    except Exception:
        return _financials_universe()


def _bake_financials_quarterly_file():
    """re-bake data/financials_quarterly.json (งบ P&L รายไตรมาส SET.or.th ให้เวอร์ชันมือถือ)
    เรียกท้ายงาน sync งบไทยทุกจุด (ปุ่ม Data Health / CLI) — set_qpl เพิ่งเปลี่ยน
    non-fatal: ล้มก็ปล่อย (ไฟล์เดิมยังอยู่) ไม่ให้กระทบงาน sync ที่สำเร็จแล้ว

    guard: ไฟล์นี้ git-tracked + push ขึ้นเว็บมือถือ และ set_qpl รายไตรมาสเก่าดึงย้อนหลัง
    ไม่ได้ — ถ้า bake ได้ 0 หุ้น หรือหด <80% ของไฟล์เดิม (financials.db restore จาก backup
    เก่าก่อนมี set_qpl / set_data.json ว่างชั่วคราว) ห้ามเขียนทับ (เหมือน core.store._check_stock_count)
    เขียนแบบ atomic (.tmp + os.replace) กันไฟล์ค้างสภาพ truncate ถ้า process ตายกลาง json.dump"""
    path = os.path.join(BASE_DIR, "data", "financials_quarterly.json")
    try:
        fq = financials_store.bake_quarterly_pl(BASE_DIR, _quarterly_bake_universe())
        new_count = len(fq.get("set", {}))
        old_count = 0
        try:
            with open(path, encoding="utf-8") as f:
                old_count = len(json.load(f).get("set", {}))
        except Exception:
            old_count = 0
        if new_count == 0 or (old_count > 50 and new_count < old_count * 0.8):
            print(f"[FinancialsQuarterly] ข้าม bake — ได้ {new_count} หุ้น (เดิม {old_count}) "
                  f"ผิดปกติ ไม่เขียนทับไฟล์สะสม")
            return
        os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
        tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(fq, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)
        _fin_quarterly_cache.clear()
        print(f"[FinancialsQuarterly] bake data/financials_quarterly.json — {new_count} หุ้น")
    except Exception as e:
        print(f"[FinancialsQuarterly] bake ล้มเหลว (ข้าม): {e}")


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
                    # ตั้ง ts=0 แทน clear() ทั้งก้อน — clear() แข่งกับ get_dr_data()/
                    # _rebuild_dr_cache() ที่กำลังอ่าน _dr_cache["ts"] อยู่พอดี (TOCTOU)
                    # กลายเป็น KeyError -> 500 ดิบไม่มี JSON error body (บั๊กเดียวกับที่
                    # แก้แล้วใน dr_full_refresh() ดู comment ที่นั่น)
                    if _dr_cache.get("result"):
                        _dr_cache["ts"] = 0
                    print(f"[DR-sync] ก่อนดึงงบ: underlying ใหม่ {st.get('added', 0)}, "
                          f"series ใหม่ {st.get('appended', 0)}, ยัง map ไม่ได้ {st.get('unmapped', 0)}")
            except Exception as e:
                print(f"[DR-sync] ข้าม (sync ล้มเหลว ใช้ลิสต์เดิม): {e}")
            symbols = _dr_financials_universe()
        target = symbols if symbols else _financials_universe()
        # yahoo_q = งบรายไตรมาส (สะสมทุกรอบ sync — ใช้กรอง QoQ/YoY-Q ใน Screener)
        # finnomena_q = งบไตรมาสย้อนยาว ~20 ปี (backfill ครั้งเดียวได้ streak/เร่งตัว/TTM เต็มสูตร)
        srcs = tuple(sources) if sources else ("yahoo", "set", "set_qpl", "set_cashflow",
                                                "set_balance", "set_health", "set_factsheet",
                                                "yahoo_q", "finnomena_q")

        def cb(current, total, msg):
            _update(current=current, total=total, message=msg)

        result = financials_store.sync_all(BASE_DIR, target, sources=srcs, callback=cb,
                                           is_dr=is_dr, skip_up_to_date=skip_up_to_date,
                                           auto_rebuild=False)   # rebuild เองแบบ sync ทันทีด้านล่าง
        _fin_analytics_cache.clear()   # ข้อมูลเปลี่ยน — บังคับคำนวณ growth/PEG/FCF ใหม่รอบถัดไป
        _sector_compare_cache.clear()
        _qpl_parsed_cache.clear()
        _fin_quarterly_cache.clear()   # set_qpl เปลี่ยน — bake งบรายไตรมาสใหม่รอบถัดไป
        if is_dr:
            _clear_qpl_mirror_caches(unis=("dr",))   # DR sync แตะแค่ DR: keys — us/hk/jp ไม่กระทบ
        _market_trend_cache.clear()
        _sector_trend_cache.clear()
        # rebuild factor snapshot ทันที (ไม่รอ debounce 300 วิ + ครอบ sync < 5 หุ้นที่เดิม
        # schedule_rebuild ไม่ยิงเลย) — ครอบทั้งไทย+DR เสมอ (ดู factor_snapshot.build_snapshot)
        # ไม่แตะ mirror US/HK/JP: _run_financials_sync ไม่เขียน FINN:{ex}: keys ที่ build_mirror
        # อ่าน (mirror rebuild อยู่ที่ปุ่ม "อัพเดทงบการเงินทั้งหมด" หน้า Data Health / Mirror ทั้งตลาด)
        if result.get("ok", 0) > 0:
            _update(message="กำลังคำนวณ factor snapshot ใหม่...")
            try:
                factor_snapshot.build_snapshot(BASE_DIR)
            except Exception as e:
                print(f"[FinancialsSync] build_snapshot ล้มเหลว (งบ sync สำเร็จแล้ว): {e}")
        if not is_dr and result.get("ok", 0) > 0:
            _bake_financials_quarterly_file()   # set_qpl หุ้นไทยเปลี่ยน -> re-bake ไฟล์เว็บมือถือ
        _warmup_fin_dependent_caches(mirror_universes=("dr",) if is_dr else ())
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
    # try/except ครอบ cold path เหมือน sector_compare()/market_trend() (ดูคอมเมนต์ที่นั่น) —
    # เดิม endpoint นี้ไม่มีเลยทั้งที่เป็นตัวที่ _compute() ของทั้งสองฝั่งเรียกใช้ผลลัพธ์ต่อ
    # exception ตอนคำนวณสดครั้งแรก/หลัง .clear() จะหลุดเป็น Flask 500 HTML แทน JSON error
    try:
        return jsonify(_financials_analytics_core(yahoo_only))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"คำนวณ growth/PEG/FCF ล้มเหลว: {e}"}), 500


def _financials_analytics_core(yahoo_only: bool):
    """ตัวคำนวณจริงของ /api/financials-analytics แยกออกมาจาก view function เพราะ
    sector_compare()'s _compute() ต้องเรียกใช้ผลลัพธ์นี้จาก background thread ของ
    _swr_get_or_refresh (ไม่มี Flask request context ในนั้น) — เดิมเรียก
    financials_analytics().get_json() ตรงๆ ซึ่งบรรทัดแรกอ่าน request.args ทำให้ throw
    RuntimeError('Working outside of request context') ทุกครั้งที่ sector-compare
    revalidate เบื้องหลัง (ถูก catch เงียบๆ ใน _bg() ผลคือ cache ไม่เคย refresh อัตโนมัติ)"""
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
        # pe_map มาจาก set_data.json (หุ้นไทยล้วน) — ห้ามส่งเข้า DR: symbol ที่ชนกัน (เช่น 'META'
        # = META Corp ไทย vs Meta Platforms underlying ของ DR) จะได้ PEG จาก P/E บริษัทไทยที่ไม่
        # เกี่ยวกัน ส่วนตัวที่ไม่ชนก็ได้ None อยู่แล้ว — ยังไม่มีแหล่ง P/E รายตัวสำหรับ DR
        # (dr_cache.json ไม่มี field 'pe') PEG ฝั่ง DR จึงเป็น None ทั้งหมดจนกว่าจะมีแหล่ง
        dr_pe_map: dict = {}
        return {
            "set": _compute_fin_analytics_for(set_symbols, False, pe_map, mktcap_map, yahoo_only=yahoo_only, fin_sector_syms=fin_sector_syms),
            "dr": _compute_fin_analytics_for(dr_symbols, True, dr_pe_map, dr_mktcap_map, yahoo_only=yahoo_only),
        }

    # lock กันหลายแท็บ/request ที่มาชนตอน cache หมดอายุพร้อมกันคำนวณซ้ำซ้อนกัน (ดูคอมเมนต์
    # ที่ _fin_analytics_lock) — stale-while-revalidate (ดู _swr_get_or_refresh): ถ้ามีค่าเก่า
    # อยู่แล้วแค่หมดอายุ ตอบค่าเก่าทันทีแล้วคำนวณใหม่เบื้องหลัง ไม่บล็อก request คำนวณสด
    # แบบ synchronous เฉพาะตอนไม่เคยมีค่าเลย (cold หลัง restart/clear)
    return _swr_get_or_refresh(slot, _fin_analytics_lock, _FIN_ANALYTICS_CACHE_TTL, _compute)


# งบกำไรขาดทุนรายไตรมาส (SET.or.th official, source 'set_qpl') — bake เป็น
# data/financials_quarterly.json ให้เวอร์ชันมือถือ/iPad แสดง ≥8 ไตรมาสล่าสุด
# (Yahoo ให้แค่ ~5 ไตรมาส · Finnomena ห้ามขึ้น GitHub) set_qpl สะสมถาวรอยู่แล้ว
# ใน financials.db ผ่าน _merge_set_qpl_payload — endpoint นี้แค่ดึงออกมา
_fin_quarterly_cache: dict = {}
_fin_quarterly_lock = threading.Lock()
_FIN_QUARTERLY_TTL = 3 * 3600


@app.route("/api/financials-quarterly")
def financials_quarterly():
    """คืน {"as_of": "...", "set": {sym: {"q":[...], "revenue":[...], "gross_profit":[...],
    "op_profit":[...], "net_profit":[...]}}} — 16 ไตรมาสล่าสุดต่อหุ้นไทย จาก set_qpl
    ไม่มี financials.db (เช่นบน CI) -> {"set": {}} ให้ run_static_update.py คงไฟล์เดิม"""
    def _compute():
        return financials_store.bake_quarterly_pl(BASE_DIR, _quarterly_bake_universe())

    with _fin_quarterly_lock:
        slot = _fin_quarterly_cache.setdefault("th", {})
    try:
        return jsonify(_swr_get_or_refresh(slot, _fin_quarterly_lock, _FIN_QUARTERLY_TTL, _compute))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"ดึงงบรายไตรมาสล้มเหลว: {e}", "set": {}}), 500


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

        fin_data = _financials_analytics_core(yahoo_only=False)
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


def _compute_qpl_mirror_parsed(uni):
    """โหลดชั้นดิบของ growth screener เวอร์ชันต่างประเทศ (?universe=dr|us|hk|jp) —
    financials_store._load_mirror_qpl_all (finnomena_q ฐาน + yahoo_q ทับ) + เติม sector/name/
    mkt_cap เฉพาะที่หาได้ต่อตลาด

    namespace ใน financials.db (เช็คจริง 2026-08-30):
      DR portfolio  -> 'DR:{mnemonic}'   (finnomena_q + yahoo_q)
      mirror US/HK  -> 'FINN:{EX}:{tkr}' (finnomena_q ล้วน — ไม่มี yahoo_q)
      Nikkei 225    -> 'DR:{numeric}'    (yahoo_q ล้วน จาก _run_jp_index_sync q_targets step —
                       'FINN:JP:' มีแค่ yahoo รายปี ไม่มีรายไตรมาส) ~202/225 ตัว
    DR/US/HK ไม่มี sector ให้ (factor_snapshot ไม่เก็บ + dr_cache.json ก็ไม่มี), JP มีจาก jp_index_metrics"""
    like = {"dr": "DR:%", "us": "FINN:US:%", "hk": "FINN:HK:%", "jp": "DR:%"}[uni]
    name_prefix = {"dr": "DR:", "us": "FINN:US:", "hk": "FINN:HK:", "jp": None}[uni]
    allowed = None
    sector_by_symbol, name_override, mktcap_override = {}, {}, {}

    # ชื่อบริษัท — payload ดิบมักเก็บแค่รหัส (name == ticker) get_names_bulk ขุดจาก yahoo/yahoo_q
    # ที่มีชื่อจริง (เหมือน /api/mirror-symbols-named) · JP ใช้ชื่อจาก jp_index_metrics แทน
    if name_prefix:
        name_override.update(financials_store.get_names_bulk(BASE_DIR, name_prefix))

    if uni == "dr":
        allowed = set(_dr_financials_universe())
        try:
            with open(os.path.join(BASE_DIR, "dr_cache.json"), encoding="utf-8") as f:
                for s in json.load(f).get("stocks", []):
                    if s.get("mkt_cap") is not None:
                        mktcap_override[s["sym"]] = s["mkt_cap"]
        except Exception:
            pass
    elif uni in ("us", "hk"):
        # ชุดเดียวกับ Screener+ ?universe=us/hk — หุ้น mirror ที่ผ่าน quality filter
        # (>=12 ไตรมาส + มี PE จริง — ดู factor_snapshot.build_mirror_snapshot) ตัด OTC/junk ออกแล้ว
        allowed = set(factor_snapshot.get_mirror_symbols(BASE_DIR).get(uni.upper(), []))
    elif uni == "jp":
        from sources import jp_index_metrics
        # ยึด jp_index_metrics เป็นแหล่งชื่อ/sector/mkt_cap + กรอง universe ให้เป็นเฉพาะ
        # สมาชิก Nikkei 225 จริง (กัน DR:{numeric} ของ HK ที่ leading-zero เช่น '0700'
        # หลุดเข้ามา — Nikkei ใช้เลข 4 หลักไม่มี 0 นำ ไม่ชนกันอยู่แล้ว แต่ cross-check ชื่อไว้อีกชั้น)
        allowed = set()
        for s in jp_index_metrics.load_local(BASE_DIR).get("stocks", []):
            sym = s.get("symbol") or ""
            if not sym.endswith(".T"):
                continue
            bare = sym[:-2]
            allowed.add(bare)
            if s.get("sector"):
                sector_by_symbol[bare] = s["sector"]
            if s.get("name"):
                name_override[bare] = s["name"]
            if s.get("mkt_cap") is not None:
                mktcap_override[bare] = s["mkt_cap"]

    loaded = financials_store._load_mirror_qpl_all(BASE_DIR, like, allowed_bare=allowed)
    return {
        "parsed": loaded["parsed"],
        "cashflow_parsed": loaded["cashflow_parsed"],
        "sector_by_symbol": sector_by_symbol,
        "name_by_symbol": {**loaded["name_by_symbol"], **name_override},
        "mktcap_by_symbol": {**loaded["mktcap_by_symbol"], **mktcap_override},
    }


@app.route("/api/quarterly-growth-screener")
def quarterly_growth_screener():
    """หน้าใหม่ "🚀 หุ้นโตแรงรายไตรมาส" — ลิสหุ้น SET+mai ทั้งตลาดแบบแบน (ไม่จัดกลุ่ม sector ต่างจาก
    /api/sector-compare) revenue/net_profit QoQ%/YoY% ต่อหุ้น เลือกไตรมาสย้อนหลังได้ผ่าน query
    param ?quarter=YYYY-Q (ไม่ใส่/parse ไม่ได้/ไม่มีในข้อมูล -> ไตรมาสล่าสุด) ดู
    financials_store.get_qpl_growth_screener สำหรับ logic เต็ม

    แคชแยกชั้นจาก /api/sector-compare แม้อ่านตาราง set_qpl เดียวกัน (ดูคอมเมนต์ _qpl_parsed_cache
    ด้านบน) — ชั้นโหลดดิบ (sqlite+JSON parse) แคช 24h, ชั้นคำนวณ QoQ/YoY รันสดทุก request ตาม
    quarter param ที่ขอมา

    ?universe=dr|us|hk|jp — หุ้นต่างประเทศ: อ่าน finnomena_q/yahoo_q แทน set_qpl
    (financials_store._load_mirror_qpl_all) get_qpl_growth_screener ตัวเดียวกันคำนวณต่อ —
    cache แยกต่อ universe (_qpl_mirror_caches) ไม่ใส่/th -> หุ้นไทยเหมือนเดิม"""
    quarter = request.args.get("quarter") or None
    uni = (request.args.get("universe") or "th").lower()

    if uni in ("dr", "us", "hk", "jp"):
        try:
            slot, lock = _qpl_mirror_slot(uni)
            cached = _swr_get_or_refresh(slot, lock, _QPL_PARSED_CACHE_TTL,
                                         lambda: _compute_qpl_mirror_parsed(uni))
            result = financials_store.get_qpl_growth_screener(
                cached["parsed"], cached["sector_by_symbol"], target_quarter=quarter,
                name_by_symbol=cached["name_by_symbol"], cashflow_parsed=cached["cashflow_parsed"],
                mktcap_by_symbol=cached["mktcap_by_symbol"])
            for row in result.get("stocks", []):
                row["market"] = uni
        except Exception as e:
            traceback.print_exc()
            return jsonify({"error": f"โหลดข้อมูลเติบโตรายไตรมาส ({uni}) ล้มเหลว: {e}"}), 500
        return jsonify(result)

    def _compute_parsed():
        sector_by_symbol = {}
        name_by_symbol = {}
        mktcap_by_symbol = {}
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    for s in json.load(f).get("stocks", []):
                        if s.get("market") in ("SET", "mai") and s.get("sector"):
                            sector_by_symbol[s["symbol"]] = s["sector"]
                        if s.get("name"):
                            name_by_symbol[s["symbol"]] = s["name"]
                        if s.get("mkt_cap") is not None:
                            mktcap_by_symbol[s["symbol"]] = s["mkt_cap"]
            except Exception:
                pass
        parsed = financials_store._load_set_qpl_all(BASE_DIR, sector_by_symbol)
        # กำไรมีเงินสดหนุนจริงไหม (OCF vs กำไรสุทธิ) — คีย์ (year,q) เดียวกับ set_qpl พอดี
        # (ทั้งคู่ derive จาก endDate จริงแบบเดียวกัน) compose ได้ตรงๆ ไม่ต้องแปลงคีย์เพิ่ม
        cashflow_parsed = financials_store._load_set_qpl_all(
            BASE_DIR, sector_by_symbol, source="set_cashflow")
        return {"parsed": parsed, "sector_by_symbol": sector_by_symbol,
                "name_by_symbol": name_by_symbol, "cashflow_parsed": cashflow_parsed,
                "mktcap_by_symbol": mktcap_by_symbol}

    # try/except ครอบทั้งก้อน เหมือน /api/sector-compare ด้านบน — กัน error ที่ไม่คาดคิดหลุดเป็น
    # หน้า 500 HTML ของ Flask ที่ frontend parse JSON ไม่ได้
    try:
        cached = _swr_get_or_refresh(_qpl_parsed_cache, _qpl_parsed_lock,
                                      _QPL_PARSED_CACHE_TTL, _compute_parsed)
        result = financials_store.get_qpl_growth_screener(
            cached["parsed"], cached["sector_by_symbol"], target_quarter=quarter,
            name_by_symbol=cached["name_by_symbol"], cashflow_parsed=cached["cashflow_parsed"],
            mktcap_by_symbol=cached["mktcap_by_symbol"])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"โหลดข้อมูลเติบโตรายไตรมาสล้มเหลว: {e}"}), 500
    return jsonify(result)


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
    metrics_file = {"us": "us_index_metrics.json", "hk": "hk_index_metrics.json",
                     "jp": "jp_index_metrics.json"}[uni]
    # factor_mirror_at เปลี่ยนทุกครั้งที่ build_mirror_snapshot()/build_mirror_snapshot_yahoo_only()
    # รัน (US+HK+JP ใช้ meta key เดียวกัน — rebuild ตลาดหนึ่งจะ bust cache ของตลาดอื่นไปด้วย ไม่เป็น
    # ไร แค่ recompute เกินความจำเป็นนานๆ ครั้ง ไม่ใช่ทุก request) + mtime ของ index metrics json
    # (overlay technical เปลี่ยนตาม Index Sync/Quick Update ที่ไม่ได้ผูกกับ build_mirror_snapshot)
    return (financials_store._get_meta(BASE_DIR, "factor_mirror_at"),
            _m(os.path.join(BASE_DIR, "data", metrics_file)))


def _get_screener_mirror_rows(uni):
    """คืนเฉพาะ rows จาก _get_screener_mirror_data() — ดู docstring ที่นั่น"""
    return _get_screener_mirror_data(uni)[0]


def _get_screener_mirror_data(uni):
    """cache (rows, meta) ของ Screener+ US/HK/JP หลัง overlay technical แล้ว ต่อ universe —
    เดิมไม่มี cache เลย ทุก request (สลับตลาด/ติ๊ก filter/เรียงคอลัมน์) ยิง scan+overlay ใหม่ทั้งก้อน
    วัดจริง US ~0.3-1.7 วิ/ครั้ง ทั้งที่ผลลัพธ์เหมือนเดิมทุกครั้งจนกว่าข้อมูลต้นทางจะเปลี่ยนจริง
    (build_mirror_snapshot/Index Sync) — key ตาม _screener_mirror_cache_key() auto-bust ตรงจุด
    ไม่ต้องเดา TTL (ต่างจาก _sector_compare_cache ที่ไม่มีตัวบอกเวอร์ชันชัดเจนแบบนี้)

    meta (factor_snapshot.mirror_snapshot_meta — เปิด connection + query GROUP BY) เดิมถูกเรียก
    สดทุก request ใน factor_screener() แม้ rows จะ cache hit แล้วก็ตาม (code review 2026-09-02)
    เลยย้ายมาคำนวณคู่กับ rows ครั้งเดียวตอน cache miss แทน

    return list/dict เดียวกันข้ามหลาย request ได้อย่างปลอดภัย — filter/sort ท้าย factor_screener()
    อ่านอย่างเดียว ไม่เคยเขียนทับ field ของ row ที่ cache ไว้"""
    key = _screener_mirror_cache_key(uni)
    with _screener_mirror_lock:
        cached = _screener_mirror_cache.get(uni)
        if cached and cached[0] == key:
            return cached[1], cached[2]
        rows = _build_screener_mirror_rows(uni)
        meta = factor_snapshot.mirror_snapshot_meta(BASE_DIR)
        _screener_mirror_cache[uni] = (key, rows, meta)
        return rows, meta


def _overlay_mirror_technicals(rows, index_stocks, suffix="", override_name=False):
    """overlay technical (rs/EMA200/52W high/rvol/stage/fcf_yield) + ซ่อน Z-Score กลุ่มการเงิน
    ให้ rows ของ mirror universe หนึ่งตัว จาก index_stocks (list จาก us/hk/jp_index_metrics.json)
    — เดิมก็อปวางทั้งก้อนนี้ 3 รอบต่อ US/HK/JP ใน _build_screener_mirror_rows จนเริ่ม drift จริง
    (JP มี name-override ที่ US/HK ไม่มี) รวมเป็นฟังก์ชันเดียวกันไม่ให้แก้ที่หนึ่งแล้วลืมอีกสองที่
    (code review 2026-09-02)

    suffix: symbol ในชุด mirror เป็นรหัสดิบ (เช่น "0700"/"7203") ส่วน hk/jp_index_metrics ใช้
    รูปแบบ yfinance ("0700.HK"/"7203.T") — ต่อ suffix ก่อน lookup (US ไม่มี suffix)
    override_name: เอาชื่อจาก index_stocks มาทับ r['name'] — เฉพาะ JP ที่ไม่มี mirror_names.json
    ให้ (Finnomena ไม่มีข้อมูลตลาดนี้เลย) US/HK มีชื่อจากช่องทางอื่น (/api/mirror-names) แล้ว"""
    by_sym = {s["symbol"]: s for s in index_stocks}
    for r in rows:
        lookup = r["symbol"] + suffix if suffix else r["symbol"]
        s = by_sym.get(lookup)
        if override_name and s and s.get("name"):
            r["name"] = s["name"]
        r["rs"] = s.get("rs_score") if s else None
        r["pct_vs_ema200"] = (round((s["price"] / s["ema200"] - 1) * 100, 2)
                               if s and s.get("price") and s.get("ema200") else None)
        r["pct_off_high52"] = (round((s["price"] / s["high_52w"] - 1) * 100, 2)
                                if s and s.get("price") and s.get("high_52w") else None)
        r["rvol"] = (round(s["vol_today"] / s["vol_avg20"], 4)
                     if s and s.get("vol_today") and s.get("vol_avg20") else None)
        r["stage"] = s.get("stage") if s else None
        # fcf_yield ต้องใช้ mkt_cap สด — us/hk/jp_index_metrics.json ไม่เก็บ mkt_cap เลย
        # (ยืนยันแล้วเหมือนที่ /api/tearsheet เจอ) คำนวณเองจาก price สด × shares_out ล่าสุด
        # (งบ Yahoo annual, มาจาก factor_snapshot อยู่แล้วใน r) แทน — มีค่าเฉพาะหุ้นในดัชนีหลัก
        # ที่มี price สด ตัวนอกดัชนียังเป็น None
        mc = s.get("mkt_cap") if s else None
        if mc is None and s and s.get("price") and r.get("shares_out"):
            mc = s["price"] * r["shares_out"]
        r["fcf_yield"] = (r["fcf"] / mc * 100) if (r.get("fcf") is not None and mc) else None
        # ซ่อน Z-Score ของกลุ่มการเงิน (แบงก์/ประกัน) เหมือนที่ /api/tearsheet และ
        # /api/peer-compare ทำไว้แล้ว (เจอตอนรีวิว 2026-08-01 ว่า Screener+ ยังไม่ซ่อน
        # ทำให้ JPM/BAC ฯลฯ ที่มีงบ Yahoo sync แล้วโชว์ Z-Score หลอกตาในตาราง ทั้งที่
        # สูตร Altman ใช้ไม่ได้กับงบดุลสถาบันการเงิน) — เช็คได้เฉพาะสมาชิกดัชนีหลักที่มี
        # sector จาก index_metrics.json เท่านั้น (ตัวนอกดัชนีไม่มี sector ให้เช็ค)
        if s and s.get("sector") in factor_snapshot.FINANCIAL_SECTOR_NAMES:
            r["z_score"], r["z_zone"] = None, None
            r["z_excluded_reason"] = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"


def _build_screener_mirror_rows(uni):
    rows = factor_snapshot.get_mirror_snapshot(BASE_DIR, uni.upper())

    # overlay technical (rs/EMA200/52W high/rvol/stage) ให้เฉพาะตัวที่อยู่ใน
    # S&P500+Dow+NDX (~518 ตัว จาก us_index_metrics.json — ราคาราย daily จาก
    # us_prices.db) / HSI+HSCEI+HSTECH (~105 ตัว จาก hk_index_metrics.json —
    # ราคาราย daily จาก hk_prices.db) — mirror ตัวอื่นๆ นอกดัชนีหลักยังไม่มีราคา
    # รายวันเก็บไว้ เลยยังเป็น None เหมือนเดิม
    if uni == "us":
        from sources import us_index_metrics
        _overlay_mirror_technicals(rows, us_index_metrics.load_local(BASE_DIR).get("stocks", []))
    elif uni == "hk":
        from sources import hk_index_metrics
        _overlay_mirror_technicals(rows, hk_index_metrics.load_local(BASE_DIR).get("stocks", []), suffix=".HK")
    elif uni == "jp":
        from sources import jp_index_metrics
        # rows ทั้งหมดเป็นสมาชิก Nikkei225 อยู่แล้ว (build_mirror_snapshot_yahoo_only รับ
        # เฉพาะ universe ที่ curate มาแล้ว ไม่ใช่ raw universe หมื่นตัวแบบ US/HK) เลย overlay
        # ราคา/technical ได้ครบ 225/225 ไม่มีตัวนอกดัชนีเหมือน US/HK ที่ทำได้แค่บางส่วน
        _overlay_mirror_technicals(rows, jp_index_metrics.load_local(BASE_DIR).get("stocks", []),
                                    suffix=".T", override_name=True)
    return rows


@app.route("/api/factor-screener")
def factor_screener():
    """Deep Screener — ตารางปัจจัยพื้นฐานต่อหุ้น (รวม Finnomena+Yahoo+SET) จาก factor_snapshot
    (local-only, precompute ด้วย build_snapshot.py) overlay pe/mkt_cap สดจาก set_data.json
    เพื่อคำนวณ peg/fcf_yield ที่อิงราคาปัจจุบัน — คืน {rows: [...], meta: {...}}

    ?universe=us / hk : คืนหุ้น mirror ทั้งตลาด (นอก universe หลัก, งบ Finnomena ล้วน)
    แทนชุดหลัก (ไทย+DR) — โหลดแยก เพราะชุดใหญ่ (US ~11k, HK ~2k) rows หลัง overlay cache ไว้
    (ดู _get_screener_mirror_rows) filter/sort/limit ด้านล่างคำนวณสดทุกครั้งเพราะเบามาก

    ?universe=jp : ต่างจาก us/hk ตรงที่ไม่มี Finnomena รองรับตลาดนี้เลย — ชุด mirror ของ JP
    เลยมีแค่ 225 ตัว (สมาชิก Nikkei225 เท่าที่มีใน jp_index_metrics.json ไม่ใช่ทั้งตลาดจริง
    หลักพัน/หมื่นตัวแบบ US/HK) แต่ตัวเลข PE/PBV/ROE/margin/F-Score/Z-Score/CAGR มาจาก
    build_mirror_snapshot_yahoo_only() (Yahoo annual ล้วน) เก็บใน factor_snapshot_mirror
    table เดียวกัน ครบเกิน 90% แล้ว (เช็ค 2026-08-28) — field ที่ต้องใช้ finnomena_q รายไตรมาส
    (P/S, PEG, %YoY/QoQ รายไตรมาส, div_yield) ยังเป็น None ถาวรเพราะไม่มีต้นทาง"""
    uni = (request.args.get("universe") or "").lower()
    if uni in ("us", "hk", "jp"):
        # ชุด mirror ใหญ่มาก (US ~17k) — กรองฝั่ง server ส่งเฉพาะผลลัพธ์ (≤ limit) — JP เล็กกว่า
        # มาก (225 ตัว) แต่ใช้ path เดียวกันเพราะ shape ข้อมูลเหมือนกันทุกอย่าง
        rows, mirror_meta = _get_screener_mirror_data(uni)

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
                if not isinstance(cv, (int, float)) or isinstance(cv, bool):
                    # "v" หายไปหรือไม่ใช่ตัวเลข (URL ถูกแก้มือ/บุ๊คมาร์คเก่า) — ข้าม filter
                    # นี้แทนที่จะ throw TypeError ตอนเทียบ v < cv
                    continue
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
                        "meta": mirror_meta})

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

    # CG/ESG + ผู้ถือหุ้นใหญ่/free float + โควตาต่างชาติ (SET.or.th ทางการ) — หุ้นไทย
    # ล้วน ไม่มีทางมีค่าสำหรับ DR/mirror US/HK เลย (ดู PLAN_set_api_expansion.txt
    # งาน #1/#2/#3) coverage ต่างกันตามชุด (ESG ~38%, free float/foreign room สูงกว่า
    # มาก) — ไม่ raise ถ้ายังไม่เคย sync (get_company_map คืน {} เฉยๆ)
    company_map = set_company.get_company_map(BASE_DIR)
    # ฉันทามตินักวิเคราะห์ (Yahoo: ราคาเป้าหมาย + Buy/Hold/Sell) — market='TH' เท่านั้น
    # (DR/mirror ไม่ overlay ที่นี่ ดูการ์ดในหน้า Tearsheet แทน) · coverage ~40% เอียง
    # cap ใหญ่ — ทุก filter ต้อง "ไม่มีข้อมูล = ไม่กรองทิ้ง" ยกเว้น has_analyst_coverage
    # ที่ตั้งใจให้ opt-in · คืน {} เฉยๆ ถ้ายังไม่เคย sync
    analyst_map = analyst_consensus.get_map(BASE_DIR, "TH")

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
            comp = company_map.get(r["symbol"]) or {}
            r["cg_score"] = comp.get("cg_score")
            r["esg_rating"] = comp.get("esg_rating")
            r["esg_rank"] = comp.get("esg_rank")
            r["free_float"] = comp.get("free_float")
            r["total_shareholder"] = comp.get("total_shareholder")
            r["nvdr_pct_holding"] = comp.get("nvdr_pct_holding")
            r["foreign_room_used_pct"] = comp.get("foreign_room_used_pct")
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
            # ราคาเป้าหมายนักวิเคราะห์ — upside คำนวณสดเทียบราคาปัจจุบัน (ไม่ใช่
            # target_upside_pct ที่อิง price_at_fetch เก่าตอน sync) ให้ตรงกับคอลัมน์ราคาอื่น
            ac = analyst_map.get(r["symbol"]) or {}
            _tm, _px = ac.get("target_mean"), s.get("price")
            r["analyst_target_upside"] = (round((_tm / _px - 1) * 100, 2)
                                          if (_tm and _px and _px > 0) else None)
            r["analyst_rec_mean"] = ac.get("rec_mean")
            r["analyst_buy_pct"] = ac.get("buy_pct")
            r["has_analyst_coverage"] = bool(ac.get("target_mean") is not None
                                             or (ac.get("rec_total") or 0) > 0)
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
            # ฉันทามตินักวิเคราะห์ overlay เฉพาะ market='TH' — DR ให้เป็น None/False ชัดเจน
            # (shape JSON สม่ำเสมอทุกแถว) ดูข้อมูลนักวิเคราะห์ของ DR ได้ในการ์ดหน้า Tearsheet
            r["analyst_target_upside"] = None
            r["analyst_rec_mean"] = None
            r["analyst_buy_pct"] = None
            r["has_analyst_coverage"] = False

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
    meta = factor_snapshot.snapshot_meta(BASE_DIR)
    meta.update(set_company.get_meta(BASE_DIR))   # esg_*/shareholder_*/foreign_* count+updated_at
    _ac_th_meta = analyst_consensus.get_meta(BASE_DIR).get("TH", {})
    if _ac_th_meta.get("count"):
        meta["analyst_count"] = _ac_th_meta["count"]
        meta["analyst_with_target"] = _ac_th_meta.get("with_target", 0)
        meta["analyst_updated_at"] = _ac_th_meta.get("updated_at")
    return jsonify({"rows": rows, "meta": meta})


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
    ?market=TH|US|HK|JP|DR : default TH · US/HK/JP สมาชิกดัชนีหลัก (us/hk/jp_index_metrics.json
                              ~848 ตัว) ใช้ sector ที่มีอยู่แล้ว · ถ้า symbol ที่ระบุเป็นหุ้น
                              mirror นอกดัชนีหลัก ดึง sector แบบ on-demand ให้ตัวนั้นแล้ว pin
                              เข้ากลุ่ม sector เดียวกัน (peer ที่เห็นยังจำกัดแค่สมาชิกดัชนีหลัก
                              เท่านั้น — ยังไม่มี sector ของหุ้นอื่นนอกดัชนีแบบ bulk, JP ไม่มี
                              on-demand เลยเพราะ Finnomena ไม่มีข้อมูลตลาดญี่ปุ่น) · DR:
                              resolve symbol เป็น underlying US/HK/JP ผ่าน dr_universe ก่อน
                              (ต้องระบุ symbol เสมอ, ไม่รองรับ sector โดยตรง — region อื่นที่ไม่ใช่
                              US/HK/JP ยัง 501) · ตัวเลข factor ของ JP มาจาก
                              build_mirror_snapshot_yahoo_only() (Yahoo annual ล้วน ไม่มี
                              finnomena_q) — PE/PBV/ROE/margin/F-Score/Z-Score/CAGR มีจริง
                              (เช็ค 2026-08-28: >90% coverage) แต่ P/S, PEG, %YoY/QoQ รายไตรมาส,
                              div_yield เป็น None ถาวร (ต้องใช้งบรายไตรมาสที่ JP ไม่มีต้นทาง)
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
        if region not in ("US", "HK", "JP"):
            return jsonify({"rows": [], "median": None, "count": 0,
                            "meta": {"note": f"หุ้น DR {symbol} (underlying ตลาด {region or '?'})"
                                             f" ยังไม่รองรับ — รองรับเฉพาะ underlying ตลาด US/HK/JP"}}), 501
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
            try:
                hdr = mirror_ondemand.fetch_header(BASE_DIR, mkt, raw_symbol)
            except Exception:
                hdr = None
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

    # CG/ESG rating (SET.or.th ทางการ) — overlay ตอน query เฉพาะตลาดไทย (universe US/HK/JP
    # ไม่มีแนวคิดนี้) ดู PLAN_set_api_expansion.txt งาน #3C · คืน {} ถ้ายังไม่เคย sync ESG
    company_map = set_company.get_company_map(BASE_DIR) if mkt == "TH" else {}

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
        cm = company_map.get(sym) or {}
        row["esg_rating"] = cm.get("esg_rating")
        row["esg_rank"] = cm.get("esg_rank")
        row["cg_score"] = cm.get("cg_score")
        row["setesg_index"] = cm.get("setesg_index")
        row["djsi_index"] = cm.get("djsi_index")
        rows.append(row)

    num_cols = ["mkt_cap", "pe", "pbv", "div_yield", "peg", "ps_value", "roe", "roa",
                "gross_margin", "net_margin", "de_ratio", "interest_coverage", "ocf_ni_ratio",
                "f_score", "z_score", "rev_cagr", "profit_cagr", "rev_ttm_yoy", "profit_ttm_yoy",
                "revenue_streak", "growth_score", "rule_of_40", "esg_rank", "cg_score"]
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
    # coverage ESG ในกลุ่มนี้ (ดู PLAN_set_api_expansion.txt งาน #3C) — SET ประเมินแค่ ~38%
    # ของกระดาน ต้องบอกให้เห็นว่ากี่ตัวในกลุ่มมีเรตติ้ง ไม่งั้นผู้ใช้เดาว่าทำไมช่องว่าง
    esg_have = sum(1 for r in rows if r.get("esg_rating") or r.get("cg_score") is not None)
    esg_meta = set_company.get_meta(BASE_DIR) if mkt == "TH" else {}
    esg_as_of = max((v.get("esg_as_of") for v in company_map.values() if v.get("esg_as_of")),
                    default=None)
    return jsonify({
        "rows": rows, "median": median, "count": len(rows),
        "meta": {"level": level, "group": group_key, "widened": widened,
                 "base_symbol": symbol or None, "market": mkt, "computed_at": computed_at,
                 "ondemand_note": ondemand_note, "dr_symbol": dr_symbol,
                 "esg_have": esg_have, "esg_total_board": esg_meta.get("esg_count"),
                 "esg_as_of": esg_as_of},
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
    try:
        finn = financials_store.get(BASE_DIR, sym, "finnomena_q", con=con)
    except Exception:
        finn = None
    if finn is None:
        try:
            fresh = financials_store.fetch_finnomena_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "finnomena_q", fresh)
            finn = financials_store.get(BASE_DIR, sym, "finnomena_q", con=con)
        except Exception:
            finn = None
    try:
        yq = financials_store.get(BASE_DIR, sym, "yahoo_q", con=con)
    except Exception:
        yq = None
    if yq is None:
        try:
            fresh = financials_store.fetch_yahoo_quarterly(sym)
            financials_store.upsert(BASE_DIR, sym, "yahoo_q", fresh)
            yq = financials_store.get(BASE_DIR, sym, "yahoo_q", con=con)
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
    try:
        return financials_store.compute_full_report(finn, yq, set_series=set_series)["quarters"]
    except Exception:
        return None


def _qkey(row):
    """คีย์ไตรมาสแบบเรียงเชิงเส้น (year_ad*4+q) — เทียบ 2 ไตรมาสว่าติดกันจริงตามปฏิทินได้ตรงๆ
    ต่างจากอิงตำแหน่งใน list (quarters[-4:] ฯลฯ) ที่ผิดเงียบๆ ถ้าบางไตรมาสไม่มีข้อมูลในทุกแหล่งเลย
    (list จะขาดช่วงตรงนั้นไปโดยไม่รู้ตัว ทำให้ 'ตำแหน่ง -4 ถึง -1' ไม่ใช่ 4 ไตรมาสติดกันจริง)"""
    return row["year_ad"] * 4 + row["q"]


def _qmap(quarters):
    return {_qkey(q): q for q in quarters}


def _ttm_sum(qmap, anchor_key, field):
    """ผลรวม field ของ 4 ไตรมาสปฏิทินจริงที่ต่อเนื่องกัน จบที่ anchor_key (anchor, -1, -2, -3
    เทียบผ่าน qmap) — None ถ้าไตรมาสไหนในช่วงนี้ไม่มีอยู่จริงในชุดข้อมูล (กัน TTM ข้ามไตรมาสที่
    ขาดหายไปกลางช่วงแบบเงียบๆ) หรือมีงวดไหนไม่มีค่า field นี้เลย"""
    vals = []
    for off in range(4):
        row = qmap.get(anchor_key - off)
        if row is None or row.get(field) is None:
            return None
        vals.append(row[field])
    return sum(vals)


def _latest_ttm_anchor(qmap, anchor_key, field, lookback=4):
    """anchor ล่าสุด (<= anchor_key, ย้อนได้สูงสุด lookback ไตรมาสปฏิทินจริง) ที่ _ttm_sum(field)
    ครบ 4 ไตรมาสจริง — งบกระแสเงินสด/งบดุลจาก Yahoo มักตามหลัง P&L (SET/Finnomena) 1 งวด ทำให้
    anchor ที่ยึดไตรมาส P&L ล่าสุดได้ cfo/cfi TTM = None ทั้งที่มีงบครบถึงงวดก่อนหน้า (ไตรมาส
    ใหม่สุดในงบผสานของหุ้นไทยส่วนใหญ่เป็น SET P&L ล้วน) คืน None ถ้าย้อนครบ lookback แล้วยังไม่มี
    ช่วง TTM ไหนครบ"""
    for off in range(lookback):
        k = anchor_key - off
        if _ttm_sum(qmap, k, field) is not None:
            return k
    return None


def _peer_group_pct_change(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 2)


def _avg_last4(qmap, anchor_key, field):
    """ค่าเฉลี่ย field จาก 4 ไตรมาสปฏิทินล่าสุดที่จบที่ anchor_key ที่มีค่าจริง (ไม่ต้องครบ 4
    ไตรมาสเป๊ะเหมือน _ttm_sum แต่ยังยึดไตรมาสตามปฏิทินจริงผ่าน qmap ไม่ใช่ตำแหน่งใน list) ใช้กับ
    field งบดุล ณ จุดเวลา เช่น total_assets/inventory/accounts_receivable คืน None ถ้าไม่มีค่า
    เลยสักไตรมาส"""
    vals = []
    for off in range(4):
        row = qmap.get(anchor_key - off)
        if row is not None and row.get(field) is not None:
            vals.append(row[field])
    return (sum(vals) / len(vals)) if vals else None


def _latest_value(qmap, anchor_key, field, lookback=4):
    """ค่า field ล่าสุดที่ไม่ใช่ None ย้อนดูสูงสุด lookback ไตรมาสปฏิทินจริงจาก anchor_key (เทียบ
    ผ่าน qmap — กันเผลอย้อนไกลเกิน lookback ไตรมาสจริงตอนข้อมูลมีช่วงขาดหายกลางทาง ซึ่งอิงตำแหน่ง
    ใน list เดิมจะย้อนไกลเกินโดยไม่รู้ตัว) — งบดุล/กระแสเงินสดรายไตรมาสจาก Yahoo มักตามหลัง P&L
    ที่มาจาก SET/Finnomena อยู่ 1 งวด (ไตรมาสล่าสุดสุดของ total_assets/total_debt/current_assets
    ฯลฯ อาจยังไม่มีค่าทั้งที่ revenue/net_profit มาแล้ว) ใช้แทนอ่านไตรมาสล่าสุดตรงๆ สำหรับ
    อัตราส่วน ณ จุดเวลาที่ต้องการค่าล่าสุดเท่าที่มีจริง"""
    for off in range(lookback):
        row = qmap.get(anchor_key - off)
        if row is not None:
            v = row.get(field)
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
            # กันหุ้นตัวเดียวพัง (เช่น DB lock ชั่วคราว/ข้อมูลงบเพี้ยน) ทำให้ทั้ง endpoint 500
            # แทนที่จะแค่ข้ามหุ้นตัวนั้นไป (เดียวกับแนวทางของ get_financials_sankey_sector ด้านล่าง)
            try:
                rows.append(_peer_group_detail_row(sym, entry, f, con))
            except Exception as e:
                print(f"[peer_group_detail] คำนวณหุ้น {sym} ล้มเหลว: {str(e)[:120]}")
                rows.append({"symbol": sym, "name": entry.get("name") or sym, "no_data": True})
    finally:
        if con is not None:
            con.close()

    return jsonify({"rows": rows, "meta": {"base_symbol": base_symbol, "market": mkt}})


def _peer_group_detail_row(sym, entry, f, con):
    """คำนวณ 1 แถวของ /api/peer-group-detail สำหรับหุ้นตัวเดียว — แยกออกมาจาก loop หลักให้
    ผู้เรียกครอบ try/except ต่อหุ้นได้ (ดูคอมเมนต์ใน peer_group_detail ด้านบน)"""
    quarters = _peer_group_quarters(sym, con=con)
    if not quarters:
        return {"symbol": sym, "name": entry.get("name") or sym, "no_data": True}

    qmap = _qmap(quarters)
    last = quarters[-1]
    anchor = _qkey(last)
    prior_q = qmap.get(anchor - 1)
    yoy_q = qmap.get(anchor - 4)

    rev_ttm = _ttm_sum(qmap, anchor, "revenue")
    np_ttm = _ttm_sum(qmap, anchor, "net_profit")
    gp_ttm = _ttm_sum(qmap, anchor, "gross_profit")
    op_ttm = _ttm_sum(qmap, anchor, "operating_profit")
    cogs_ttm = _ttm_sum(qmap, anchor, "cogs")
    # งบกระแสเงินสด (Yahoo) ตามหลัง P&L 1 งวดเสมอ — ยึด anchor ของ CFO เอง (งวดล่าสุดที่มี TTM
    # ครบ 4 ไตรมาส) ไม่ใช่ anchor ของ P&L ไม่งั้น CFO/FCF/margin ทั้งกลุ่มขึ้น "–" เกือบทุกตัว
    # เมื่อไตรมาสใหม่สุดเป็น SET P&L ล้วน · cfi/capex ใช้ anchor เดียวกับ cfo ให้ FCF สอดคล้องกัน
    cf_anchor = _latest_ttm_anchor(qmap, anchor, "cfo")
    cfo_ttm = _ttm_sum(qmap, cf_anchor, "cfo") if cf_anchor is not None else None
    cfi_ttm = _ttm_sum(qmap, cf_anchor, "cfi") if cf_anchor is not None else None
    capex_ttm = _ttm_sum(qmap, cf_anchor, "capex") if cf_anchor is not None else None
    fcf_approx = (cfo_ttm + cfi_ttm) if (cfo_ttm is not None and cfi_ttm is not None) else None

    equity_avg = _avg_last4(qmap, anchor, "total_equity")
    assets_avg = _avg_last4(qmap, anchor, "total_assets")
    inventory_avg = _avg_last4(qmap, anchor, "inventory")
    ar_avg = _avg_last4(qmap, anchor, "accounts_receivable")

    mkt_cap = entry.get("mkt_cap")
    pe = (mkt_cap / np_ttm) if (mkt_cap and np_ttm and np_ttm > 0) else f.get("pe_value")
    # PBV จาก mkt_cap/equity งวดล่าสุด (สอดคล้องกับ P/E ที่ TTM ด้านบน) — fallback ไป
    # pbv_value ของ factor_snapshot (Finnomena) ถ้า total_equity รายไตรมาสไม่มี (ดู docstring
    # ของฟังก์ชันนี้ — งบดุลรายไตรมาสมีแค่บาง symbol) — ใช้ _latest_value แทน last.get() ตรงๆ
    # เพราะงบดุลจาก Yahoo มักตามหลัง P&L อยู่ 1 งวด (ไตรมาสล่าสุดสุดมี revenue/net_profit
    # แต่ total_equity/total_debt/current_assets ยังไม่ sync มา)
    equity_latest = _latest_value(qmap, anchor, "total_equity")
    pbv = (mkt_cap / equity_latest) if (mkt_cap and equity_latest and equity_latest > 0) else f.get("pbv_value")
    ibd_latest = _latest_value(qmap, anchor, "total_debt")
    assets_latest = _latest_value(qmap, anchor, "total_assets")
    current_assets_latest = _latest_value(qmap, anchor, "current_assets")
    current_liabilities_latest = _latest_value(qmap, anchor, "current_liabilities")
    cash_latest = _latest_value(qmap, anchor, "cash")
    # ROIC = NOPAT TTM (กำไรจากการดำเนินงาน หลังหักภาษี) ÷ Invested Capital (งวดล่าสุด) — สูตร
    # เดียวกับ ROIC (TTM) ใน "📌 อัตราส่วนหลัก" ของหน้างบรวมทุกแหล่ง (dashboard.js บรรทัด
    # ~17850) ทำตรงนี้ให้ตรงกันเป๊ะ (เดิมเคยลองใช้ flat 20% แทน แต่จะได้ตัวเลข ROIC ไม่ตรงกับ
    # ที่หน้าอื่นโชว์สำหรับหุ้นตัวเดียวกัน — clamp อัตราภาษี 0-60% กันไตรมาสขาดทุน/มีรายการพิเศษ
    # ทำให้ tax_expense/pretax_profit เพี้ยนสุดขั้ว)
    pretax_ttm = _ttm_sum(qmap, anchor, "pretax_profit")
    tax_ttm = _ttm_sum(qmap, anchor, "tax_expense")
    roic = None
    if (op_ttm is not None and pretax_ttm is not None and tax_ttm is not None
            and ibd_latest is not None and equity_latest is not None and cash_latest is not None):
        tax_rate = min(0.6, max(0, tax_ttm / abs(pretax_ttm))) if pretax_ttm != 0 else 0
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
        # งบดุล ณ จุดเวลา — ใช้งวดล่าสุดที่ "มีค่าจริง" (Yahoo ตามหลัง P&L 1 งวด) ไม่ใช่ last
        # ตรงๆ ไม่งั้นคอลัมน์ Assets/Equity/IBD เป็น "–" เมื่อไตรมาสใหม่สุดเป็น SET P&L ล้วน
        "q_cfo": last.get("cfo"), "total_assets": assets_latest,
        "total_equity": equity_latest, "ibd": ibd_latest,
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
        prior_ttm = _ttm_sum(qmap, anchor - 4, "net_profit")
        row["ttm_net_profit_yoy"] = _peer_group_pct_change(np_ttm, prior_ttm)
        row["net_profit_ttm_prior"] = prior_ttm
    # CAGR 3 ปีเป๊ะ (ต่างจาก rev_cagr/profit_cagr ของ factor_snapshot ที่เป็นเต็มช่วงข้อมูล) —
    # เทียบ TTM ปัจจุบันกับ TTM ที่จบเมื่อ 12 ไตรมาสก่อน ต้องมีอย่างน้อย 16 ไตรมาสถึงจะมีข้อมูล
    # พอทำ TTM สองช่วงที่ไม่ทับกัน (เฉพาะกรณีทั้งคู่เป็นบวก — ฐานลบ/ศูนย์ทำ CAGR ไม่มีความหมาย)
    if len(quarters) >= 16:
        rev_ttm_3y_ago = _ttm_sum(qmap, anchor - 12, "revenue")
        if rev_ttm and rev_ttm_3y_ago and rev_ttm > 0 and rev_ttm_3y_ago > 0:
            row["rev_cagr_3y"] = round(((rev_ttm / rev_ttm_3y_ago) ** (1 / 3) - 1) * 100, 2)
        np_ttm_3y_ago = _ttm_sum(qmap, anchor - 12, "net_profit")
        if np_ttm and np_ttm_3y_ago and np_ttm > 0 and np_ttm_3y_ago > 0:
            row["profit_cagr_3y"] = round(((np_ttm / np_ttm_3y_ago) ** (1 / 3) - 1) * 100, 2)
    return row


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
                qmap = _qmap(quarters)
                anchor = _qkey(quarters[-1])
                rev_ttm = _ttm_sum(qmap, anchor, "revenue")
                if not rev_ttm or rev_ttm <= 0:
                    continue
                for f in _SANKEY_SECTOR_PCT_FIELDS:
                    v = _ttm_sum(qmap, anchor, f)
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


_tearsheet_th_universe_cache: dict = {}


def _tearsheet_universe_map(mkt):
    """คืน {symbol: entry} ของตลาดที่ tearsheet รองรับ — TH จาก set_data.json (ทุกหุ้น),
    US/HK จาก us_index_metrics.json/hk_index_metrics.json (เฉพาะสมาชิกดัชนีหลัก S&P500+
    Dow+NDX / HSI+HSCEI+HSTECH ~623 ตัว — scope ที่ตัดสินใจไว้ใน PLAN_stock_study_suite.txt
    เพราะ mirror ทั้งก้อนไม่มีราคารายวัน/sector เก็บไว้ ยกเว้นกลุ่มดัชนีหลักนี้)
    field ชื่อเดียวกันทุกไฟล์ (ret_1d/rs_score/stage/price_history ฯลฯ) โค้ดข้างล่างเลยใช้ร่วมกันได้"""
    out = {}
    if mkt == "TH":
        # set_data.json ~12MB — cache ตาม mtime กัน parse ซ้ำทุก hit ของ /api/financials-sankey-sector
        # และ /api/financials-merged-report (path US/HK/JP) — dict อ่านอย่างเดียวใน call site เลย
        # แชร์ instance เดียวได้ (pattern เดียวกับ _dh_th_universe_cache / index_metrics_common.load_local)
        try:
            mtime = os.path.getmtime(DATA_FILE)
        except OSError:
            return out
        cached = _tearsheet_th_universe_cache.get("v")
        if cached is not None and _tearsheet_th_universe_cache.get("mtime") == mtime:
            return cached
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                for s in json.load(f).get("stocks", []):
                    out[s["symbol"]] = s
            _tearsheet_th_universe_cache.update(mtime=mtime, v=out)
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
            # mirror_candidates() อาจ cold-start ไป urlopen ที่ Finnomena (ไม่มี except ภายใน) และ
            # fetch_header() ไปดึง Yahoo สด — ทั้งคู่พังได้จาก network/timeout ไม่ใช่แค่ "ไม่พบหุ้น"
            # กันไม่ให้ 500 ทั้ง response เหมือน get_dividends/financials_store.get ด้านล่าง
            try:
                if not any(name == raw_sym for ex, name, _sid in financials_store.mirror_candidates((mkt,))):
                    return jsonify({"error": f"ไม่พบหุ้น {sym} ในตลาด {mkt}"}), 404
                from sources import mirror_ondemand
                s = mirror_ondemand.fetch_header(BASE_DIR, mkt, raw_sym)
            except Exception as e:
                return jsonify({"error": f"ดึงข้อมูลหุ้น {sym} ไม่สำเร็จ: {e}"}), 502
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
        # _factors_for/_div_cagr_5y เปิด sqlite connection เอง (busy_timeout=5000) เหมือน
        # get_dividends/financials_store.get ด้านล่าง — กัน "database is locked" ไม่ให้ทั้ง
        # response หายไปเพราะแค่ factor เสริม (เหตุผลเดียวกับ comment ที่ get_dividends ด้านล่าง)
        try:
            f = factor_snapshot._factors_for(BASE_DIR, lite_dr_sym, is_dr=True) or {}
            if f:
                f.setdefault("div_cagr_5y", factor_snapshot._div_cagr_5y(BASE_DIR, lite_dr_sym, "DR"))
        except Exception:
            f = {}
    else:
        try:
            if mkt == "TH":
                snap_rows = {r["symbol"]: r for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}
            else:
                snap_rows = {r["symbol"]: r for r in factor_snapshot.get_mirror_snapshot(BASE_DIR, mkt)}
        except Exception:
            snap_rows = {}
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

    # live_scalable: ค่านี้อิงราคาชุดเดียวกับ header.price (daily snapshot จาก set_data.json/
    # index_metrics) หรือไม่ — ถ้าใช่ frontend rescale ด้วย (ราคาสด ÷ header.price) ได้ตรง
    # ถ้าไม่ (ตกไปใช้ f.get('*_value') ของ Finnomena = ราคา ณ สิ้นไตรมาส) การ rescale ด้วย
    # header.price จะให้ ratio≈1 = โชว์ค่าไตรมาสเก่าโดยติดป้ายว่า "สด" (ดู _tsLoadLiveValuation)
    valuation = {
        "pe": {
            "value": s.get("pe") if s.get("pe") is not None else f.get("pe_value"),
            "live_scalable": s.get("pe") is not None,
            "percentile": f.get("pe_percentile"), "label": _label(f.get("pe_percentile")),
            "mean": f.get("pe_mean"), "median": f.get("pe_median"), "n": f.get("pe_n"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("pe")
                                             if (th_map.get(sy) or {}).get("pe") is not None
                                             else (snap_rows.get(sy) or {}).get("pe_value")),
        },
        "pbv": {
            "value": s.get("pbv") if s.get("pbv") is not None else f.get("pbv_value"),
            "live_scalable": s.get("pbv") is not None,
            "percentile": f.get("pbv_percentile"), "label": _label(f.get("pbv_percentile")),
            "mean": f.get("pbv_mean"), "median": f.get("pbv_median"), "n": f.get("pbv_n"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("pbv")
                                             if (th_map.get(sy) or {}).get("pbv") is not None
                                             else (snap_rows.get(sy) or {}).get("pbv_value")),
        },
        "ps": {
            # P/S ไม่เคยถูกเก็บใน set_data.json/index_metrics (0/930 TH, 0 US/HK/JP) — value นี้
            # มาจาก Finnomena สิ้นไตรมาสเสมอ → live_scalable=False ตายตัว
            "value": f.get("ps_value"), "live_scalable": False,
            "percentile": f.get("ps_percentile"),
            "label": _label(f.get("ps_percentile")),
            "mean": f.get("ps_mean"), "median": f.get("ps_median"), "n": f.get("ps_n"),
            "sector_median": _sector_median(lambda sy: (snap_rows.get(sy) or {}).get("ps_value")),
        },
        "div_yield": {
            # s.get('div_yield') = None ทั้งหมดสำหรับ US/HK/JP (0/518, 0/105, 0/225) — ต้อง
            # fallback มา factor snapshot เหมือน pe/pbv ไม่งั้นการ์ด "ปันผล%" ว่างทั้ง universe US/HK/JP
            "value": s.get("div_yield") if s.get("div_yield") is not None else f.get("div_yield"),
            "sector_median": _sector_median(lambda sy: (th_map.get(sy) or {}).get("div_yield")),
        },
        "ev_ebitda": {
            "value": f.get("ev_ebitda_value"), "percentile": f.get("ev_ebitda_percentile"),
            "label": _label(f.get("ev_ebitda_percentile")),
            "mean": f.get("ev_ebitda_mean"), "median": f.get("ev_ebitda_median"),
            "n": f.get("ev_ebitda_n"),
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
    z_variant = f.get("z_variant")
    if is_financial and mkt != "TH":
        # z_variant ต้อง null ด้วย ไม่ใช่แค่ z_score/z_zone — frontend (_tsQualityHtml) ใช้
        # "z_variant === null" เป็นตัวบ่งชี้ว่าถูก exclude แล้วโชว์ z_excluded_reason แทน ถ้า
        # ปล่อย z_variant เดิมไว้ (ไม่ null เพราะ factor_snapshot_mirror ไม่รู้ sector ตอนคำนวณ)
        # จะไม่เข้าเงื่อนไขไหนเลย ทำให้ทั้ง Z-Score tile และคำอธิบายเหตุผลหายไปเงียบๆ
        z_score, z_zone, z_variant = None, None, None
        z_reason = "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"

    quality = {
        "roe": f.get("roe"), "de_ratio": f.get("de_ratio"), "gross_margin": f.get("gross_margin"),
        "fcf_yield": round(fcf / mkt_cap * 100, 2) if (fcf is not None and mkt_cap) else None,
        "dividend_coverage": f.get("dividend_coverage"),
        "f_score": f.get("f_score"), "f_score_max": f.get("f_score_max"),
        "f_score_detail": f.get("f_score_detail"),
        "z_score": z_score, "z_variant": z_variant, "z_zone": z_zone,
        "z_excluded_reason": z_reason,
        # cash_cycle จาก Yahoo (factor_snapshot) — ใช้ cross-check กับ q_cash_cycle
        # ทางการของ SET ในการ์ด "🩺 สุขภาพการเงิน (SET)" (ดู PLAN_set_api_expansion.txt
        # งาน #5C) None สำหรับ mirror US/HK ที่ cash_cycle ไม่น่าเชื่อถือ (ดู
        # factor_snapshot.py) และ mirror อยู่แล้วเพราะไม่ใช่หุ้นไทย ไม่โชว์การ์ดนี้อยู่ดี
        "cash_cycle": f.get("cash_cycle"),
    }

    # _mirror_sym ตัด suffix ".HK" ก่อนเสมอ — ตาราง dividends เก็บรหัสดิบไม่มี suffix
    # เหมือน factor_snapshot mirror (ดู sync_dividends_batch/get_dividends_endpoint)
    # lite: dividends อยู่ใต้ market "DR" (key = DR sym) ถ้าเคย sync จากหน้าปันผล/batch
    # get_dividends()/financials_store.get() เปิด connection ใหม่ (busy_timeout=5000) — ถ้า
    # background job (sync-all/mirror) กำลังเขียน financials.db นานเกิน 5s พอดี อาจได้
    # sqlite3.OperationalError: database is locked ต้องกันไว้ไม่ให้ Tearsheet ทั้งหน้า 500
    # (pattern เดียวกับ /api/financials-full, /api/calendar-events) — เคสนี้เสี่ยงกว่าที่อื่น
    # เพราะ Watchlist ยิง Tearsheet header หลายหุ้นพร้อมกันผ่าน Promise.all
    try:
        div_rows, _div_synced = financials_store.get_dividends(BASE_DIR, fkey, "DR" if lite else mkt)
    except Exception:
        div_rows, _div_synced = [], None
    dividend = {
        # เหมือน valuation.div_yield — s.get('div_yield') ว่างทั้ง US/HK/JP, fallback factor snapshot
        "yield": s.get("div_yield") if s.get("div_yield") is not None else f.get("div_yield"),
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
        # เหตุผลเดียวกับ get_dividends ด้านบน — กัน sqlite "database is locked" ไม่ให้ทั้ง
        # response (header/valuation/quality ที่คำนวณสำเร็จแล้ว) หายไปเพราะแค่ field เสริม
        # (DCF forecast) พังตอน DB ถูกล็อกชั่วคราว
        try:
            y_payload = financials_store.get(BASE_DIR, yahoo_key, "yahoo", is_dr=yahoo_is_dr)
        except Exception:
            y_payload = None
        if y_payload:
            try:
                dcf["forecast"] = financials_store.compute_dcf_forecast_inputs(y_payload)
            except Exception:
                pass

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

    # CG/ESG rating (SET.or.th ทางการ) — badge ในหัว Tearsheet เฉพาะหุ้นไทยที่มีเรตติ้ง
    # (ดู PLAN_set_api_expansion.txt งาน #3B) None = ไม่โชว์ badge เลย (SET ครอบ ~38% ของกระดาน
    # เท่านั้น ไม่มีค่า ≠ คะแนนแย่) · ต้องเคยกด sync ESG ในหน้า Screener+ ก่อนถึงจะมีข้อมูล
    esg = set_company.get_esg(BASE_DIR, sym) if (mkt == "TH" and not lite) else None

    try:
        meta_computed_at = (factor_snapshot.snapshot_meta(BASE_DIR).get("computed_at") if mkt == "TH"
                             else factor_snapshot.mirror_snapshot_meta(BASE_DIR).get("computed_at"))
    except Exception:
        meta_computed_at = None
    return jsonify({
        "symbol": sym, "market": mkt, "header": header, "valuation": valuation, "quality": quality,
        "dividend": dividend, "dcf": dcf, "valuation_models": valuation_models, "dr_symbol": dr_symbol,
        "esg": esg,
        "lite": lite, "underlying_yf": lite_yf, "yf_symbol": yf_symbol,
        "meta": {"computed_at": meta_computed_at,
                 "has_factors": bool(f) if lite else (_mirror_sym(mkt, sym) in snap_rows)},
    })


_dcf_screener_rebuild_lock = threading.Lock()
_pbv_pe_screener_rebuild_lock = threading.Lock()


@app.route("/api/dcf-screener")
def dcf_screener_endpoint():
    """🎯 DCF Screener — ผลลัพธ์ DCF Model (พยากรณ์เต็มรูปแบบ) ที่คำนวณไว้แล้วทั้งตลาดหุ้นไทย
    (ดู sources/dcf_screener.py) อ่านจาก cache ในตาราง dcf_screener ตรงๆ ไม่คำนวณสด —
    กดปุ่ม "⟳ คำนวณ DCF ใหม่ทั้งตลาด" (POST /api/dcf-screener/rebuild) ก่อนถึงจะมีข้อมูล"""
    rows = dcf_screener.get_snapshot(BASE_DIR)
    return jsonify({"rows": rows, "meta": dcf_screener.snapshot_meta(BASE_DIR, rows=rows)})


@app.route("/api/dcf-screener/rebuild", methods=["POST"])
def dcf_screener_rebuild():
    """คำนวณ DCF ใหม่ทั้งตลาด — ไม่ยิง network เพิ่ม (อ่านจากงบที่ sync ไว้แล้ว) จึงรันแบบ sync
    ในคำขอเดียวได้เลย ไม่ต้อง background thread/progress bar เหมือน sync งบชุดใหญ่

    body (ไม่ใส่ = ใช้ค่าเริ่มต้นทั้งหมด): {rf_pct, beta, erp_pct, terminal_growth_pct, years,
    g13_pct, g45_pct, ebit_margin_pct, tax_rate_pct, da_pct, capex_pct, nwc_pct, wacc_pct,
    use_analyst_growth} — resolve_assumptions() clamp/กันค่าพังให้แล้วฝั่ง dcf_screener

    ล็อกกันคำขอซ้อน (2 แท็บ/double-submit) ยิง rebuild พร้อมกัน — ตัวหลังรอคิวแทนที่จะรัน
    DELETE+INSERT ชนกัน ผลลัพธ์สุดท้ายไม่แน่นอนว่าตาราง dcf_screener จะเหลือ assumptions ชุดไหน"""
    body = request.get_json(silent=True) or {}
    with _dcf_screener_rebuild_lock:
        result = dcf_screener.build_snapshot(BASE_DIR, assumptions=body)
    return jsonify(result)


@app.route("/api/pbv-pe-screener")
def pbv_pe_screener_endpoint():
    """⚖️ Fair Value P/B·P/E — ผล Justified P/B + Justified P/E (ROE-based) ที่คำนวณไว้แล้ว
    ทั้งตลาดหุ้นไทย (ดู sources/pbv_pe_screener.py) อ่านจาก cache ในตาราง pbv_pe_screener ตรงๆ
    ไม่คำนวณสด — กดปุ่ม "⟳ คำนวณใหม่ทั้งตลาด" (POST /api/pbv-pe-screener/rebuild) ก่อนถึงจะมีข้อมูล"""
    rows = pbv_pe_screener.get_snapshot(BASE_DIR)
    return jsonify({"rows": rows, "meta": pbv_pe_screener.snapshot_meta(BASE_DIR, rows=rows)})


@app.route("/api/pbv-pe-screener/rebuild", methods=["POST"])
def pbv_pe_screener_rebuild():
    """คำนวณ Justified P/B + Justified P/E ใหม่ทั้งตลาด — ไม่ยิง network เพิ่ม (อ่านจาก
    factor_snapshot + set_data.json ที่มีอยู่แล้ว) จึงรันแบบ sync ในคำขอเดียวได้เลย

    body (ไม่ใส่ = ใช้ค่าเริ่มต้นทั้งหมด): {g_pct, rf_pct, beta, erp_pct, coe_pct, roe_pct}
    — resolve_assumptions() clamp/กันค่าพังให้แล้วฝั่ง pbv_pe_screener

    ล็อกกันคำขอซ้อน (2 แท็บ/double-submit) ยิง rebuild พร้อมกัน — เหมือน dcf_screener_rebuild
    ด้านบน (ตัวหลังรอคิวแทนที่จะรัน DELETE+INSERT ชนกัน)"""
    body = request.get_json(silent=True) or {}
    with _pbv_pe_screener_rebuild_lock:
        result = pbv_pe_screener.build_snapshot(BASE_DIR, assumptions=body)
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
    try:
        return jsonify(factor_snapshot.get_mirror_symbols(BASE_DIR))
    except Exception:
        # factor_snapshot_mirror table ถูก DELETE+bulk INSERT ทับระหว่างงาน "Mirror ทั้งตลาด"
        # — ถ้าเจอ DB locked พอดี fallback เป็นค่าว่างเหมือน mirror_names() แทนที่จะ 500
        return jsonify({})


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
    def _fresh_cache():
        c = _us_index_diff_cache.get("result")
        if c and (time.time() - _us_index_diff_cache.get("ts", 0) < _US_INDEX_DIFF_CACHE_TTL):
            return c
        return None

    cached = _fresh_cache()
    if cached:
        return jsonify(cached)
    # กัน worker ค้างซ้อน — มีคนกำลังเช็คกับ Wikipedia อยู่แล้ว รอไม่เกิน ~2s เผื่อมันเสร็จพอดี
    # (จะได้ตอบจาก cache) ไม่งั้นแจ้งให้ลองใหม่แทนที่จะเปิด urlopen ชุดใหม่ซ้อนเข้าไปอีก
    if not _us_index_diff_lock.acquire(timeout=2):
        cached = _fresh_cache() or _us_index_diff_cache.get("result")
        if cached:
            return jsonify(cached)
        return jsonify({"error": "กำลังเช็คกับ Wikipedia อยู่ โปรดลองใหม่อีกครั้งในสักครู่"}), 503
    try:
        cached = _fresh_cache()
        if cached:
            return jsonify(cached)
        mirror_us = factor_snapshot.get_mirror_symbols(BASE_DIR).get("US", [])
        result, _live = us_index_membership.diff_membership(BASE_DIR, mirror_us=mirror_us)
        _us_index_diff_cache["result"] = result
        _us_index_diff_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        _us_index_diff_lock.release()


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
                                               skip_up_to_date=True, auto_rebuild=False)
            if result.get("ok", 0) > 0:
                # สมาชิกดัชนีใหม่ที่เพิ่ง sync งบ (namespace DR:) ต้องเข้า factor_snapshot ฝั่ง DR
                # ด้วย ไม่งั้น Tearsheet/Peer ของตัวใหม่โชว์ค่าเก่า/ว่าง — เดิม route นี้ล้าง cache
                # เฉยๆ ไม่ rebuild (mirror FINN: ไม่กระทบ เพราะ route นี้ไม่เขียน FINN: keys)
                _update(message="กำลังคำนวณ factor snapshot ใหม่...")
                try:
                    factor_snapshot.build_snapshot(BASE_DIR)
                except Exception as e:
                    print(f"[USIndexSync] build_snapshot ล้มเหลว (งบ sync สำเร็จแล้ว): {e}")
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
                                               skip_up_to_date=True, auto_rebuild=False)
            if result.get("ok", 0) > 0:
                # สมาชิกดัชนีใหม่ (namespace DR:) ต้องเข้า factor_snapshot ฝั่ง DR — ดูคอมเมนต์
                # ที่ US index sync ด้านบน
                _update(message="กำลังคำนวณ factor snapshot ใหม่...")
                try:
                    factor_snapshot.build_snapshot(BASE_DIR)
                except Exception as e:
                    print(f"[HKIndexSync] build_snapshot ล้มเหลว (งบ sync สำเร็จแล้ว): {e}")
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
        except (ValueError, TypeError):
            # ValueError = สตริงผิดฟอร์แมต · TypeError = updated_at ไม่ใช่สตริง (ไฟล์ถูกแก้มือ/
            # เสียหายจนเป็นตัวเลข/None-in-list) — /api/us-index-heatmap กับ /api/jp-index-heatmap
            # ไม่มี try/except ครอบ route จึงกลายเป็น 500 ทั้งหน้าแทนที่จะ fallback เป็นเวลาปัจจุบัน
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
# n_fetched/n_live ตั้ง key ไว้ตั้งแต่ต้น (ค่า None) แม้ยังไม่เคยรันสักรอบ — เดิม POST รอบแรก
# ต่อ region จะ state.update(...) เพิ่ม key จาก 3 เป็น 5 ตัว ระหว่างที่ GET /heatmap-live-status
# อีก request กำลัง jsonify dict ก้อนเดียวกันอยู่ ชนกันเป็น "dictionary changed size during
# iteration" (RuntimeError 500) ทำให้ poll loop ฝั่ง frontend พังทั้งที่ live-update เดินต่อได้
_HM_LIVE_STATE = {
    "US": {"running": False, "error": None, "done": False, "n_fetched": None, "n_live": None},
    "HK": {"running": False, "error": None, "done": False, "n_fetched": None, "n_live": None},
    "JP": {"running": False, "error": None, "done": False, "n_fetched": None, "n_live": None},
}
# กัน race check-then-set ต่อ region (เดิมไม่มี lock — กดปุ่ม Live ซ้ำเร็วๆ/หลายแท็บ
# พร้อมกันในภูมิภาคเดียวกันชนกันยิง Yahoo ซ้อนได้ ก่อน running ถูกตั้งจริง) — ใช้ _lock ตัว
# เดียวกับ us/hk/jp_index_full_refresh แทนที่จะแยกล็อกเอง (เดิมเคยมี _hm_live_lock ของตัวเอง
# แต่สอง handler เช็ค flag ของกันและกันคนละ lock กัน ยังมี TOCTOU race เหลืออยู่ได้ — read/write
# ของทั้ง _state["running"] และ _HM_LIVE_STATE[region]["running"] ต้องอยู่ใน critical section
# เดียวกันถึงจะกันได้จริง)

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
            scope_n = len(scope) or 1
            if n < scope_n * 0.5:
                # ดึงข้อมูลกลับมาได้ไม่ถึงครึ่งของ scope ที่ขอ — fetch_gap_batch เงียบๆ ข้าม
                # หุ้นจำนวนมาก (แค่ print error ไม่ raise) = Yahoo rate-limit/บล็อกจริง
                state["error"] = ("ตลาดยังเปิดอยู่แต่ดึงราคาจาก Yahoo ได้ไม่ถึงครึ่ง "
                                   f"({n}/{scope_n} ตัว) — Yahoo Finance อาจ rate-limit ชั่วคราว "
                                   "(กดถี่เกินไป) หรือเน็ตมีปัญหา ลองรอสัก 1-2 นาทีแล้วกดใหม่")
            elif live_map and len(live_map) < n * 0.2:
                state["error"] = (f"ตลาดยังเปิดอยู่แต่ได้ราคา Live แค่ {len(live_map)}/{n} ตัว — "
                                   "Yahoo Finance น่าจะ rate-limit บางส่วน หุ้นที่เหลือจะโชว์ % ปิด"
                                   "เมื่อวานแทนราคาสด ลองรอสัก 1-2 นาทีแล้วกดใหม่")
            # live_map ว่างเปล่าทั้งที่ n เกือบเต็ม scope = วันนี้ตลาดไม่ได้เปิดเทรด (เสาร์-อาทิตย์
            # หรือวันหยุดตลาด — is_latest_bar_stable ไม่รู้จักปฏิทินวันหยุด เลยยังนับว่าเป็นเวลา
            # ทำการ) Yahoo ไม่มีแท่งของ "วันนี้" ให้สักตัว ราคาปิดล่าสุดถูกต้องอยู่แล้ว — ไม่ใช่
            # rate-limit ไม่ต้อง error (เดิม len(live_map) < n*0.2 → 0 < ~100 เป็นจริงเสมอ
            # เด้ง error หลอกทุกวันหยุดตลาดที่ผู้ใช้เผลอกดปุ่มช่วงเวลาทำการ)
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
    with _lock:
        if state["running"]:
            return jsonify({"status": "running"})
        # เช็ค _state["running"] (ล็อกงานยาวหลัก เช่น Index Max/Quick Update) ด้วย ภายใต้
        # _lock เดียวกับที่ us/hk/jp_index_full_refresh ใช้ check-then-set ของตัวเอง — กัน
        # งานซ้อนกับ full-refresh ของตลาดเดียวกันได้จริง (ทั้งสองเขียน {region}_prices.db/
        # {region}_index_metrics.json ไฟล์เดียวกัน) ต่างจาก "running" (งาน live-update ของ
        # region นี้เองกำลังทำอยู่แล้ว — poll ต่อได้ปกติ) ตรงที่ไม่เคยเริ่มงานอะไรเลย จึง return
        # "busy" แยกเพื่อไม่ให้ frontend เข้าใจผิดว่ามีงานให้ poll แล้วโชว์ผลรอบเก่าซ้ำ
        if _state["running"]:
            return jsonify({"status": "busy"})
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
    if market == "JP":
        return sym + ".T"   # Yahoo ต้องมี suffix .T ไม่งั้นหาข่าวหุ้นญี่ปุ่นไม่เจอ (ดู fetch_yahoo_full)
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
    from sources.yahoo import _TimeoutSession
    rows = []
    for n in (yf.Ticker(yf_ticker, session=_TimeoutSession()).news or []):
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
            stripped = title[:-(len(publisher) + 3)].strip()
            if stripped:   # กัน title ที่เป็น " - ชื่อสำนัก" ล้วน (ตัดแล้วเหลือ '') → row ว่างคลิกไม่ได้
                title = stripped
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
        # deadline ร่วม 30 วิ สำหรับทุกแหล่ง (ไม่ใช่ 30 วิ/แหล่ง) — เดิม f.result(timeout=30)
        # ต่อ future ทีละตัว ถ้า yahoo ค้าง 30 วิ แล้ว google ค้างอีก 30 set อีก 30 รวมเป็น ~90 วิ
        # เกิน _fetchTimeout ฝั่ง frontend (35 วิ) หน้าเว็บขึ้น "โหลดข่าวไม่สำเร็จ" ทั้งที่ backend
        # ยังวิ่งอยู่ · ตอนนี้รวมทุกแหล่งไม่เกิน ~30 วิ แหล่งที่ยังไม่เสร็จนับเป็น error ของแหล่งนั้น
        deadline = time.monotonic() + 30
        for name, f in futs.items():
            try:
                rows.extend(f.result(timeout=max(0.0, deadline - time.monotonic())))
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
        entry = dict(result)
        if errors:
            # ผลไม่ครบ (บางแหล่ง rate-limit/ค้างชั่วคราว) — cache สั้นลงเหลือ ~3 นาที
            # ด้วยการ backdate ts ของ cache (ไม่กระทบ ts ที่ส่งให้ client โชว์ "ณ X ที่แล้ว")
            # เผื่อแหล่งที่ล่มกลับมาปกติแล้วผู้ใช้จะได้ผลครบโดยไม่ต้องรอเต็ม 15 นาที
            entry["ts"] = time.time() - (_STOCK_NEWS_CACHE_TTL - 180)
        _stock_news_cache[cache_key] = entry
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
            # ยก rs_history ที่สะสมมาต่อ — เดิมสร้าง dict ใหม่ทิ้ง key นี้ ทำให้ทุกรอบ
            # Quick Update/Full Refresh _compute_idx_rs เห็น hist=[] แล้ว regenerate จาก
            # weekly backfill ใหม่ทั้งชุด จุดที่สะสมรายวัน (RS Trend arrow) ไม่เคยอยู่รอด
            # (rs_set คำนวณใหม่ทุกรอบอยู่แล้วใน _compute_idx_rs ไม่ต้องยกมา)
            "rs_history": (entry or {}).get("rs_history", []),
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

    # ลบดัชนีที่ถูกถอดออกจาก INDEX_INFO แล้ว (เช่น ^MINE.BK ที่หุ้นตัวสุดท้ายใน sector
    # เพิกถอนไปหมด) — เดิม result เริ่มจาก dict(existing) แล้ว fetch loop ข้างบนวิ่งตาม
    # INDEX_INFO ปัจจุบันเท่านั้น ทำให้ตัวที่ถอดออกไม่เคยถูกอัปเดต "และ" ไม่เคยถูกลบออก
    # ค้างเป็นค่าเก่าแช่แข็งถาวรในไฟล์ (พบ ^MINE.BK ค้างตั้งแต่ 2026-08-01 บนเว็บมือถือ/ไอแพด)
    for stale_sym in [s for s in result if s not in INDEX_INFO]:
        del result[stale_sym]

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


def _fetch_indices_tv_guarded(existing: dict, full_refresh: bool = False):
    """เรียก _fetch_indices_tv ภายใต้ _indices_job_lock/_indices_job_state เสมอ — เดิม
    /api/indices-quick-update และ /api/indices-refresh คุมด้วย lock/state คู่นี้ แต่
    _run_refresh/_run_quick (Full Refresh/Quick Update หลัก) เรียก _fetch_indices_tv ตรงๆ
    ข้าม lock ไปเลย ถ้าผู้ใช้กดปุ่มอัปเดตดัชนีเองพร้อมกับ Full Refresh/Quick Update พอดี
    จะได้ 2 thread เขียน _indices_cache/indices_cache.json แข่งกัน (lost update) — รวมทุก
    caller มาผ่านจุดเดียวนี้กัน race ให้ครบทุกทาง คืน (None, None) ถ้ามี job อื่นถืออยู่แล้ว
    (caller เดิม catch Exception เป็น warning อยู่แล้ว ไม่ทำให้ Full Refresh/Quick Update ทั้งรอบล้ม)"""
    with _indices_job_lock:
        if _indices_job_state["running"]:
            print("[Indices] ข้ามอัปเดตดัชนี — มี indices job อื่นกำลังรันอยู่แล้ว")
            return None, None
        _indices_job_state["running"] = True
    try:
        return _fetch_indices_tv(existing, full_refresh=full_refresh)
    finally:
        with _indices_job_lock:
            _indices_job_state["running"] = False


# payload ~4.5MB ไม่บีบอัด (เต็มไปด้วย dates/closes รายวันของทุกดัชนี ซึ่ง frontend ใช้ทำกราฟ
# ใน openIdxChartModal จริง ตัดทิ้งแบบ _dr_light ไม่ได้) — cache gzip ไว้ต่อ id(data) ก้อนเดียว
# กัน compress ซ้ำทุก request (data ถูก reassign เป็น dict ใหม่ทุกครั้งที่เนื้อหาเปลี่ยนจริง)
# lock คุ้ม check+compute+write ก้อนเดียว แล้วอ่าน raw/gz เป็น local var ก่อนปล่อย lock — กัน
# request คนละ snapshot (waitress หลาย thread) แย่ง id/raw/gz ของกันและกันตอน refresh คาบเกี่ยว
_indices_gz_cache = {"id": None, "raw": None, "gz": None}
_indices_gz_lock = threading.Lock()


def _indices_response(data):
    import gzip as _gzip
    key = id(data)
    with _indices_gz_lock:
        if _indices_gz_cache["id"] != key:
            raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
            gz = _gzip.compress(raw, compresslevel=6)
            _indices_gz_cache.update(id=key, raw=raw, gz=gz)
        else:
            raw = _indices_gz_cache["raw"]
            gz = _indices_gz_cache["gz"]
    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        return Response(gz, mimetype="application/json",
                        headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"})
    return Response(raw, mimetype="application/json")


@app.route("/api/indices")
def get_indices():
    """เสิร์ฟข้อมูลดัชนีจากไฟล์ หรือดึงใหม่ถ้าไม่มีไฟล์"""
    global _indices_cache
    data = _indices_cache.get("data")
    # เกจความพร้อม RS: ใช้ ^SET.BK (ประวัติ ~20 ปีเสมอ → rs_set ไม่เคยเป็น None) ไม่ใช่
    # entry แรกของ dict ซึ่งลำดับสุ่มตาม completion order ตอนสร้างไฟล์ครั้งแรก — ถ้า entry
    # แรกบังเอิญเป็นดัชนี sector ประวัติสั้น (rs_set=None) fast path ไม่เคยเข้าเลย ทุก
    # request จะ re-read ไฟล์ + _compute_idx_rs (rank ~1200 หุ้น/ดัชนี) + เขียน disk ซ้ำ
    def _rs_gate(d):
        return (d or {}).get("^SET.BK") or next(iter((d or {}).values()), {})
    g = _rs_gate(data)
    # ส่งจาก memory cache ถ้ามี rs_set และ rs_history ครบแล้ว
    if data and g.get("rs_set") is not None and len(g.get("rs_history", [])) >= 4:
        return _indices_response(data)
    # โหลดจากไฟล์ (หรือ recompute ถ้า rs_history ยังน้อย)
    if os.path.exists(INDICES_FILE):
        try:
            with open(INDICES_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            data = saved["data"]
            g2 = _rs_gate(data)
            need_rs = g2.get("rs_set") is None or len(g2.get("rs_history", [])) < 4
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
    if (_state["running"] or _dr_refresh_state["running"] or _indices_job_state["running"]
            or _flow_fetch_state["running"]):
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

def _dh_esc(s):
    """escape ตัวแปรภายนอก (ticker/ชื่อบริษัทจาก scrape) ก่อนปนกับ HTML literal ใน note —
    ป้องกัน XSS ถ้าแหล่งข้อมูล (Wikipedia/SET.or.th) มี &<>"' ปนในชื่อ (code review 2026-09-02
    กลุ่ม A) — escape rule เดียวกับ _escHtml ฝั่ง dashboard.js"""
    return html.escape(str(s), quote=True)

def _dh_pct_color(pct, ok=99.5, warn=90):
    """สูตร %→สี ใช้ร่วม 3 จุด (financials_coverage_by_source/financials_quarter_freshness/
    us_index_coverage_by_index) — เดิมพิมพ์ if/else ซ้ำทั้ง 3 ที่ (code review 2026-09-02 กลุ่ม
    B2) threshold ต่าง default ได้ต่อ call site (financials_quarter_freshness ใช้ 95/50)"""
    return "var(--green)" if pct >= ok else ("var(--yellow)" if pct >= warn else "var(--red)")

def _dh_pct_status(pct, ok=99.5, warn=90):
    """สูตร %→status (ok/warn/red) คู่กับ _dh_pct_color threshold เดียวกัน"""
    return "ok" if pct >= ok else ("warn" if pct >= warn else "red")

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


def _dh_recent_business_day_cutoff(today, n=2):
    """คืนวันที่ก่อนหน้า "วันทำการล่าสุด n วัน" (จ-ศ เท่านั้น ไม่รู้จักวันหยุดนักขัตฤกษ์ เหมือน
    เกณฑ์อื่นในไฟล์นี้) — ใช้ตัดวันทำการล่าสุดออกจากการเช็ค "รูขาดข้อมูล" ของ _dh_gap_check_item
    (code review 2026-09-02): เดิมใช้ `today - timedelta(days=2)` ตรงๆ ซึ่งเป็น calendar days
    ไม่ใช่ business days — เช็คเช้าวันจันทร์จะตัดออกแค่เสาร์-อาทิตย์ (ที่ถูกกรองทิ้งอยู่แล้วโดย
    weekday<5) ทำให้วันศุกร์ (วันทำการล่าสุดจริง) ยังโดนเช็คอยู่ ทั้งที่ตั้งใจยกเว้น 2 วันทำการ
    ล่าสุดเสมอไม่ว่าจะมีวันหยุดสุดสัปดาห์คั่นหรือไม่"""
    d = today
    seen = 0
    while seen < n:
        if d.weekday() < 5:
            seen += 1
            if seen == n:
                return d - timedelta(days=1)
        d -= timedelta(days=1)
    return d


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
    cutoff = _dh_recent_business_day_cutoff(today, 2)
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
            sample = ', '.join(_dh_esc(s) for s in new_sample[:8])
            more = '…' if d["new_count"] > len(new_sample) else ''
            parts.append(f"ใหม่ {d['new_count']} ตัว ({sample}{more})")
        if d.get("removed_count"):
            rem_sample = d.get("removed_sample") or []
            sample = ', '.join(_dh_esc(s) for s in rem_sample[:8])
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
    # rank ความรุนแรง ใช้ max แทนการทับตรงๆ (code review 2026-09-02) — เดิม status ถูก
    # assign ทับตรงๆ ทุกจุด ทำให้ check หลังๆ (missing-from-metrics/sector Unknown ≤3 ตัว)
    # ลด severity จาก red (membership.json รูปแบบผิดปกติ) ลงมาเป็น warn ได้ ทั้งที่ปัญหาแรก
    # ยังไม่หาย — ใช้ _dh_bump ไล่เก็บค่าแย่สุดแทน
    _sev_rank = {"ok": 0, "na": 0, "warn": 1, "red": 2}
    def _dh_bump(new_status):
        nonlocal status
        if _sev_rank.get(new_status, 0) > _sev_rank.get(status, 0):
            status = new_status
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
            _dh_bump("red")
            problems.append(f"{k} รูปแบบผิดปกติ ({type(members).__name__} ไม่ใช่ list)")
            continue
        n = len(members)
        union |= set(members)
        lo, hi = bounds.get(k, (0, 10 ** 9))
        if not (lo <= n <= hi):
            _dh_bump("warn")
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
        _dh_bump("warn")
        more = '…' if len(missing) > 8 else ''
        problems.append(f"หายจาก metrics {len(missing)} ตัว ({', '.join(_dh_esc(s) for s in missing[:8])}{more})")

    unknown = [s.get("symbol") for s in stocks if not s.get("sector") or s.get("sector") == "Unknown"]
    if unknown:
        _dh_bump("red" if len(unknown) > 3 else "warn")
        more = '…' if len(unknown) > 8 else ''
        problems.append(f"sector Unknown {len(unknown)} ตัว ({', '.join(_dh_esc(s) for s in unknown[:8])}{more})")

    note = " · ".join(problems) if problems else f"{len(union)} ตัว ({market_code}) ครบ ไม่มี sector Unknown"
    return _dh_quality_item(key, label, status, note)


_dh_th_universe_cache = {"mtime": None, "tickers": None, "stocks": None, "data_as_of": None}


def _dh_th_universe_data():
    """โหลด+cache set_data.json ทั้งก้อนตาม mtime (tickers derive แล้ว + stocks ดิบ +
    data_as_of) — ใช้ร่วมกันระหว่าง _dh_th_universe_tickers() กับ block q_set_data กัน parse
    JSON ~12MB ซ้ำ 2 รอบต่อการ compute data health 1 ครั้ง (code review 2026-09-02 กลุ่ม B3)
    raise เมื่ออ่าน/parse ไฟล์ไม่ได้ (caller ที่ต้องรายละเอียด error จัดการเอง — ดู block
    q_set_data) หรือไฟล์หาย (os.path.getmtime OSError)"""
    mtime = os.path.getmtime(DATA_FILE)
    if _dh_th_universe_cache["mtime"] == mtime:
        return _dh_th_universe_cache
    with open(DATA_FILE, encoding="utf-8") as f:
        d = json.load(f)
    stocks = d.get("stocks") or []
    tickers = {s["ticker"] for s in stocks if s.get("ticker")}
    _dh_th_universe_cache.update(mtime=mtime, tickers=tickers, stocks=stocks,
                                  data_as_of=d.get("data_as_of"))
    return _dh_th_universe_cache


def _dh_th_universe_tickers():
    """คืน set ของ ticker (.BK) ที่เป็นหุ้นสามัญจริงในดัชนี SET/mai หลัก (~930 ตัว) — อ่านจาก
    set_data.json (cache ตาม mtime กันอ่านซ้ำทุกครั้งที่เปิดหน้า Data Health) ไม่ใช้ทุก
    ticker ใน set_prices.db ตรงๆ เพราะ DB เก็บปนกับ derivative warrant/DR/กองทุนที่ดึงผ่าน
    ฟีเจอร์อื่น (เจอจริง: set_prices.db มี 1,814 ticker ทั้งที่หุ้นสามัญจริงมีแค่ ~930 ตัว —
    DW พวกนี้เทรดเบาบางมาก ราคานิ่งซ้ำกันหลายวันเป็นเรื่องปกติ ถ้าไม่กรองออกจะ false-positive
    ชนกับเช็ค 'ราคาซ้ำผิดปกติ' ด้านล่างทันที) คืน None ถ้าอ่านไม่ได้ (caller ควร skip filter)"""
    try:
        data = _dh_th_universe_data()
    except Exception:
        return None
    return data["tickers"]


def _dh_stale_close_check(key, label, db_path, dup_threshold=20, min_universe=30, universe=None,
                           universe_required=False):
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

    universe_required — True ถ้า dup_threshold ที่ส่งมาถูก calibrate เฉพาะ universe ที่กรองแล้ว
    เท่านั้น (เช่น TH dup_threshold=600) — ถ้า universe เป็น None (โหลด set_data.json ไม่สำเร็จ
    ชั่วคราว) จะข้ามการเช็ครอบนี้เป็น "na" แทนที่จะ fallback ไปสแกนทั้ง DB แบบไม่กรอง (เดิม
    `if universe else [...]` จะ fallback แบบเงียบๆ กวาดทุก ticker รวม DW/warrant เข้าไปในการ
    นับ ทั้งที่ threshold=600 ถูก calibrate ไว้เฉพาะ ~929 ตัวหลังกรอง — ปน DW เข้ามาแล้วนับด้วย
    threshold เดิมจะ false-positive/false-negative ปนกันโดยไม่มีสัญญาณเตือนเลยว่ากำลังใช้ universe
    ผิดชุด, code review 2026-09-02)

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
    if universe_required and universe is None:
        return _dh_quality_item(key, label, "na",
            "ข้ามการเช็ครอบนี้ — โหลด universe หุ้นไทยที่กรองแล้วไม่ได้ (set_data.json อ่านไม่ได้"
            "ชั่วคราว เช่น Quick Update กำลังเขียนทับพอดี) threshold ที่ตั้งไว้ calibrate เฉพาะ"
            " universe ที่กรองแล้วเท่านั้น — ลองเช็คใหม่อีกครั้ง")
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

_dh_cc_cache = {"cc": None, "ts": 0}
_dh_cc_lock  = threading.Lock()


def _get_cash_cycle_crosscheck_cached(fresh=False):
    """cache ผลดิบของ _compute_cash_cycle_crosscheck() แยกจาก _data_health_cache (TTL เดียวกัน
    คือ _DATA_HEALTH_TTL) — ทั้ง _compute_data_health (badge สรุปใน /api/data-health) และ route
    /api/data-health/cash-cycle-crosscheck-full (ตารางเต็มตอนกด "และอีก N ตัว") เรียกผ่าน
    ฟังก์ชันนี้แทนเรียก _compute_cash_cycle_crosscheck() ตรงๆ กันคำนวณ full-table scan +
    JSON deserialize ~900 แถวซ้ำสิ่งที่เพิ่งคำนวณไปหมาดๆ ภายในไม่กี่วินาที (code review
    2026-09-02 กลุ่ม B5) — ใช้ cache เดียวกันทั้งสองจุดเพื่อไม่ให้ badge สรุปกับตารางเต็มเห็น
    ตัวเลขไม่ตรงกัน (invalidate cache เดียวกัน ไม่ใช่คนละ TTL แยกอิสระ) fresh=True (จาก
    /api/data-health?fresh=1) บังคับคำนวณใหม่เสมอ"""
    if not fresh:
        cached = _dh_cc_cache.get("cc")
        if cached is not None and (time.time() - _dh_cc_cache.get("ts", 0) < _DATA_HEALTH_TTL):
            return cached
    with _dh_cc_lock:
        if not fresh:
            cached = _dh_cc_cache.get("cc")
            if cached is not None and (time.time() - _dh_cc_cache.get("ts", 0) < _DATA_HEALTH_TTL):
                return cached
        cc = _compute_cash_cycle_crosscheck()
        _dh_cc_cache["cc"] = cc
        _dh_cc_cache["ts"] = time.time()
        return cc


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
        result = _compute_data_health(fresh=fresh)
        _data_health_cache["result"] = result
        _data_health_cache["ts"] = time.time()
        return jsonify(result)


@app.route("/api/data-health/cash-cycle-crosscheck-full")
def data_health_cash_cycle_crosscheck_full():
    """รายชื่อคู่หุ้นวงจรเงินสด SET vs Yahoo ที่ต่างกันเกิน threshold ครบทุกตัว (ไม่ตัดโชว์
    12 ตัวแรกเหมือน note ใน /api/data-health) — แยก route ต่างหากเพราะ /api/data-health
    ถูกเรียกบ่อยมาก (badge เช็คทุกครั้งเปิดแอป/หลัง Quick Update) ไม่อยากยัดตารางเต็ม
    (~470+ แถว) เข้าไปทุกครั้งทั้งที่แทบไม่มีใครกดดู โหลดเฉพาะตอนกด "และอีก N ตัว" จริงๆ —
    เรียกผ่าน _get_cash_cycle_crosscheck_cached() แทนเรียกตรง กัน compute ซ้ำสิ่งที่
    /api/data-health เพิ่งคำนวณไปหมาดๆ (code review 2026-09-02 กลุ่ม B5)"""
    try:
        cc = _get_cash_cycle_crosscheck_cached()
        rows = [{"symbol": s, "yahoo": y, "set": t, "diff": round(abs(y - t), 1)}
                for s, y, t in cc["diverge"]]
        return jsonify({"diff_days": cc["diff_days"], "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500


def _compute_cash_cycle_crosscheck():
    """คำนวณ cross-check วงจรเงินสด SET vs Yahoo ทั้งตลาด — คืน dict สถิติ + list คู่ที่
    ต่างกันเกิน threshold ครบทุกตัว (ไม่ตัด) ให้ทั้ง _compute_data_health (ตัดโชว์ 12 ตัวแรก
    ในตาราง note) และ route /api/data-health/cash-cycle-crosscheck-full (list เต็มตอนกด
    "และอีก N ตัว") ใช้ลอจิกเดียวกัน กันคำนวณซ้ำ/ผลเพี้ยนกันสองจุด"""
    _CC_DIFF_DAYS = 15        # เท่ากับ threshold ในการ์ด Tearsheet
    _CC_PLAUSIBLE = 600       # |วงจรเงินสด| เกินนี้ = นอกช่วงที่เป็นไปได้ของธุรกิจจริง
    _CC_ABSURD = 1000         # เกินนี้ = สูตรระเบิด (ตัวหารใกล้ 0) ไม่ใช่แค่ outlier
    _cc_yahoo = {r["symbol"]: r.get("cash_cycle")
                 for r in factor_snapshot.get_snapshot(BASE_DIR, is_dr=False)}

    def _cc_latest_amount(acc):
        for _v in reversed((acc or {}).get("values") or []):
            if _v.get("amount") is not None:
                return _v["amount"]
        return None

    _cc_pairs = _cc_absurd = 0
    _cc_plausible = []          # (sym, yahoo, set) ที่ทั้งคู่อยู่ในช่วงเป็นไปได้
    for _sym, _pl in financials_store.iter_source_payloads(BASE_DIR, "set_health"):
        _acc = None
        for _th in (_pl.get("themes") or []):
            for _cat in (_th.get("categories") or []):
                _acc = next((a for a in (_cat.get("accounts") or [])
                             if a.get("code") == "q_cash_cycle"), None)
                if _acc:
                    break
            if _acc:
                break
        _set_cc = _cc_latest_amount(_acc)
        _y_cc = _cc_yahoo.get(_sym)
        if _set_cc is None or _y_cc is None:
            continue
        _cc_pairs += 1
        if abs(_y_cc) > _CC_ABSURD:
            _cc_absurd += 1
        if abs(_y_cc) <= _CC_PLAUSIBLE and abs(_set_cc) <= _CC_PLAUSIBLE:
            _cc_plausible.append((_sym, round(_y_cc, 1), round(_set_cc, 1)))
    _cc_diverge = sorted((t for t in _cc_plausible if abs(t[1] - t[2]) >= _CC_DIFF_DAYS),
                         key=lambda t: abs(t[1] - t[2]), reverse=True)
    return {
        "diff_days": _CC_DIFF_DAYS,
        "pairs": _cc_pairs,
        "absurd": _cc_absurd,
        "plausible": len(_cc_plausible),
        "diverge": _cc_diverge,
    }


def _compute_data_health(fresh=False):
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
        universe=_dh_th_universe_tickers(), universe_required=True))

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

    # หุ้น IPO เพิ่งเข้าตลาดแต่ยังไม่มีในฐานราคาหุ้นไทย — คู่ตรงข้ามของ delisted ที่ระบบมีอยู่แล้ว
    # (ดู PLAN_set_api_expansion.txt งาน #6B) เทียบ /api/set/ipo/recently (ดึงสด) กับ ticker ใน
    # set_prices.db · หุ้น IPO จะยังไม่มีในฐานราคา/งบ จนกว่าจะรัน Full Refresh รอบถัดไป (Quick
    # Update ดึง listedCompanies ใหม่ แต่ไม่ backfill ราคาย้อนหลังของตัวใหม่)
    try:
        from sources.set_api import fetch_ipo_recently
        _ipo_recent = (_ipo_list_cache.get("result") or {}).get("recently")
        if _ipo_recent is None:
            # SET.or.th อาจบล็อค/ล่มชั่วคราว — ไม่ควรทำให้ item นี้แดง (ไม่ใช่ข้อมูลในเครื่องพัง)
            try:
                _ipo_recent = fetch_ipo_recently()
            except Exception:
                _ipo_recent = None
        _th_have = {t[:-3] if t.endswith(".BK") else t
                    for t in price_store.get_last_dates(BASE_DIR)}
        _today_iso = _dh_dt.now().date().isoformat()
        _ipo_missing = []
        for _r in _ipo_recent or []:
            _s = (_r.get("symbol") or "").upper().strip()
            _ftd = _r.get("first_trade_date") or ""
            # เฉพาะตัวที่เทรดแล้วจริง (firstTradeDate ผ่านมาแล้ว) — ตัวที่ยังไม่เทรดไม่ควรมีในฐานราคาอยู่แล้ว
            if not _s or (_ftd and _ftd > _today_iso):
                continue
            if _s not in _th_have:
                _ipo_missing.append((_s, _ftd, _r.get("name") or ""))
        _ipo_tip = ('<span class="scr-tip-icon" onclick="_tipIconToggle(this,event)">?'
                    '<div class="scr-tip-box" style="width:270px">'
                    '<strong>หุ้น IPO ยังไม่เข้าระบบ</strong><br>'
                    'หุ้นที่เพิ่งเข้าตลาดจะยังไม่มีในฐานราคา/งบของ dashboard จนกว่าจะรัน '
                    'Full Refresh รอบถัดไป — เมนูอื่น (Screener/Tearsheet/Valuation) จะยังหาหุ้น'
                    'พวกนี้ไม่เจอ<hr>Quick Update ดึงรายชื่อ listedCompanies ใหม่ให้ แต่ไม่ '
                    'backfill ราคาย้อนหลังของตัวใหม่ — ต้อง ⟳ Full Refresh</div></span>')
        if _ipo_recent is None:
            # None = ดึงจริงล้มเหลว (exception ด้านบน) ต่างจาก [] = ดึงสำเร็จแต่ไม่มี IPO ใหม่
            # ช่วงนี้ (สถานะปกติส่วนใหญ่) — เดิมใช้ `not _ipo_recent` ปนสองเคสนี้เข้าด้วยกัน
            # ทำให้วันที่ไม่มี IPO ใหม่เลยรายงานผิดว่า "ดึงจาก SET.or.th ไม่สำเร็จ" (code review
            # 2026-09-02)
            _ipo_status = "na"
            _ipo_note = _ipo_tip + " ดึงรายชื่อ IPO จาก SET.or.th ไม่สำเร็จ — ลองใหม่ภายหลัง"
        elif _ipo_missing:
            _ipo_status = "warn"
            _ipo_rows = "".join(
                f'<tr><td style="padding:2px 10px 2px 0"><b>{_dh_esc(_m[0])}</b></td>'
                f'<td style="color:var(--text2)">{_dh_esc((_m[2] or "")[:38])}</td>'
                f'<td style="color:var(--text2);padding-left:8px;white-space:nowrap">เข้าตลาด {_dh_esc(_m[1] or "?")}</td></tr>'
                for _m in _ipo_missing[:20])
            _ipo_note = (f'{_ipo_tip} <b>{len(_ipo_missing)}</b> ตัวเทรดแล้วแต่ยังไม่มีในฐานราคาหุ้นไทย:'
                         f'<table style="border-collapse:collapse;margin-top:4px">{_ipo_rows}</table>'
                         + (f'<div style="color:var(--text2);margin-top:2px">…และอีก {len(_ipo_missing) - 20} ตัว</div>'
                            if len(_ipo_missing) > 20 else '')
                         + '<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                           'รัน ⟳ Full Refresh เพื่อดึงราคา+งบของหุ้นใหม่เข้าระบบ · '
                           'ดูราคาจอง vs ราคาปัจจุบันได้ที่ปฏิทิน → 🆕 IPO</div>')
        else:
            _ipo_status = "ok"
            _ipo_note = (f'{_ipo_tip} หุ้น IPO ที่เพิ่งเข้าตลาด ({len(_ipo_recent)} ตัว) '
                         f'มีในฐานราคาหุ้นไทยครบแล้ว')
        items.append({
            "key": "ipo_not_in_universe",
            "label": "หุ้น IPO เข้าใหม่ยังไม่เข้าฐานราคา",
            "category": "หุ้นเข้าใหม่/ถูกถอด",
            "last_at": None, "age_hours": None,
            "status": _ipo_status, "note": _ipo_note,
        })
    except Exception as e:
        items.append(_dh_error_item("ipo_not_in_universe",
                                     "หุ้น IPO เข้าใหม่ยังไม่เข้าฐานราคา", "หุ้นเข้าใหม่/ถูกถอด", str(e)[:160]))

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

    try:
        items.append(_dh_item(
            "hk_prices", "ราคาหุ้นดัชนี HK (hk_prices.db)", "ราคา/เทคนิค",
            _dh_parse(_hk_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
            missing_note="ยังไม่เคยอัพเดทหุ้น HK ในเครื่องนี้", optional=True))
    except Exception as e:
        items.append(_dh_error_item("hk_prices", "ราคาหุ้นดัชนี HK (hk_prices.db)", "ราคา/เทคนิค", str(e)[:160]))

    try:
        items.append(_dh_item(
            "jp_prices", "ราคาหุ้นดัชนี JP (jp_prices.db)", "ราคา/เทคนิค",
            _dh_parse(_jp_store.get_meta(BASE_DIR, "updated_at")), 30, 72,
            missing_note="ยังไม่เคยอัพเดทหุ้น JP ในเครื่องนี้", optional=True))
    except Exception as e:
        items.append(_dh_error_item("jp_prices", "ราคาหุ้นดัชนี JP (jp_prices.db)", "ราคา/เทคนิค", str(e)[:160]))

    # dup-close check 3 ตลาดรวมเป็น loop เดียว (เดิมก็อป 3 บล็อกเกือบเหมือนกัน — code review
    # 2026-09-02 กลุ่ม B1) ต่างกันแค่ key/label/DB_FILE ต่อตลาด
    for _mkt, _store in (("US", _us_store), ("HK", _hk_store), ("JP", _jp_store)):
        items.append(_dh_stale_close_check(
            f"{_mkt.lower()}_prices_stale_dup", f"ราคาซ้ำแท่งก่อนหน้าผิดปกติ (หุ้น {_mkt})",
            os.path.join(BASE_DIR, _store.DB_FILE)))

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
                          ("set", "SET company-highlight"), ("set_qpl", "SET P&L รายไตรมาส"),
                          ("set_cashflow", "SET กระแสเงินสด"), ("set_balance", "SET งบดุล"),
                          ("set_health", "SET Financial Health"), ("set_factsheet", "SET Factsheet")]
        # set_health/set_factsheet ไม่นับเข้า worst_pct/สถานะรวม — เป็น informational เฉยๆ:
        #  · set_health: SET.or.th endpoint นี้ไม่ครอบกลุ่มการเงิน/ประกัน/REIT เลย (404 เป็นระบบ
        #    วัดจริง 2026-08-26: ~30-40% ของตลาด) เพดาน coverage ต่ำกว่า 100% ถาวร
        #  · set_factsheet: หุ้นที่เพิ่ง IPO (ยังไม่มีงบยื่น → ratio/growth/cash_cycle ว่าง) หรือ
        #    ตราสารผิดแบบ อาจไม่มีแถวเลย ทำให้ % หล่นต่ำกว่า 99.5 → row ค้างเหลืองถาวร ทั้งที่
        #    sync ครบทุกตัวที่ทำได้แล้ว (false alarm แบบเดียวกับ set_health / ESG งาน #3)
        # ถ้านับรวมจะทำให้ item นี้ค้างสถานะเหลือง/แดงตลอดไปทั้งที่ไม่มีอะไรผิดปกติ — ยังโชว์
        # ตัวเลขให้เห็นตามปกติ (มี ' *') แค่ไม่ใช้ตัดสิน status
        _fin_cov_informational = {"set_health", "set_factsheet"}
        _fin_cov = financials_store.get_coverage(BASE_DIR, _financials_universe(),
                                                  sources=[k for k, _ in _fin_cov_srcs])
        _fin_cov_rows = []
        _fin_cov_worst_pct = 100.0
        for _k, _label in _fin_cov_srcs:
            _c = _fin_cov.get(_k, {"covered": 0, "total": 0})
            _total = _c["total"]
            _pct = (_c["covered"] / _total * 100) if _total else 0.0
            if _k not in _fin_cov_informational:
                _fin_cov_worst_pct = min(_fin_cov_worst_pct, _pct)
            _color = _dh_pct_color(_pct)
            _note_mark = ' *' if _k in _fin_cov_informational else ''
            _fin_cov_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_label}{_note_mark}</td>'
                f'<td style="text-align:right;color:{_color};font-weight:600">'
                f'{_c["covered"]}/{_total} ({_pct:.1f}%)</td></tr>')
        _fin_cov_status = _dh_pct_status(_fin_cov_worst_pct)
        _fin_cov_extra = ('<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                           '* SET Financial Health (ไม่ครอบกลุ่มการเงิน/ประกัน/REIT ~30-40% ของตลาด) '
                           'และ SET Factsheet (หุ้น IPO ใหม่ยังไม่มีงบยื่น) เพดาน coverage ต่ำกว่า 100% '
                           'ถาวรโดยไม่ใช่บั๊ก — ไม่นับรวมในสถานะข้างบน</div>')
        if _fin_cov_worst_pct < 90:
            _fin_cov_extra += ('<div style="margin-top:6px">ยังไม่ sync ทั้งตลาด — กดปุ่ม '
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
            _color = _dh_pct_color(_pct)
            _finq_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_finq_labels[_k]}</td>'
                f'<td style="text-align:right;color:{_color};font-weight:600">'
                f'{_c["fresh"]}/{_total} ({_pct:.1f}%)</td>'
                f'<td style="padding-left:10px;color:var(--text2)">เป้าหมาย {_c["target"]}</td></tr>')
        # threshold หลวมกว่า item 'มีข้อมูล' ด้านบนมาก (99.5/90) เพราะบริษัทมีสิทธิ์ยื่นงบช้าได้
        # ถึง 45-60 วันหลังปิดไตรมาส (_target_period ไม่รอ deadline ถือว่า 'ควรมีแล้ว' ทันทีที่
        # ไตรมาสปฏิทินปิด) — % ต่ำตอนไตรมาสเพิ่งปิดใหม่ๆ เป็นเรื่องปกติ ไม่ใช่ sync ล้มเหลว
        _finq_status = _dh_pct_status(_finq_worst_pct, ok=95, warn=50)
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

    # ฉันทามตินักวิเคราะห์ (Yahoo: ราคาเป้าหมาย/Buy-Hold-Sell) — coverage หุ้นไทยต่ำ
    # โดยธรรมชาติ (~40% เอียงไป cap ใหญ่) เหมือน SET Financial Health/ESG → ใช้ "ความสด"
    # ตัดสิน status ไม่ใช่ coverage (หุ้นเล็กไม่มีนักวิเคราะห์ตาม ไม่ใช่บั๊ก)
    try:
        from datetime import timezone as _tz
        _ac_meta = analyst_consensus.get_meta(BASE_DIR)
        _ac_th = _ac_meta.get("TH", {})
        _ac_count = _ac_th.get("count", 0)
        _ac_target = _ac_th.get("with_target", 0)
        _ac_dr = sum(_ac_meta.get(m, {}).get("count", 0) for m in ("US", "HK"))
        _ac_ts = _ac_th.get("updated_at")   # UTC naive string จาก sources/analyst_consensus
        _ac_age_h = None
        _ac_parsed = _dh_parse(_ac_ts)
        if _ac_parsed:
            _ac_age_h = (_dh_dt.now(_tz.utc).replace(tzinfo=None) - _ac_parsed).total_seconds() / 3600
        if not _ac_count:
            _ac_status = "warn"
            _ac_note = ('ยังไม่เคย sync — กดปุ่ม "🔄 อัพเดทงบการเงินทั้งหมด" (รวม sync '
                        'นักวิเคราะห์ให้แล้ว รอบแรก ~10 นาที) หรือ sync แยกในหน้า Screener+')
        else:
            # sync อยู่ใน financials-update-all (7 วัน/รอบ) — เกิน ~10 วันถือว่าค้าง
            _ac_status = "ok" if (_ac_age_h is not None and _ac_age_h <= 24 * 10) else "warn"
            _ac_note = (f'หุ้นไทย {_ac_count} ตัว (มีราคาเป้าหมาย {_ac_target}) '
                        f'+ DR underlying US/HK {_ac_dr} ตัว '
                        f'· sync พร้อมงบการเงิน (skip ตัวที่ &lt;7 วัน) '
                        f'· coverage หุ้นไทยต่ำเป็นปกติ — หุ้นเล็กไม่มีนักวิเคราะห์ตาม')
        items.append({
            "key": "analyst_consensus",
            "label": "ฉันทามตินักวิเคราะห์ (Yahoo)",
            "category": "งบการเงิน",
            "last_at": _ac_ts, "age_hours": round(_ac_age_h, 1) if _ac_age_h is not None else None,
            "status": _ac_status, "note": _ac_note,
        })
    except Exception as e:
        items.append(_dh_error_item("analyst_consensus",
                                     "ฉันทามตินักวิเคราะห์ (Yahoo)", "งบการเงิน", str(e)[:160]))

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
            _gcolor = _dh_pct_color(_gpct)
            _uim_rows.append(
                f'<tr><td style="padding:2px 10px 2px 0">{_glabel}</td>'
                f'<td style="text-align:right;color:{_gcolor};font-weight:600">'
                f'{_gcovered}/{_gtotal} ({_gpct:.1f}%)</td></tr>')
            if _gmissing:
                _uim_missing_by_group[_glabel] = [name for _, name in _gmissing]
        _uim_status = _dh_pct_status(_uim_worst_pct)
        _uim_extra = ""
        if _uim_missing_by_group:
            _uim_missing_html = "".join(
                f'<div style="margin-top:4px"><b>{lbl}</b> ({len(syms)}): {", ".join(_dh_esc(s) for s in syms)}</div>'
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
    # (ตัด "set_history" ออกจากรายการนี้แล้ว — DUAL_WRITE_JSON=False ตั้งแต่ 2026-07-16
    # (ดู core/store.py) ทำให้ set_history.json ไม่ถูกเขียนอีกต่อไป mtime เลยค้างถาวร
    # ไล่ warn→red ไปเรื่อยๆ โดย Quick Update/Full Refresh แก้ให้ไม่ได้จริง)

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
        # ใช้ cache เดียวกับ _dh_th_universe_tickers() (เรียกไปแล้วก่อนหน้านี้ในฟังก์ชัน) กัน
        # parse set_data.json ~12MB ซ้ำรอบสอง (code review 2026-09-02 กลุ่ม B3)
        _sd_cache = _dh_th_universe_data()
        _n_stocks = len(_sd_cache["stocks"])
        _q_status = "ok" if _n_stocks >= 800 else ("warn" if _n_stocks >= 500 else "red")
        items.append(_dh_quality_item(
            "q_set_data", "จำนวนหุ้นใน set_data.json", _q_status,
            f"{_n_stocks} ตัว (ปกติ ~900-950 · <800 = เริ่มผิดปกติ, <500 = พร่องชัดเจน)",
            last_at=_sd_cache["data_as_of"]))
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

    # cross-check วงจรเงินสด: SET ทางการ (q_cash_cycle จาก Financial Health Check ที่สะสมใน DB)
    # vs ที่ dashboard คำนวณเองจาก Yahoo (factor_snapshot.cash_cycle) — ดู
    # PLAN_set_api_expansion.txt งาน #5C · ทำเป็น item รวมทั้งตลาด ต่างจาก cross-check รายตัวใน
    # การ์ด 🩺 Tearsheet (_tsLoadFinHealthMini) ที่เห็นทีละหุ้น
    #
    # ⚠ item นี้เป็น "ตารางอ้างอิง" ไม่ใช่ pass/fail — 2 วิธีคำนวณคนละฐาน (SET ใช้งบรายปี/
    # ครึ่งปีที่ยื่น + สูตรของ SET เอง · เราใช้ DIO+DSO−DPO เฉลี่ย 4 ไตรมาสจาก Yahoo) วัดจริง
    # 2026-08: ต่างกัน ≥15 วันราว 70% ของตัวเป็นเรื่องปกติเชิงโครงสร้าง จะไม่เขียวสนิท —
    # status ขึ้น warn เฉพาะตอน
    # ค่าฝั่ง Yahoo "เพี้ยนชัด" (|cash cycle| > 1000 วัน = รายได้/ต้นทุนใกล้ 0 หารระเบิด) เยอะ
    # ผิดปกติ ซึ่งแปลว่า compute_cash_cycle พังจริง ไม่ใช่แค่ต่างสูตร
    try:
        _cc = _get_cash_cycle_crosscheck_cached(fresh=fresh)
        _CC_DIFF_DAYS = _cc["diff_days"]
        _cc_pairs = _cc["pairs"]
        _cc_absurd = _cc["absurd"]
        _cc_np = _cc["plausible"]
        _cc_diverge = _cc["diverge"]
        _cc_dn = len(_cc_diverge)
        _cc_dpct = (_cc_dn / _cc_np * 100) if _cc_np else 0.0
        _cc_apct = (_cc_absurd / _cc_pairs * 100) if _cc_pairs else 0.0
        _cc_tip = ('<span class="scr-tip-icon" onclick="_tipIconToggle(this,event)">?'
                   '<div class="scr-tip-box" style="width:280px">'
                   '<strong>วงจรเงินสด: SET vs Yahoo</strong><br>'
                   'SET ให้ q_cash_cycle ทางการ (สูตร SET, งบรายปี/ครึ่งปีที่บริษัทยื่น) · '
                   'dashboard คำนวณเอง DIO+DSO−DPO เฉลี่ย 4 ไตรมาสจากงบ Yahoo<hr>'
                   '<span style="color:var(--text2)">คนละฐาน — ต่างกันเกินครึ่งของตัวเป็นเรื่อง'
                   'ปกติ ไม่ใช่บั๊ก · item นี้เป็นตารางอ้างอิงไว้เช็คหุ้นรายตัวว่าตัวเลขฝั่งไหน'
                   'น่าเชื่อกว่า · warn เฉพาะตอนค่าฝั่ง Yahoo "เพี้ยนชัด" (สูตรระเบิด) เยอะ'
                   'ผิดปกติ</span></div></span>')
        if _cc_pairs < 20:
            _cc_status = "na"
            _cc_note = (f'{_cc_tip} เทียบได้ {_cc_pairs} ตัว (ยังน้อย) — sync SET Financial '
                        f'Health + งบ Yahoo ให้ครบก่อน (ปุ่ม "🔄 อัพเดทงบการเงินทั้งหมด")')
        else:
            _cc_status = "warn" if (_cc_apct >= 12 or _cc_dpct >= 90) else "ok"
            _cc_rows = "".join(
                f'<tr><td style="padding:2px 10px 2px 0"><b>{_d[0]}</b></td>'
                f'<td style="text-align:right;color:var(--text2)">Yahoo {_d[1]:.0f}</td>'
                f'<td style="text-align:right;color:var(--text2);padding-left:8px">SET {_d[2]:.0f}</td>'
                f'<td style="text-align:right;color:#e8a33d;padding-left:8px">Δ{abs(_d[1]-_d[2]):.0f} วัน</td></tr>'
                for _d in _cc_diverge[:12])
            _cc_note = (f'{_cc_tip} เทียบ {_cc_pairs} ตัว · ทั้งคู่อยู่ในช่วงปกติ {_cc_np} ตัว → '
                        f'ต่างกัน ≥{_CC_DIFF_DAYS} วัน <b>{_cc_dn}</b> ({_cc_dpct:.0f}%) · '
                        f'ค่าฝั่ง Yahoo เพี้ยนชัด (สูตรระเบิด) <b>{_cc_absurd}</b> ({_cc_apct:.0f}%)'
                        + (f'<table style="border-collapse:collapse;margin-top:4px">{_cc_rows}</table>'
                           if _cc_diverge else '')
                        + (f'<div style="margin-top:2px"><a href="javascript:void(0)" '
                           f'onclick="_dhShowCashCycleFull()" style="color:var(--blue)">'
                           f'…และอีก {_cc_dn - 12} ตัว (คลิกดูทั้งหมด)</a></div>'
                           if _cc_dn > 12 else '')
                        + '<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                          'ต่างกันเกินครึ่ง = ปกติ (คนละสูตร/งวด) · ถ้า "เพี้ยนชัด" พุ่งสูง = '
                          'compute_cash_cycle (Yahoo) พังจริง ให้เช็ค financials_store.compute_cash_cycle'
                          '</div>')
        items.append(_dh_quality_item(
            "q_cash_cycle_crosscheck", "วงจรเงินสด cross-check (SET vs Yahoo) — อ้างอิง",
            _cc_status, _cc_note))
    except Exception as e:
        items.append(_dh_quality_item(
            "q_cash_cycle_crosscheck", "วงจรเงินสด cross-check (SET vs Yahoo) — อ้างอิง",
            "red", f"เช็คไม่ได้: {str(e)[:120]}"))

    # สะสม PE/PBV/BVPS/DivYield รายวันทางการจาก SET.or.th (งาน #4B) — endpoint historical-trading
    # ให้ย้อนหลังแค่ ~6 เดือน เก็บเองทุกวันผ่าน Quick Update ถึงจะได้ประวัติยาวกว่านั้น (ดู
    # sources/set_daily_val.py) · item นี้โชว์ว่าสะสมได้กี่ตัว/ช่วงยาวสุดเท่าไหร่แล้ว
    try:
        _dv_meta = set_daily_val.get_meta(BASE_DIR)
        _dv_tip = ('<span class="scr-tip-icon" onclick="_tipIconToggle(this,event)">?'
                   '<div class="scr-tip-box" style="width:270px">'
                   '<strong>PE/PBV รายวันทางการ (สะสมเอง)</strong><br>'
                   'SET.or.th ให้ย้อนหลังแค่ ~118 วันทำการ (~6 เดือน) ตายตัว — เก็บสะสมเองทุก '
                   'Quick Update เพื่อได้ประวัติทางการยาวกว่านั้น (pattern เดียวกับ SET P&L '
                   'รายไตรมาส)<hr><span style="color:var(--text2)">ใช้เป็นเส้นตรวจสอบใน Valuation '
                   'Band · ยิ่งสะสมนาน ยิ่งเทียบย้อนหลังได้ไกล · โตอัตโนมัติ ไม่ต้องกดเอง</span>'
                   '</div></span>')
        _dv_ts = _dh_parse(_dv_meta.get("updated_at"))
        # sources/set_daily_val.py เก็บ synced_at เป็น UTC naive string (datetime.now(timezone.utc))
        # ต่างจาก _dh_age_hours ที่เทียบกับ local naive datetime.now() ตรงๆ — ถ้าไม่แปลง จะทำให้
        # อายุที่โชว์เพี้ยนเท่ากับ UTC offset ของเครื่อง (เช่น +7 ชม.ในไทย) ใช้ pattern เดียวกับ
        # analyst_consensus item ด้านบน (code review 2026-09-02)
        from datetime import timezone as _tz
        _dv_age_h = ((_dh_dt.now(_tz.utc).replace(tzinfo=None) - _dv_ts).total_seconds() / 3600
                     if _dv_ts else None)
        if not _dv_meta.get("row_count"):
            _dv_status = "na"
            _dv_note = (f'{_dv_tip} ยังไม่เคยสะสม — จะเริ่มเก็บเองรอบ Quick Update ถัดไป '
                        f'(หรือกดปุ่มด้านขวา)')
        else:
            # ค้างเกิน ~10 วัน = Quick Update ไม่ได้รันช่วงนี้ (เหมือนเกณฑ์ analyst_consensus)
            _dv_status = "ok" if (_dv_age_h is not None and _dv_age_h <= 24 * 10) else "warn"
            _dv_note = (f'{_dv_tip} สะสมแล้ว <b>{_dv_meta["symbol_count"]}</b> ตัว · '
                        f'{_dv_meta["row_count"]:,} แถว · ช่วง {_dv_meta["oldest_date"]}–'
                        f'{_dv_meta["newest_date"]} · ประวัติยาวสุด {_dv_meta["max_span_days"]} วัน'
                        + ('<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                           'ยังไม่เกิน ~6 เดือน (เท่าที่ SET ให้อยู่แล้ว) — สะสมต่อไปเรื่อย ๆ '
                           'ประวัติจะยาวขึ้นเอง</div>'
                           if _dv_meta["max_span_days"] <= 190 else
                           '<div style="margin-top:6px;color:var(--text2);font-size:11px">'
                           'มีประวัติทางการยาวกว่าที่ SET ให้ย้อนหลังแล้ว — ส่วนเกินนี้สร้างใหม่ไม่ได้'
                           '</div>'))
        items.append(_dh_quality_item(
            "set_daily_valuation", "PE/PBV รายวันทางการ (สะสมเอง, set_daily_val)",
            _dv_status, _dv_note, last_at=_dv_meta.get("updated_at")))
    except Exception as e:
        items.append(_dh_quality_item(
            "set_daily_valuation", "PE/PBV รายวันทางการ (สะสมเอง, set_daily_val)",
            "red", f"เช็คไม่ได้: {str(e)[:120]}"))

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
        # .get(...,0)+1 แทน summary[status] += 1 ตรงๆ (code review 2026-09-02) — เดิมถ้า item
        # ไหนมี status นอก {ok,warn,red,na} (เช่น typo ในอนาคต) จะ KeyError จนทั้ง /api/data-health
        # 500 ทันที ทั้งที่ items แต่ละตัวข้างบนถูกครอบ try/except ไว้เพื่อกันแบบนี้อยู่แล้ว
        summary[it["status"]] = summary.get(it["status"], 0) + 1

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
    # ต้องรอรอบสำรองถัดไปกว่าจะเห็นตารางนี้ — ทำเครื่องหมายไว้ (_dest_dir_guessed) เพื่อบอกใน
    # note ตรงๆ ว่านี่คือค่าเดา ไม่ใช่ dest_dir ที่ยืนยันจาก run_log จริง กันเข้าใจผิดว่า
    # "เข้าไม่ถึงโฟลเดอร์ปลายทาง" หมายถึงปลายทางจริงพังเสมอ (code review 2026-09-02)
    _dest_dir_guessed = not dest_dir
    if not dest_dir:
        dest_dir = r"C:\SET_Dashboard_backup"  # โฟลเดอร์ mirror ของ Google Drive for Desktop
    _guess_note = (" (เดาจาก path เริ่มต้น — ไม่พบ dest_dir ที่บันทึกไว้จาก run_log จริง)"
                   if _dest_dir_guessed else "")

    if not os.path.isdir(dest_dir):
        return jsonify({"dest_dir": dest_dir, "files": [],
                         "note": f"เข้าไม่ถึงโฟลเดอร์ปลายทาง{(' ' + dest_dir) if dest_dir else ''}"
                                 f"{_guess_note} — external drive ถอดอยู่หรือ Google Drive ยังไม่ sync?"})

    try:
        entries = os.listdir(dest_dir)
    except Exception as e:
        return jsonify({"dest_dir": dest_dir, "files": [], "note": f"อ่านโฟลเดอร์ไม่ได้: {e}{_guess_note}"})

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

    return jsonify({"dest_dir": dest_dir, "files": files, "note": _guess_note or None})


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
    "set_daily_val_sync": "📈 สะสม PE/PBV รายวันทางการ (SET.or.th)",
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
    # v ต้องเป็น dict เท่านั้น (ปกติเขียนโดย run_log.record_run เสมอ) — กันไฟล์ status
    # เสีย/ถูกแก้มือทำ record ผิดรูปจน .get() throw AttributeError จน route คืน 500
    failed = [{"source": k, "label": _UPDATE_STATUS_LABEL.get(k, k), **v}
              for k, v in raw.items() if isinstance(v, dict) and not v.get("ok")]
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
# lock กัน thundering herd ตอน cache หมดอายุ — เหมือน _data_health_lock (code review 2026-09-02):
# เดิมไม่มี lock เลย 2 แท็บ/รีเควสต์พร้อมกันตอน cache miss จะยิง SET.or.th+fetch_stock_list ซ้ำ
# ซ้อนกันทั้งคู่แทนที่ตัวหลังจะรอผลตัวแรก
_set_universe_diff_lock = threading.Lock()

@app.route("/api/set-universe-check-updates")
def set_universe_check_updates():
    """เทียบรายชื่อหุ้น SET+mai ที่ซื้อขายจริง (โหลดสดจาก SET.or.th) กับ universe
    ที่ใช้ดึงงบการเงินในเครื่อง (_financials_universe — กรอง DW/DR series/ดัชนีกลุ่ม/
    หุ้นแขวนถาวรออกแล้ว) รายงานตัวใหม่/ตัวที่หายไปเท่านั้น ไม่แก้อะไรให้อัตโนมัติ

    ตัวที่หายไป (removed) จะพยายามเทียบกับ oldSymbols ของ /api/set/stock/list
    (sources/set_api.py::fetch_stock_list) ก่อนว่า "เปลี่ยนชื่อย่อ" หรือ "เพิกถอนจริง"
    (ดู PLAN_set_api_expansion.txt งาน #8) — ดึงไม่สำเร็จก็ไม่ทำให้ endpoint พัง แค่ไม่มี
    renamed_to ให้ (เหมือนเดิมก่อนมีฟีเจอร์นี้ ผู้ใช้ยังเช็คมือได้)"""
    def _cached_ok():
        cached = _set_universe_diff_cache.get("result")
        return cached if (cached and (time.time() - _set_universe_diff_cache.get("ts", 0)
                                       < _SET_UNIVERSE_DIFF_TTL)) else None
    cached = _cached_ok()
    if cached:
        return jsonify(cached)
    with _set_universe_diff_lock:
        cached = _cached_ok()
        if cached:
            return jsonify(cached)
        try:
            from set_data_fetcher import load_set_symbols
            live = load_set_symbols(BASE_DIR)
            live_map = {s["symbol"]: s for s in live}
            local_syms = set(_financials_universe())
            new_syms = sorted(set(live_map.keys()) - local_syms)
            removed_syms = sorted(local_syms - set(live_map.keys()))
            tracked_delisted = sum(1 for k in delisted_log.read_log(BASE_DIR) if k.startswith("TH:"))

            old_to_new = {}
            try:
                from sources.set_api import fetch_stock_list
                for s in fetch_stock_list():
                    for old in s.get("old_symbols") or []:
                        old_to_new[old] = s["symbol"]
            except Exception as e:
                print(f"[set-universe-check] fetch_stock_list (oldSymbols) ไม่สำเร็จ ({e}) — ข้ามการเช็คเปลี่ยนชื่อ")

            result = {
                "live_count": len(live_map), "local_count": len(local_syms),
                "new": [{"symbol": s, "name": live_map[s]["name"], "market": live_map[s]["market"],
                         "sector": live_map[s]["sector"]} for s in new_syms],
                "removed": [{"symbol": s, "renamed_to": old_to_new.get(s)} for s in removed_syms],
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
# lock กัน thundering herd เหมือน _set_universe_diff_lock ด้านบน (code review 2026-09-02)
_mirror_diff_lock = threading.Lock()

@app.route("/api/mirror-check-updates")
def mirror_check_updates():
    def _cached_ok():
        cached = _mirror_diff_cache.get("result")
        return cached if (cached and (time.time() - _mirror_diff_cache.get("ts", 0)
                                       < _MIRROR_DIFF_TTL)) else None
    cached = _cached_ok()
    if cached:
        return jsonify(cached)
    with _mirror_diff_lock:
        cached = _cached_ok()
        if cached:
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
    (Yahoo+SET+SET-QPL+SET-CashFlow+SET-Balance+SET-Health+Finnomena) → sync งบ DR
    (Yahoo+Finnomena) → sync งบสมาชิกดัชนีหลัก US
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
                                          sources=("yahoo", "set", "set_qpl", "set_cashflow",
                                                   "set_balance", "set_health", "set_factsheet",
                                                   "yahoo_q", "finnomena_q"),
                                          callback=_th_cb, is_dr=False,
                                          skip_up_to_date=True, auto_rebuild=False)

        syms_dr = _dr_financials_universe()
        _update(current=50, total=100, message=f"sync งบ DR {len(syms_dr)} ตัว...")

        def _dr_cb(done, total, msg):
            pct = 50 + (done / max(total, 1) * 20)
            _update(current=round(pct), total=100, message=f"งบ DR: {done}/{total}")
        r_dr = financials_store.sync_all(BASE_DIR, syms_dr,
                                          sources=("yahoo", "yahoo_q", "finnomena_q"),
                                          callback=_dr_cb, is_dr=True,
                                          skip_up_to_date=True, auto_rebuild=False)

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

        # ฉันทามตินักวิเคราะห์ (Yahoo: ราคาเป้าหมาย/Buy-Hold-Sell/EPS estimate) — ข้าม
        # ตัวที่ sync ภายใน 7 วัน (รอบแรกที่ตารางว่างจะช้า ~10 นาที, รอบถัดไปเกือบ 0)
        # non-fatal: ล้มก็ปล่อย ไม่ให้กระทบ factor snapshot / งบที่ sync สำเร็จแล้ว
        try:
            def _ac_cb(done, total, msg):
                _update(current=92, total=100, message=f"ฉันทามตินักวิเคราะห์: {done}/{total}")
            ac_ok, ac_tot = analyst_consensus.sync_th(BASE_DIR, callback=_ac_cb, max_age_days=7)
            # underlying ต่างประเทศของ DR (region US/HK) — coverage Yahoo เกือบ 100% ใช้ในการ์ด
            # Tearsheet ของหุ้น DR (~250 ตัว, รอบแรก ~1-2 นาที, รอบถัดไปข้ามเกือบหมด)
            dr_ok, dr_tot = analyst_consensus.sync_dr(BASE_DIR, callback=_ac_cb, max_age_days=7)
            if ac_tot or dr_tot:
                print(f"[FinancialsUpdateAll] analyst consensus: TH {ac_ok}/{ac_tot} · "
                      f"DR underlying {dr_ok}/{dr_tot} sync ใหม่")
        except Exception as e:
            print(f"[FinancialsUpdateAll] analyst consensus sync error (ข้าม): {e}")

        # build_snapshot / build_mirror_snapshot* non-fatal — งบ Yahoo/SET/Finnomena sync
        # สำเร็จแล้ว ณ จุดนี้ ถ้า snapshot พัง (payload เสีย/ดิสก์เต็ม) ไม่ควรทำให้ทั้ง run
        # นับล้มเหลว + ข้าม _bake_financials_quarterly_file()/เคลียร์ cache ท้ายงานไปด้วย
        # (pattern เดียวกับ _run_financials_sync L5022 + _run_us_index_sync)
        _update(current=93, total=100, message="กำลังคำนวณ factor snapshot ใหม่...")
        try:
            factor_snapshot.build_snapshot(BASE_DIR)
        except Exception as e:
            print(f"[FinancialsUpdateAll] build_snapshot ล้มเหลว (งบ sync สำเร็จแล้ว ข้าม): {e}")
        _bake_financials_quarterly_file()   # re-bake data/financials_quarterly.json (งบไตรมาสเว็บมือถือ)
        idx_changed = bool(r_idx["ok"] or r_idx["fail"])
        try:
            if refreshed_mirror or idx_changed:
                _update(current=97, total=100, message="กำลัง rebuild mirror snapshot US/HK...")
                factor_snapshot.build_mirror_snapshot(BASE_DIR, exchanges=("US", "HK"))
            if idx_changed:
                _update(current=99, total=100, message="กำลัง rebuild mirror snapshot JP...")
                factor_snapshot.build_mirror_snapshot_yahoo_only(
                    BASE_DIR, "JP", jp_syms, price_by_ticker=jp_price_by_ticker)
        except Exception as e:
            print(f"[FinancialsUpdateAll] build_mirror_snapshot ล้มเหลว (ข้าม): {e}")
        _fin_analytics_cache.clear()
        _sector_compare_cache.clear()
        _qpl_parsed_cache.clear()
        _clear_qpl_mirror_caches()
        _market_trend_cache.clear()
        _sector_trend_cache.clear()
        _warmup_fin_dependent_caches(mirror_universes=("dr", "us", "hk", "jp"))

        elapsed_min = (time.time() - t0) / 60
        summary = (f"เสร็จแล้ว! หุ้นไทย {r_th['ok']}/{r_th['total']} (พลาด {r_th['fail']}, "
                   f"ข้าม {r_th['skipped']} คู่ที่มีงวดล่าสุดอยู่แล้ว) · "
                   f"DR {r_dr['ok']}/{r_dr['total']} (พลาด {r_dr['fail']}, ข้าม {r_dr['skipped']}) · "
                   f"ดัชนีหลัก US/HK/JP {r_idx['ok']}/{r_idx['total']} (ข้าม {r_idx['skipped']} ที่มีแล้ว)"
                   + (f" · mirror ค้นบ่อย {refreshed_mirror}/{len(searched)}" if searched else "")
                   + f" · ใช้เวลา {elapsed_min:.0f} นาที")
        _update(done=True, message=summary)
        # ระบบต้องมีของให้ sync จริง (total>0) แล้วไม่สำเร็จเลยสักตัว (ok==0) แต่พลาดจริง (fail>0)
        # ถึงจะนับว่า "ล้มเหลว" — total==0 (ข้ามหมดเพราะสดอยู่แล้ว) ไม่ใช่ความล้มเหลว เดิม record_run
        # เขียน True เสมอไม่ว่าผลจริงจะเป็นอย่างไร ทำให้ sync ที่พังทั้งกระบวน (เช่น SET.or.th บล็อก IP)
        # ยังโชว์ "🕐 สถานะ" เขียวเหมือนสำเร็จ (code review 2026-09-02)
        def _dh_sync_group_ok(r):
            return not (r.get("total") and not r.get("ok") and r.get("fail"))
        _fin_sync_ok = _dh_sync_group_ok(r_th) and _dh_sync_group_ok(r_dr) and _dh_sync_group_ok(r_idx)
        run_log.record_run(BASE_DIR, "financials_sync", _fin_sync_ok, summary)
    except Exception as e:
        # _update(done=True,...) ต้องมาก่อน record_run() เสมอ — กัน SSE ค้างสถานะ
        # "กำลังทำงาน" ตลอดไปถ้า record_run เขียนไฟล์พังกลางทาง
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "financials_sync", False, str(e))
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
        # เหตุผลเดียวกับ financials_sync ด้านบน — total ตัวที่ยิงจริง (ok+empty+fail) > 0 แต่ไม่มี
        # ok/empty เลยสักตัว มีแต่ fail = ล้มเหลวทั้งกระบวนจริง (เช่น Finnomena บล็อก) ไม่ใช่แค่
        # "หุ้นกลุ่มนี้ไม่มีงบ" (empty) ซึ่งเป็นสถานะปกติ (code review 2026-09-02)
        _mirror_total = result.get("ok", 0) + result.get("empty", 0) + result.get("fail", 0)
        _mirror_sync_ok = not (_mirror_total and not result.get("ok") and not result.get("empty")
                                and result.get("fail"))
        run_log.record_run(BASE_DIR, "mirror_finnomena", _mirror_sync_ok, summary)
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
        # เหตุผลเดียวกับ _run_mirror_finnomena_force_full ด้านบน (code review 2026-09-02)
        _mirror_total = result.get("ok", 0) + result.get("empty", 0) + result.get("fail", 0)
        _mirror_sync_ok = not (_mirror_total and not result.get("ok") and not result.get("empty")
                                and result.get("fail"))
        run_log.record_run(BASE_DIR, "mirror_finnomena_normal", _mirror_sync_ok, summary)
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

        fund_warnings = _refresh_svc.run_with_progress(cb, BASE_DIR, period=period) or []
        _market_internals_cache.clear()
        _breadth_cache.clear()
        # set_data.json เพิ่ง rewrite ใหม่ (หุ้นเข้า/ออก/เปลี่ยนชื่อได้) — _qpl_parsed_cache เก็บ
        # sector_by_symbol ที่ derive จากไฟล์นี้ไว้นานสุด 24h เดิม ไม่ล้างจะทำให้ Growth Screener
        # โชว์หุ้นเก่าที่หายไปแล้วค้างอยู่จนกว่าจะมี financials sync รอบถัดไป
        _qpl_parsed_cache.clear()
        _bump_cache_gen()

        # อัพเดท Indices (full history) + Capital Flow ล้มได้โดยไม่ทำให้ทั้งรอบ
        # ล้ม (ราคาหุ้น SET หลักได้ไปแล้ว) แต่ต้องโผล่ในผลลัพธ์รอบนี้ ไม่งั้น
        # run_log บันทึกว่า "สำเร็จ" ทั้งที่มีส่วนล้มเงียบๆ — ผู้ใช้ไม่มีทางรู้
        # (fund_warnings มาจาก run_with_progress เอง — เช่น fundamentals ดึงไม่สำเร็จ
        # ทั้ง SET API และ Yahoo แล้ว fallback ค่าเก่าแทน)
        warnings = list(fund_warnings)

        _update(current=98, total=100, message="อัพเดท Indices...")
        try:
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv_guarded(existing, full_refresh=True)
            if result is not None:
                _indices_cache["data"] = result
        except Exception as e:
            print(f"[FullRefresh] Indices error: {e}")
            warnings.append(f"Indices ล้มเหลว: {e}")

        # อัพเดท Capital Flow (SET + S50 Futures + Thai Bond — เดิม Full Refresh ดึงแค่ SET
        # ตัวเดียว ต่างจาก Quick Update ที่ดึงครบ 3 ตลาด ทำให้กด Full Refresh แล้วแท็บ S50/Bond
        # ยังโชว์ข้อมูลค้างนานสุด 4 ชม. (TTL cache) ทั้งที่หน้าเว็บบอกว่าเพิ่งอัพเดทเสร็จ)
        try:
            _fetch_flow_data()
        except Exception as e:
            print(f"[FullRefresh] Capital Flow error: {e}")
            warnings.append(f"Capital Flow ล้มเหลว: {e}")
        try:
            _fetch_flow_s50_data()
        except Exception as e:
            print(f"[FullRefresh] S50 Futures Flow error: {e}")
            warnings.append(f"S50 Futures Flow ล้มเหลว: {e}")
        try:
            _fetch_flow_bond_data()
        except Exception as e:
            print(f"[FullRefresh] Bond Flow error: {e}")
            warnings.append(f"Bond Flow ล้มเหลว: {e}")

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
        # _update(done=True,...) ต้องมาก่อน record_run() เสมอ (ไม่ใช่ทีหลัง) —
        # กัน SSE ค้างสถานะ "กำลังทำงาน" ตลอดไปถ้า record_run เขียนไฟล์พังกลางทาง
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
        run_log.record_run(BASE_DIR, "full_refresh", False, str(e))
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
        # อ่านใต้ lock เดียวกับตอน rebuild/merge — บน Windows ถ้ามี read handle ค้างอยู่ตอน
        # _atomic_write_json ทำ os.replace() ฝั่งเขียนจะโดน PermissionError แล้ว refresh 500
        with _market_stats_lock:
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
        with _market_stats_lock:
            with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
                data = json.load(f)
    except Exception:
        return jsonify({"updated_at": None})
    if not isinstance(data, dict):
        # ไฟล์เขียนไม่สมบูรณ์ (เช่น null / [] จาก partial write) — อย่าให้ .get() โยน 500
        return jsonify({"updated_at": None})
    pe_latest = ((data.get("pe") or {}).get("dates") or [None])[-1]
    pbv_latest = ((data.get("pbv") or {}).get("dates") or [None])[-1]
    # เดือนล่าสุดของข้อมูลที่มาจาก Market_Statistics_Month_th_TH.xls เท่านั้น
    # (ปันผล/มูลค่าหลักทรัพย์/จำนวนบริษัทจดทะเบียน) — Table_PE/PBV.xls ไม่มี series พวกนี้
    # อาจไม่ตรงกับ pe_date/pbv_date เมื่อ rebuild ด้วย Table_PE/PBV.xls ที่เก่ากว่า
    _monthly_dates = [
        d for d in (
            ((data.get("div_yield") or {}).get("dates") or [None])[-1],
            ((data.get("mkt_cap") or {}).get("dates") or [None])[-1],
            ((data.get("breadth") or {}).get("dates") or [None])[-1],
        ) if d
    ]
    monthly_latest = max(_monthly_dates) if _monthly_dates else None
    return jsonify({
        "updated_at": pe_latest,
        "pe_date": pe_latest,
        "pbv_date": pbv_latest,
        "monthly_date": monthly_latest,
    })


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
    import pandas as pd
    from datetime import datetime as _dt

    # calc_stats: ใช้ตัวเดียวกับ sources/set_market_stats_monthly + import_market_stats.py
    # (เดิม copy สูตร percentile/zscore/median/bands ไว้ 3 ที่ — แก้จุดเดียวไม่ครบเสี่ยงเพี้ยน)
    from sources.set_market_stats_monthly import calc_stats

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
            except Exception as e:
                # ไฟล์เดิมเสีย/อ่านไม่ขึ้น — อย่า rebuild ทับแบบเงียบๆ เพราะ output ที่ได้จะขาด
                # div_yield/mkt_cap/breadth ที่สะสมไว้ (โค้ดคงซีรีส์พวกนี้จาก old เท่านั้น) +
                # ขาดเดือนท้ายที่ merge จากไฟล์รายเดือน — เหมือน /api/refresh-market-stats-monthly
                # ที่ abort ในเคสเดียวกัน (code review 2026-09-04)
                return jsonify({"ok": False, "error": f"อ่าน set_market_stats.json เดิมไม่สำเร็จ: {e} "
                                                      "— ไม่เขียนทับ เพื่อกันประวัติปันผล/มูลค่าหลักทรัพย์/breadth หาย"}), 500
        if not isinstance(old, dict):
            old = None
        if old:
            old_latest = ((old.get("pe") or {}).get("dates") or [None])[-1]
            # shrink guard: Table_PE/PBV.xls โตขึ้นเรื่อยๆ (เติมเดือนล่าสุด) ไม่เคยสั้นลง —
            # ไฟล์ดาวน์โหลดไม่ครบ (สายหลุด) parse ได้ไม่กี่แถวจะผ่าน emptiness guard ข้างบนแล้ว
            # rebuild ทับ ทำให้ประวัติ P/E-P/BV ย้อนหลังหายถาวร (re-append คืนให้เฉพาะเดือนที่
            # "ใหม่กว่า" งวดท้ายไฟล์ ของเก่ากว่านั้นหายเกลี้ยง) — เผื่อ revision เล็กน้อย 3 เดือน
            for _k, _new in (("pe", pe_data), ("pbv", pbv_data)):
                _old_n = len((old.get(_k) or {}).get("dates") or [])
                if _old_n and len(_new["dates"]) < _old_n - 3:
                    return jsonify({"ok": False, "error":
                        f"ไฟล์ {_k.upper()} อ่านได้ {len(_new['dates'])} เดือน แต่ของเดิมมี {_old_n} เดือน "
                        "— ไฟล์ .xls อาจดาวน์โหลดไม่ครบ ไม่เขียนทับเพื่อกันประวัติย้อนหลังหาย "
                        "(ถ้าตั้งใจ rebuild จากไฟล์สั้น ให้ลบ set_market_stats.json เดิมก่อน)"}), 400

        new_latest = pe_data["dates"][-1] if pe_data["dates"] else None

        output = {
            "updated_at": _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            # copy list ออกจาก pe_data/pbv_data — ด้านล่าง re-append เดือนท้ายลง output[..]["dates"]
            # แบบ in-place ถ้าใช้ list เดียวกัน pe_data['dates'] จะถูกดันตาม ทำให้ pe_range/pe_months
            # ใน response (ที่ควรบอก "ช่วงข้อมูลในไฟล์ .xls") เพี้ยนเกินจริง
            "pe":  {"dates": list(pe_data["dates"]),
                    "series": {k: list(v) for k, v in pe_data["series"].items()},
                    "stats": {k: calc_stats(v) for k, v in pe_data["series"].items()}},
            "pbv": {"dates": list(pbv_data["dates"]),
                    "series": {k: list(v) for k, v in pbv_data["series"].items()},
                    "stats": {k: calc_stats(v) for k, v in pbv_data["series"].items()}},
        }
        # ซีรีส์ที่มีเฉพาะใน Market_Statistics_Month_th_TH.xls (Table_PE/PBV ไม่มี) ต้องคงไว้ —
        # เดิม rebuild ทับทั้งไฟล์ ทำให้ประวัติปันผล/มูลค่าหลักทรัพย์/breadth ที่สะสมมาหายทั้งชุด
        for _k in ("div_yield", "mkt_cap", "breadth"):
            if old and _k in old:
                output[_k] = old[_k]

        # Table_PE/PBV.xls ที่ดาวน์โหลดมืออาจเก่ากว่าเดือนล่าสุดที่ merge จาก
        # Market_Statistics_Month_th_TH.xls (/api/refresh-market-stats-monthly) ไว้แล้ว —
        # ถ้า rebuild ทับตรงๆ เดือนท้ายๆ (SET/mai P/E-P/BV) ที่สะสมไว้จะหาย แถม pe/pbv จะจบ
        # คนละเดือนกับ div_yield/mkt_cap/breadth ที่เพิ่งคงไว้ข้างบน — เติมเดือนที่ใหม่กว่างวด
        # ท้ายของ .xls กลับเข้าไป (คอลัมน์กลุ่มดัชนีย่อยที่ไฟล์รายเดือนไม่มีจะเป็น None)
        for _key in ("pe", "pbv"):
            if not (old and isinstance(old.get(_key), dict)):
                continue
            o_dates = old[_key].get("dates") or []
            o_series = old[_key].get("series") or {}
            n = output[_key]
            last_new = n["dates"][-1] if n["dates"] else ""
            appended = False
            for i, d in enumerate(o_dates):
                if d <= last_new or d in n["dates"]:
                    continue
                n["dates"].append(d)
                for c in n["series"]:
                    ov = o_series.get(c) or []
                    n["series"][c].append(ov[i] if i < len(ov) else None)
                appended = True
            if appended:
                n["stats"] = {k: calc_stats(v) for k, v in n["series"].items()}

        new_latest = output["pe"]["dates"][-1] if output["pe"]["dates"] else new_latest

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
            try:
                with open(_MARKET_STATS_FILE, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                return jsonify({"ok": False, "error": f"อ่าน set_market_stats.json เดิมไม่สำเร็จ: {e}"}), 500
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
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
        v = trim_outliers([x for x in vals if isinstance(x, (int, float)) and x > 0])
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
_cache_gen_lock = threading.Lock()

def _bump_cache_gen():
    # +=1 บน dict ไม่ atomic ข้าม bytecode — ล็อกกันสอง thread bump พร้อมกันแล้วนับตกไป 1 ครั้ง
    with _cache_gen_lock:
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
# in-flight guard — Overview โหลดครั้งแรกยิง /api/market-internals ซ้อนกันได้ (renderAll +
# showPage setTimeout) ตอน cache ยังว่าง ทั้งสอง request จะรัน compute_breadth (pandas EMA/
# rolling ~1,200 หุ้น ~1-2 วิ) พร้อมกันบน dev server — pattern เดียวกับ breadth/screener mirror
_market_internals_lock = threading.Lock()

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

    with _market_internals_lock:
        # request ที่สองรอ lock แล้วเจอผลจาก request แรกใน cache — ไม่ต้อง compute ซ้ำ
        cached = _market_internals_cache.get("data")
        if cached:
            return jsonify(cached)

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
        # jp_index_membership.diff_membership รับ mirror_jp= เหมือน US/HK — reuse _check_wiki_index
        # ตัวเดียวกัน (ได้ mirror_gap ติดมาด้วย) ไม่ต้องเขียนลูปนับซ้ำ
        from sources import jp_index_membership
        _check_wiki_index("JP", jp_index_membership, "JP")


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

        fund_warnings = _refresh_svc.run_quick_update(cb, BASE_DIR) or []
        if fund_warnings:
            # fund_warnings มาจาก run_quick_update เอง — fundamentals (P/E, P/BV, Market Cap,
            # Div Yield) ดึงไม่สำเร็จทั้งหมด (SET API + Yahoo ล้มเหลวทั้งคู่) ต้องโผล่ใน
            # failed_steps เหมือนขั้นตอนเสริมอื่นๆ ไม่งั้นเงียบหาย (code review 2026-08-27:
            # เดิม run_quick_update ไม่มี warnings/return เลย ต่างจาก run_with_progress)
            print(f"[QuickUpdate] Fundamentals warning: {'; '.join(fund_warnings)}")
            failed_steps.append("Fundamentals")
        _market_internals_cache.clear()
        _breadth_cache.clear()
        _bump_cache_gen()

        def _insider():
            from sources import sec_store as _sec_store
            # ต้องผ่าน _sec_first_sync_lock เดียวกับ _ensure_sec_db_ready()/insider_sync()
            # ไม่งั้น Quick Update เขียน sec_filings.db พร้อมกับ request /api/insider-trades
            # ที่มาชนกันเองได้ (เคยเจอ sqlite throw "database is locked" มาแล้ว)
            with _sec_first_sync_lock:
                _sec_store.sync_insider_trades(BASE_DIR)
                _sec_store.sync_major_changes(BASE_DIR)
            _sec_store.bake_backup(BASE_DIR)   # no-op นอก CI — ดู sec_store.bake_backup
            _invalidate_flow_signals()   # insider เป็น 1 ใน 3 ชั้นของสัญญาณรวม
        _sub_step("Insider/ผู้ถือหุ้นใหญ่", 91, "อัพเดท Insider/ผู้ถือหุ้นใหญ่...", _insider)

        def _indices():
            global _indices_cache
            existing = _load_indices_existing()
            result, _stats = _fetch_indices_tv_guarded(existing, full_refresh=False)
            if result is not None:
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

        # สะสม PE/PBV/BVPS/DivYield รายวันทางการจาก SET.or.th (งาน #4B) — endpoint ล็อค
        # ความลึก ~6 เดือน ต้องเก็บเองทุกวันถึงจะได้ประวัติยาวกว่านั้น (pattern เดียวกับ SET
        # P&L รายไตรมาส) · งานหลักย้ายไปทำใน run_quick_update แล้ว (tier 0 ของ
        # _fetch_fundamentals_with_fallback — sync_all ที่นั่นทั้งสะสมประวัติ + ป้อน
        # pe/pbv/mkt_cap/div_yield ให้ set_data.json ในนัดเดียว ตัดการยิง highlight-data
        # ~930 req/วันซ้ำ ทั้งคู่ได้ค่าเดียวกันเป๊ะ ยืนยัน 2026-08-27) · ตรงนี้เหลือไว้เป็น
        # safety net: idempotent (skip_up_to_date → no-op ทันทีถ้า tier 0 sync ไปแล้ว) —
        # ครอบเคส run_quick_update early-return ("ไม่มีข้อมูลใหม่"/"เป็นปัจจุบันแล้ว") ที่
        # เธรด fundamentals ไม่ได้เริ่ม + คืน failed_steps ให้เห็นถ้า sync ล้มจริง
        def _daily_val():
            # ต้องมี callback ผูกกับ _update — เดิมไม่มีเลย ทำให้ตอนที่ safety-net นี้เป็น
            # จุดเดียวที่ต้องรันจริง (run_quick_update early-return ก่อนเริ่ม tier-0 thread)
            # _state['message']/['current'] ค้างเฉยๆ ตลอดที่ sync_all ทำงาน (อาจนานถึงหลัก
            # สิบนาทีถ้า SET.or.th ช้า) เสี่ยงชน SSE stall-timeout 20 นาที (progress_stream)
            # ทั้งที่งานจริงยังทำอยู่ (code review 2026-08-27)
            def _dv_cb(current, total, msg):
                _update(current=99, total=100, message=msg)
            n, total_t, rows = set_daily_val.sync_all(BASE_DIR, skip_up_to_date=True, callback=_dv_cb)
            if total_t:
                print(f"[QuickUpdate] PE/PBV รายวัน (SET) safety net: สะสม {n}/{total_t} ตัว (+{rows} แถว)")
        _sub_step("PE/PBV รายวัน (SET)", 99, "สะสม PE/PBV รายวันทางการ (SET.or.th)...", _daily_val)

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
    """sync insider/major แยกกันตาม meta key ของแต่ละอัน (ตั้งค่าเฉพาะตอน sync สำเร็จ
    ดู sec_store.sync_insider_trades/sync_major_changes) — เดิมเช็คแค่ db_exists() ทั้ง
    ไฟล์ ทำให้ถ้า sync_insider_trades() สำเร็จ (สร้างไฟล์ DB แล้ว) แต่ sync_major_changes()
    พังตอน sync ครั้งแรกสุด (network hiccup ฯลฯ) db_exists() จะเป็น True ถาวรและ
    major-changes ไม่ถูก retry อีกเลย เช็คแยกทำให้ retry ต่อได้จนกว่าจะสำเร็จจริง"""
    with _sec_first_sync_lock:
        if not sec_store._get_meta(BASE_DIR, "insider_last_synced_at"):
            sec_store.sync_insider_trades(BASE_DIR)
        if not sec_store._get_meta(BASE_DIR, "major_last_synced_at"):
            sec_store.sync_major_changes(BASE_DIR)


@app.route("/api/insider-trades")
def insider_trades():
    """ผู้บริหารซื้อขายหุ้น (แบบ 59) — อ่านจากฐานข้อมูลสะสม (sec_filings.db)
    ไม่ยิง SEC สดทุกครั้งอีกต่อไป ข้อมูลใหม่เข้ามาจาก sync_insider_trades()
    (เรียกตอน Quick Update / auto-update cron) เท่านั้น"""
    from datetime import datetime as _dt, timedelta as _td

    try:
        days = int(request.args.get("days", 30))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))

    if not sec_store._get_meta(BASE_DIR, "insider_last_synced_at"):
        # ยังไม่เคย sync สำเร็จเลย -> sync ครั้งแรก (ช้า ~2-3 นาที ดึงย้อนหลัง 180 วัน)
        try:
            _ensure_sec_db_ready()
        except Exception as e:
            return jsonify({"error": f"sync ครั้งแรกล้มเหลว: {e}"}), 500

    try:
        records = sec_store.query_insider_trades(BASE_DIR, days)
        last_synced = sec_store._get_meta(BASE_DIR, "insider_last_synced_at")
    except Exception:
        # sec_filings.db อาจถูก Quick Update/insider-sync เขียนอยู่พอดี (sqlite locked ชั่วคราว)
        # ถือว่ายังไม่มีข้อมูลตอนนี้แทน 500 (pattern เดียวกับ /api/calendar-events) — กัน
        # Tearsheet ที่ยิง endpoint นี้พร้อมกันหลายหุ้นผ่าน Watchlist ทั้งหน้าล้มเพราะ lock ชั่วคราว
        records = []
        try:
            last_synced = sec_store._get_meta(BASE_DIR, "insider_last_synced_at")
        except Exception:
            last_synced = None
    return jsonify({
        "records": records,
        "days": days,
        "from": (_dt.now() - _td(days=days)).strftime("%Y-%m-%d"),
        "to":   _dt.now().strftime("%Y-%m-%d"),
        "fetched_at": last_synced or "-",
    })


@app.route("/api/major-changes")
def major_changes():
    """ผู้ถือหุ้นรายใหญ่เปลี่ยนแปลง (แบบ 246-2) — อ่านจากฐานข้อมูลสะสม (sec_filings.db)"""
    from datetime import datetime as _dt, timedelta as _td

    try:
        days = int(request.args.get("days", 30))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))

    if not sec_store._get_meta(BASE_DIR, "major_last_synced_at"):
        try:
            _ensure_sec_db_ready()
        except Exception as e:
            return jsonify({"error": f"sync ครั้งแรกล้มเหลว: {e}"}), 500

    try:
        records = sec_store.query_major_changes(BASE_DIR, days)
        last_synced = sec_store._get_meta(BASE_DIR, "major_last_synced_at")
    except Exception:
        # เหตุผลเดียวกับ /api/insider-trades — sqlite lock ชั่วคราวไม่ควรทำให้ Tearsheet 500
        records = []
        try:
            last_synced = sec_store._get_meta(BASE_DIR, "major_last_synced_at")
        except Exception:
            last_synced = None
    return jsonify({
        "records": records,
        "days": days,
        "from": (_dt.now() - _td(days=days)).strftime("%Y-%m-%d"),
        "to":   _dt.now().strftime("%Y-%m-%d"),
        "fetched_at": last_synced or "-",
    })


@app.route("/api/insider-sync", methods=["POST"])
def insider_sync():
    """sync ฐานข้อมูลสะสม SEC filings แบบ manual (เรียกจากปุ่มในหน้า Insider) — ใช้
    _sec_first_sync_lock เดียวกับ auto-sync ครั้งแรก (_ensure_sec_db_ready) กันแข่งกัน
    เขียน sec_filings.db พร้อมกัน (เช่น 2 แท็บเปิดพร้อมกัน หรือกดปุ่มระหว่าง auto-sync
    รอบแรกกำลัง backfill 180 วันอยู่) ซึ่งเคยชนกันจน sqlite throw 'database is locked'"""
    # กันยิงซ้อน — 2 แท็บกดปุ่มพร้อมกัน หรือกดระหว่าง auto-sync ครั้งแรกกำลัง backfill
    # 180 วันอยู่ เดิม with _sec_first_sync_lock บล็อกเฉยๆ ทำให้แต่ละ request รอคิวแล้ว
    # ดึง SEC ซ้ำอีกรอบเต็มๆ ต่อกัน (หลายนาที) — ถ้ามีงาน sync อยู่แล้ว ตอบกลับทันที
    if not _sec_first_sync_lock.acquire(blocking=False):
        return jsonify({"ok": False, "error": "มีงาน sync SEC กำลังทำงานอยู่ ลองใหม่อีกครั้ง"}), 429
    try:
        n1 = sec_store.sync_insider_trades(BASE_DIR)
        n2 = sec_store.sync_major_changes(BASE_DIR)
        _invalidate_flow_signals()   # insider เป็น 1 ใน 3 ชั้นของสัญญาณรวม
        return jsonify({"ok": True, "insider_fetched": n1, "major_fetched": n2})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _sec_first_sync_lock.release()


@app.route("/api/sync-esg", methods=["POST"])
def sync_esg_route():
    """sync CG/ESG rating ทั้งตลาดแบบ manual (เรียกจากปุ่มในหน้า Screener+) — 1 call
    เดียวได้ทั้งตลาด (/api/set/esg/list) เร็ว ไม่ต้อง progress bar (ดู
    sources/set_company.py::sync_esg — coverage ~38% ของหุ้นสามัญ ปกติ)"""
    try:
        n = set_company.sync_esg(BASE_DIR)
        meta = set_company.get_meta(BASE_DIR)
        return jsonify({"ok": True, "fetched": n, "count": meta["esg_count"],
                        "updated_at": meta["esg_updated_at"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sync-shareholders", methods=["POST"])
def sync_shareholders_route():
    """sync ผู้ถือหุ้นใหญ่+free float ทั้งกระดานแบบ manual (เรียกจากปุ่มในหน้า
    Screener+/Insider) — พารัลเลลทั้งกระดาน ~8 วิ (ดู
    sources/set_company.py::sync_shareholders) ไม่ต้อง progress bar"""
    try:
        n, total = set_company.sync_shareholders(BASE_DIR)
        meta = set_company.get_meta(BASE_DIR)
        return jsonify({"ok": True, "fetched": n, "total": total,
                        "count": meta["shareholder_count"],
                        "updated_at": meta["shareholder_updated_at"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sync-foreign-room", methods=["POST"])
def sync_foreign_room_route():
    """sync โควตาต่างชาติทั้งกระดานแบบ manual (เรียกจากปุ่มในหน้า Screener+/Tearsheet)
    — พารัลเลลทั้งกระดาน ~7 วิ (ดู sources/set_company.py::sync_profiles)"""
    try:
        n, total = set_company.sync_profiles(BASE_DIR)
        meta = set_company.get_meta(BASE_DIR)
        return jsonify({"ok": True, "fetched": n, "total": total,
                        "count": meta["foreign_count"],
                        "updated_at": meta["foreign_updated_at"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _run_analyst_consensus_sync():
    """sync ฉันทามตินักวิเคราะห์จาก Yahoo — หุ้นไทยทั้งกระดาน + underlying US/HK ของ DR
    งานพื้นหลังยาว (รอบแรก ~10-12 นาที เพราะยิง yfinance ทีละตัว 3 เธรด, รอบถัดไปข้าม
    ตัวที่ sync <7 วันเกือบหมด)"""
    t0 = time.time()
    try:
        def _cb(done, total, msg):
            pct = (done / max(total, 1)) * 98
            _update(current=round(pct), total=100, message=msg)
        n_ok, n_tot = analyst_consensus.sync_th(BASE_DIR, callback=_cb, max_age_days=7)
        dr_ok, dr_tot = analyst_consensus.sync_dr(BASE_DIR, callback=_cb, max_age_days=7)
        meta = analyst_consensus.get_meta(BASE_DIR)
        th_meta = meta.get("TH", {})
        dr_cnt = sum(meta.get(m, {}).get("count", 0) for m in ("US", "HK"))
        dr_holders = sum(meta.get(m, {}).get("with_holders", 0) for m in ("US", "HK"))
        elapsed = (time.time() - t0) / 60
        if n_tot == 0 and dr_tot == 0:
            summary = "ข้อมูลนักวิเคราะห์เป็นปัจจุบันแล้ว (ทุกตัว sync ภายใน 7 วัน)"
        else:
            summary = (f"เสร็จแล้ว! sync ใหม่ หุ้นไทย {n_ok}/{n_tot} · DR underlying {dr_ok}/{dr_tot} "
                       f"· ในฐานข้อมูล: หุ้นไทยมีราคาเป้าหมาย {th_meta.get('with_target', 0)}/"
                       f"{th_meta.get('count', 0)} ตัว, DR underlying {dr_cnt} ตัว "
                       f"(มีโครงสร้างผู้ถือหุ้น {dr_holders} ตัว) "
                       f"· ใช้เวลา {elapsed:.0f} นาที")
        _update(done=True, message=summary)
        run_log.record_run(BASE_DIR, "analyst_consensus_sync", True, summary)
    except Exception as e:
        _update(done=True, error=str(e), message=f"เกิดข้อผิดพลาด: {e}")
        run_log.record_run(BASE_DIR, "analyst_consensus_sync", False, str(e))
    finally:
        _update(running=False)


@app.route("/api/sync-analyst-consensus", methods=["POST"])
def sync_analyst_consensus_route():
    """ปุ่ม 'sync ฉันทามตินักวิเคราะห์' (หน้า Screener+ / Data Health) — งานพื้นหลังยาว
    เหมือน financials-update-all ใช้ SSE /api/progress ติดตาม"""
    with _lock:
        if _state["running"]:
            return jsonify({"error": "กำลังดึงข้อมูลอยู่แล้ว โปรดรอสักครู่"}), 409
        _state.update(running=True, done=False, error=None, current=0, total=100,
                      message="กำลังเริ่ม sync ฉันทามตินักวิเคราะห์...")
    threading.Thread(target=_run_analyst_consensus_sync, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/analyst-consensus/<symbol>")
def analyst_consensus_one(symbol):
    """ฉันทามตินักวิเคราะห์ของหุ้น 1 ตัวจาก DB (Yahoo: ราคาเป้าหมาย mean/high/low +
    Buy/Hold/Sell + EPS/รายได้ estimate + โครงสร้างผู้ถือหุ้น major/institutional/insider
    สำหรับ underlying US/HK) — อ่านเร็ว ไม่ยิง Yahoo สด ใช้กับการ์ดใน Tearsheet
    คืน 404 ถ้ายังไม่เคย sync ตัวนี้ (coverage ~40% ของหุ้นไทย เอียง cap ใหญ่)

    ?market=US|HK|JP สำหรับ underlying ของ DR (default TH)"""
    market = (request.args.get("market") or "TH").upper()
    d = analyst_consensus.get_one(BASE_DIR, symbol, market)
    if not d:
        return jsonify({"error": f"ยังไม่มีข้อมูลนักวิเคราะห์ของ {symbol} — "
                                  f"อาจไม่มีนักวิเคราะห์ตาม หรือยังไม่ได้ sync"}), 404
    return jsonify(d)


@app.route("/api/company-ownership/<symbol>")
def company_ownership(symbol):
    """ผู้ถือหุ้นใหญ่ + โควตาต่างชาติของหุ้น 1 ตัว — อ่านจาก DB ที่ sync ไว้แล้ว
    (set_company.get_ownership, เร็ว ไม่ยิง SET สด) ใช้กับการ์ด "🏛 โครงสร้างผู้ถือหุ้น"
    ใน Tearsheet + section ผู้ถือหุ้นใหญ่ปัจจุบันในหน้า Insider คืน 404 ถ้ายังไม่เคย
    sync หุ้นตัวนี้เลย (ต้องกด sync ก่อนอย่างน้อย 1 ครั้ง)"""
    d = set_company.get_ownership(BASE_DIR, symbol.upper().strip())
    if not d:
        return jsonify({"error": f"ยังไม่มีข้อมูลผู้ถือหุ้น/โควตาต่างชาติของ {symbol} — "
                                  f"กด sync ในหน้า Screener+ ก่อน"}), 404
    return jsonify(d)


_foreign_room_check_cache: dict = {}
_FOREIGN_ROOM_CHECK_TTL = 900   # 15 นาที เหมือน check-trading-halt-th


@app.route("/api/check-foreign-room", methods=["POST"])
def check_foreign_room():
    """sync โควตาต่างชาติทั้งกระดานสดแล้วคืนตัวที่ "ห้องต่างชาติใช้ไปแล้วมากสุด"
    เรียงจากมากไปน้อย + เทียบกับ Watchlist แยกไว้ (แนวเดียวกับ check_trading_halt_th)
    ใช้กับปุ่ม "🌏 เช็คห้องต่างชาติใกล้เต็ม" ในหน้า Valuation"""
    cached = _foreign_room_check_cache.get("result")
    if cached and (time.time() - _foreign_room_check_cache.get("ts", 0) < _FOREIGN_ROOM_CHECK_TTL):
        return jsonify(cached)
    try:
        set_company.sync_profiles(BASE_DIR)
        rows = set_company.get_foreign_room_ranking(BASE_DIR)
        try:
            with open(WATCHLIST_FILE, encoding="utf-8") as f:
                wl_all = json.load(f)
        except Exception:
            wl_all = []
        wl_syms = {s for s in wl_all if isinstance(s, str) and ":" not in s}
        watchlist_hits = [r for r in rows if r["symbol"] in wl_syms]
        meta = set_company.get_meta(BASE_DIR)
        result = {"ok": True, "as_of": meta["foreign_updated_at"], "total_count": len(rows),
                  "watchlist_hits": watchlist_hits, "rows": rows}
        _foreign_room_check_cache["result"] = result
        _foreign_room_check_cache["ts"] = time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# Short Sales
# ============================================================

_SHORT_DATA_FILE = os.path.join(BASE_DIR, "short_sales_data.json")
_short_data_cache = None
_short_data_ts    = 0.0

def _load_short_data():
    # cache invalidation อิง mtime ล้วน (ไม่มี TTL) — เขียนไฟล์ใหม่เมื่อไหร่ mtime เปลี่ยน
    # รอบถัดไปโหลดใหม่เอง
    global _short_data_cache, _short_data_ts
    if not os.path.exists(_SHORT_DATA_FILE):
        return None
    mtime = os.path.getmtime(_SHORT_DATA_FILE)
    if _short_data_cache and mtime == _short_data_ts:
        return _short_data_cache
    try:
        with open(_SHORT_DATA_FILE, encoding="utf-8") as f:
            _short_data_cache = json.load(f)
    # ValueError ครอบทั้ง json.JSONDecodeError และ UnicodeDecodeError (อ่านโดนกลางไฟล์
    # ที่ import_short_sales.py กำลังเขียน multi-byte อยู่) — คืน cache เดิมแทน 500
    except (ValueError, OSError) as e:
        print(f"[Short Sales] failed to load {_SHORT_DATA_FILE}: {e}")
        return _short_data_cache
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
                # หุ้นที่เพิ่งสร้างจาก fallback ล้วนๆ (ไม่โผล่ในชุด live วันนี้) top-level
                # short_pos/short_pos_pct จะค้าง 0 ทั้งที่ .daily มีค่าจริง -> /api/short-sales
                # จะคืน 0/0 แล้วโดน default filter ของตารางซ่อนหาย sync ให้ตรง snapshot ล่าสุด
                # (เทียบ nvdr_daily_update ที่ทำแบบเดียวกัน) — ตัวที่อยู่ในชุด live วันนี้ถูก
                # เขียนทับด้วยค่าสดในลูปด้านล่างอยู่แล้ว
                if s["daily"]:
                    _last = s["daily"][-1]
                    s["short_pos"]     = _last.get("short_pos", 0)
                    s["short_pos_pct"] = _last.get("short_pos_pct", 0)
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
# background refresh ของ /api/market-flow (stale-while-revalidate) — ลงทะเบียนเข้า
# _any_job_running() ด้วย ไม่งั้นกด "↺ Restart" ตอนกำลัง fetch อยู่จะตัดทิ้งเงียบๆ
_flow_fetch_state = {"running": False}
_flow_fetch_lock = threading.Lock()


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


def _load_flow_accumulator(file_path, github_fallback_fn, tag, fields=None):
    """โหลดไฟล์สะสม flow (market/s50/bond) + เติมวันที่ขาดจาก GitHub fallback —
    คืน dict {date: row} · fields != None = เก็บเฉพาะ key พวกนั้น (SET flow กรอง;
    S50/Bond เก็บ dict ดิบทั้งก้อน) · ไม่ทับของเดิมจาก fallback (ไฟล์ local อาจแก้มือไว้)
    (สกัดจากส่วนต้นที่ 3 pipeline ทำเหมือนกัน — ดู CAPITAL_FLOW_DEFERRED_FINDINGS ข้อ 3)"""
    def _norm(r0):
        return r0 if fields is None else {k: r0.get(k) for k in fields}
    rows_by_date = {}
    try:
        with open(file_path, encoding="utf-8") as f:
            for r0 in (json.load(f).get("rows") or []):
                if r0.get("date"):
                    rows_by_date[r0["date"]] = _norm(r0)
    except Exception:
        pass
    try:
        for r0 in github_fallback_fn():
            if r0.get("date") and r0["date"] not in rows_by_date:
                rows_by_date[r0["date"]] = _norm(r0)
    except Exception as e:
        print(f"[{tag}] GitHub fallback ไม่ได้ ({e})")
    return rows_by_date


def _finalize_flow_result(rows_by_date, sources, file_path, cache, post_sort=None,
                          stale_source_label="ไฟล์สะสม (ดึงใหม่ไม่สำเร็จ)"):
    """sort by date → (optional) post_sort hook (SET flow คำนวณ chg) → build result dict
    {rows, fetched_at, latest_date, sources} → เขียนไฟล์สะสม (atomic) เฉพาะตอนรันบน GitHub
    Actions + มี source ใหม่จริง (เจ้าของไฟล์เดียว กัน auto-push ชน commit ของ Actions) →
    อัพเดท cache ในหน่วยความจำ · latest_date = วันที่ของแถวจริงล่าสุด (ต่างจาก fetched_at ที่
    เป็นเวลา "ตอนนี้" เสมอ — ใช้เช็คว่าข้อมูลค้างจริงไหม)
    (สกัดจากส่วนท้ายที่ 3 pipeline ทำเหมือนกัน — ดู CAPITAL_FLOW_DEFERRED_FINDINGS ข้อ 3)"""
    rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    if post_sort:
        post_sort(rows)
    result = {"rows": rows, "fetched_at": time.strftime("%Y-%m-%d %H:%M"),
              "latest_date": rows[-1]["date"] if rows else None,
              "sources": sources or [stale_source_label]}
    if sources and _IS_GITHUB_ACTIONS:
        _atomic_write_json(file_path, {"rows": rows, "updated_at": result["fetched_at"]})
    cache["data"] = result
    cache["ts"]   = time.time()
    return result


def _fetch_flow_data():
    """รวมข้อมูล Capital Flow จากไฟล์สะสม + GitHub fallback + siamchart + SET API —
    คำนวณ chg แล้วอัพเดท _flow_cache เสมอ · บันทึกไฟล์สะสม (atomic) เฉพาะตอนรันบน GitHub
    Actions เท่านั้น — เครื่อง local ไม่เขียนไฟล์ 3 ไฟล์ flow (market/s50/bond) นี้อีกแล้ว
    กัน auto-push ยิงทับ/ชน commit ของ Actions (ดู _IS_GITHUB_ACTIONS) เครื่อง local ยังได้
    ข้อมูลสดครบเหมือนเดิมผ่าน _flow_cache ในหน่วยความจำ แค่ไม่ persist ลงไฟล์ที่ push ขึ้น repo
    ล้มเฉพาะเมื่อไม่มีข้อมูลเลยจริงๆ"""
    rows_by_date = _load_flow_accumulator(
        _MARKET_FLOW_FILE, _fetch_flow_market_github_fallback, "Flow",
        fields=("date", "fund", "foreign", "retail", "set"))

    sources = []
    try:
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZoneInfo
        now_th = _dt.now(_ZoneInfo("Asia/Bangkok"))
        today_th = now_th.strftime("%Y-%m-%d")
        market_closed_today = (now_th.hour, now_th.minute) >= (17, 45)
        for r0 in _fetch_flow_siamchart():
            if not r0.get("date"):
                continue   # กัน str(None)[:10] == 'None' หลุดเข้า rows_by_date เป็น key ปลอม
            if r0["date"] == today_th and not market_closed_today:
                # ตลาดยังไม่ปิดวันนี้ — เลขของ siamchart ยังไม่นิ่ง (ไหลระหว่างวัน) ข้ามไปก่อน
                # กันเขียนทับ SET-official ที่นิ่งแล้วจาก run รอบหลัง (SET-official เอง gate
                # ด้วยเวลาเดียวกันนี้อยู่แล้ว — ดู _fetch_flow_set_official)
                continue
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

    def _add_chg(rows):
        for i in range(1, len(rows)):
            prev, curr = rows[i - 1].get("set"), rows[i].get("set")
            rows[i]["chg"] = round(curr - prev, 2) if prev and curr else None
        rows[0]["chg"] = None

    return _finalize_flow_result(
        rows_by_date, sources, _MARKET_FLOW_FILE, _flow_cache, post_sort=_add_chg,
        stale_source_label="ไฟล์สะสม (ดึงใหม่ไม่สำเร็จทั้งสองแหล่ง)")


def _kick_flow_refresh():
    """refresh _flow_cache ของ /api/market-flow ใน background thread — stale-while-revalidate
    เหมือน _kick_dr_rebuild ของหน้า DR · เดิม route เรียก _fetch_flow_data() ตรงๆ ใน request
    path ซึ่ง _fetch_flow_siamchart() retry 3 ครั้ง x timeout 20s + sleep 3s = บล็อค worker
    thread ของ waitress ได้สูงสุด ~70s ตอน siamchart ช้า/ล่ม
    (S50/Bond ไม่ต้องมี — ดึงครั้งเดียวจบ ไม่มี retry loop แบบนี้)"""
    if not _flow_fetch_lock.acquire(blocking=False):
        return
    _flow_fetch_state["running"] = True

    def _bg():
        try:
            # thread อื่น (หรือ Quick Update/Full Refresh) เพิ่ง refresh เสร็จระหว่างรอ lock
            if _flow_cache.get("data") and time.time() - _flow_cache.get("ts", 0) < _FLOW_CACHE_TTL:
                return
            _fetch_flow_data()
            print("[Flow] background refresh เสร็จ")
        except Exception as e:
            print(f"[Flow] background refresh ล้มเหลว: {e}")
        finally:
            _flow_fetch_state["running"] = False
            _flow_fetch_lock.release()

    threading.Thread(target=_bg, daemon=True).start()


@app.route("/api/market-flow")
def market_flow():
    """ดึงข้อมูล net buy/sell รายวัน จาก siamchart.com

    cache หมดอายุ: ตอบ cache เก่าทันที + refresh เบื้องหลัง (stale-while-revalidate) — ผู้ใช้
    ไม่ต้องรอ siamchart retry ~70s บน worker thread (frontend มี timeout 90s เป็นตาข่ายกันไว้
    ชั้นสุดท้ายอยู่แล้ว) · ?fresh=1 = บังคับดึงสดแบบ blocking (ใช้ตอน bake static — ห้ามได้ของเก่า)"""
    fresh = request.args.get("fresh") == "1"
    cached = _flow_cache.get("data")
    if not fresh and cached:
        if time.time() - _flow_cache.get("ts", 0) >= _FLOW_CACHE_TTL:
            _kick_flow_refresh()   # หมดอายุ — refresh เบื้องหลัง แล้วคืนของเก่าไปก่อน
        return jsonify(cached)
    try:
        return jsonify(_fetch_flow_data())
    except Exception as e:
        import traceback
        traceback.print_exc()
        # ทุกแหล่งล้มพร้อมกัน (ไฟล์สะสม+GitHub fallback+siamchart+SET API) แต่ยังมี cache
        # เดิมที่ valid ตอนนี้ (แค่ TTL หมดอายุ) ค้างอยู่ในหน่วยความจำ — คืนของเก่านั้นแทน 500
        # ดีกว่าคืน error ทั้งที่มีข้อมูลใช้ได้จริงอยู่แล้ว
        if _flow_cache.get("data"):
            return jsonify(_flow_cache["data"])
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
    rows_by_date = _load_flow_accumulator(
        _S50_FLOW_FILE, _fetch_flow_s50_github_fallback, "S50 Flow")

    sources = []
    try:
        row = _fetch_flow_tfex_today()
        rows_by_date[row["date"]] = row
        sources.append("tfex.co.th")
    except Exception as e:
        print(f"[S50 Flow] TFEX ไม่ได้ ({e})")

    if not rows_by_date:
        raise RuntimeError("ไม่มีข้อมูล S50 Futures flow (TFEX ไม่ได้ + ไม่มีไฟล์สะสม)")

    return _finalize_flow_result(rows_by_date, sources, _S50_FLOW_FILE, _flow_s50_cache)


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
        if _flow_s50_cache.get("data"):
            return jsonify(_flow_s50_cache["data"])
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
    เขียนไฟล์สะสมเฉพาะตอนรันบน Actions เท่านั้น — ดู _fetch_flow_data)

    ThaiBMA เสิร์ฟแถว "วันนี้" ตั้งแต่ก่อนตลาดปิด (เจอจริง 2026-09-01: โผล่มาตั้งแต่ ~09:00
    ICT) ด้วยตัวเลข NetFlow ชั่วคราวที่ยังแกว่งทั้งวัน (-4331 ตอนเช้า -> -4569 ตอนบ่าย) —
    gate แถววันปัจจุบันไว้จนพ้น 17:45 เวลาไทยเหมือน _fetch_flow_set_official ที่ gate SET"""
    import urllib.request as _ur
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZoneInfo
    ctx = ssl_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.thaibma.or.th/EN/Market/NR/NRDaily.aspx",
        "X-Requested-With": "XMLHttpRequest",
    }
    now_th = _dt.now(_ZoneInfo("Asia/Bangkok"))
    today_th = now_th.strftime("%Y-%m-%d")
    bond_final_today = (now_th.hour, now_th.minute) >= (17, 45)

    rows_by_date = _load_flow_accumulator(
        _BOND_FLOW_FILE, _fetch_flow_bond_github_fallback, "Bond Flow")
    if not bond_final_today:
        # เคลียร์แถววันนี้ที่รอบก่อนหน้า (ก่อนมี gate นี้) เผลอ commit ไว้ในไฟล์สะสม/GitHub
        # fallback — ตัวเลขยังไม่นิ่ง ยังไม่ควรโชว์เลยจนกว่าจะสิ้นวัน
        rows_by_date.pop(today_th, None)

    sources = []
    try:
        req = _ur.Request("https://www.thaibma.or.th/nrdaily/GetNR/", headers=headers)
        with _ur.urlopen(req, context=ctx, timeout=20) as r:
            raw = json.loads(r.read().decode("utf-8", "ignore"))
        bad = 0
        for item in raw:
            try:
                date = str(item.get("Asof") or "")[:10]
                net = item.get("NetFlow")
                if not date or net is None:
                    continue
                if date == today_th and not bond_final_today:
                    continue   # ตัวเลข NetFlow ของวันนี้ยังไหลอยู่ — ข้ามจนพ้น 17:45
                rows_by_date[date] = {"date": date, "foreign": round(float(net), 2)}
            except (ValueError, TypeError):
                # กัน 1 แถวข้อมูลผิดรูปทำให้ทั้ง loop แตกก่อนถึง sources.append ด้านล่าง —
                # เดิมไม่มี try/except รายแถว exception เดียวหลุดออกจาก for ทำให้ทั้งรอบ
                # ThaiBMA ถูกนับเป็น "ล่มทั้งแหล่ง" ทั้งที่ merge แถวอื่นเข้า rows_by_date
                # ไปแล้วบางส่วน (เหมือน siamchart loop ที่กันเคสนี้อยู่แล้ว)
                bad += 1
                continue
        if bad:
            print(f"[Bond Flow] ThaiBMA ข้าม {bad} แถวข้อมูลผิดรูป")
        sources.append("thaibma.or.th")
    except Exception as e:
        print(f"[Bond Flow] ThaiBMA ไม่ได้ ({e}) — ใช้ไฟล์สะสม")

    if not rows_by_date:
        raise RuntimeError("ไม่มีข้อมูล Thai Bond flow (ThaiBMA ไม่ได้ + ไม่มีไฟล์สะสม)")

    return _finalize_flow_result(rows_by_date, sources, _BOND_FLOW_FILE, _flow_bond_cache)


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
        if _flow_bond_cache.get("data"):
            return jsonify(_flow_bond_cache["data"])
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
        daily = v.get("daily") or []
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
    try:
        with open(_NVDR_DATA_FILE, encoding="utf-8") as f:
            _nvdr_data_cache = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[NVDR] failed to load {_NVDR_DATA_FILE}: {e}")
        return _nvdr_data_cache
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
                # ถ้าหุ้นตัวนี้ยังไม่เคยอยู่ในไฟล์เดิม (เพิ่งสร้างจาก fallback ล้วนๆ) top-level
                # nvdr_pct/nvdr_shares จะค้างที่ 0 จนกว่าจะโผล่ในชุด live ของวันนี้ (items ด้านล่าง)
                # ทั้งที่ .daily มีค่าจริงอยู่แล้ว — sync top-level ให้ตรงกับ snapshot ล่าสุดของ
                # daily ทันที กันบั๊ก nvdr_pct=0 ค้างถาวรตอนหุ้นนั้นหายจาก live batch วันนี้พอดี
                if s["daily"]:
                    last = s["daily"][-1]
                    s["nvdr_pct"]    = last.get("nvdr_pct", 0)
                    s["nvdr_shares"] = last.get("nvdr_shares", 0)
        except Exception as e:
            print(f"[nvdr] GitHub fallback ไม่ได้ ({e})")

        updated = 0
        skipped = 0
        for item in items:
            try:
                sym  = item.get("symbol")
                if not sym:
                    continue
                pct_raw = item.get("percentOfPaidUpCapital")
                if pct_raw is None:
                    # field หายจาก SET API วันนั้น (เจอจริง) — ห้ามเดาเป็น 0 (`or 0` เดิม
                    # เขียนค่า 0% ปลอมถาวรลง daily history) ปล่อยให้ except ด้านล่างข้าม
                    # หุ้นตัวนี้ไปแทน ค่าเดิมของ stocks[sym] จะไม่ถูกแตะ
                    raise ValueError(f"missing percentOfPaidUpCapital for {sym!r}")
                pct  = pct_raw
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
            except Exception as e:
                # กันหุ้นตัวเดียว field ผิดรูป (เช่น SET API ส่ง type แปลกมา) ทำให้ทั้งวันไม่ถูก
                # บันทึกเลยสักตัว — เดิมไม่มี try/except รายตัว exception หลุดออกจาก loop ก่อนถึง
                # _atomic_write_json ด้านล่าง ทำให้ข้อมูลทุกหุ้นของวันนั้นหายไปเงียบๆ ทั้งหมด
                skipped += 1
                print(f"[nvdr] skip {item.get('symbol')!r}: {type(e).__name__}: {e}")
        if skipped:
            print(f"[nvdr] skipped {skipped} malformed item(s) this run")

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

    # DR underlying stocks from cache (ราคา foreign currency) — cache อาจค้างได้หลายวันถ้าไม่มี
    # ใครเปิดหน้า DR เลย (มีแค่ /api/dr ที่ rebuild ให้) เช็ค TTL เองแล้ว kick rebuild เบื้องหลัง
    # กัน checkAlerts() เช็คราคาแช่แข็งตั้งแต่ server restart ครั้งล่าสุดแบบไม่มีใครรู้
    dr_result = _dr_cache.get("result")
    if dr_result:
        if _dr_cache.get("ts") is not None and time.time() - _dr_cache["ts"] >= _DR_CACHE_TTL:
            _kick_dr_rebuild()
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

    # หุ้น mirror นอกดัชนีหลัก (US/HK) ที่มีการตั้ง price alert ไว้จาก Watchlist — เติมราคาจาก
    # cache on-demand (mirror_ondemand, ดู sources/mirror_ondemand.py) ไม่งั้น checkAlerts()
    # จะไม่เจอราคาเลย (ไม่อยู่ใน 3 แหล่งบนที่ครอบแค่สมาชิกดัชนีหลัก) ทำให้ alert เงียบตลอดไป
    if os.path.exists(PRICE_ALERTS_FILE):
        try:
            with open(PRICE_ALERTS_FILE, encoding="utf-8") as f:
                alerts = json.load(f)
            for a in alerts:
                sym = a.get("symbol") if isinstance(a, dict) else None
                if not isinstance(sym, str) or sym in prices:
                    continue
                market = "US" if sym.startswith("US:") else "HK" if sym.startswith("HK:") else None
                if not market:
                    continue
                payload, _stale = financials_store.get_mirror_ondemand(BASE_DIR, sym[3:], market)
                if payload and payload.get("price") is not None:
                    prices[sym] = payload["price"]
        except Exception:
            pass

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
    try:
        rows = flow_signals.build_flow_signals(BASE_DIR)
        result = {"stocks": rows, "count": len(rows),
                  "generated_at": _dt.now(_tz(_td(hours=7))).strftime("%Y-%m-%d %H:%M")}
        _flow_signals_cache["result"] = result
        _flow_signals_cache["ts"] = now
        return jsonify(result)
    except Exception as e:
        print(f"[FlowSignals] {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


def _nvdr_stock_view(v):
    """แปลง record ดิบ {nvdr_pct, nvdr_shares, paid_up_shares, daily} เป็น shape ที่ frontend
    ใช้จริง {nvdr_pct, nvdr_shares, daily_count, last_snap, prev_snap, daily_tail} — ใช้ร่วมกัน
    ทั้ง nvdr_summary และ nvdr_symbol กันสอง route คืน shape ต่างกัน (เดิม nvdr_symbol คืน record
    ดิบตรงๆ ไม่มี daily_count/last_snap/prev_snap/daily_tail ที่ทุกจุดอื่นในแอปคาดหวัง)"""
    daily = v.get("daily", [])
    return {
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


@app.route("/api/nvdr")
def nvdr_summary():
    """คืน NVDR% ทุกหุ้น (compact) + prev/last snap"""
    data = _load_nvdr_data()
    if not data:
        # ดึงสดถ้ายังไม่มีไฟล์
        try:
            trade_date, items = _fetch_nvdr_outstanding()
            out = {}
            skipped = 0
            for item in items:
                try:
                    sym = item.get("symbol")
                    if not sym: continue
                    pct_raw = item.get("percentOfPaidUpCapital")
                    if pct_raw is None:
                        raise ValueError(f"missing percentOfPaidUpCapital for {sym!r}")
                    out[sym] = {"nvdr_pct": round(pct_raw, 4),
                                "nvdr_shares": int(item.get("nvdrInvestment") or 0),
                                "daily_count": 0, "last_snap": None, "prev_snap": None,
                                "daily_tail": []}
                except Exception as e:
                    # กันหุ้นตัวเดียว field ผิดรูปทำให้ทั้ง response ว่างเปล่า (เหมือน
                    # nvdr_daily_update) — เดิมไม่มี try/except รายตัวตรงนี้
                    skipped += 1
                    print(f"[nvdr] bootstrap skip {item.get('symbol')!r}: {type(e).__name__}: {e}")
            if skipped:
                print(f"[nvdr] bootstrap skipped {skipped} malformed item(s)")
            return jsonify({"updated_at": trade_date, "stocks": out})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    stocks = data.get("stocks", {})
    out = {sym: _nvdr_stock_view(v) for sym, v in stocks.items()}
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
    return jsonify({**_nvdr_stock_view(v), "symbol": sym, "updated_at": data.get("updated_at")})


# ============================================================
# Watchlist / price-alert sync ข้ามเครื่อง — เก็บเป็นไฟล์ data/watchlist.json,
# data/price_alerts.json ให้ git push/pull พาไปด้วยได้ (แยกจาก localStorage ที่ผูกกับ
# เบราว์เซอร์/เครื่องเดียว) frontend เรียก GET ตอนโหลดหน้าเพื่อ merge เข้า localStorage
# แล้วยิง POST กลับทุกครั้งที่เปลี่ยน (watchlist: union เท่านั้น ดู _wlSave/_wlSyncFromServer,
# alert: union + triggered ratchet ด้วย "id" ดู _alertsSyncFromServer ใน dashboard.js) —
# ไม่มีการลบข้ามเครื่องอัตโนมัติทั้งคู่ backend แค่เก็บ/คืนทั้งก้อนตามที่ frontend ส่งมา
# ============================================================
def _read_synced_json_or_500(path, log_prefix):
    """คืน (payload, None) ปกติ หรือ (None, error_response) ตอนอ่านพัง — ใช้ร่วมกันโดย
    get_watchlist/get_price_alerts ที่มี contract เดียวกัน: ไฟล์ไม่มี = [] ปกติ (ยังไม่เคย sync),
    อ่านพัง (เช่น JSON เสียจาก git merge conflict) = 500 ห้ามคืน [] เฉยๆ — frontend
    (_wlSyncFromServer/_alertsSyncFromServer) แยกไม่ออกระหว่าง "ว่างจริง" กับ "อ่านพัง" แล้วจะ
    sync ทับไฟล์ด้วยลิสต์ของเครื่องเดียวนี้ ทำให้ตัวที่เครื่องอื่น sync ไว้หายถาวร"""
    if not os.path.exists(path):
        return [], None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        print(f"[{log_prefix}] read error: {e}")
        return None, (jsonify({"error": "read failed"}), 500)


def _save_synced_list(body, body_key, path, item_ok, max_len=500):
    """validate + เขียนไฟล์ list ที่ sync ข้ามเครื่อง แบบ atomic — ใช้ร่วมกันโดย save_watchlist/
    save_price_alerts (ต่างกันแค่ key ใน body/ไฟล์ปลายทาง/ตัวเช็ค item เดียว)"""
    items = body.get(body_key)
    if not isinstance(items, list) or len(items) > max_len or not all(item_ok(x) for x in items):
        return jsonify({"error": f"invalid {body_key}"}), 400
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _atomic_write_json(path, items)
    return jsonify({"ok": True, "count": len(items)})


@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    data, err = _read_synced_json_or_500(WATCHLIST_FILE, "watchlist")
    return err if err else jsonify(data)


@app.route("/api/watchlist", methods=["POST"])
def save_watchlist():
    body = request.get_json(silent=True) or {}
    return _save_synced_list(body, "symbols", WATCHLIST_FILE, lambda s: isinstance(s, str))


@app.route("/api/price-alerts", methods=["GET"])
def get_price_alerts():
    data, err = _read_synced_json_or_500(PRICE_ALERTS_FILE, "price-alerts")
    return err if err else jsonify(data)


def _valid_price_alert(a):
    """เช็คโครงสร้างขั้นต่ำของ 1 alert — กัน record เพี้ยน (targetPrice หาย/ไม่ใช่ตัวเลข)
    หลุดเข้าไปสะสมใน price_alerts.json แล้ว sync กระจายไปทุกเครื่อง ทำให้ a.targetPrice.toFixed()
    ฝั่ง frontend throw ตอน render จนตาราง Watchlist/แผงแจ้งเตือนค้างทั้งหน้า (ดู _wlAlertCell)
    เช็ค triggeredPrice/triggeredAt ด้วยเหตุผลเดียวกัน — เคยหลุดผ่านได้เพราะเช็คแค่ targetPrice
    ทำให้ a.triggeredPrice.toFixed() ใน _renderWlExistingAlerts/renderAlertPanel throw แทน
    เช็ค note ด้วย (เดิมไม่เช็คเลยแม้แต่ type) — ไม่งั้นค่าที่ไม่ใช่ string/ยาวเกินไปหลุดผ่านไปได้
    แล้วโผล่ใน innerHTML ของ renderAlertPanel/_renderWlExistingAlerts แบบไม่ผ่าน escape ใดๆ
    (ดู _escHtml call ที่เพิ่มเข้าไปคู่กัน — รีวิวโค้ด 2026-09-02)"""
    if not (isinstance(a, dict) and isinstance(a.get("id"), str)):
        return False
    tp = a.get("targetPrice")
    if not (isinstance(tp, (int, float)) and not isinstance(tp, bool) and math.isfinite(tp)):
        return False
    if a.get("condition") not in ("above", "below"):
        return False
    if not isinstance(a.get("symbol"), str):
        return False
    trig_p = a.get("triggeredPrice")
    if trig_p is not None and not (isinstance(trig_p, (int, float)) and not isinstance(trig_p, bool) and math.isfinite(trig_p)):
        return False
    if a.get("triggeredAt") is not None and not isinstance(a.get("triggeredAt"), str):
        return False
    note = a.get("note")
    if note is not None and not (isinstance(note, str) and len(note) <= 300):
        return False
    return True


@app.route("/api/price-alerts", methods=["POST"])
def save_price_alerts():
    body = request.get_json(silent=True) or {}
    return _save_synced_list(body, "alerts", PRICE_ALERTS_FILE, _valid_price_alert)


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
                   "/api/quarterly-growth-screener",
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
