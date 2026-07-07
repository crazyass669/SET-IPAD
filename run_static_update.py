# -*- coding: utf-8 -*-
"""
run_static_update.py — รันโดย GitHub Actions เพื่อสร้างไฟล์ static ทั้งหมด
สำหรับเวอร์ชันเว็บ (GitHub Pages) ที่ไม่มี Flask server

ขั้นตอน:
  1. อัปเดตราคาหุ้น: quick update ถ้ามี set_prices.db (จาก Actions cache),
     ไม่มีก็ full refresh 10 ปี
  2. เปิด Flask app ผ่าน test_client แล้ว snapshot ทุก endpoint ที่หน้าเว็บใช้
     ลง data/*.json — shape ตรงกับ API จริงเสมอเพราะรันโค้ดตัวเดียวกัน
"""
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

_ICT = timezone(timedelta(hours=7))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)


def log(msg):
    print(f"[{datetime.now(_ICT).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── 1. อัปเดตราคา ────────────────────────────────────────────
from core import store as price_store

def _progress_cb(current, total, msg):
    if total > 0:
        pct = round(current / total * 100)
        if pct % 10 == 0:
            log(f"  [{pct:3d}%] {msg}")
    else:
        log(f"  {msg}")


from services import refresh as refresh_svc

if price_store.db_exists(BASE_DIR):
    log("=== พบ set_prices.db (cache) — Quick Update ===")
    try:
        refresh_svc.run_quick_update(_progress_cb, BASE_DIR)
        log("✅ Quick Update เสร็จ")
    except Exception as e:
        log(f"⚠️ Quick Update ล้ม ({e}) — fallback Full Refresh")
        refresh_svc.run_with_progress(_progress_cb, base_dir=BASE_DIR, period="10y")
else:
    log("=== ไม่มี set_prices.db — Full Refresh 10 ปี (~30-60 นาที) ===")
    refresh_svc.run_with_progress(_progress_cb, base_dir=BASE_DIR, period="10y")
    log("✅ Full Refresh เสร็จ")


# ── 2. สร้าง market stats จาก Table_PE.xls/Table_PBV.xls (ถ้ามีในรีโป) ──
if os.path.exists(os.path.join(BASE_DIR, "Table_PE.xls")):
    try:
        import subprocess
        r = subprocess.run([sys.executable, os.path.join(BASE_DIR, "import_market_stats.py")],
                           capture_output=True, text=True, timeout=120)
        log("✅ market stats จาก Table_PE/PBV เสร็จ" if r.returncode == 0
            else f"⚠️ import_market_stats ล้ม: {r.stderr[-300:]}")
    except Exception as e:
        log(f"⚠️ import_market_stats ข้าม: {e}")


# ── 3. Snapshot ทุก API endpoint ลง data/*.json ─────────────
log("=== Snapshot API endpoints -> data/ ===")
from app import app  # noqa: E402  (import หลังราคาอัปเดตเสร็จ)

client = app.test_client()

# (endpoint, ไฟล์ปลายทาง, จำเป็นไหม)  จำเป็น=True ถ้าล้มให้ exit 1
SNAPSHOTS = [
    ("/api/data",                          "set_data.json",              True),
    ("/api/breadth?range=1y",              "breadth_1y.json",            True),
    ("/api/breadth?range=3y",              "breadth_3y.json",            False),
    ("/api/breadth?range=5y",              "breadth_5y.json",            False),
    ("/api/breadth?range=all",             "breadth_all.json",           False),
    ("/api/indices",                       "indices_data.json",          False),
    ("/api/dr",                            "dr_data.json",               False),
    ("/api/nvdr",                          "nvdr_data.json",             False),
    ("/api/short-sales",                   "short_sales.json",           False),
    ("/api/market-stats",                  "market_stats.json",          False),
    ("/api/market-stats-meta",             "market_stats_meta.json",     False),
    ("/api/market-flow",                   "market_flow.json",           False),
    ("/api/market-internals",              "market_internals.json",      False),
    ("/api/rotation-alerts",               "rotation_alerts.json",       False),
    ("/api/stock-valuation-stats",         "stock_valuation_stats.json", False),
    # insider-trades/major-changes อ่านจาก sec_filings.db ที่ sync ไว้ด้านล่างแล้ว
    # (เร็วมาก ไม่ยิง SEC สดตอน bake อีกต่อไป) bake ครบทุกช่วงที่ปุ่ม UI ใช้
    ("/api/insider-trades?days=7",         "insider_trades_7.json",      False),
    ("/api/insider-trades?days=30",        "insider_trades_30.json",     False),
    ("/api/insider-trades?days=90",        "insider_trades_90.json",     False),
    ("/api/insider-trades?days=180",       "insider_trades_180.json",    False),
    ("/api/major-changes?days=7",          "major_changes_7.json",       False),
    ("/api/major-changes?days=30",         "major_changes_30.json",      False),
    ("/api/major-changes?days=90",         "major_changes_90.json",      False),
    ("/api/major-changes?days=180",        "major_changes_180.json",     False),
    ("/api/prices",                        "prices.json",                False),
]

# sync ฐานข้อมูลสะสม SEC filings ก่อน bake (เร็ว ~นาทีเดียวถ้า sync แล้วครั้งก่อน
# มีแค่หน้าต่างทับซ้อน 21 วัน / ช้า ~2-3 นาทีถ้าเป็นการ backfill ครั้งแรก 180 วัน)
log("=== Sync sec_filings.db (insider/major-changes) ===")
try:
    from sources import sec_store
    n1 = sec_store.sync_insider_trades(BASE_DIR)
    n2 = sec_store.sync_major_changes(BASE_DIR)
    log(f"✅ sync เสร็จ: insider {n1} แถว, major-changes {n2} แถว")
except Exception as e:
    log(f"⚠️ sync sec_filings.db ล้ม: {e}")

failures = []
for url, fname, required in SNAPSHOTS:
    t0 = time.time()
    try:
        resp = client.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        payload = resp.get_data(as_text=True)
        json.loads(payload)  # ตรวจว่าเป็น JSON จริง
        with open(os.path.join(DATA_DIR, fname), "w", encoding="utf-8") as f:
            f.write(payload)
        log(f"✅ {fname} ({len(payload)//1024} KB, {time.time()-t0:.1f}s)")
    except Exception as e:
        log(f"{'❌' if required else '⚠️'} {fname}: {e}")
        if required:
            failures.append(fname)

if failures:
    log(f"❌ ไฟล์จำเป็นล้มเหลว: {failures}")
    sys.exit(1)

log("=== เสร็จทั้งหมด ===")
