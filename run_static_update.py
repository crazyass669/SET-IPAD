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

import pandas as pd

_ICT = timezone(timedelta(hours=7))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)


def log(msg):
    print(f"[{datetime.now(_ICT).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── บันทึกผลรอบนี้ลง logs/update_status.json (กลไกเดียวกับ Quick Update/mirror) ──
# ใช้ atexit แทน try/except ครอบทั้งไฟล์ เพราะสคริปต์นี้เป็นโค้ด top-level เรียงยาว
# ไม่ได้อยู่ใน main() — atexit จับได้ทั้ง exception ที่หลุดออกมาและ sys.exit(1)
# หมายเหตุ: บน GitHub Actions ไฟล์ logs/ เป็น local-only (.gitignore) ไม่ตามกลับมา —
# ตัวนี้จึงมีผลกับ "รอบที่รันบนเครื่อง" เท่านั้น (ฝั่ง CI ใช้อีเมลแจ้ง workflow ล้มแทน)
import atexit  # noqa: E402
from core import run_log  # noqa: E402

_bake_result = {"ok": False, "msg": "จบก่อนถึงบรรทัดสุดท้าย (exception / ถูกยกเลิกกลางคัน)"}
atexit.register(lambda: run_log.record_run(
    BASE_DIR, "static_bake", _bake_result["ok"], _bake_result["msg"]))


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


def _lagging_tickers(base_dir):
    """หุ้นที่ last date ยังตามหลังวันล่าสุดของกระดาน (สภาพคล่องต่ำ Yahoo sync ช้า)
    ตัดหุ้นค้างนาน >14 วันออก (พักเทรด/เพิกถอน — ไม่ใช่ sync lag ให้ retry)"""
    last_map = price_store.get_last_dates(base_dir)
    if not last_map:
        return []
    max_last = pd.to_datetime(max(last_map.values()))
    stale_cut = max_last - pd.Timedelta(days=14)
    return sorted(t for t, d in last_map.items()
                  if stale_cut <= pd.to_datetime(d) < max_last)


was_quick_update = price_store.db_exists(BASE_DIR)
if was_quick_update:
    log("=== พบ set_prices.db (cache) — Quick Update ===")
    try:
        refresh_svc.run_quick_update(_progress_cb, BASE_DIR)
        log("✅ Quick Update เสร็จ")
    except Exception as e:
        log(f"⚠️ Quick Update ล้ม ({e}) — fallback Full Refresh")
        refresh_svc.run_with_progress(_progress_cb, base_dir=BASE_DIR, period="10y")
        was_quick_update = False
else:
    log("=== ไม่มี set_prices.db — Full Refresh 10 ปี (~30-60 นาที) ===")
    refresh_svc.run_with_progress(_progress_cb, base_dir=BASE_DIR, period="10y")
    log("✅ Full Refresh เสร็จ")

# ── 1b. Retry เก็บตกในรอบเดียวกัน — หุ้นสภาพคล่องต่ำที่ Yahoo ยังไม่ปล่อยราคา
# ตอน quick update รอบแรก เดิมต้องรอ CI รอบถัดไป (อาจข้ามวัน) ทำให้เวอร์ชันเว็บ
# (มือถือ/ไอแพด) ค้างข้อมูลไม่ครบนานผิดปกติ — ลองซ้ำในรอบเดียวกันได้เพราะ job มี
# timeout budget 120 นาที ใช้จริงแค่ ~4-5 นาที เหลือเผื่อเยอะพอรอ Yahoo ปล่อยข้อมูล
if was_quick_update:
    MAX_RETRY = 2
    RETRY_WAIT_SEC = 600
    for attempt in range(1, MAX_RETRY + 1):
        lagging = _lagging_tickers(BASE_DIR)
        if not lagging:
            break
        preview = lagging[:10]
        log(f"=== พบหุ้นตามหลัง {len(lagging)} ตัว — รอ {RETRY_WAIT_SEC // 60} นาทีแล้ว "
            f"retry ({attempt}/{MAX_RETRY}): {preview}{' ...' if len(lagging) > 10 else ''} ===")
        time.sleep(RETRY_WAIT_SEC)
        try:
            refresh_svc.run_quick_update(_progress_cb, BASE_DIR)
            log(f"✅ Retry quick update ({attempt}/{MAX_RETRY}) เสร็จ")
        except Exception as e:
            log(f"⚠️ Retry quick update ล้ม ({e}) — หยุด retry")
            break
    else:
        lagging = _lagging_tickers(BASE_DIR)
        if lagging:
            log(f"⚠️ ยังมีหุ้นตามหลัง {len(lagging)} ตัวหลัง retry ครบ {MAX_RETRY} รอบ — "
                f"ปล่อยไว้ รอ cron รอบถัดไป: {lagging[:10]}{' ...' if len(lagging) > 10 else ''}")

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
from app import app, _fetch_indices_tv, _load_indices_existing, short_sales_daily_update, nvdr_daily_update  # noqa: E402

client = app.test_client()

# /api/indices และ /api/short-sales ไม่ fetch สดเอง แค่อ่านจาก cache file
# ในเครื่อง (indices_cache.json, short_sales_data.json) — เดิมไฟล์พวกนี้
# ถูก .gitignore ไว้ ทำให้ CI (fresh checkout ทุกรอบ) ไม่มีไฟล์เลย 404 เงียบๆ
# ทุกรอบ auto-update มาตลอด (required=False เลยไม่มีใครสังเกตเห็น) ตอนนี้
# ไฟล์ถูก commit เข้า repo แล้ว เลย refresh ตรงๆ ก่อน snapshot ได้เลย
log("=== รีเฟรช Indices + Short Sales ===")
try:
    existing = _load_indices_existing()
    _fetch_indices_tv(existing, full_refresh=False)
    log("✅ Indices รีเฟรชเสร็จ")
except Exception as e:
    log(f"⚠️ Indices รีเฟรชล้ม: {e}")
try:
    short_sales_daily_update()
    log("✅ Short Sales รีเฟรชเสร็จ")
except Exception as e:
    log(f"⚠️ Short Sales รีเฟรชล้ม: {e}")
# NVDR ก็ต้องสะสม daily snapshot บน CI เหมือน short sales — เดิมไม่ได้เรียก
# (และไฟล์โดน gitignore) ทำให้ NVDR Δ + สัญญาณ NVDR ใน Flow Confluence
# เป็น 0 หมดบนเวอร์ชันเว็บมาตลอด
try:
    nvdr_daily_update()
    log("✅ NVDR รีเฟรชเสร็จ")
except Exception as e:
    log(f"⚠️ NVDR รีเฟรชล้ม: {e}")

# (endpoint, ไฟล์ปลายทาง, จำเป็นไหม)  จำเป็น=True ถ้าล้มให้ exit 1
SNAPSHOTS = [
    ("/api/data",                          "set_data.json",              True),
    ("/api/breadth?range=1y",              "breadth_1y.json",            True),
    ("/api/breadth?range=3y",              "breadth_3y.json",            False),
    ("/api/breadth?range=5y",              "breadth_5y.json",            False),
    ("/api/breadth?range=all",             "breadth_all.json",           False),
    ("/api/indices",                       "indices_data.json",          False),
    # fresh=1: บังคับดึงสดแบบ blocking — endpoint ปกติเป็น stale-while-revalidate
    # (ตอบ cache เก่าทันที + refresh เบื้องหลัง) ซึ่งใช้กับการ bake static ไม่ได้
    ("/api/dr?fresh=1",                    "dr_data.json",               False),
    ("/api/nvdr",                          "nvdr_data.json",             False),
    ("/api/short-sales",                   "short_sales.json",           False),
    ("/api/market-stats",                  "market_stats.json",          False),
    ("/api/market-stats-meta",             "market_stats_meta.json",     False),
    ("/api/market-flow",                   "market_flow.json",           False),
    ("/api/market-flow-s50",               "market_flow_s50.json",       False),
    ("/api/market-flow-bond",              "market_flow_bond.json",      False),
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
    # Yahoo-only growth/PEG/ratio ต่อหุ้น (ไทย+DR) — เวอร์ชันย่อของ /api/financials-analytics
    # ที่ไม่แตะ Finnomena เลย (financials.db เต็มเป็น local-only ตามกฎ ห้ามขึ้น GitHub)
    # ใช้ปลุกช่องกรองบางส่วนใน Stock Screener ที่เดิมถูกปิดทั้งหมดบนเว็บมือถือ/ไอแพด
    ("/api/financials-analytics?source=yahoo", "financials_analytics_yahoo.json", False),
    # คำอธิบายบริษัท (EN + แปลไทย) หุ้น DR — sync ด้วยปุ่มในเครื่องเป็นระยะ (ดูคู่มือ)
    # ไฟล์นี้แค่อ่าน cache local (dr_descriptions.json) มา bake ไม่ได้ fetch สดตอน CI รัน
    ("/api/dr-descriptions",                   "dr_descriptions.json",            False),
    # P/E-P/BV รายวันตลาด SET/mai (scrape จากหน้า overview ของ SET.or.th ตรงๆ —
    # ไม่ใช่ Finnomena) รันสดได้ตอน bake เลย ไม่ต้อง cache local ก่อนแบบ dr-descriptions
    ("/api/set-daily-valuation",               "set_daily_valuation.json",        False),
    # สัญญาณเงินทุนรวม insider+short+NVDR (สาธารณะ) — bake ให้หน้า "สัญญาณรวม" ใช้บนมือถือได้
    ("/api/flow-signals",                      "flow_signals.json",               False),
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

def _slim_set_data(payload_text):
    """ลดน้ำหนัก set_data.json เฉพาะเวอร์ชันเว็บ: ตัด price_history เหลือ 260 แท่ง (~1 ปี)
    เพียงพอกับทุกอย่างที่ frontend ใช้ (ret_1y ต้องการ 260, SMA200 crossover ต้องการ ~202,
    sparkline/กราฟ popup ใช้เท่าที่มี) — จาก ~12.6MB เหลือ ~7MB ให้มือถือโหลด/parse ไวขึ้น
    เวอร์ชัน local (Flask) ยังใช้ไฟล์เต็ม 500 แท่งเหมือนเดิม ไม่กระทบ"""
    d = json.loads(payload_text)
    for s in d.get("stocks", []):
        ph = s.get("price_history")
        if ph and len(ph) > 260:
            s["price_history"] = ph[-260:]
        vh = s.get("vol_history")
        if vh and len(vh) > 260:
            s["vol_history"] = vh[-260:]
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))


def _is_empty_payload(obj):
    """payload ที่ 'สำเร็จแต่ว่างเปล่า' — เกิดกับ endpoint ที่ต้องพึ่ง DB ที่ไม่มีบน CI
    (เช่น financials-analytics ต้องใช้ financials.db ซึ่งเป็น local-only) ถ้าเขียนทับ
    ไฟล์ที่ commit ไว้ด้วยของว่าง จะทำให้ช่องกรองบนเว็บมือถือหาหุ้นไม่เจอเลย"""
    if isinstance(obj, dict):
        if not obj:
            return True
        # ทุก value เป็น dict/list ว่าง = ไม่มีข้อมูลจริง (เช่น {"set":{},"dr":{}})
        vals = [v for k, v in obj.items() if k not in ("updated_at", "generated_at", "as_of", "count")]
        if vals and all(isinstance(v, (dict, list)) and not v for v in vals):
            return True
    return False


failures = []
for url, fname, required in SNAPSHOTS:
    t0 = time.time()
    try:
        resp = client.get(url)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        payload = resp.get_data(as_text=True)
        parsed = json.loads(payload)  # ตรวจว่าเป็น JSON จริง
        # กันเขียนทับไฟล์ที่มีข้อมูลอยู่แล้ว ด้วย payload ว่าง (source DB ไม่มีบน CI)
        dest = os.path.join(DATA_DIR, fname)
        if _is_empty_payload(parsed) and os.path.exists(dest) and os.path.getsize(dest) > 200:
            log(f"⏭️ {fname}: payload ว่าง (ไม่มี source DB บน CI) — คงไฟล์เดิมที่ commit ไว้")
            continue
        if fname == "set_data.json":
            payload = _slim_set_data(payload)
        with open(dest, "w", encoding="utf-8") as f:
            f.write(payload)
        log(f"✅ {fname} ({len(payload)//1024} KB, {time.time()-t0:.1f}s)")
    except Exception as e:
        log(f"{'❌' if required else '⚠️'} {fname}: {e}")
        if required:
            failures.append(fname)

if failures:
    log(f"❌ ไฟล์จำเป็นล้มเหลว: {failures}")
    _bake_result["msg"] = f"ไฟล์จำเป็นล้มเหลว: {', '.join(failures)}"
    sys.exit(1)

_bake_result.update(ok=True, msg=f"bake สำเร็จ {len(SNAPSHOTS)} endpoint")
log("=== เสร็จทั้งหมด ===")
