"""
import_eod_archive.py
อ่าน CSV archive ปี 1975-2025 → สร้าง set_history.json
จากนั้น yfinance ดึง gap ปี 2026 ถึงวันนี้
"""
import os, glob, json
import pandas as pd
from datetime import datetime
from collections import defaultdict

ARCHIVE_DIR = r"C:\Users\joeki\Downloads\set-archive_EOD_1970-LAST (1)\set-archive_EOD_1970-LAST"
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_DIR, "set_history.json")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 1. อ่าน CSV ทั้งหมด ──────────────────────────────────────
log("อ่านไฟล์ CSV archive...")
files = sorted(glob.glob(os.path.join(ARCHIVE_DIR, "*.csv")))
log(f"พบ {len(files)} ไฟล์ ({os.path.basename(files[0])} → {os.path.basename(files[-1])})")

stocks = defaultdict(lambda: {"dates": [], "closes": [], "volumes": []})

for i, f in enumerate(files):
    if i % 500 == 0:
        log(f"  อ่าน {i}/{len(files)}...")
    try:
        df = pd.read_csv(f, names=["ticker","date","open","high","low","close","vol"],
                         skiprows=1, dtype=str)
        df["ticker"] = df["ticker"].str.strip().str.replace("<","").str.replace(">","")
        df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
        df["vol"]    = pd.to_numeric(df["vol"],    errors="coerce").fillna(0)
        date_str = os.path.basename(f).replace("set-history_EOD_","").replace(".csv","")
        for _, row in df.iterrows():
            sym = row["ticker"]
            if not sym or pd.isna(row["close"]):
                continue
            # ข้ามหุ้น warrant/option/-O/-W/-F
            if any(c in sym for c in ["-","."]) :
                continue
            ticker_bk = f"{sym}.BK"
            stocks[ticker_bk]["dates"].append(date_str)
            stocks[ticker_bk]["closes"].append(round(float(row["close"]), 4))
            stocks[ticker_bk]["volumes"].append(int(row["vol"]))
    except Exception as e:
        pass

log(f"โหลดเสร็จ — {len(stocks)} tickers")

# ── 2. Sort ตามวันที่ ──────────────────────────────────────────
log("Sort ข้อมูลตามวันที่...")
for ticker, data in stocks.items():
    triples = sorted(zip(data["dates"], data["closes"], data["volumes"]))
    if triples:
        d, c, v = zip(*triples)
        data["dates"]   = list(d)
        data["closes"]  = list(c)
        data["volumes"] = list(v)

# ── 3. ไม่ต้องใช้ yfinance — ข้อมูลในโฟลเดอร์ครบถึงวานนี้แล้ว ──

# ── 4. บันทึกผ่าน store layer (SQLite + JSON ช่วง dual-write) ────
import sys
sys.path.insert(0, BASE_DIR)
from core import store

log(f"บันทึกลง {store.DB_FILE}...")
history = {
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "stocks": dict(stocks)
}
store.upsert_history_dict(BASE_DIR, history)
log(f"  {store.DB_FILE} = {os.path.getsize(store._db_path(BASE_DIR))/1024/1024:.1f} MB")

if store.DUAL_WRITE_JSON:
    log("บันทึก set_history.json (dual-write)...")
    store._atomic_write_json(HISTORY_FILE, history)
    size_mb = os.path.getsize(HISTORY_FILE) / 1024 / 1024
    log(f"  set_history.json = {size_mb:.1f} MB")

log(f"เสร็จ! {len(stocks)} tickers")
log("ขั้นตอนถัดไป: กด Full Refresh บน dashboard เพื่อสร้าง set_data.json ใหม่")
