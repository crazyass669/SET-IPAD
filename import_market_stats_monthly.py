"""
import_market_stats_monthly.py
อ่าน Market_Statistics_Month_th_TH.xls แล้ว merge เข้า set_market_stats.json ที่มีอยู่
(เพิ่ม/อัพเดทเฉพาะเดือนที่อยู่ในไฟล์ ไม่ rebuild ทับประวัติทั้งชุด) — ดู
sources/set_market_stats_monthly.py

ใช้แทน import_market_stats.py (Table_PE.xls+Table_PBV.xls) สำหรับอัพเดทรายเดือน
ปกติ เพราะไฟล์เดียว โหลดง่ายกว่า และได้ข้อมูลเสริม (ปันผล/มูลค่าหลักทรัพย์/
breadth) มาด้วย — ไฟล์ Table_PE/PBV.xls ยังเก็บไว้ใช้ import_market_stats.py
เป็น fallback กรณีไฟล์นี้มีปัญหาหรืออยากได้ประวัติยาวตั้งแต่ต้น

วิธีใช้
    python import_market_stats_monthly.py                      # ไฟล์ในโฟลเดอร์โปรเจกต์/โฟลเดอร์ดาวน์โหลด
    python import_market_stats_monthly.py path\to\file.xls ...  # ระบุเอง (ใส่ได้หลายไฟล์/ใช้ * ได้)

ใส่หลายไฟล์ได้เพื่อ "backfill" ปีเก่า: ไฟล์นี้ 1 ไฟล์ = 1 ปี เพราะงั้นถ้าโหลดของปี
ก่อนๆ จาก SET มาด้วย (Market Data > Statistics > Market Statistics เลือกปี) แล้วรัน
ทีเดียวหลายไฟล์ จะได้ประวัติ ปันผล/มูลค่าหลักทรัพย์/breadth ย้อนหลังยาวขึ้นทันที
(ต้องมี ≥ 8 เดือน หน้าเว็บถึงจะวาดแถบเทียบอดีตให้)
"""
import glob
import json
import os
import sys
from datetime import datetime

from sources.set_market_stats_monthly import merge_monthly, parse_annual_market_statistics

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(BASE_DIR, "set_market_stats.json")
FNAME = "Market_Statistics_Month_th_TH.xls"


def default_sources():
    """โฟลเดอร์โปรเจกต์ก่อน แล้วค่อยโฟลเดอร์ดาวน์โหลด (env SET_XLS_DIR หรือ ~/Downloads/dash)
    — ตรงกับที่ /api/refresh-market-stats-monthly ใช้ (ดู _resolve_xls ใน app.py)"""
    extra = os.environ.get("SET_XLS_DIR") or os.path.join(os.path.expanduser("~"), "Downloads", "dash")
    for d in (BASE_DIR, extra):
        p = os.path.join(d, FNAME)
        if os.path.exists(p):
            return [p]
    return []


args = sys.argv[1:]
files = []
for a in args:
    hits = sorted(glob.glob(a))          # PowerShell ไม่ขยาย * ให้ ต้องขยายเองที่นี่
    files.extend(hits or [a])
if not files:
    files = default_sources()
if not files:
    sys.exit(f"ไม่พบ {FNAME} (ทั้งในโฟลเดอร์โปรเจกต์และโฟลเดอร์ดาวน์โหลด) — "
             "ระบุ path เองได้: python import_market_stats_monthly.py <ไฟล์.xls>")

data = {}
if os.path.exists(OUT_FILE):
    with open(OUT_FILE, encoding="utf-8") as f:
        data = json.load(f)
else:
    print("ไม่พบ set_market_stats.json เดิม — จะเริ่มสร้างใหม่ (ไม่มีประวัติย้อนหลังลึก "
          "แนะนำรัน import_market_stats.py จาก Table_PE.xls/Table_PBV.xls ก่อนสักครั้งเพื่อได้ประวัติเต็ม)")

total_months = 0
for path in files:
    print(f"อ่าน {path}...")
    records, year_ad = parse_annual_market_statistics(path)
    months = sorted(records)
    print(f"  พบข้อมูลปี {year_ad}: {len(records)} เดือน ({months[0]} – {months[-1]})")
    data = merge_monthly(data, records)   # upsert ทีละไฟล์ ลำดับไฟล์ไม่สำคัญ (เรียงตามวันที่อยู่แล้ว)
    total_months += len(records)

data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

size = os.path.getsize(OUT_FILE) / 1024
print(f"บันทึก set_market_stats.json ({size:.0f} KB) — merge {len(files)} ไฟล์ / {total_months} เดือน")
print(f"P/E  SET ทั้งหมด : {len(data['pe']['dates'])} เดือน ({data['pe']['dates'][0]} – {data['pe']['dates'][-1]})")

pe_set = data["pe"]["stats"].get("SET", {})
pbv_set = data["pbv"]["stats"].get("SET", {})
dy_set = data["div_yield"]["stats"].get("SET", {})
mc_set = data["mkt_cap"]["stats"].get("SET", {})
print()
if pe_set:
    print(f"P/E  SET ปัจจุบัน : {pe_set.get('current')}x  (z={pe_set.get('zscore'):+}σ)")
if pbv_set:
    print(f"P/BV SET ปัจจุบัน : {pbv_set.get('current')}x  (z={pbv_set.get('zscore'):+}σ)")
if dy_set:
    print(f"Div Yield SET     : {dy_set.get('current')}%  (z={dy_set.get('zscore'):+}σ)")
if mc_set:
    print(f"Market Cap SET    : {mc_set.get('current'):,.0f} ล้านบาท")

for key in ("div_yield", "mkt_cap", "breadth"):
    dates = data.get(key, {}).get("dates") or []
    if dates:
        note = "" if len(dates) >= 8 else "  ⚠ ต่ำกว่า 8 เดือน หน้าเว็บจะยังไม่วาดแถบเทียบอดีต"
        print(f"{key:10s}: {len(dates)} เดือน ({dates[0]} – {dates[-1]}){note}")

breadth = data["breadth"]["series"]
if breadth["listed_SET"]:
    print()
    print(f"จำนวนบริษัทจดทะเบียน SET ล่าสุด : {breadth['listed_SET'][-1]}")
    print(f"  เข้าใหม่เดือนล่าสุด: {breadth['new_listed_SET'][-1]}  ถูกเพิกถอน: {breadth['delisted_SET'][-1]}")
