"""
import_market_stats_monthly.py
อ่าน Market_Statistics_Month_th_TH.xls แล้ว merge เข้า set_market_stats.json ที่มีอยู่
(เพิ่ม/อัพเดทเฉพาะเดือนล่าสุด ไม่ rebuild ทับประวัติทั้งชุด) — ดู
sources/set_market_stats_monthly.py

ใช้แทน import_market_stats.py (Table_PE.xls+Table_PBV.xls) สำหรับอัพเดทรายเดือน
ปกติ เพราะไฟล์เดียว โหลดง่ายกว่า และได้ข้อมูลเสริม (ปันผล/มูลค่าหลักทรัพย์/
breadth) มาด้วย — ไฟล์ Table_PE/PBV.xls ยังเก็บไว้ใช้ import_market_stats.py
เป็น fallback กรณีไฟล์นี้มีปัญหาหรืออยากได้ประวัติยาวตั้งแต่ต้น
"""
import json
import os
from datetime import datetime

from sources.set_market_stats_monthly import merge_monthly, parse_annual_market_statistics

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_FILE = os.path.join(BASE_DIR, "Market_Statistics_Month_th_TH.xls")
OUT_FILE = os.path.join(BASE_DIR, "set_market_stats.json")

print("อ่าน Market_Statistics_Month_th_TH.xls...")
records, year_ad = parse_annual_market_statistics(SRC_FILE)
months = sorted(records)
print(f"พบข้อมูลปี {year_ad}: {len(records)} เดือน ({months[0]} – {months[-1]})")

data = {}
if os.path.exists(OUT_FILE):
    with open(OUT_FILE, encoding="utf-8") as f:
        data = json.load(f)
else:
    print("ไม่พบ set_market_stats.json เดิม — จะเริ่มสร้างใหม่ (ไม่มีประวัติย้อนหลังลึก "
          "แนะนำรัน import_market_stats.py จาก Table_PE.xls/Table_PBV.xls ก่อนสักครั้งเพื่อได้ประวัติเต็ม)")

data = merge_monthly(data, records)
data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

size = os.path.getsize(OUT_FILE) / 1024
print(f"บันทึก set_market_stats.json ({size:.0f} KB)")
print(f"P/E  SET ทั้งหมด : {len(data['pe']['dates'])} เดือน ({data['pe']['dates'][0]} – {data['pe']['dates'][-1]})")

pe_set = data["pe"]["stats"].get("SET", {})
pbv_set = data["pbv"]["stats"].get("SET", {})
dy_set = data["div_yield"]["stats"].get("SET", {})
mc_set = data["mkt_cap"]["stats"].get("SET", {})
print()
print(f"P/E  SET ปัจจุบัน : {pe_set.get('current')}x  (z={pe_set.get('zscore'):+}σ)")
print(f"P/BV SET ปัจจุบัน : {pbv_set.get('current')}x  (z={pbv_set.get('zscore'):+}σ)")
if dy_set:
    print(f"Div Yield SET     : {dy_set.get('current')}%  (z={dy_set.get('zscore'):+}σ)")
if mc_set:
    print(f"Market Cap SET    : {mc_set.get('current'):,.0f} ล้านบาท")

breadth = data["breadth"]["series"]
if breadth["listed_SET"]:
    print()
    print(f"จำนวนบริษัทจดทะเบียน SET ล่าสุด : {breadth['listed_SET'][-1]}")
    print(f"  เข้าใหม่เดือนล่าสุด: {breadth['new_listed_SET'][-1]}  ถูกเพิกถอน: {breadth['delisted_SET'][-1]}")
