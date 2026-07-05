"""
ทดสอบ SEC OpenData API — เช็คว่ามีข้อมูลอะไรบ้าง
ตั้ง key ผ่าน env var ก่อนรัน (ห้าม hardcode ในไฟล์ — ไฟล์นี้อยู่ใน git):
    PowerShell:  $env:SEC_API_KEY = "your-key"; python test_sec_api.py
"""
import os
import sys

import requests
import json

SEC_API_KEY = os.environ.get("SEC_API_KEY", "")
if not SEC_API_KEY:
    sys.exit("ยังไม่ได้ตั้ง env var SEC_API_KEY — ดูวิธีที่หัวไฟล์")

HEADERS = {
    "accept": "application/json",
    "APIKEY": SEC_API_KEY,
}

BASE = "https://api.sec.or.th"

def test(label, url, params=None):
    print(f"\n{'='*60}")
    print(f"[{label}]")
    print(f"URL: {url}")
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"Records: {len(data)}")
                print(f"Sample: {json.dumps(data[0], ensure_ascii=False, indent=2)[:500]}")
            elif isinstance(data, dict):
                print(f"Keys: {list(data.keys())}")
                print(f"Sample: {json.dumps(data, ensure_ascii=False, indent=2)[:500]}")
        else:
            print(f"Error: {r.text[:300]}")
    except Exception as e:
        print(f"Exception: {e}")

# ── 1. กองทุน ──────────────────────────────────────────
test("กองทุนทั้งหมด",
     f"{BASE}/FundFactsheet/fund/")

test("NAV กองทุน (ตัวอย่าง KFSDIV)",
     f"{BASE}/FundFactsheet/fund/nav/KFSDIV",
     params={"startDate": "2026-01-01", "endDate": "2026-06-20"})

test("ข้อมูลกองทุน KFSDIV",
     f"{BASE}/FundFactsheet/fund/KFSDIV")

# ── 2. หุ้น SET ─────────────────────────────────────────
test("ราคาหุ้น SET (ลอง equity endpoint)",
     f"{BASE}/OpenApi/equity/price",
     params={"symbol": "PTT", "from": "2026-01-01", "to": "2026-06-20"})

test("ข้อมูลบริษัทจดทะเบียน",
     f"{BASE}/OpenApi/company/info",
     params={"symbol": "PTT"})

test("One Report",
     f"{BASE}/OpenApi/oneReport/list",
     params={"symbol": "PTT"})

# ── 3. Bond ─────────────────────────────────────────────
test("Bond ทั้งหมด",
     f"{BASE}/OpenApi/bond/list")

print("\n" + "="*60)
print("เสร็จ — ดูผลข้างบนว่า endpoint ไหนใช้ได้บ้าง")
