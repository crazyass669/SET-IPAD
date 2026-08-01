# -*- coding: utf-8 -*-
"""
Smoke test — ยิง endpoint หลักทั้งหมดของ server ที่รันอยู่:
    python tests/smoke.py [base_url]     (default: http://localhost:5001)

- required endpoints: ต้องตอบ 200 + โครงสร้างขั้นต่ำถูก ไม่งั้น FAIL
- external endpoints (พึ่งเว็บนอก ช้า/ล่มได้): แค่รายงาน ไม่ทำให้ FAIL
exit code: 0 = ผ่าน, 1 = มี required fail
"""
import json
import sys
import urllib.request
import gzip

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5001"
sys.stdout.reconfigure(encoding="utf-8")

# (path, ต้องเป็น json?, key ที่ต้องมี หรือ None)
REQUIRED = [
    ("/",                          False, None),
    ("/api/status",                True,  "has_data"),
    ("/api/data",                  True,  "stocks"),
    ("/api/history/PTT",           True,  "dates"),
    ("/api/indices",               True,  None),
    ("/api/market-stats",          True,  None),
    ("/api/market-internals",      True,  None),
    ("/api/stock-valuation-stats", True,  None),
    ("/api/short-sales",           True,  None),
    ("/api/nvdr",                  True,  None),
    ("/api/prices",                True,  None),
    ("/api/dr",                    True,  "stocks"),
]
EXTERNAL = [  # ตรวจแบบผ่อนปรน — ล้มเหลวได้โดยไม่ FAIL (แหล่งข้อมูลนอก)
    ("/api/market-flow",  True, None),
]


def hit(path, timeout=90):
    req = urllib.request.Request(BASE_URL + path,
                                 headers={"Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        return r.status, body


def main():
    failures = 0
    for path, want_json, want_key in REQUIRED:
        try:
            status, body = hit(path)
            assert status == 200, f"HTTP {status}"
            if want_json:
                d = json.loads(body)
                assert not (isinstance(d, dict) and d.get("error")), f"error: {d.get('error')}"
                if want_key:
                    assert want_key in d, f"ไม่มี key '{want_key}'"
            print(f"✅ {path}  ({len(body):,} bytes)")
        except Exception as e:
            print(f"❌ {path}  — {e}")
            failures += 1

    for path, want_json, _ in EXTERNAL:
        try:
            status, body = hit(path, timeout=30)
            print(f"🌐 {path}  HTTP {status} ({len(body):,} bytes)")
        except Exception as e:
            print(f"🌐 {path}  — ล้มเหลว (external, ไม่นับ FAIL): {e}")

    print(f"\n{'✅ SMOKE PASS' if failures == 0 else f'❌ SMOKE FAIL ({failures} endpoints)'}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
