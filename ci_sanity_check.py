# -*- coding: utf-8 -*-
"""ci_sanity_check.py — เช็คความสมเหตุสมผลของข้อมูลที่เพิ่งอัพเดท ก่อนให้ GitHub
Actions commit/push ขึ้น GitHub Pages

ทำไมต้องมี: ถ้าวันไหนแหล่งข้อมูล (SET.or.th/Yahoo/TradingView ฯลฯ) เปลี่ยนรูปแบบ
หน้าเว็บ/ล่มกลางทาง สคริปต์อาจไม่ throw error แต่ได้ข้อมูล "เพี้ยน" กลับมาแทน
(เช่น หุ้นเหลือ 50 ตัวจาก 927) ซึ่งจะถูก commit ทับข้อมูลดีทันทีโดยไม่มีใครรู้
สคริปต์นี้เทียบจำนวนรายการของแต่ละไฟล์ "ใหม่" (working tree หลังรัน
run_static_update.py) กับ "เก่า" (git HEAD ก่อนรัน) — ถ้าตกลงเกิน threshold
ให้ fail (exit 1) ทั้ง workflow กันข้อมูลเสียหลุดขึ้น production

ใช้: python ci_sanity_check.py   (รันหลัง run_static_update.py, ก่อน git commit)
"""
import io
import json
import subprocess
import sys
from datetime import date, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

def _distinct_tail_dates(d):
    """นับจำนวน "วัน" ที่ยังมีอยู่ใน daily_tail ของทั้งไฟล์ (union ข้ามหุ้น) — ใช้กับไฟล์ bake
    data/short_sales.json, data/nvdr_data.json ที่เก็บ 21 snapshot ล่าสุดต่อหุ้น ปกติได้ ~25-26
    วัน (หุ้นแต่ละตัวมีวันไม่ตรงกันเป๊ะ) แล้วนิ่งอยู่แถวนั้น

    ทำไมต้องนับ "ความลึก" แยกจากการนับ "จำนวนหุ้น": ไฟล์สะสม short_sales_data.json/
    nvdr_data.json อยู่ใน .gitignore ฝั่ง CI จึงพึ่ง actions/cache ถ้า cache miss รอบนั้นจะเริ่ม
    จากศูนย์ — แต่ SET API ยังคืนหุ้นครบ ~900 ตัวเหมือนเดิม เกณฑ์ที่นับ len(stocks) เลยได้
    ratio 100% ผ่านฉลุยทั้งที่ประวัติเหลือวันเดียว (แถวสองรายการนั้นยัง [skip] เพราะ .gitignore
    ทำให้ไม่มี HEAD เดิมให้เทียบด้วยซ้ำ) ตัวนี้จับตรงจุดนั้น"""
    dates = set()
    for v in (d.get("stocks") or {}).values():
        for t in (v.get("daily_tail") or []):
            if t and t[0]:
                dates.add(t[0])
    return len(dates)


# (path, ฟังก์ชันดึงจำนวนรายการจาก dict ที่ json.load ได้, สัดส่วนต่ำสุดที่ยอมรับ)
CHECKS = [
    ("data/set_data.json",      lambda d: len(d.get("stocks", [])),        0.80),
    ("indices_cache.json",      lambda d: len(d.get("data", {})),          0.80),
    ("short_sales_data.json",   lambda d: len(d.get("stocks", [])),        0.70),
    ("nvdr_data.json",          lambda d: len(d.get("stocks", [])),        0.70),
    ("market_flow_data.json",   lambda d: len(d.get("rows", [])),          0.95),
    ("s50_flow_data.json",      lambda d: len(d.get("rows", [])),          0.95),
    ("bond_flow_data.json",     lambda d: len(d.get("rows", [])),          0.95),
    # ความลึกประวัติของไฟล์ bake ที่ commit จริง (เว็บ/iPad + ตัว fallback ของเครื่อง local
    # อ่านจาก 2 ไฟล์นี้) — 0.70 เผื่อวันเก่าหลุดออก/วันใหม่เข้าตามปกติ แต่ยังจับเคสหดฮวบ
    ("data/short_sales.json",   _distinct_tail_dates,                      0.70),
    ("data/nvdr_data.json",     _distinct_tail_dates,                      0.70),
]

# ── freshness check ─────────────────────────────────────────────────────────
# ratio check ด้านบนจับได้แค่ "จำนวนแถวลดผิดปกติ" — ถ้า pipeline ล้มเหลวแบบเงียบๆ
# (เช่น SET.or.th ตอบ error/รูปแบบเปลี่ยน) ไฟล์จะค้างวันเดิมแต่จำนวนแถวเท่าเดิม
# (ratio = 100%) หลุดผ่าน check บนไปได้สบายๆ จุดนี้เช็ค "วันที่ข้อมูลล่าสุด" แทน
# เทียบกับวันทำการล่าสุด (เว้นเสาร์-อาทิตย์ อย่างหยาบๆ ไม่รู้จักวันหยุดนักขัตฤกษ์)
# แค่ "เตือน" (ไม่ exit 1) เพราะวันหยุดยาว/วันหยุดพิเศษที่สคริปต์ไม่รู้จักจะกลาย
# เป็น false positive บล็อค commit ข้อมูลดีไปโดยไม่จำเป็น
FRESHNESS_CHECKS = [
    ("nvdr_data.json",        lambda d: d.get("updated_at")),
    ("short_sales_data.json", lambda d: d.get("last_api_update")),
    ("market_flow_data.json", lambda d: (d.get("rows") or [{}])[-1].get("date")
                                          if d.get("rows") else None),
]
MAX_STALE_BUSINESS_DAYS = 2  # เกินนี้ (ไม่นับเสาร์-อาทิตย์) ถือว่าค้างผิดปกติ


def _last_business_day(d):
    while d.weekday() >= 5:  # 5=เสาร์, 6=อาทิตย์
        d -= timedelta(days=1)
    return d


def _business_days_between(d1, d2):
    """นับจำนวนวันทำการ (จ-ศ) ระหว่าง d1 (เก่ากว่า) กับ d2 แบบหยาบๆ ไม่รู้จักวันหยุดนักขัตฤกษ์"""
    n = 0
    d = d1
    while d < d2:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


def check_freshness():
    today = _last_business_day(date.today())
    for path, extract in FRESHNESS_CHECKS:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            continue
        raw = extract(data)
        if not raw:
            print(f"[freshness][WARN] {path}: ไม่พบวันที่ข้อมูลล่าสุดในไฟล์")
            continue
        try:
            last_date = date.fromisoformat(str(raw)[:10])
        except ValueError:
            print(f"[freshness][WARN] {path}: parse วันที่ไม่ได้ ({raw!r})")
            continue
        stale = _business_days_between(last_date, today)
        if stale > MAX_STALE_BUSINESS_DAYS:
            print(f"[freshness][WARN] {path}: ข้อมูลล่าสุด {last_date} ค้างมาแล้ว "
                  f"{stale} วันทำการ (>{MAX_STALE_BUSINESS_DAYS}) — เช็ค log ว่า "
                  f"pipeline ล้มเหลวหรือไม่ (ไม่ block commit)")
        else:
            print(f"[freshness][OK] {path}: ข้อมูลล่าสุด {last_date}")


def _git_show(path):
    """อ่านเนื้อไฟล์จาก git HEAD (ก่อนรัน update รอบนี้) — คืน None ถ้าไม่เคยมีมาก่อน"""
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{path}"],
            capture_output=True, timeout=30,
        )
        if out.returncode != 0:
            return None
        return json.loads(out.stdout.decode("utf-8"))
    except Exception:
        return None


def main():
    failures = []
    for path, extract, min_ratio in CHECKS:
        try:
            with open(path, encoding="utf-8") as f:
                new_data = json.load(f)
        except FileNotFoundError:
            print(f"[skip] {path} ไม่มีไฟล์นี้ในรอบนี้")
            continue

        new_count = extract(new_data)
        old_data = _git_show(path)
        if old_data is None:
            print(f"[skip] {path} ไม่มี HEAD เดิมให้เทียบ (ไฟล์ใหม่) — new={new_count}")
            continue

        old_count = extract(old_data)
        if old_count == 0:
            print(f"[skip] {path} HEAD เดิมมี 0 รายการ ข้ามเช็ค — new={new_count}")
            continue

        ratio = new_count / old_count
        status = "OK" if ratio >= min_ratio else "FAIL"
        print(f"[{status}] {path}: {old_count} -> {new_count} ({ratio:.0%}, ต้อง >= {min_ratio:.0%})")
        if ratio < min_ratio:
            failures.append(f"{path}: {old_count} -> {new_count} ({ratio:.0%} < {min_ratio:.0%})")

    print()
    check_freshness()

    if failures:
        print("\n[ci_sanity_check] ล้มเหลว — ข้อมูลลดลงผิดปกติ ไม่ commit:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print("\n[ci_sanity_check] ผ่านทุกเช็ค")


if __name__ == "__main__":
    main()
