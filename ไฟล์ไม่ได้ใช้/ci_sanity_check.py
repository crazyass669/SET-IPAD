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

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# (path, ฟังก์ชันดึงจำนวนรายการจาก dict ที่ json.load ได้, สัดส่วนต่ำสุดที่ยอมรับ)
CHECKS = [
    ("data/set_data.json",      lambda d: len(d.get("stocks", [])),        0.80),
    ("indices_cache.json",      lambda d: len(d.get("data", {})),          0.80),
    ("short_sales_data.json",   lambda d: len(d.get("stocks", [])),        0.70),
    ("nvdr_data.json",          lambda d: len(d.get("stocks", [])),        0.70),
    ("market_flow_data.json",   lambda d: len(d.get("rows", [])),          0.95),
    ("s50_flow_data.json",      lambda d: len(d.get("rows", [])),          0.95),
    ("bond_flow_data.json",     lambda d: len(d.get("rows", [])),          0.95),
]


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

    if failures:
        print("\n[ci_sanity_check] ล้มเหลว — ข้อมูลลดลงผิดปกติ ไม่ commit:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print("\n[ci_sanity_check] ผ่านทุกเช็ค")


if __name__ == "__main__":
    main()
