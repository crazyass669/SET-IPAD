# -*- coding: utf-8 -*-
"""สำรองไฟล์ที่สร้างใหม่ไม่ได้ ไปปลายทางนอกเครื่อง (external drive / cloud sync folder)

ทำไมต้องมี: งบ Finnomena ย้อน 16 ปีที่ mirror มาแล้ว "เป็นของเราแล้ว" อยู่ใน
financials.db (~630MB) ถาวร — แต่ Finnomena ไม่มี API ทางการ ถ้าปิด/เปลี่ยน API
เมื่อไหร่ ข้อมูลก้อนนี้จะ "สร้างใหม่ไม่ได้อีกเลย" เช่นเดียวกับ set_prices.db
(ราคาย้อนหลัง 10 ปี + หุ้น delisted ที่ Yahoo/SET ดึงใหม่ไม่ได้แล้ว — survivorship
bias fix), sec_filings.db (insider/ผู้ถือหุ้นใหญ่) และ delisted_log.json
(ประวัติหุ้นเข้า/ออก) financials_backups/ ในเครื่องช่วยกันไฟล์เสีย/เขียนพลาดได้
แต่ไม่ช่วยถ้าเครื่อง/ฮาร์ดดิสก์พังทั้งลูก — ต้องมีสำเนานอกเครื่องด้วย

us_prices.db/hk_prices.db/jp_prices.db (~440MB รวม) ต่างจาก 4 ไฟล์ข้างบนตรงที่
"regenerate ได้" จริง (Yahoo ยังมีประวัติเต็มของหุ้นดัชนีหลัก US/HK/JP ที่ยังเทรดอยู่
เสมอ ผ่านปุ่ม Index Max หรือ backfill_*_index_prices.py) แต่ backfill ใหม่ทั้งชุด
(500+225+100 ตัว ประวัติ period=max) ใช้เวลานานมาก — สำรองไว้เพื่อประหยัดเวลา
ไม่ใช่เพื่อกันข้อมูลหายถาวรแบบ 4 ไฟล์บน

ใช้:
    python backup_financials_offsite.py D:\\Backups\\SET_Dashboard
    (หรือดับเบิลคลิก backup_financials_offsite.bat แล้วพิมพ์ path เมื่อถาม)

แนะนำ: เดือนละครั้งพอ (หน้า Data Health ในแอปจะเตือนเองถ้าเกิน 35 วัน) —
เก็บสำรอง 3 ชุดล่าสุดที่ปลายทาง ชุดเก่ากว่านั้นลบทิ้งอัตโนมัติกันโฟลเดอร์บวม
"""
import io
import json
import os
import sqlite3
import sys
import shutil
import time
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import run_log  # noqa: E402

# (ชื่อไฟล์ต้นทาง, ต้องมีอยู่จริงถึงจะสำรอง — False = ข้ามเงียบๆ ถ้าไม่เจอ)
FILES = [
    ("financials.db", True),
    ("set_prices.db", True),
    ("sec_filings.db", False),
    ("delisted_log.json", False),
    ("us_prices.db", False),
    ("hk_prices.db", False),
    ("jp_prices.db", False),
]


def _backup_db_file(src, dest):
    """คัดลอก SQLite DB ด้วย Online Backup API แทน shutil.copy2 — financials.db/
    set_prices.db/sec_filings.db เปิด WAL mode ทั้งหมด (ดู core/store.py,
    sources/financials_store.py, sources/sec_store.py) และแอปหลัก/scheduled task
    อาจกำลังเขียนอยู่ตอนสำรอง copy2 ก็อปไบต์ไฟล์ .db ดิบตรงๆ โดยไม่รู้จัก -wal เลย
    เสี่ยงได้ snapshot ที่ขาด commit ล่าสุด หรือแย่กว่านั้นคือไฟล์ torn เปิดไม่ขึ้นอีกเลย
    backup() ของ sqlite3 ใช้ Online Backup API จริง ได้ snapshot consistent เสมอ
    แม้กำลังมีคนเขียนพร้อมกัน (pattern เดียวกับ financials_store.backup_db)"""
    src_con = sqlite3.connect(src)
    try:
        dst_con = sqlite3.connect(dest)
        try:
            src_con.backup(dst_con)
        finally:
            dst_con.close()
    finally:
        src_con.close()


def _backup_one(fname, required, dest_dir):
    src = os.path.join(BASE, fname)
    if not os.path.exists(src):
        if required:
            raise FileNotFoundError(f"ไม่พบ {src} — ยังไม่เคยรันงบการเงินเลยหรือเปล่า?")
        print(f"[Backup] ข้าม {fname} (ยังไม่มีไฟล์นี้)", flush=True)
        return None

    stem, ext = os.path.splitext(fname)
    stamp = datetime.now().strftime("%Y%m%d")
    dest = os.path.join(dest_dir, f"{stem}_{stamp}{ext}")
    size_mb = os.path.getsize(src) / 1024 / 1024
    print(f"[Backup] กำลังคัดลอก {fname} ({size_mb:.0f} MB) -> {dest} ...", flush=True)
    t0 = time.time()
    if ext == ".db":
        _backup_db_file(src, dest)
    else:
        shutil.copy2(src, dest)
    elapsed = time.time() - t0
    print(f"[Backup] เสร็จใน {elapsed:.0f} วินาที", flush=True)

    # เก็บสำรองแค่ 3 ชุดล่าสุดต่อไฟล์ที่ปลายทาง ชุดเก่ากว่าลบทิ้งอัตโนมัติ
    existing = sorted(
        f for f in os.listdir(dest_dir)
        if f.startswith(f"{stem}_") and f.endswith(ext)
    )
    for old in existing[:-3]:
        try:
            os.remove(os.path.join(dest_dir, old))
            print(f"[Backup] ลบสำรองเก่า: {old}", flush=True)
        except Exception as e:
            print(f"[Backup] ลบ {old} ไม่สำเร็จ (ไม่หยุด): {e}", flush=True)

    return f"{fname} ({size_mb:.0f} MB, {elapsed:.0f} วิ)"


def _fresh_enough(min_age_days):
    """เช็คว่าสำรองล่าสุด (จาก run_log) ยังใหม่กว่า min_age_days วันไหม — ใช้กับ
    trigger 'At log on' ที่ยิงทุกครั้งที่ล็อกอิน กันไม่ให้สำรองซ้ำถี่เกินจำเป็น
    (trigger รายสัปดาห์ปกติไม่ควรโดนเงื่อนไขนี้บล็อค เพราะห่างกันเกิน min_age_days
    อยู่แล้วตามธรรมชาติ — ยกเว้นเพิ่งสำรองไปจริงๆ ไม่นาน ซึ่งก็ถูกต้องที่จะข้าม)"""
    status = run_log.read_status(BASE).get("offsite_backup")
    if not status or not status.get("ok"):
        return False
    try:
        last = datetime.strptime(status["at"], "%Y-%m-%d %H:%M:%S")
    except (KeyError, ValueError):
        return False
    return (datetime.now() - last).days < min_age_days


def main():
    args = [a for a in sys.argv[1:]]
    min_age_days = None
    if "--min-age-days" in args:
        i = args.index("--min-age-days")
        min_age_days = int(args[i + 1])
        del args[i:i + 2]

    if len(args) < 1:
        print("ใช้: python backup_financials_offsite.py <โฟลเดอร์ปลายทาง> [--min-age-days N]")
        print(r"เช่น: python backup_financials_offsite.py D:\Backups\SET_Dashboard --min-age-days 3")
        sys.exit(1)
    dest_dir = args[0]

    if min_age_days is not None and _fresh_enough(min_age_days):
        print(f"[Backup] สำรองล่าสุดยังใหม่กว่า {min_age_days} วัน — ข้ามรอบนี้ (ใช้กับ trigger 'At log on')", flush=True)
        return

    try:
        os.makedirs(dest_dir, exist_ok=True)
        # เช็คว่าเขียนปลายทางได้จริงก่อนเริ่มคัดลอกไฟล์ใหญ่ (กันเจอ error กลางทาง
        # หลังรอนาน เช่น external drive ถอดอยู่/ไม่มีสิทธิ์เขียน)
        probe = os.path.join(dest_dir, ".write_test")
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)

        results = []
        for fname, required in FILES:
            r = _backup_one(fname, required, dest_dir)
            if r:
                results.append(r)

        # เก็บ dest_dir เป็น JSON (แนวเดียวกับ finnomena_mirror_last) ให้ Data Health
        # อ่าน dest_dir ไปสแกนดูวันที่สำรองจริงของแต่ละไฟล์ได้เอง ไม่ต้องพึ่ง path
        # hardcode — ถ้าเปลี่ยนปลายทางวันไหน หน้า Data Health จะตามเปลี่ยนเองอัตโนมัติ
        message = json.dumps({"dest_dir": dest_dir, "summary": "; ".join(results)},
                              ensure_ascii=False)
        run_log.record_run(BASE, "offsite_backup", True, message)
        print("[Backup] บันทึกสถานะแล้ว — หน้า Data Health ในแอปจะเห็นว่าสำรองล่าสุดเมื่อไหร่", flush=True)
    except Exception as e:
        run_log.record_run(BASE, "offsite_backup", False, str(e))
        print(f"[!] สำรองไม่สำเร็จ: {e}", flush=True)
        raise


if __name__ == "__main__":
    main()
