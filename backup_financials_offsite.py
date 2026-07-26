# -*- coding: utf-8 -*-
"""สำรองไฟล์ที่สร้างใหม่ไม่ได้ ไปปลายทางนอกเครื่อง (external drive / cloud sync folder)

ทำไมต้องมี: งบ Finnomena ย้อน 16 ปีที่ mirror มาแล้ว "เป็นของเราแล้ว" อยู่ใน
financials.db (~630MB) ถาวร — แต่ Finnomena ไม่มี API ทางการ ถ้าปิด/เปลี่ยน API
เมื่อไหร่ ข้อมูลก้อนนี้จะ "สร้างใหม่ไม่ได้อีกเลย" เช่นเดียวกับ set_prices.db
(ราคาย้อนหลัง 10 ปี + หุ้น delisted ที่ Yahoo/SET ดึงใหม่ไม่ได้แล้ว — survivorship
bias fix), sec_filings.db (insider/ผู้ถือหุ้นใหญ่) และ delisted_log.json
(ประวัติหุ้นเข้า/ออก) financials_backups/ ในเครื่องช่วยกันไฟล์เสีย/เขียนพลาดได้
แต่ไม่ช่วยถ้าเครื่อง/ฮาร์ดดิสก์พังทั้งลูก — ต้องมีสำเนานอกเครื่องด้วย

ใช้:
    python backup_financials_offsite.py D:\\Backups\\SET_Dashboard
    (หรือดับเบิลคลิก backup_financials_offsite.bat แล้วพิมพ์ path เมื่อถาม)

แนะนำ: เดือนละครั้งพอ (หน้า Data Health ในแอปจะเตือนเองถ้าเกิน 35 วัน) —
เก็บสำรอง 3 ชุดล่าสุดที่ปลายทาง ชุดเก่ากว่านั้นลบทิ้งอัตโนมัติกันโฟลเดอร์บวม
"""
import io
import json
import os
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
]


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


def main():
    if len(sys.argv) < 2:
        print("ใช้: python backup_financials_offsite.py <โฟลเดอร์ปลายทาง>")
        print(r"เช่น: python backup_financials_offsite.py D:\Backups\SET_Dashboard")
        sys.exit(1)
    dest_dir = sys.argv[1]

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
