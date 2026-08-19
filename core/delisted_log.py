# -*- coding: utf-8 -*-
"""core/delisted_log.py — บันทึกวันที่ "ตรวจพบครั้งแรก" ว่าหุ้นตัวหนึ่งน่าจะ
เพิกถอน/แขวนถาวร/delisted แทนที่จะแค่กรองออกจาก universe เงียบๆ (เดิมทำแบบนั้น
ทั้งฝั่งหุ้นไทยใน app.py:_financials_universe และฝั่ง mirror US/HK ใน
sources/factor_snapshot.py:build_mirror_snapshot)

ประโยชน์: backtest รุ่นถัดไปจะรู้ว่าหุ้นตัวนั้น "ยังอยู่จริง" ณ จุดเวลาไหน
(point-in-time membership) ต่อยอดงานแก้ survivorship bias ที่ทำไปแล้วใน
set_prices.db — ต่างจาก run_log.py ที่เก็บแค่ "ผลรันล่าสุด" อันนี้เป็น log
สะสมระยะยาว (เก็บวันที่ตรวจพบครั้งแรกไว้ถาวร ไม่ทับ)"""
import json
import os
import time
from datetime import datetime


def _log_path(base_dir):
    return os.path.join(base_dir, "delisted_log.json")


def _read(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
    # Windows: antivirus/OneDrive บางทีล็อคไฟล์ชั่วครู่ระหว่างที่เพิ่งเขียน .tmp เสร็จ
    # (เจอจริงตอน build_mirror_snapshot เรียกฟังก์ชันนี้รัวๆ หลายพันครั้งติดกัน) — retry สั้นๆ
    # ก่อนยอมแพ้ แทนที่จะ crash ทั้ง batch เพราะไฟล์ log ที่ไม่ critical นี้
    for attempt in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            if attempt == 4:
                # เดิม raise ทิ้งที่นี่ขัดกับเจตนาของคอมเมนต์ข้างบน (ไม่ critical, ไม่ควร
                # crash ทั้ง batch) ถ้า caller (เช่น build_mirror_snapshot เรียกรัวๆ ใน
                # thread งานพื้นหลัง) ไม่มี try/except ห่ออยู่ exception นี้จะทำให้ job
                # ทั้งก้อนพังกลางทาง — log เตือนแล้วปล่อยผ่าน ไฟล์ log สะสมนี้จะขาดรายการ
                # ของรอบนี้ไปแค่รอบเดียว (รอบหน้าที่ universe ยังกรองตัวเดิมซ้ำจะเขียนติดใหม่)
                print(f"[delisted_log] เขียนไฟล์ไม่สำเร็จหลัง retry 5 ครั้ง ({e}) — ข้าม (ไม่ critical)")
                try:
                    os.remove(tmp)
                except Exception:
                    pass
                return
            time.sleep(0.2 * (attempt + 1))


def _apply(data, symbol, market, reason, last_seen=None):
    """upsert 1 รายการลง dict ที่อ่านมาแล้ว — คืน True ถ้ามีอะไรเปลี่ยนจริง"""
    key = f"{market}:{symbol}"
    cur = data.get(key)
    if cur is not None:
        new_last_seen = last_seen or cur.get("last_seen")
        if cur.get("last_seen") == new_last_seen and cur.get("reason") == reason:
            return False
        cur["last_seen"] = new_last_seen
        cur["reason"] = reason
        return True
    data[key] = {
        "symbol": symbol,
        "market": market,
        "reason": reason,
        "detected_at": datetime.now().strftime("%Y-%m-%d"),
        "last_seen": last_seen,
    }
    return True


def record_delisted_bulk(base_dir, entries):
    """upsert หลายรายการในรอบเดียว — อ่านไฟล์ครั้งเดียว เขียนครั้งเดียว และ
    "ไม่เขียนเลย" ถ้าไม่มีรายการไหนเปลี่ยนจริง (กรณีปกติของ universe ที่กรอง
    ตัวเดิมซ้ำทุกรอบ) — เดิม caller วน record_delisted() ทีละตัวซึ่งอ่าน+เขียน
    ทั้งไฟล์ทุกครั้ง (วัดจริง: /api/data-health เขียนไฟล์ 276 รอบ = ~3 วินาที
    ต่อการเปิดหน้า 1 ครั้ง ทั้งที่ข้อมูลไม่เปลี่ยนเลยสักตัว)

    entries = iterable ของ (symbol, market, reason, last_seen)
    คืนจำนวนรายการที่เปลี่ยนจริง"""
    entries = list(entries)
    if not entries:
        return 0
    path = _log_path(base_dir)
    data = _read(path)
    changed = 0
    for symbol, market, reason, last_seen in entries:
        if _apply(data, symbol, market, reason, last_seen=last_seen):
            changed += 1
    if changed:
        _write(path, data)
    return changed


def record_delisted(base_dir, symbol, market, reason, last_seen=None):
    """upsert ตาม (market, symbol) — เก็บ detected_at ของครั้งแรกที่เจอไว้เสมอ
    (ไม่ทับ แม้ถูกเรียกซ้ำทุกรอบที่ universe ยังกรองตัวเดิมออก) อัพเดตแค่
    last_seen/reason ถ้าเปลี่ยน

    ถ้าต้องบันทึกหลายตัวติดกัน ใช้ record_delisted_bulk() แทน — ตัวนี้อ่าน+เขียน
    ทั้งไฟล์ต่อ 1 การเรียก"""
    record_delisted_bulk(base_dir, [(symbol, market, reason, last_seen)])


def read_log(base_dir):
    try:
        with open(_log_path(base_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
