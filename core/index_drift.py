# -*- coding: utf-8 -*-
"""core/index_drift.py — เก็บผลเช็ค "หุ้นเข้าใหม่/ถูกถอด" ล่าสุดที่รันอัตโนมัติ (piggyback
ท้าย Quick Update สัปดาห์ละครั้ง — ดู _run_index_drift_checks ใน app.py) ไว้ที่
index_drift_status.json (local-only เหมือน delisted_log.json) ให้ /api/data-health อ่านแบบ
เบา (แค่เปิดไฟล์ ไม่ยิง Wikipedia/SET.or.th สดทุกครั้งที่เปิดแอป/หน้า Data Health)

ต่างจาก run_log.py (เก็บแค่ pass/fail ของกลไก) และ delisted_log.py (เก็บสะสมถาวรทีละตัว) —
อันนี้เก็บ "ผลเปรียบเทียบล่าสุด" (จำนวน new/removed) ต่อ universe หนึ่งค่าต่อหนึ่ง key
เขียนทับทุกครั้งที่เช็ค ไม่สะสมประวัติ (ดู PLAN_universe_data_health.txt งานเสริม)"""
import json
import os
from datetime import datetime


def _path(base_dir):
    return os.path.join(base_dir, "index_drift_status.json")


def record_check(base_dir, key, new_count, removed_count, new_sample=None, removed_sample=None, error=None):
    """เขียนทับผลเช็คของ universe หนึ่ง (key เช่น 'TH'/'US'/'HK'/'JP') — เก็บแค่ sample
    สั้นๆ (20 ตัวแรก) พอให้ Data Health โชว์ตัวอย่างได้ ไม่ต้องเก็บ list เต็ม"""
    path = _path(base_dir)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    data[key] = {
        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "new_count": new_count,
        "removed_count": removed_count,
        "new_sample": (new_sample or [])[:20],
        "removed_sample": (removed_sample or [])[:20],
        "error": error,
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def read_status(base_dir):
    try:
        with open(_path(base_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
