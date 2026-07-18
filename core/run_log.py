# -*- coding: utf-8 -*-
"""core/run_log.py — บันทึกผลการรันล่าสุดของแต่ละกลไกอัพเดท (Quick Update, Full
Refresh, GitHub Actions auto-update, update_financials.py, mirror_finnomena.py)
ลง logs/update_status.json — ใช้โชว์ banner เตือนตอนเปิดแอปถ้ารอบล่าสุดล้มเหลว
แม้ผู้ใช้จะไม่ได้เฝ้าหน้าจอตอนรันจริง (โดยเฉพาะ GitHub Actions ที่รันตอนไม่มีใครดู)

เก็บแค่ "ผลล่าสุด" ต่อ source เดียว (ไม่ใช่ log สะสมยาว) — พอสำหรับ banner เตือน
ถ้าอยากได้ประวัติยาวย้อนหลัง ดูจาก logs/dashboard.log (rotating log ของ app.py) แทน"""
import json
import os
from datetime import datetime


def _log_path(base_dir):
    d = os.path.join(base_dir, "logs")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "update_status.json")


def record_run(base_dir, source, ok, message=""):
    """เรียกตอนจบการรันของกลไกอัพเดทแต่ละตัว (สำเร็จหรือล้มเหลวก็เรียก) —
    เขียนทับผลของ source เดิม (เก็บแค่รอบล่าสุด)"""
    path = _log_path(base_dir)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    data[source] = {
        "ok": bool(ok),
        "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "message": str(message)[:500],
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def read_status(base_dir):
    try:
        with open(_log_path(base_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
