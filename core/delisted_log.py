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
from datetime import datetime


def _log_path(base_dir):
    return os.path.join(base_dir, "delisted_log.json")


def record_delisted(base_dir, symbol, market, reason, last_seen=None):
    """upsert ตาม (market, symbol) — เก็บ detected_at ของครั้งแรกที่เจอไว้เสมอ
    (ไม่ทับ แม้ถูกเรียกซ้ำทุกรอบที่ universe ยังกรองตัวเดิมออก) อัพเดตแค่
    last_seen/reason ถ้าเปลี่ยน"""
    path = _log_path(base_dir)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    key = f"{market}:{symbol}"
    if key in data:
        data[key]["last_seen"] = last_seen or data[key].get("last_seen")
        data[key]["reason"] = reason
    else:
        data[key] = {
            "symbol": symbol,
            "market": market,
            "reason": reason,
            "detected_at": datetime.now().strftime("%Y-%m-%d"),
            "last_seen": last_seen,
        }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, path)


def read_log(base_dir):
    try:
        with open(_log_path(base_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
