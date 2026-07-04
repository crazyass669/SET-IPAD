# -*- coding: utf-8 -*-
"""core/store.py — persistence layer: atomic write, history I/O, write guards

seam สำหรับอนาคต: ถ้าย้าย set_history.json ไป SQLite ให้เปลี่ยนเฉพาะไฟล์นี้
"""
import json
import os
from datetime import datetime

OUT_FILE     = "set_data.json"
HISTORY_FILE = "set_history.json"


# ============================================================
# 1b. History helpers — load / merge / save set_history.json
# ============================================================

def _atomic_write_json(path, obj):
    """เขียน JSON แบบ atomic: เขียนลง .tmp ก่อนแล้ว os.replace ทับ
    ป้องกันไฟล์เสียหายถ้า process ตาย/ไฟดับกลางคัน"""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _check_stock_count(base_dir, new_count, min_ratio=0.8):
    """กันการเขียน set_data.json ทับด้วย universe ที่หดผิดปกติ
    (เช่น Yahoo ล่มทำให้ดึงได้ไม่กี่ตัว) — raise เพื่อให้ caller restore backup"""
    old_total = 0
    try:
        with open(os.path.join(base_dir, OUT_FILE), encoding="utf-8") as f:
            old_total = json.load(f).get("total", 0) or 0
    except Exception:
        return  # ไม่มีไฟล์เดิม/อ่านไม่ได้ — เขียนได้เลย
    if old_total > 0 and new_count < old_total * min_ratio:
        raise ValueError(
            f"ดึงข้อมูลได้แค่ {new_count}/{old_total} หุ้น (<{int(min_ratio*100)}%) — "
            f"อาจเกิดจากแหล่งข้อมูลล่ม จึงไม่บันทึกทับข้อมูลเดิม"
        )


def load_history(base_dir):
    path = os.path.join(base_dir, HISTORY_FILE)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _merge_history(existing, new_dates, new_closes, new_volumes):
    """Merge new bars into existing, upsert by date (overwrite if exists), keep sorted."""
    if not existing or not existing.get("dates"):
        return new_dates, new_closes, new_volumes
    data_map = {d: (c, v) for d, c, v in zip(existing["dates"], existing["closes"], existing["volumes"])}
    for d, c, v in zip(new_dates, new_closes, new_volumes):
        data_map[d] = (c, v)  # overwrite ถ้ามีอยู่แล้ว
    triples = sorted((d, c, v) for d, (c, v) in data_map.items())
    if not triples:
        return [], [], []
    dates, closes, volumes = zip(*triples)
    return list(dates), list(closes), list(volumes)


def save_history(all_data_map, base_dir, existing_hist=None):
    """
    all_data_map: {ticker -> {close: pd.Series, volume: pd.Series}}
    Merges with existing_hist and writes set_history.json.
    Returns the new history dict.
    """
    stocks_hist = {}
    if existing_hist:
        stocks_hist = dict(existing_hist.get("stocks", {}))

    for ticker, data in all_data_map.items():
        close  = data["close"]
        volume = data["volume"]
        new_dates   = [d.strftime("%Y-%m-%d") for d in close.index]
        new_closes  = [round(float(c), 4) for c in close]
        new_volumes = [int(v) for v in volume]
        existing = stocks_hist.get(ticker)
        merged_d, merged_c, merged_v = _merge_history(
            existing, new_dates, new_closes, new_volumes
        )
        stocks_hist[ticker] = {
            "dates":   merged_d,
            "closes":  merged_c,
            "volumes": merged_v,
        }

    new_hist = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stocks": stocks_hist,
    }
    path = os.path.join(base_dir, HISTORY_FILE)
    _atomic_write_json(path, new_hist)
    return new_hist


