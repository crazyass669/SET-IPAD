# -*- coding: utf-8 -*-
"""sources/jp_index_metrics.py — RS/EMA/Stage/52W ของสมาชิกดัชนี Nikkei 225
เก็บผลลง data/jp_index_metrics.json ให้เมนูสายเทคนิคอ่านต่อได้โดยไม่ต้องคำนวณสดทุกครั้ง
logic จริงอยู่ที่ sources/index_metrics_common.py (ใช้ร่วมกับ us_index_metrics.py/
hk_index_metrics.py) — ไฟล์นี้เหลือแค่ cfg เฉพาะตลาด JP"""
import os

from core import jp_store
from sources import index_metrics_common as _common
from sources import jp_index_membership

OUT_FILE = os.path.join("data", "jp_index_metrics.json")

_CFG = dict(
    membership=jp_index_membership,
    store=jp_store,
    index_keys=jp_index_membership.INDEXES,   # ("NIKKEI225",)
    sector_keys_order=("NIKKEI225_sector",),
    # ja.wikipedia ให้ sector ครบทุกตัวอยู่แล้ว (225/225 ยืนยันแล้ว) ไม่ต้อง fallback —
    # ต่างจาก HK ที่ HSTECH ไม่มี sector จาก Wikipedia ต้องเติมจาก dr_universe · หมายเหตุ:
    # sector_fallback_dr=True ใน index_metrics_common._compute_all_rows เช็คแค่ suffix
    # ".HK" ตายตัว (ไม่ได้ generic ตาม cfg) จะไม่ทำงานกับ ".T" อยู่แล้วแม้เปิดไว้ก็ตาม
    sector_fallback_dr=False,
    name_map_predicate=lambda yf_t: yf_t.endswith(".T"),
    market_code="JP",
    out_file=OUT_FILE,
)


def build(base_dir, callback=None, live_map=None):
    return _common.build(base_dir, _CFG, callback, live_map=live_map)


def load_local(base_dir):
    return _common.load_local(base_dir, OUT_FILE)
