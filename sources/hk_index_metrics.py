# -*- coding: utf-8 -*-
"""sources/hk_index_metrics.py — RS/EMA/Stage/52W ของสมาชิกดัชนี HSI + HSCEI + HSTECH
เก็บผลลง data/hk_index_metrics.json ให้เมนูสายเทคนิคอ่านต่อได้โดยไม่ต้องคำนวณสดทุกครั้ง
logic จริงอยู่ที่ sources/index_metrics_common.py (ใช้ร่วมกับ us_index_metrics.py) — ไฟล์
นี้เหลือแค่ cfg เฉพาะตลาด HK"""
import os

from core import hk_store
from sources import index_metrics_common as _common
from sources import hk_index_membership

OUT_FILE = os.path.join("data", "hk_index_metrics.json")

_CFG = dict(
    membership=hk_index_membership,
    store=hk_store,
    index_keys=hk_index_membership.INDEXES,   # ("HSI","HSCEI","HSTECH")
    sector_keys_order=("HSI_sector", "HSCEI_sector"),
    # HSTECH ไม่มี sector จาก Wikipedia (bullet list ไม่มีคอลัมน์ industry) — เติมจาก
    # dr_universe.py (field 'ind') สำหรับตัวที่ทับซ้อนบางส่วน ที่เหลือปล่อย "Unknown"
    sector_fallback_dr=True,
    # ticker HK ใน dr_universe เป็นรูปแบบ yfinance "NNNN.HK" ตรงกับ ticker ในดัชนี HK พอดี
    name_map_predicate=lambda yf_t: yf_t.endswith(".HK"),
    market_code="HK",
    out_file=OUT_FILE,
)


def build(base_dir, callback=None, live_map=None):
    return _common.build(base_dir, _CFG, callback, live_map=live_map)


def update_live_prices(base_dir, live_map):
    return _common.update_live_prices(base_dir, _CFG, live_map)


def load_local(base_dir):
    return _common.load_local(base_dir, OUT_FILE)
