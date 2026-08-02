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
    # sector/industry มาจาก yfinance (Yahoo taxonomy) ไม่ใช่ Wikipedia — HSI wiki table มีแค่
    # Sub-index หยาบ 4 กลุ่ม (หุ้น HSI-only กองรวมเป็น "Commerce & Industry" ก้อนเดียว) และ
    # HSCEI Industry column ก็ไม่ครอบคลุมตัวที่อยู่แค่ HSTECH — ดู
    # hk_index_membership._fetch_sector_map (เติมตอน sync_membership, ครบ 105/105 ตอนทดสอบ)
    sector_keys_order=("sector",),
    industry_key="industry",
    # เผื่อ yfinance บางตัวดึงไม่ได้ชั่วคราว (rate-limit) — เติมจาก dr_universe.py (field 'ind')
    # เป็น fallback สุดท้ายเท่านั้น (setdefault ใน index_metrics_common ไม่ทับของจริงจาก yfinance)
    sector_fallback_dr=True,
    # ticker HK ใน dr_universe เป็นรูปแบบ yfinance "NNNN.HK" ตรงกับ ticker ในดัชนี HK พอดี
    name_map_predicate=lambda yf_t: yf_t.endswith(".HK"),
    market_code="HK",
    out_file=OUT_FILE,
)


def build(base_dir, callback=None, live_map=None):
    return _common.build(base_dir, _CFG, callback, live_map=live_map)


def update_live_prices(base_dir, live_map, scope_tickers):
    return _common.update_live_prices(base_dir, _CFG, live_map, scope_tickers)


def load_local(base_dir):
    return _common.load_local(base_dir, OUT_FILE)
