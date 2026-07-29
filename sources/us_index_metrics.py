# -*- coding: utf-8 -*-
"""sources/us_index_metrics.py — RS/EMA/Stage/52W ของสมาชิกดัชนี S&P 500 + Dow + Nasdaq 100
เก็บผลลง data/us_index_metrics.json ให้เมนูสายเทคนิคอ่านต่อได้โดยไม่ต้องคำนวณสดทุกครั้ง
logic จริงอยู่ที่ sources/index_metrics_common.py (ใช้ร่วมกับ hk_index_metrics.py) — ไฟล์
นี้เหลือแค่ cfg เฉพาะตลาด US"""
import os

from core import us_store
from sources import index_metrics_common as _common
from sources import us_index_membership

OUT_FILE = os.path.join("data", "us_index_metrics.json")

_CFG = dict(
    membership=us_index_membership,
    store=us_store,
    index_keys=us_index_membership.INDEXES,   # ("SP500","DOW","NDX")
    # ลำดับสำคัญ: SP500/DOW ใช้ GICS (มาตรฐานสากล) ส่วน NDX ใช้ ICB (parse จาก Wikipedia
    # หน้า Nasdaq-100) — ต้องให้ GICS ทับทีหลังเสมอ ไม่งั้นหุ้นเมกะแคปที่อยู่ทั้ง S&P 500
    # และ NDX (AAPL/MSFT/GOOGL/NVDA ฯลฯ ~88 ตัว) จะโดน ICB ทับ GICS กลายเป็น sector คนละ
    # มาตรฐานกับหุ้น S&P 500 ตัวอื่น ทำให้ Sector Ranking/filter/Rotation จัดกลุ่มผิดที่
    sector_keys_order=("NDX_sector", "DOW_sector", "SP500_sector"),
    sector_fallback_dr=False,
    # ticker US ใน dr_universe เป็น yf ticker บริสุทธิ์ไม่มี "." (BRK-B ใช้ขีดกลางแทนจุด)
    name_map_predicate=lambda yf_t: "." not in yf_t,
    market_code="US",
    out_file=OUT_FILE,
)


def build(base_dir, callback=None, live_map=None):
    return _common.build(base_dir, _CFG, callback, live_map=live_map)


def update_live_prices(base_dir, live_map):
    return _common.update_live_prices(base_dir, _CFG, live_map)


def load_local(base_dir):
    return _common.load_local(base_dir, OUT_FILE)
