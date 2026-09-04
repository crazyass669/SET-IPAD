# -*- coding: utf-8 -*-
"""core/us_store.py — ราคา OHLC ของสมาชิกดัชนี US (S&P 500 / Dow / Nasdaq 100) เก็บแยก
เป็น us_prices.db (คนละไฟล์กับ set_prices.db) เพราะ:
  1. กัน ticker ชนกัน — บางตัวย่อซ้ำกันข้ามตลาด (เช่น 'A' เป็นได้ทั้งหุ้นไทยและ Agilent US)
  2. set_prices.db โตแล้ว 4.4M+ แถว ไม่อยากบวมเพิ่มจากตลาดที่ query pattern ต่างกัน
schema/query logic ใช้ร่วมกับ core/hk_store.py ผ่าน core/store_factory.py (เดิมสอง
ไฟล์นี้ก็อปกันทุกบรรทัดยกเว้น DB_FILE — รวมไว้ที่เดียวกันโค้ดตกยุค)"""
from core.store_factory import make_store

DB_FILE = "us_prices.db"

_s = make_store(DB_FILE)
_connect = _s._connect   # detect_ca_mismatch reuse connection เดียวทั้งลูป (ดู services/refresh.py)
db_exists = _s.db_exists
init_db = _s.init_db
get_meta = _s.get_meta
get_last_dates = _s.get_last_dates
upsert_bars = _s.upsert_bars
get_closes_map = _s.get_closes_map
delete_ticker_bars = _s.delete_ticker_bars
get_ohlc_series = _s.get_ohlc_series
iter_all_series = _s.iter_all_series
iter_recent_series = _s.iter_recent_series
get_all_tickers = _s.get_all_tickers
