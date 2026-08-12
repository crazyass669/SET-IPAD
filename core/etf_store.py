# -*- coding: utf-8 -*-
"""core/etf_store.py — ราคา OHLC ของ ETF ที่จดทะเบียนบน SET โดยตรง เก็บแยกเป็น
etf_prices.db (คนละไฟล์กับ set_prices.db/us_prices.db/hk_prices.db/jp_prices.db) —
schema/query logic ใช้ร่วมกับตลาดอื่นผ่าน core/store_factory.py

ต่างจาก US/HK/JP (ที่อัพเดทรายวันผ่าน yfinance ล้วนเพราะไม่มี SET API ของตลาดต่างประเทศ)
ตรงที่ ETF อัพเดทรายวันหลักผ่าน SET API เหมือนหุ้นไทย (ดู app.py::_etf_price_update ที่
เทียบ pattern กับ services/refresh.py::run_quick_update) — ใช้ store เดียวกันเพราะ
schema (ticker/date/OHLC/adj_close/volume) เหมือนกันทุกตลาด ไม่ต้องเขียนใหม่"""
from core.store_factory import make_store

DB_FILE = "etf_prices.db"

_s = make_store(DB_FILE)
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
