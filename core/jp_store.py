# -*- coding: utf-8 -*-
"""core/jp_store.py — ราคา OHLC ของสมาชิกดัชนี JP (Nikkei 225) เก็บแยกเป็น jp_prices.db
(คนละไฟล์กับ set_prices.db/us_prices.db/hk_prices.db) เหตุผลเดียวกับ core/hk_store.py
(กัน ticker ชนกัน + กัน DB หลักบวม) schema/query logic ใช้ร่วมกับ core/us_store.py,
core/hk_store.py ผ่าน core/store_factory.py"""
from core.store_factory import make_store

DB_FILE = "jp_prices.db"

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
