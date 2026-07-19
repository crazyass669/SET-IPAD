# -*- coding: utf-8 -*-
"""sources/index_metrics_common.py — คำนวณ RS/EMA/Stage/52W ของสมาชิกดัชนีต่างประเทศ
(US: S&P500+Dow+NDX, HK: HSI+HSCEI+HSTECH) จากราคาที่เก็บไว้แล้วใน <market>_prices.db
(ไม่ยิง yfinance ซ้ำ) ใช้ร่วมกับ sources/us_index_metrics.py และ hk_index_metrics.py
ที่เป็น thin wrapper ส่ง cfg มาบอกความต่างเฉพาะตลาด (เดิมสองไฟล์นั้นก็อปกันเกือบทั้งหมด
ต่างแค่ sector merge order/name_map predicate — เพิ่มตลาดที่ 4 แค่เพิ่ม cfg ใหม่)

ใช้สูตรชุดเดียวกับหุ้นไทยทั้งหมดจาก set_data_fetcher.process_stock() + core.metrics.rank_rs()
(single source of truth — ห้าม copy สูตรมาคำนวณเองซ้ำ)"""
import json
import os
from datetime import datetime

import pandas as pd

from core.metrics import rank_rs
from set_data_fetcher import process_stock, sanitize
from sources.dr_universe import load_dr_universe


def _series(ohlc, key):
    if not ohlc or not ohlc.get(key):
        return None
    idx = pd.to_datetime(ohlc["dates"])
    return pd.Series(ohlc[key], index=idx, dtype=float)


def build(base_dir, cfg, callback=None):
    """คำนวณ metrics ทุกตัวในดัชนีตาม cfg แล้วเขียนทับ cfg['out_file'] คืนจำนวนตัวที่สำเร็จ

    cfg keys:
      membership, store   — module ของตลาดนั้น (เช่น us_index_membership, us_store)
      index_keys           — tuple ชื่อดัชนี เช่น ("SP500","DOW","NDX")
      sector_keys_order    — tuple key ของ sector map ใน membership local เรียงจาก
                             ทับก่อน->ทับทีหลัง (ตัวหลังชนะ — ดู cfg ของ US ที่ต้องให้
                             GICS (SP500/DOW) ทับ ICB (NDX) เพราะหุ้นเมกะแคปอยู่ทั้งคู่)
      sector_fallback_dr   — True ถ้าต้องเติม sector จาก dr_universe (field 'ind') สำหรับ
                             ตัวที่ยังไม่มี sector (HSTECH ไม่มี sector จาก Wikipedia)
      name_map_predicate   — fn(yf_ticker_upper) -> bool บอกว่า dr_universe entry นี้
                             เป็นของตลาดนี้หรือไม่ (ใช้เอาชื่อบริษัทมาเติม)
      market_code          — "US"/"HK" ฯลฯ ใส่ใน info['market']
      out_file             — path (relative to base_dir) ของ JSON output
    """
    membership_local = cfg["membership"].load_local(base_dir)
    index_keys = cfg["index_keys"]
    sets = {k: set(membership_local.get(k, [])) for k in index_keys}

    sector_map = {}
    for k in cfg["sector_keys_order"]:
        sector_map.update(membership_local.get(k, {}))
    if cfg.get("sector_fallback_dr"):
        for e in load_dr_universe(base_dir):
            yf_t = (e.get("yf") or "").upper()
            if yf_t.endswith(".HK") and yf_t not in sector_map and e.get("ind"):
                sector_map[yf_t] = e["ind"]

    # ชื่อบริษัทจริง — เอาจาก DR universe ที่ curate ไว้แล้ว ที่เหลือ fallback เป็น
    # ticker เฉยๆ (ไม่คุ้มไปดึงชื่อจาก Yahoo สดทีละตัวสำหรับแค่ title กราฟ)
    name_pred = cfg["name_map_predicate"]
    name_map = {}
    for e in load_dr_universe(base_dir):
        yf_t = (e.get("yf") or "").upper()
        if yf_t and name_pred(yf_t):
            name_map[yf_t] = e.get("name")

    store = cfg["store"]
    tickers = sorted(set().union(*sets.values()))
    # เปิด connection เดียวอ่านทุก ticker (iter_all_series) แทนเปิดทีละ connection ต่อตัว
    all_series = dict(store.iter_all_series(base_dir))

    stocks = []
    for i, ticker in enumerate(tickers):
        if callback and i % 50 == 0:
            callback(i, len(tickers), f"คำนวณ metrics {i}/{len(tickers)}...")
        ohlc = all_series.get(ticker)
        if not ohlc or len(ohlc.get("dates", [])) < 5:
            continue
        close = _series(ohlc, "closes")
        volume = _series(ohlc, "volumes")
        high = _series(ohlc, "highs")
        low = _series(ohlc, "lows")
        info = {
            "symbol": ticker, "ticker": ticker, "name": name_map.get(ticker, ticker),
            "market": cfg["market_code"], "industry": sector_map.get(ticker, "Unknown"),
            "sector": sector_map.get(ticker, "Unknown"),
        }
        row = process_stock(info, close, volume, high=high, low=low)
        if row is None:
            continue
        for k in index_keys:
            row[f"in_{k.lower()}"] = ticker in sets[k]
        stocks.append(row)

    stocks = rank_rs(stocks)

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total": len(stocks),
        "stocks": sanitize(stocks),
    }
    path = os.path.join(base_dir, cfg["out_file"])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    os.replace(tmp, path)
    return len(stocks)


_load_cache = {}   # (base_dir, out_file) -> (mtime, result) — กัน parse JSON ซ้ำทุก request
                   # (ดู pattern เดียวกันใน sources/dr_universe.py::_dr_universe_cache)


def load_local(base_dir, out_file):
    path = os.path.join(base_dir, out_file)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {"updated_at": None, "total": 0, "stocks": []}
    key = (base_dir, out_file)
    cached = _load_cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    with open(path, encoding="utf-8") as f:
        result = json.load(f)
    _load_cache[key] = (mtime, result)
    return result
