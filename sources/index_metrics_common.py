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
import time
from datetime import datetime

import pandas as pd

from core.metrics import rank_rs
from set_data_fetcher import process_stock, sanitize
from sources import financials_store
from sources.dr_universe import load_dr_universe


def _series(ohlc, key):
    if not ohlc or not ohlc.get(key):
        return None
    idx = pd.to_datetime(ohlc["dates"])
    return pd.Series(ohlc[key], index=idx, dtype=float)


def _raw_ticker(ticker):
    """ตัด suffix แบบ yfinance ('.HK'/'.T') ให้เหลือรหัสดิบ — ใช้ lookup namespace mirror
    'FINN:{ex}:{raw}' ใน financials.db (เก็บรหัสดิบไม่มี suffix เสมอ ดู _mirror_sym ใน app.py)
    US ไม่มี suffix อยู่แล้วคืนค่าเดิม"""
    for suf in (".HK", ".T"):
        if ticker.endswith(suf):
            return ticker[:-len(suf)]
    return ticker


def _compute_all_rows(base_dir, cfg, callback=None):
    """คำนวณ metrics ดิบ (ยังไม่ rank_rs) ของทุก ticker ในดัชนีตาม cfg — แยกออกจาก build()
    ให้ compute_ondemand_row() เรียกใช้ร่วมกันได้ (เติมหุ้นนอกดัชนี 1 ตัวแล้ว rank รวมกัน)
    กันเขียน loop คำนวณต่อ ticker ซ้ำ 2 ที่

    cfg keys: ดู docstring build()"""
    membership_local = cfg["membership"].load_local(base_dir)
    index_keys = cfg["index_keys"]
    sets = {k: set(membership_local.get(k, [])) for k in index_keys}

    sector_map = {}
    for k in cfg["sector_keys_order"]:
        sector_map.update(membership_local.get(k, {}))
    # industry_key — คีย์แยกต่างหากสำหรับ industry ละเอียดกว่า sector (เช่น HK ที่ดึงทั้ง
    # sector/industry จริงจาก yfinance — ดู hk_index_membership._fetch_sector_map) ถ้าไม่ตั้ง
    # (US/JP) industry จะเท่ากับ sector เหมือนเดิม
    industry_map = (membership_local.get(cfg["industry_key"], {})
                     if cfg.get("industry_key") else sector_map)
    if cfg.get("sector_fallback_dr"):
        for e in load_dr_universe(base_dir):
            yf_t = (e.get("yf") or "").upper()
            if yf_t.endswith(".HK") and e.get("ind"):
                sector_map.setdefault(yf_t, e["ind"])
                industry_map.setdefault(yf_t, e["ind"])

    # ชื่อบริษัทจริง — เอาจาก DR universe ที่ curate ไว้แล้ว ที่เหลือ fallback เป็น
    # ticker เฉยๆ (ไม่คุ้มไปดึงชื่อจาก Yahoo สดทีละตัวสำหรับแค่ title กราฟ)
    name_pred = cfg["name_map_predicate"]
    name_map = {}
    for e in load_dr_universe(base_dir):
        yf_t = (e.get("yf") or "").upper()
        if yf_t and name_pred(yf_t):
            name_map[yf_t] = e.get("name")

    # ตัวที่ dr_universe ไม่ได้ curate ไว้ (ส่วนใหญ่) — เติมชื่อจาก financials.db mirror
    # (namespace 'FINN:{market}:{raw}', payload Yahoo ที่ sync ไว้แล้วตอน Quick Update/
    # Index Max ผ่าน sync_mirror_yahoo_index) ไม่ต้องยิง Yahoo เพิ่ม เดิมปล่อยเป็น ticker
    # เฉยๆ ทำให้ตาราง/chart modal โชว์แค่รหัสหุ้นล้วนๆ ส่วนใหญ่ของ US/HK/JP
    mirror_names = financials_store.get_names_bulk(base_dir, f"FINN:{cfg['market_code']}:")

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
        name = name_map.get(ticker) or mirror_names.get(_raw_ticker(ticker)) or ticker
        info = {
            "symbol": ticker, "ticker": ticker, "name": name,
            "market": cfg["market_code"], "industry": industry_map.get(ticker, "Unknown"),
            "sector": sector_map.get(ticker, "Unknown"),
        }
        row = process_stock(info, close, volume, high=high, low=low)
        if row is None:
            continue
        for k in index_keys:
            row[f"in_{k.lower()}"] = ticker in sets[k]
        stocks.append(row)
    return stocks


_all_rows_cache = {}   # (base_dir, market_code) -> (ts, stocks) — TTL สั้น กัน compute_ondemand_row
                        # อ่าน+คำนวณ cohort ทั้งดัชนี (~500-600 ตัวจาก prices.db) ซ้ำทุกครั้งที่
                        # ผู้ใช้เปิดหุ้น mirror ตัวใหม่ติดกันในช่วงเวลาสั้นๆ (เช่นไล่เปิดทีละตัว
                        # ใน Screener+) — build() ไม่ใช้ cache นี้ (ต้อง fresh เสมอ รันเป็น
                        # background job แยกมี progress callback อยู่แล้ว)
_ALL_ROWS_TTL = 300   # วินาที — ราคาระหว่างวันขยับได้ แต่ 5 นาทีไม่กระทบ RS/stage/EMA200 อยู่แล้ว


def _compute_all_rows_cached(base_dir, cfg):
    """เหมือน _compute_all_rows แต่ cache ผลไว้ _ALL_ROWS_TTL วินาที — ใช้เฉพาะ path on-demand
    (compute_ondemand_row) คืน copy ของ list เสมอ (ไม่ใช่ reference คืน) กัน caller append
    หุ้น on-demand เข้าไปใน list ที่แชร์กับ call อื่นโดยไม่ตั้งใจ"""
    key = (base_dir, cfg["market_code"])
    now = time.time()
    cached = _all_rows_cache.get(key)
    if cached and (now - cached[0] < _ALL_ROWS_TTL):
        return list(cached[1])
    stocks = _compute_all_rows(base_dir, cfg)
    _all_rows_cache[key] = (now, stocks)
    return list(stocks)


def build(base_dir, cfg, callback=None, live_map=None):
    """คำนวณ metrics ทุกตัวในดัชนีตาม cfg แล้วเขียนทับ cfg['out_file'] คืนจำนวนตัวที่สำเร็จ

    cfg keys:
      membership, store   — module ของตลาดนั้น (เช่น us_index_membership, us_store)
      index_keys           — tuple ชื่อดัชนี เช่น ("SP500","DOW","NDX")
      sector_keys_order    — tuple key ของ sector map ใน membership local เรียงจาก
                             ทับก่อน->ทับทีหลัง (ตัวหลังชนะ — ดู cfg ของ US ที่ต้องให้
                             GICS (SP500/DOW) ทับ ICB (NDX) เพราะหุ้นเมกะแคปอยู่ทั้งคู่)
      industry_key         — (optional) key ของ industry map ใน membership local ที่ละเอียด
                             กว่า sector (เช่น HK ใช้ "industry" จาก yfinance คู่กับ "sector" —
                             ดู hk_index_metrics.py) ไม่ตั้ง = industry เท่ากับ sector (US/JP)
      sector_fallback_dr   — True ถ้าต้องเติม sector/industry จาก dr_universe (field 'ind')
                             สำหรับตัวที่ yfinance/Wikipedia ดึงไม่ได้ (ใช้ setdefault — ไม่ทับ
                             ค่าที่มีอยู่แล้ว)
      name_map_predicate   — fn(yf_ticker_upper) -> bool บอกว่า dr_universe entry นี้
                             เป็นของตลาดนี้หรือไม่ (ใช้เอาชื่อบริษัทมาเติม)
      market_code          — "US"/"HK" ฯลฯ ใส่ใน info['market']
      out_file             — path (relative to base_dir) ของ JSON output

    live_map — {ticker: {"live_price":, "live_chg":}} จาก _run_index_gap_update (app.py) —
    ราคาที่ยังไม่นิ่ง (pre-market/ระหว่างวัน) ที่ถูกตัดออกจาก <mkt>_prices.db ก่อนคำนวณ
    ret_1d ฯลฯ (เพื่อไม่ให้ history ถาวรมีแท่งที่ยังไหลอยู่) — merge เข้า stock row เป็น field
    เสริม live_price/live_chg ให้ Heatmap US/HK/JP โชว์ราคา Live ได้เหมือน DR/DRx"""
    stocks = _compute_all_rows(base_dir, cfg, callback)
    if live_map:
        for row in stocks:
            live = live_map.get(row["symbol"])
            if live:
                row["live_price"] = live["live_price"]
                row["live_chg"] = live["live_chg"]
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


def update_live_prices(base_dir, cfg, live_map, scope_tickers):
    """Merge live_price/live_chg เข้า cfg['out_file'] ที่มีอยู่แล้วตรงๆ โดยไม่คำนวณ
    RS/EMA/Stage/52W ใหม่ทั้งดัชนี — ใช้ตอนผู้ใช้กดปุ่ม Live ของ Heatmap ขณะดูแค่แท็บ
    ดัชนีย่อยเดียว (เช่น Dow 30 ตัว) ซึ่งเดิมเรียก build() เต็มรูปแบบทุกครั้ง (คำนวณทั้ง
    ~518 ตัวของ US แม้ gap-update มาแค่ 30 ตัว เพราะ RS percentile ต้อง rank เทียบกลุ่ม
    ทั้งหมด) ทำให้ปุ่มที่ควรเร็ว (แค่ 30 ตัว) กลับช้าเท่าดึงทั้งดัชนี — ตอนนี้ RS/EMA/Stage
    เต็มรูปแบบคำนวณเฉพาะตอน Quick Update/Index Max เท่านั้น (วันละครั้ง) ปุ่มนี้แค่อัพเดท
    ราคาที่โชว์บน Heatmap ให้สดขึ้น ไม่กระทบอันดับ/สถิติที่เหลือ

    scope_tickers — ticker ทั้งหมดที่ gap-update "เช็คแล้ว" รอบนี้ (ดู _run_index_gap_update)
    ตัวที่อยู่ใน scope แต่ไม่ติดใน live_map (ราคานิ่งแล้ว/ตลาดปิด/แท่งวันนี้ยังไม่ขึ้น) ต้อง
    เคลียร์ live_price/live_chg เก่าทิ้ง ไม่งั้นค้างราคาของรอบก่อน (อาจข้ามวัน) แสดงเป็น
    ราคา "สด" ปลอมๆ ต่อไปเรื่อยๆ — เดิมโค้ดนี้ merge เฉพาะที่มีใน live_map แต่ไม่เคลียร์ตัว
    ที่หลุด ทำให้กดปุ่ม Live ตอนตลาดปิด (live_map ว่าง) แล้ว live_price เก่าค้างอยู่ทั้งไฟล์

    คืน True ถ้า merge สำเร็จ (มีไฟล์เดิมอยู่แล้ว), False ถ้ายังไม่เคยมีไฟล์ (ต้องให้ build()
    เต็มรูปแบบรันก่อนอย่างน้อย 1 ครั้ง)"""
    path = os.path.join(base_dir, cfg["out_file"])
    try:
        with open(path, encoding="utf-8") as f:
            out = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    scope = set(scope_tickers or ())
    live_map = live_map or {}
    for row in out.get("stocks", []):
        sym = row.get("symbol")
        if sym not in scope:
            continue
        live = live_map.get(sym)
        if live:
            live = sanitize(live)  # กัน NaN/Infinity หลุดเข้าไฟล์ (out.get("stocks") ผ่าน build()
                                    # ที่ sanitize() ทั้งก้อนแล้ว แต่ path นี้ merge ตรงๆ ไม่ผ่าน build())
            row["live_price"] = live["live_price"]
            row["live_chg"] = live["live_chg"]
        else:
            row.pop("live_price", None)
            row.pop("live_chg", None)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    os.replace(tmp, path)
    _load_cache.pop((base_dir, cfg["out_file"]), None)
    return True


def compute_ondemand_row(base_dir, cfg, ticker, close, volume, high, low, name, sector, industry):
    """คำนวณ metrics (ราคา/return/RS/stage/EMA/ATR/sparkline) ของหุ้นตัวเดียวที่ไม่ใช่สมาชิก
    ดัชนีหลัก โดย rank RS เทียบกับสมาชิกดัชนีหลักทั้งหมดที่มีราคาอยู่แล้วใน <mkt>_prices.db
    (ไม่ fetch เพิ่มสำหรับสมาชิกเดิม, ticker ที่ขอมาไม่ได้เขียนทับ cfg['out_file'] — คำนวณสด
    ในหน่วยความจำเท่านั้น) ใช้ตอน on-demand fetch หุ้น mirror US/HK นอกดัชนีหลัก (ดู
    sources/mirror_ondemand.py) คืน None ถ้าราคาที่ให้มาไม่พอคำนวณ (< 5 แท่ง)"""
    stocks = _compute_all_rows_cached(base_dir, cfg)
    info = {
        "symbol": ticker, "ticker": ticker, "name": name or ticker,
        "market": cfg["market_code"], "industry": industry or sector or "Unknown",
        "sector": sector or "Unknown",
    }
    new_row = process_stock(info, close, volume, high=high, low=low)
    if new_row is None:
        return None
    for k in cfg["index_keys"]:
        new_row[f"in_{k.lower()}"] = False
    stocks.append(new_row)

    # ไม่เรียก validate_stocks() ตรงนี้ — build() (persisted path) ก็ไม่เรียกเหมือนกัน ต้องให้
    # ทั้งสอง path ไม่มี dq ติดตัวเหมือนกัน (rank_rs อ่าน s['dq']['rs_eligible'] แบบ default=True
    # เมื่อไม่มี dq เลย — ดู core/metrics.py::rank_rs) ไม่งั้น cohort ที่ใช้ rank percentile จะ
    # ไม่ตรงกัน (persisted rank บนทุกตัว, on-demand rank บนตัวที่ validate ผ่านเท่านั้น)
    stocks = rank_rs(stocks)
    result = next((s for s in stocks if s["symbol"] == ticker), None)
    return sanitize([result])[0] if result else None


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
