# -*- coding: utf-8 -*-
"""services/refresh.py — orchestration ของ Full Refresh / Quick Update

ประกอบ pipeline: sources (download) -> set_data_fetcher (calc) ->
core.metrics (validate/rank) -> core.store (persist)
"""
import json
import os
from datetime import datetime

import pandas as pd

from core.metrics import validate_stocks, rank_rs, summarize_groups
from core.store import (OUT_FILE, _atomic_write_json, _check_stock_count,
                        load_history, save_history)
from sources.yahoo import fetch_all_batch, fetch_gap_batch, fetch_market_caps_parallel
from set_data_fetcher import load_set_symbols, process_stock, sanitize


# ============================================================
# 5. run_with_progress — API สำหรับ Flask
# ============================================================

def run_with_progress(callback, base_dir=None, period="max"):
    """
    Full Refresh: ดาวน์โหลด history ทุกตัว บันทึก set_history.json + set_data.json
    period: "2y" | "5y" | "10y" | "max"
    callback(current: int, total: int, message: str)
    """
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    callback(0, 100, "กำลังอ่านรายชื่อหุ้น...")
    symbols = load_set_symbols(base_dir)
    total   = len(symbols)

    callback(0, total, f"พบ {total} หุ้น — เริ่ม batch download ({period} history)...")

    tickers  = [s["ticker"] for s in symbols]
    sym_map  = {s["ticker"]: s for s in symbols}
    all_data = fetch_all_batch(tickers, callback=callback, period=period)

    callback(total, total, f"บันทึก set_history.json ({len(all_data)} หุ้น)...")
    existing_hist = load_history(base_dir)
    save_history(all_data, base_dir, existing_hist=existing_hist)

    callback(total, total, f"ดาวน์โหลดเสร็จ — คำนวณ metrics ({len(all_data)}/{total} หุ้น)...")

    stocks = []
    for i, info_dict in enumerate(symbols):
        tick = info_dict["ticker"]
        d    = all_data.get(tick)
        if d is None:
            continue
        result = process_stock(info_dict, d["close"], d["volume"])
        if result:
            stocks.append(result)
        if i % 100 == 0:
            callback(i, total, f"คำนวณ {i}/{total}...")

    callback(0, total, f"ดึง Fundamentals ({len(stocks)} หุ้น) แบบ parallel...")
    cap_tickers = [s["ticker"] for s in stocks]
    try:
        fundamentals = fetch_market_caps_parallel(cap_tickers, callback=callback)
    except Exception as e:
        print(f"[Fundamentals] ดึงไม่สำเร็จ ({e}) — ข้ามไป ใช้ค่า None แทน")
        fundamentals = {}
    for s in stocks:
        fund = fundamentals.get(s["ticker"]) or {}
        s["mkt_cap"]   = fund.get("mkt_cap")
        s["pe"]        = fund.get("pe")
        s["pbv"]       = fund.get("pbv")
        s["div_yield"] = fund.get("div_yield")

    data_as_of = max(
        (d["close"].index[-1].strftime("%Y-%m-%d") for d in all_data.values() if len(d["close"]) > 0),
        default=None
    )

    callback(total, total, f"ตรวจสอบคุณภาพข้อมูล + คำนวณ RS Rank ({len(stocks)} หุ้น)...")
    dq_summary = validate_stocks(stocks, data_as_of)
    stocks     = rank_rs(stocks)

    industries = summarize_groups(stocks, "industry")
    sectors    = summarize_groups(stocks, "sector")

    output = {
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "update_type": "Full Refresh",
        "data_as_of":  data_as_of,
        "total":       len(stocks),
        "dq_summary":  dq_summary,
        "stocks":      stocks,
        "industries":  industries,
        "sectors":     sectors,
    }

    _check_stock_count(base_dir, len(stocks))
    out_path = os.path.join(base_dir, OUT_FILE)
    _atomic_write_json(out_path, sanitize(output))

    callback(total, total, f"บันทึกเสร็จ! {len(stocks)} หุ้น")


# ============================================================
# 6. run_quick_update — ดาวน์โหลดแค่วันที่ขาด แล้ว recalculate
# ============================================================

def run_quick_update(callback, base_dir=None):
    """
    Quick Update: โหลด set_history.json → download gap → recalculate metrics
    ไม่ดึง fundamentals (ใช้ค่าเดิม) → บันทึก set_history.json + set_data.json
    """
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    callback(0, 100, "โหลด set_history.json...")
    history = load_history(base_dir)
    if not history:
        raise ValueError("ไม่พบ set_history.json — กรุณา Full Refresh ก่อน")

    # หา last date ที่เก่าที่สุดในทุกหุ้น (เพื่อครอบคลุมหุ้นที่ตามหลัง)
    last_dates = [
        data["dates"][-1]
        for data in history["stocks"].values()
        if data.get("dates")
    ]
    if not last_dates:
        raise ValueError("ไม่มีข้อมูลใน history")

    min_last  = min(last_dates)
    start_dt  = pd.to_datetime(min_last)  # re-fetch วันล่าสุดเสมอ เผื่อดึงก่อนตลาดปิด
    today     = pd.Timestamp.now().normalize()

    if start_dt > today:
        callback(100, 100, "ข้อมูลเป็นปัจจุบันแล้ว ไม่มีวันใหม่")
        return

    start_date = start_dt.strftime("%Y-%m-%d")
    callback(0, 100, f"ดาวน์โหลดข้อมูลใหม่ตั้งแต่ {start_date}...")

    symbols = load_set_symbols(base_dir)
    total   = len(symbols)
    tickers = [s["ticker"] for s in symbols]

    new_data = fetch_gap_batch(tickers, start_date, callback=callback)
    if not new_data:
        callback(100, 100, "ไม่มีข้อมูลใหม่ (อาจเป็นวันหยุด)")
        return

    callback(total, total, f"Merge history ({len(new_data)} หุ้น มีข้อมูลใหม่)...")
    history = save_history(new_data, base_dir, existing_hist=history)

    callback(0, total, f"คำนวณ metrics ใหม่ ({total} หุ้น)...")
    stocks = []
    for i, info_dict in enumerate(symbols):
        tick      = info_dict["ticker"]
        hist_data = history["stocks"].get(tick)
        if not hist_data or not hist_data.get("dates"):
            continue
        try:
            dates  = pd.to_datetime(hist_data["dates"])
            close  = pd.Series(hist_data["closes"],  index=dates, dtype=float)
            volume = pd.Series(hist_data["volumes"], index=dates, dtype=float)
        except Exception:
            continue
        result = process_stock(info_dict, close, volume)
        if result:
            stocks.append(result)
        if i % 100 == 0:
            callback(i, total, f"คำนวณ {i}/{total}...")

    # คงค่า fundamentals เดิมไว้ (ไม่ดึงใหม่ใน Quick Update)
    existing_data_path = os.path.join(base_dir, OUT_FILE)
    if os.path.exists(existing_data_path):
        try:
            with open(existing_data_path, encoding="utf-8") as f:
                old = json.load(f)
            fund_map = {s["ticker"]: {k: s.get(k) for k in ("mkt_cap","pe","pbv","div_yield")}
                        for s in old.get("stocks", [])}
            for s in stocks:
                fund = fund_map.get(s["ticker"]) or {}
                for k in ("mkt_cap","pe","pbv","div_yield"):
                    s[k] = fund.get(k)
        except Exception:
            pass

    data_as_of = max(
        (data["dates"][-1] for data in history["stocks"].values() if data.get("dates")),
        default=None
    )

    callback(total, total, "ตรวจสอบคุณภาพข้อมูล + คำนวณ RS Rank...")
    dq_summary = validate_stocks(stocks, data_as_of)
    stocks     = rank_rs(stocks)
    industries = summarize_groups(stocks, "industry")
    sectors    = summarize_groups(stocks, "sector")

    output = {
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "update_type": "Quick Update",
        "data_as_of":  data_as_of,
        "total":       len(stocks),
        "dq_summary":  dq_summary,
        "stocks":      stocks,
        "industries":  industries,
        "sectors":     sectors,
    }
    _check_stock_count(base_dir, len(stocks))
    out_path = os.path.join(base_dir, OUT_FILE)
    _atomic_write_json(out_path, sanitize(output))

    callback(total, total,
             f"Quick Update เสร็จ! {len(stocks)} หุ้น (ดาวน์โหลดใหม่ {len(new_data)} หุ้น)")


