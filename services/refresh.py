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
from core.store import (OUT_FILE, DUAL_WRITE_JSON, _atomic_write_json,
                        _check_stock_count, get_closes_map, get_last_dates,
                        iter_all_series, load_history, save_history)
from sources.yahoo import fetch_all_batch, fetch_gap_batch, fetch_market_caps_parallel
from services.rotation import update_rotation_state
from set_data_fetcher import load_set_symbols, process_stock, sanitize


def detect_ca_mismatch(base_dir, new_data, tol=0.005, min_bad=2):
    """Split detector: เทียบราคาแท่ง overlap (ที่ดึงมาใหม่ vs ที่เก็บไว้)
    ถ้า Yahoo เพิ่งปรับราคาย้อนหลัง (แตกพาร์/รวมพาร์/ปันผลเป็นหุ้น) แท่งเดิม
    จะไม่ตรงกับที่เก็บไว้ทั้งแถบ — คืน list ของ ticker ที่ต้อง refetch เต็ม

    min_bad=2: ต้องเพี้ยนอย่างน้อย 2 แท่ง กัน false positive จากการแก้ข้อมูล
    จุดเดียว/ความต่างจากการปัดเศษ"""
    suspects = []
    for tick, d in new_data.items():
        try:
            dates  = [x.strftime("%Y-%m-%d") for x in d["close"].index]
            if len(dates) < 2:
                continue
            closes = {dt: float(c) for dt, c in zip(dates, d["close"])}
            # ตัดแท่งล่าสุดออก (วันนี้เป็นราคาใหม่จริง ไม่ใช่ overlap)
            stored = get_closes_map(base_dir, tick, dates[:-1])
            bad = 0
            for dt, sc in stored.items():
                nc = closes.get(dt)
                if nc is None or sc is None or sc <= 0:
                    continue
                if abs(nc - sc) / sc > tol:
                    bad += 1
                    if bad >= min_bad:
                        suspects.append(tick)
                        break
        except Exception:
            continue
    return suspects


MAX_CA_REPAIR = 30   # เพดานซ่อมต่อรอบ — mismatch เกินนี้ = ผิดปกติทั้งกระดาน


def _repair_ca_tickers(base_dir, new_data, suspects, callback):
    """refetch เต็ม (period=max) เฉพาะหุ้นที่ตรวจพบ CA — แทนข้อมูลใน new_data
    คืน set ของ ticker ที่ซ่อมสำเร็จ (ให้ save_history replace ทั้ง series)"""
    if len(suspects) > MAX_CA_REPAIR:
        print(f"[CA] mismatch {len(suspects)} ตัว — มากผิดปกติ "
              f"(แหล่งข้อมูลอาจเปลี่ยนฐานทั้งกระดาน?) ซ่อมรอบนี้ {MAX_CA_REPAIR} ตัวแรก "
              f"ที่เหลือรอรอบถัดไป")
    repaired = set()
    for i, tick in enumerate(suspects[:MAX_CA_REPAIR]):
        callback(i, len(suspects[:MAX_CA_REPAIR]),
                 f"พบ corporate action — refetch เต็ม {tick} ({i+1}/{min(len(suspects), MAX_CA_REPAIR)})...")
        try:
            full = fetch_all_batch([tick], period="max")
            fd = full.get(tick)
            # sanity: ข้อมูลใหม่ต้องยาวพอ ไม่ใช่ response ขาดๆ
            if fd is not None and len(fd["close"]) >= 100:
                new_data[tick] = fd
                repaired.add(tick)
                print(f"[CA] repaired {tick}: refetch {len(fd['close'])} แท่ง")
            else:
                print(f"[CA] {tick}: refetch ได้ข้อมูลสั้นผิดปกติ — ข้าม (คงข้อมูลเดิม)")
        except Exception as e:
            print(f"[CA] {tick}: refetch ล้มเหลว ({e}) — ข้าม")
    return repaired


def _update_rotation_safe(base_dir, data_as_of, sectors, industries):
    """quadrant alert ห้ามทำ refresh ล่ม — ดัก error แล้ว log อย่างเดียว"""
    try:
        update_rotation_state(base_dir, data_as_of, sectors, industries)
    except Exception as e:
        print(f"[Rotation] update state failed: {e}")


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

    callback(total, total, f"บันทึกราคา ({len(all_data)} หุ้น)...")
    existing_hist = load_history(base_dir) if DUAL_WRITE_JSON else None
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

    callback(0, total, f"ดึง Fundamentals ({len(stocks)} หุ้น)...")
    cap_tickers = [s["ticker"] for s in stocks]
    # Primary: SET API (เร็ว ~20 วิ + ข้อมูลจากเจ้าของตลาด) -> fallback: Yahoo
    fundamentals = {}
    try:
        from sources.set_api import fetch_fundamentals
        fundamentals = fetch_fundamentals(cap_tickers, callback=callback)
        print(f"[Fundamentals] SET API: {len(fundamentals)}/{len(cap_tickers)} ตัว")
    except Exception as e:
        print(f"[Fundamentals] SET API ล้มเหลว ({e}) — fallback Yahoo...")
        callback(0, total, f"Fundamentals fallback Yahoo ({len(stocks)} หุ้น)...")
        try:
            fundamentals = fetch_market_caps_parallel(cap_tickers, callback=callback)
        except Exception as e2:
            print(f"[Fundamentals] Yahoo ก็ล้มเหลว ({e2}) — ใช้ค่า None แทน")
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
    _update_rotation_safe(base_dir, data_as_of, sectors, industries)

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

    # หา last date ที่เก่าที่สุดในทุกหุ้น (เพื่อครอบคลุมหุ้นที่ตามหลัง)
    # query จาก SQLite ตรงๆ — ไม่ต้อง parse JSON 98MB อีก
    callback(0, 100, "ตรวจสอบวันล่าสุดของข้อมูล...")
    last_map = get_last_dates(base_dir)
    if not last_map:
        raise ValueError("ไม่พบข้อมูลราคา (set_prices.db) — กรุณา Full Refresh ก่อน")

    min_last  = min(last_map.values())
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

    # ── Split detector: เทียบแท่ง overlap ก่อนบันทึก — ถ้า Yahoo เพิ่งปรับ
    # ราคาย้อนหลัง (แตกพาร์ ฯลฯ) ต้อง refetch เต็มเฉพาะตัว ไม่งั้น series
    # จะเป็นฐานเก่าต่อฐานใหม่ → return/RS ปลอม
    callback(total, total, "ตรวจ corporate action (เทียบราคาแท่ง overlap)...")
    suspects = detect_ca_mismatch(base_dir, new_data)
    repaired = set()
    if suspects:
        print(f"[CA] พบ overlap mismatch: {suspects[:MAX_CA_REPAIR]}")
        repaired = _repair_ca_tickers(base_dir, new_data, suspects, callback)

    callback(total, total, f"บันทึกราคา ({len(new_data)} หุ้น มีข้อมูลใหม่"
             + (f", replace {len(repaired)} ตัวจาก CA" if repaired else "") + ")...")
    existing_hist = load_history(base_dir) if DUAL_WRITE_JSON else None
    save_history(new_data, base_dir, existing_hist=existing_hist,
                 replace_tickers=repaired)

    callback(0, total, f"คำนวณ metrics ใหม่ ({total} หุ้น)...")
    sym_map = {s["ticker"]: s for s in symbols}
    stocks  = []
    done    = 0
    # stream จาก SQLite ทีละหุ้น — ไม่ต้องถือ history ทั้งก้อนใน RAM
    for tick, hist_data in iter_all_series(base_dir):
        info_dict = sym_map.get(tick)
        if not info_dict or not hist_data.get("dates"):
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
        done += 1
        if done % 100 == 0:
            callback(done, total, f"คำนวณ {done}/{total}...")

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

    data_as_of = max(get_last_dates(base_dir).values(), default=None)

    callback(total, total, "ตรวจสอบคุณภาพข้อมูล + คำนวณ RS Rank...")
    dq_summary = validate_stocks(stocks, data_as_of)
    stocks     = rank_rs(stocks)
    industries = summarize_groups(stocks, "industry")
    sectors    = summarize_groups(stocks, "sector")
    _update_rotation_safe(base_dir, data_as_of, sectors, industries)

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


