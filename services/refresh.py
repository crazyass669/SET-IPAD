# -*- coding: utf-8 -*-
"""services/refresh.py — orchestration ของ Full Refresh / Quick Update

ประกอบ pipeline: sources (download) -> set_data_fetcher (calc) ->
core.metrics (validate/rank) -> core.store (persist)
"""
import json
import logging
import os
from collections import Counter
from datetime import datetime

import pandas as pd

from core import store as _default_ca_store
from core.metrics import validate_stocks, rank_rs, summarize_groups
from core.store import (OUT_FILE, DUAL_WRITE_JSON, _atomic_write_json,
                        _check_stock_count, get_closes_map, get_closes_on_date,
                        get_last_dates, iter_all_series, iter_recent_series,
                        load_history, save_history)
from sources.yahoo import fetch_all_batch, fetch_gap_batch, fetch_market_caps_parallel
from services.rotation import update_rotation_state
from set_data_fetcher import load_set_symbols, process_stock, sanitize


def detect_ca_mismatch(base_dir, new_data, tol=0.005, min_bad=2, store=None):
    """Split detector: เทียบราคาแท่ง overlap (ที่ดึงมาใหม่ vs ที่เก็บไว้)
    ถ้า Yahoo เพิ่งปรับราคาย้อนหลัง (แตกพาร์/รวมพาร์/ปันผลเป็นหุ้น) แท่งเดิม
    จะไม่ตรงกับที่เก็บไว้ทั้งแถบ — คืน list ของ ticker ที่ต้อง refetch เต็ม

    min_bad=2: ต้องเพี้ยนอย่างน้อย 2 แท่ง กัน false positive จากการแก้ข้อมูล
    จุดเดียว/ความต่างจากการปัดเศษ

    store=None → core.store (หุ้นไทย, set_prices.db) — ส่ง core.us_store/core.hk_store
    เข้ามาแทนเพื่อใช้ split detector ตัวเดียวกันกับหุ้น US/HK (โครง get_closes_map เหมือนกัน)"""
    store = store or _default_ca_store
    suspects = []
    for tick, d in new_data.items():
        try:
            dates  = [x.strftime("%Y-%m-%d") for x in d["close"].index]
            if len(dates) < 2:
                continue
            closes = {dt: float(c) for dt, c in zip(dates, d["close"])}
            # ตัดแท่งล่าสุดออก (วันนี้เป็นราคาใหม่จริง ไม่ใช่ overlap)
            stored = store.get_closes_map(base_dir, tick, dates[:-1])
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


def detect_flat_price_tickers(base_dir, days=14):
    """หา ticker ที่ราคาปิด `days` แท่งล่าสุด (default 14 วันเทรด) เท่ากันเป๊ะทุกแท่ง —
    สัญญาณว่าอาจโดนเครื่องหมาย SP/NC/NP/H พักเทรดอยู่ แต่แหล่งราคาที่ใช้เติมข้อมูล (เช่น
    chart-quotation fallback ของ SET.or.th) ไม่มี field เครื่องหมายให้เช็คตรงๆ เลยได้ราคา
    อ้างอิงที่ค้างนิ่งมาเป็นแท่งใหม่ทุกวันโดยไม่รู้ตัว (เจอจริงกับ WELL 2569-08 — ราคา 0.71
    นิ่งมากกว่าเดือนระหว่างโดน SP/NC/NP พร้อมกัน) คืน list ticker ที่ต้องสงสัยเฉยๆ (ยังไม่เช็ค
    เครื่องหมายจริง — ต่อด้วย sources.set_api.fetch_signs_batch)

    ใช้ iter_recent_series (query ตาม clustered index ticker+date) แทน iter_all_series
    เต็มตาราง — เร็วกว่ามากตามที่ core/store.py คอมเมนต์ไว้ (~2 วิ ทั้งกระดาน)"""
    suspects = []
    for t, s in iter_recent_series(base_dir, days):
        closes = [c for c in s["closes"] if c is not None]
        if len(closes) >= days and len(set(closes)) == 1:
            suspects.append(t)
    return suspects


MAX_CA_REPAIR = 30   # เพดานซ่อมต่อรอบ — mismatch เกินนี้ = ผิดปกติทั้งกระดาน


def _repair_ca_tickers(base_dir, new_data, suspects, callback):
    """refetch เต็ม (period=max) เฉพาะหุ้นที่ตรวจพบ CA — แทนข้อมูลใน new_data
    คืน set ของ ticker ที่ซ่อมสำเร็จ (ผู้เรียกต้อง delete ticker เดิมออกก่อน upsert
    ทั้ง series ใหม่ — ดู save_history's replace_tickers ของหุ้นไทย หรือ
    delete_ticker_bars + upsert_bars ตรงๆ ของ US/HK gap-update)"""
    if len(suspects) > MAX_CA_REPAIR:
        logging.warning(f"[CA] mismatch {len(suspects)} ตัว — มากผิดปกติ "
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
                logging.info(f"[CA] repaired {tick}: refetch {len(fd['close'])} แท่ง")
            else:
                logging.warning(f"[CA] {tick}: refetch ได้ข้อมูลสั้นผิดปกติ — ข้าม (คงข้อมูลเดิม)")
        except Exception as e:
            logging.warning(f"[CA] {tick}: refetch ล้มเหลว ({e}) — ข้าม")
    return repaired


def _update_rotation_safe(base_dir, data_as_of, sectors, industries):
    """quadrant alert ห้ามทำ refresh ล่ม — ดัก error แล้ว log อย่างเดียว"""
    try:
        update_rotation_state(base_dir, data_as_of, sectors, industries)
    except Exception as e:
        logging.warning(f"[Rotation] update state failed: {e}")


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
    all_data = fetch_all_batch(tickers, callback=callback, period=period)

    # ── Split detector: ถ้าเลือก period สั้นกว่า Max แล้วมี corporate action
    # (แตกพาร์ ฯลฯ) เกิดขึ้นภายในช่วงที่ดึงมา แท่งเก่าที่เคยบันทึกไว้ (จาก Quick
    # Update ก่อนหน้า) จะยังเป็นฐานราคาก่อนแตกพาร์ ต้อง refetch เต็ม (period=max)
    # เฉพาะตัวที่เพี้ยน ไม่งั้น INSERT OR REPLACE จะเขียนทับด้วยฐานใหม่บางส่วน
    # แล้วทิ้งรอยต่อฐานเก่า/ใหม่ไว้ถาวร (ดู _repair_ca_tickers)
    callback(total, total, "ตรวจ corporate action (เทียบราคาแท่ง overlap)...")
    suspects = detect_ca_mismatch(base_dir, all_data)
    repaired = set()
    if suspects:
        logging.info(f"[CA] Full Refresh พบ overlap mismatch: {suspects[:MAX_CA_REPAIR]}")
        repaired = _repair_ca_tickers(base_dir, all_data, suspects, callback)
        unrepaired = set(suspects) - repaired
        if unrepaired:
            logging.warning(f"[CA] Full Refresh {len(unrepaired)} ตัวซ่อมไม่สำเร็จ — "
                  f"บันทึกด้วยข้อมูลที่ดึงมาได้ตามปกติ (รอรอบถัดไป retry): {sorted(unrepaired)}")

    callback(total, total, f"บันทึกราคา ({len(all_data)} หุ้น)...")
    existing_hist = load_history(base_dir) if DUAL_WRITE_JSON else None
    save_history(all_data, base_dir, existing_hist=existing_hist, replace_tickers=repaired)

    # คำนวณ metrics จาก SQLite ทั้งฐาน (ไม่ใช่แค่ all_data ที่จำกัดตาม period ที่เลือก)
    # — ตัวที่ดาวน์โหลดพลาดรอบนี้ (แต่เคยมีอยู่ใน DB) จะไม่หายจากหน้าจอ และ ATH/
    # Return ระยะยาวยังคำนวณจากประวัติเต็มเสมอ ไม่ว่าจะเลือก period ไหนตอน refresh
    callback(0, total, f"คำนวณ metrics ({total} หุ้น)...")
    sym_map = {s["ticker"]: s for s in symbols}
    stocks  = []
    done    = 0
    for tick, hist_data in iter_all_series(base_dir):
        info_dict = sym_map.get(tick)
        if not info_dict or not hist_data.get("dates"):
            continue
        try:
            dates  = pd.to_datetime(hist_data["dates"])
            close  = pd.Series(hist_data["closes"],  index=dates, dtype=float)
            volume = pd.Series(hist_data["volumes"], index=dates, dtype=float)
            high = pd.Series(hist_data["highs"], index=dates, dtype=float) if hist_data.get("highs") else None
            low  = pd.Series(hist_data["lows"],  index=dates, dtype=float) if hist_data.get("lows") else None
        except Exception:
            continue
        result = process_stock(info_dict, close, volume, high=high, low=low)
        if result:
            stocks.append(result)
        done += 1
        if done % 100 == 0:
            callback(done, total, f"คำนวณ {done}/{total}...")

    callback(0, total, f"ดึง Fundamentals ({len(stocks)} หุ้น)...")
    cap_tickers = [s["ticker"] for s in stocks]
    # Primary: SET API (เร็ว ~20 วิ + ข้อมูลจากเจ้าของตลาด) -> fallback: Yahoo
    fundamentals = {}
    try:
        from sources.set_api import fetch_fundamentals
        fundamentals = fetch_fundamentals(cap_tickers, callback=callback)
        logging.info(f"[Fundamentals] SET API: {len(fundamentals)}/{len(cap_tickers)} ตัว")
    except Exception as e:
        logging.warning(f"[Fundamentals] SET API ล้มเหลว ({e}) — fallback Yahoo...")
        callback(0, total, f"Fundamentals fallback Yahoo ({len(stocks)} หุ้น)...")
        try:
            fundamentals = fetch_market_caps_parallel(cap_tickers, callback=callback)
        except Exception as e2:
            logging.warning(f"[Fundamentals] Yahoo ก็ล้มเหลว ({e2}) — ใช้ค่า None แทน")
            fundamentals = {}
    for s in stocks:
        fund = fundamentals.get(s["ticker"]) or {}
        s["mkt_cap"]   = fund.get("mkt_cap")
        s["pe"]        = fund.get("pe")
        s["pbv"]       = fund.get("pbv")
        s["div_yield"] = fund.get("div_yield")

    data_as_of = max(get_last_dates(base_dir).values(), default=None)

    callback(total, total, f"ตรวจสอบคุณภาพข้อมูล + คำนวณ RS Rank ({len(stocks)} หุ้น)...")
    dq_summary = validate_stocks(stocks, data_as_of)
    stocks     = rank_rs(stocks)

    # เติมสถิติฤดูกาล "เดือนปัจจุบัน" (seas_ret/seas_hit) สำหรับ screener — non-critical
    try:
        from sources.price_analytics import annotate_seasonality
        annotate_seasonality(base_dir, stocks)
    except Exception as e:
        logging.warning(f"[Seasonality] ข้าม: {e}")

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

    symbols = load_set_symbols(base_dir)
    total   = len(symbols)
    tickers = [s["ticker"] for s in symbols]

    # ตัดหุ้นที่เพิกถอน/ควบรวมออกจาก set_prices.db ไปแล้ว (ไม่อยู่ใน symbols ล่าสุดจาก SET
    # แต่ยังมีแถวเก่าค้างอยู่ใน DB เก็บไว้ backtest — เช่น BPP.BK ควบรวมเข้า BANPU) ออกจาก
    # last_map ก่อนคำนวณ min_last/stale_tickers ไม่งั้นหุ้นตัวเดียวที่หลุด universe ไปแล้ว
    # จะลาก min_last ให้ไม่เท่า max_last ตลอดกาล (จนกว่าจะครบ 14 วันเข้าเกณฑ์ stale_cut เอง)
    # ทำให้ SET API fast path ด้านล่างถูกข้ามทั้งกระดานทุกรอบทั้งที่หุ้นอื่นพร้อม sync จริง
    ticker_set = set(tickers)
    delisted_in_db = {t for t in last_map if t not in ticker_set}
    if delisted_in_db:
        logging.info(f"[QuickUpdate] ตัด {len(delisted_in_db)} ticker ที่หลุด universe ปัจจุบันแล้ว "
              f"ออกจากการหา start_date: {sorted(delisted_in_db)[:10]}")
        last_map = {t: d for t, d in last_map.items() if t in ticker_set}
    if not last_map:
        raise ValueError("ไม่พบหุ้นที่ตรงกับ universe ปัจจุบันเลยใน set_prices.db")

    # หุ้นเข้าใหม่ที่ไม่เคยมีราคาใน set_prices.db มาก่อนเลย (เพิ่งเข้าตลาดจริง) —
    # ไม่อยู่ใน last_map เลยไม่โดนนับใน active_map/stale_tickers ด้านล่าง ถ้าไม่กันไว้ตรงนี้
    # SET API fast path (quote_tickers/remaining ผูกกับ active_map ล้วน) จะข้ามตัวพวกนี้
    # เงียบๆ ทุกรอบ ทั้งที่ fast path สำเร็จปกติ (เจอจริง 2569-08-13 กรณี WELL เข้า mai
    # ใหม่ กด Quick Update แล้วไม่โผล่) ต้องไล่ผ่าน Yahoo แทนเสมอ (ดูจุดใช้ด้านล่าง)
    never_fetched = ticker_set - set(last_map.keys())
    if never_fetched:
        logging.info(f"[QuickUpdate] หุ้นเข้าใหม่ยังไม่เคยมีราคาในเครื่อง {len(never_fetched)} ตัว: "
              f"{sorted(never_fetched)[:10]}")

    # ตัดหุ้นค้างนาน (พักเทรด/เพิกถอน เช่น ACAP ค้างเป็นเดือน) ออกจากการหา
    # start_date — ไม่งั้นตัวเดียวลากให้ดาวน์โหลดย้อนหลังหลายเดือนทั้งกระดานทุกรอบ
    # หุ้นพวกนี้ไม่มีแท่งใหม่อยู่แล้ว การเริ่มดึงจากวันใหม่ไม่ทำให้ข้อมูลมันเสีย
    max_last     = pd.to_datetime(max(last_map.values()))
    stale_cut    = max_last - pd.Timedelta(days=14)
    stale_tickers = {t for t, d in last_map.items() if pd.to_datetime(d) < stale_cut}
    active_map   = {t: d for t, d in last_map.items() if t not in stale_tickers}
    if stale_tickers:
        logging.info(f"[QuickUpdate] ข้ามหุ้นค้างนาน {len(stale_tickers)} ตัว (วันล่าสุดเก่ากว่า {stale_cut.date()})")

    min_last  = min(active_map.values())
    start_dt  = pd.to_datetime(min_last)  # re-fetch วันล่าสุดเสมอ เผื่อดึงก่อนตลาดปิด
    today     = pd.Timestamp.now().normalize()

    if start_dt > today:
        callback(100, 100, "ข้อมูลเป็นปัจจุบันแล้ว ไม่มีวันใหม่")
        return

    start_date = start_dt.strftime("%Y-%m-%d")

    # วันที่ที่หุ้น active ส่วนใหญ่ sync ตรงกัน (mode) — ใช้แทนการบังคับให้ทุกตัวต้องตรงกัน
    # เป๊ะ 100% (min_last == max_last แบบเดิม) เจอเคสจริง 12 ส.ค. 2569: มีหุ้นหยิบมือ
    # (CTARAF/ISSARA/L&E/MJLF) sync ล้ำหน้าหุ้นอื่นทั้งกระดานไปก่อนผ่านช่องทางอื่น ถ้าบังคับ
    # ตรงกันเป๊ะ หุ้นล้ำหน้ากลุ่มนี้จะบล็อก fast-path ทั้งกระดานทุกรอบทั้งที่ไม่มีปัญหาจริง —
    # sanity check (เทียบ prior กับราคาปิดที่เก็บไว้) ด้านล่างกรองหุ้นที่ไม่ตรง mode ออกเอง
    # อยู่แล้ว ต้องมีอย่างน้อย 90% ของหุ้น active sync ตรง mode ถึงจะเชื่อว่าตลาดปิดสนิทจริง
    # พอลอง fast path (กันกรณี sync กระจัดกระจายทั้งกระดานจริงๆ ซึ่งควร fallback Yahoo เต็ม)
    date_counts = Counter(active_map.values())
    mode_date, mode_n = date_counts.most_common(1)[0]
    sync_ratio = mode_n / len(active_map)

    # ── Fast path: SET internal API (list-by-symbols) — ราคาล่าสุดทั้งกระดาน
    # นัดเดียว (<1 วิ) แทน Yahoo ที่มัก lag ปล่อยแท่งปิดหุ้นไทยช้าเป็นชั่วโมง เพราะ endpoint
    # นี้ไม่มี field วันที่ตรงๆ ในตัวเอง (marketDateTime คือเวลา query ไม่ใช่วันเทรด —
    # ยืนยันแล้วว่าต่างกันได้เมื่อ query ก่อนตลาดเปิด/วันหยุด) ให้แค่ snapshot วันล่าสุดวันเดียว
    # (ไม่มี historical range เลย) — ปลอดภัยเฉพาะกรณี mode_date ขาดจริงๆ แค่ 1 วันเทรด เช็ค
    # ด้วยปฏิทินวันเทรดจริงจาก SET เอง (fetch_trading_calendar_tail คืนวันเทรดล่าสุด 2 วัน)
    # ว่า "วันก่อนวันล่าสุด" ตรงกับ mode_date ไหม — ถ้าไม่ตรง (mode_date ค้างเกิน 1 วันเทรด
    # เช่น ไม่ได้กด Quick Update มาหลายวัน) ต้องข้าม fast path ทั้งกลุ่ม ไม่งั้นจะเติมแค่วัน
    # asof แล้วข้ามวันตรงกลางไปเงียบๆ ถาวร (sanity check prior ด้านล่างช่วยกันอีกชั้น แต่ไม่
    # 100% กับหุ้นที่ราคานิ่ง/ไม่มีการเทรดในช่วงที่ขาด — ต้องมีเช็ค deterministic นี้ด้วย)
    new_data = None
    if sync_ratio >= 0.9:
        try:
            from sources.set_api import fetch_quotes_batch, fetch_trading_calendar_tail
            callback(0, total, "เช็คปฏิทินวันเทรดล่าสุด (SET API)...")
            cal = fetch_trading_calendar_tail()
            asof = cal[-1]
            prev_trading_date = cal[-2] if len(cal) >= 2 else None
            if asof > mode_date and prev_trading_date == mode_date:
                quote_tickers = [t for t, d in active_map.items() if d == mode_date]
                callback(0, total, f"ดึงราคาล่าสุด {asof} ({len(quote_tickers)} หุ้น, SET API)...")
                quotes = fetch_quotes_batch(quote_tickers, callback=callback)
                stored_close = get_closes_on_date(base_dir, mode_date)
                idx = pd.DatetimeIndex([pd.Timestamp(asof)])
                fast_data, mismatched = {}, 0
                for tick, q in quotes.items():
                    sc = stored_close.get(tick)
                    # sanity: prior ต้องตรงราคาปิดที่เก็บไว้จริง — กัน SET API คืน
                    # ข้อมูลเพี้ยน/คนละวัน ไม่งั้นจะเขียนแท่งผิดฐานราคาลง DB ถาวร
                    if sc and q["prior"] is not None and abs(q["prior"] - sc) / sc > 0.005:
                        mismatched += 1
                        continue
                    fast_data[tick] = {
                        "open":   pd.Series([q["open"]],   index=idx),
                        "high":   pd.Series([q["high"]],   index=idx),
                        "low":    pd.Series([q["low"]],    index=idx),
                        "close":  pd.Series([q["close"]],  index=idx),
                        "volume": pd.Series([q["volume"]], index=idx),
                    }
                if mismatched:
                    logging.info(f"[QuickUpdate] SET API: prior ไม่ตรงราคาปิดเดิม ข้าม {mismatched} ตัว")
                if fast_data:
                    new_data = fast_data
                    logging.info(f"[QuickUpdate] SET API fast path: {len(new_data)}/{len(quote_tickers)} "
                          f"หุ้น (asof={asof})")
                    # หุ้นค้างนาน (stale) + หุ้นที่ sync ไม่ตรง mode (ตามหลังอยู่ แต่ยังไม่ทัน
                    # เกณฑ์ 14 วัน) + หุ้นเข้าใหม่ที่ไม่เคยมีราคาเลย — เช็คแยกผ่าน Yahoo
                    # (list สั้น) ว่ามีข้อมูลใหม่ไหม (never_fetched คำนวณไว้ด้านบนสุดของฟังก์ชัน)
                    remaining = (stale_tickers | {t for t, d in active_map.items() if d < mode_date}
                                 | never_fetched)
                    if remaining:
                        try:
                            catchup = fetch_gap_batch(list(remaining), start_date, callback=callback)
                            new_data.update(catchup)
                        except Exception as e:
                            logging.warning(f"[QuickUpdate] เช็คหุ้นค้างหลังผ่าน Yahoo ล้มเหลว: {e}")
            else:
                logging.info(f"[QuickUpdate] SET API fast path ข้าม: mode_date ขาดมากกว่า 1 วันเทรด "
                      f"(mode_date={mode_date}, วันก่อนล่าสุดจริง={prev_trading_date}, asof={asof}) "
                      f"— fallback Yahoo gap-fill")
        except Exception as e:
            logging.warning(f"[QuickUpdate] SET API fast path ล้มเหลว ({e}) — fallback Yahoo gap-fill เต็มรูปแบบ")
            new_data = None

    if new_data is None:
        callback(0, total, f"ดาวน์โหลดข้อมูลใหม่ตั้งแต่ {start_date}...")
        new_data = fetch_gap_batch(tickers, start_date, callback=callback)

    # Yahoo ได้ 0 ตัวทั้งชุด — อาจเป็นวันหยุดจริง (SET ก็ไม่มีแท่งใหม่เหมือนกัน ก็จะ
    # ได้ผลว่างเปล่าซ้ำ ไม่เสียหาย) หรือ Yahoo ล่ม/ไม่ตอบหลายวันติด (พบจริงได้ เช่น
    # yfinance โดน rate-limit/บล็อกยาว) — ลองสำรองผ่าน SET.or.th chart-quotation แทน
    # ก่อนยอมแพ้ ได้แค่ close+volume ไม่มี OHLC เต็ม แต่ดีกว่าราคาหยุดนิ่งไปหลายวัน
    # พอ Yahoo ฟื้นแล้ว Quick Update รอบถัดไปจะเติม OHLC ให้เองผ่าน Yahoo ตามปกติ
    if not new_data:
        try:
            from sources.set_api import fetch_price_history_batch
            callback(0, total, f"Yahoo ไม่มีข้อมูลใหม่ — ลองสำรองผ่าน SET.or.th ตั้งแต่ {start_date}...")
            new_data = fetch_price_history_batch(tickers, start_date, callback=callback)
            if new_data:
                logging.info(f"[QuickUpdate] SET API (chart-quotation) fallback: {len(new_data)} หุ้น "
                      f"(close+volume เท่านั้น ไม่มี OHLC เต็ม — Yahoo รอบถัดไปจะเติมให้)")
        except Exception as e:
            logging.warning(f"[QuickUpdate] SET API (chart-quotation) fallback ล้มเหลว: {e}")

    if not new_data:
        callback(100, 100, "ไม่มีข้อมูลใหม่ (อาจเป็นวันหยุด)")
        return

    # หุ้นค้างนานที่ถูกตัดออกจากการคำนวณ start_date (ด้านบน) แต่กลับมามีแท่งใหม่
    # ในรอบนี้ (พักเทรดจบ/กลับมาซื้อขาย) — ดึงแค่ตั้งแต่ start_date (ซึ่งอาจใหม่กว่า
    # วันสุดท้ายที่มันมีข้อมูลมาก) จะเหลือรูช่วงที่ขาดหายถาวร (ไม่มีวันไล่ตามทีหลัง
    # เพราะ get_last_dates ของมันจะกลายเป็นวันล่าสุดทันที) ต้อง refetch เต็มช่วง
    # ที่ขาดเฉพาะตัวเหล่านี้ ตั้งแต่วันสุดท้ายที่มันมีข้อมูลจริงๆ
    reactivated = stale_tickers & new_data.keys()
    if reactivated:
        per_ticker_start = min(last_map[t] for t in reactivated)
        logging.info(f"[QuickUpdate] หุ้นค้างนานกลับมาเทรด {len(reactivated)} ตัว — "
              f"refetch ช่วงที่ขาดตั้งแต่ {per_ticker_start}: {sorted(reactivated)}")
        try:
            refill = fetch_gap_batch(list(reactivated), per_ticker_start, callback=callback)
            new_data.update(refill)
        except Exception as e:
            logging.warning(f"[QuickUpdate] refetch หุ้นค้างนานที่กลับมาเทรดล้มเหลว: {e}")

    # ── Split detector: เทียบแท่ง overlap ก่อนบันทึก — ถ้า Yahoo เพิ่งปรับ
    # ราคาย้อนหลัง (แตกพาร์ ฯลฯ) ต้อง refetch เต็มเฉพาะตัว ไม่งั้น series
    # จะเป็นฐานเก่าต่อฐานใหม่ → return/RS ปลอม
    callback(total, total, "ตรวจ corporate action (เทียบราคาแท่ง overlap)...")
    suspects = detect_ca_mismatch(base_dir, new_data)
    repaired = set()
    if suspects:
        logging.info(f"[CA] พบ overlap mismatch: {suspects[:MAX_CA_REPAIR]}")
        repaired = _repair_ca_tickers(base_dir, new_data, suspects, callback)
        # ตัวที่ตรวจพบ mismatch แต่ซ่อมไม่สำเร็จ (refetch ล้มเหลว/ข้อมูลสั้นผิดปกติ
        # หรือเกิน MAX_CA_REPAIR ต่อรอบ) ห้าม upsert แท่งฐานใหม่ทับแท่งฐานเก่า —
        # ไม่งั้น series จะกลายเป็นฐานเก่าต่อฐานใหม่ (return/RS ปลอม) แถมรอบหน้า
        # detector จะเทียบกับแท่งที่เพิ่งเขียนทับ (ฐานใหม่แล้ว) เลยไม่เจอ mismatch
        # อีก = ไม่มีวันซ่อม ปล่อยให้ตามหลัง 1 วัน รอรอบถัดไป detector เจอซ้ำแล้ว retry
        unrepaired = set(suspects) - repaired
        for t in unrepaired:
            new_data.pop(t, None)
        if unrepaired:
            logging.warning(f"[CA] {len(unrepaired)} ตัวซ่อมไม่สำเร็จ — ข้ามการบันทึกรอบนี้ "
                  f"(รอรอบถัดไป retry): {sorted(unrepaired)}")

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
            # highs/lows มาจาก iter_all_series (additive) — คำนวณ ATR จริง; None ถ้าไม่มี
            high = pd.Series(hist_data["highs"], index=dates, dtype=float) if hist_data.get("highs") else None
            low  = pd.Series(hist_data["lows"],  index=dates, dtype=float) if hist_data.get("lows") else None
        except Exception:
            continue
        result = process_stock(info_dict, close, volume, high=high, low=low)
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
    try:
        from sources.price_analytics import annotate_seasonality
        annotate_seasonality(base_dir, stocks)
    except Exception as e:
        logging.warning(f"[Seasonality] ข้าม: {e}")
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

    # ตรวจซ้ำหุ้นราคาปิดคงที่ผิดปกติ >= 14 วันเทรด แล้วเช็คเครื่องหมาย SP/NC/NP/H จริง
    # จาก SET API ยืนยันสาเหตุ — non-critical, ล้มได้โดยไม่กระทบผลลัพธ์หลักของ Quick Update
    flat_note = ""
    try:
        flat_tickers = detect_flat_price_tickers(base_dir)
        if flat_tickers:
            from sources.set_api import fetch_signs_batch
            signs = fetch_signs_batch(flat_tickers)
            if signs:
                detail = ", ".join(f"{t}({signs[t]})" for t in sorted(signs))
                logging.warning(f"[QuickUpdate] ราคาคงที่ {len(flat_tickers)} ตัว — "
                      f"ยืนยันติดเครื่องหมาย {len(signs)} ตัว: {detail}")
                flat_note = f" · พบหุ้นติดเครื่องหมาย {len(signs)} ตัว ({detail[:80]})"
            else:
                logging.info(f"[QuickUpdate] ราคาคงที่ {len(flat_tickers)} ตัว "
                      f"แต่เช็คเครื่องหมายไม่เจอ (อาจไม่ใช่ SP/NC/NP/H หรือ SET API ตอบไม่ครบ): "
                      f"{sorted(flat_tickers)[:10]}")
    except Exception as e:
        logging.warning(f"[QuickUpdate] ตรวจหุ้นราคาคงที่ล้มเหลว: {e}")

    callback(total, total,
             f"Quick Update เสร็จ! {len(stocks)} หุ้น (ดาวน์โหลดใหม่ {len(new_data)} หุ้น)"
             + flat_note)


