# -*- coding: utf-8 -*-
"""sources/mirror_ondemand.py — header (ราคา/return/RS/stage/sector) + factor แบบ on-demand
สำหรับหุ้น mirror US/HK ที่ไม่ใช่สมาชิกดัชนีหลัก (~4,485 จาก ~5,108 ตัว) เปิด Tearsheet/Peer
Compare ของหุ้นกลุ่มนี้ได้โดยไม่ต้องมี pipeline ราคารายวันแบบ us_prices.db/hk_prices.db ครบทั้ง
universe — ดึงเฉพาะตัวที่ผู้ใช้เปิดดูจริง (period=2y ครั้งเดียว, cache ผลไว้ 1 วัน)

RS/stage คำนวณเทียบกับสมาชิกดัชนีหลักที่มีราคาอยู่แล้วใน <mkt>_prices.db (ไม่ fetch เพิ่มสำหรับ
สมาชิกเดิม — ดู index_metrics_common.compute_ondemand_row)"""
from datetime import datetime

from sources import financials_store as fs
from sources import factor_snapshot
from sources import index_metrics_common
from sources import yahoo as yahoo_src

_CFG_BY_EX = {}


def _cfg_for(ex):
    """lazy import cfg ของ us_index_metrics/hk_index_metrics — กัน circular import ตอนโหลดโมดูล"""
    if ex not in _CFG_BY_EX:
        if ex == "US":
            from sources import us_index_metrics as m
        elif ex == "HK":
            from sources import hk_index_metrics as m
        else:
            raise ValueError(f"ไม่รองรับตลาด {ex} สำหรับ mirror on-demand")
        _CFG_BY_EX[ex] = m._CFG
    return _CFG_BY_EX[ex]


# Yahoo Ticker.info['sector'] ใช้ taxonomy ของตัวเอง (Financial Services, Healthcare, Consumer
# Cyclical ฯลฯ) ต่างจาก GICS ที่ us/hk_index_metrics.json ใช้ (Financials, Health Care, Consumer
# Discretionary ฯลฯ — scrape จาก Wikipedia, ดู sources/index_metrics_common.py) ถ้าไม่แปลงก่อน
# หุ้น on-demand จะไม่ match กลุ่มไหนเลยใน Peer Compare (sector string ตรงตัวไม่เจอ กลายเป็น
# กลุ่มของตัวเองคนเดียว) — map เฉพาะชื่อที่ต่างกันจริง ตัวที่เขียนเหมือนกันอยู่แล้วไม่ต้องแตะ
_YAHOO_TO_GICS_SECTOR = {
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Basic Materials": "Materials",
}


def _normalize_sector(yahoo_sector):
    return _YAHOO_TO_GICS_SECTOR.get(yahoo_sector, yahoo_sector)


def _yf_ticker(ex, ticker):
    """แปลงรหัสดิบ (namespace mirror ใช้เสมอ ไม่มี suffix) เป็น ticker ที่ยิง yfinance ได้จริง —
    HK ต้องมี '.HK' ต่อท้าย (เหมือน hk_index_metrics.json) ตลาดอื่นใช้ตรงๆ"""
    return f"{ticker}.HK" if ex == "HK" else ticker


def fetch_header(base_dir, ex, ticker, force=False):
    """คืน header dict (field ชื่อเดียวกับ entry ใน _tearsheet_universe_map) ของหุ้น mirror
    US/HK ตัวเดียวที่ไม่ใช่สมาชิกดัชนีหลัก — คืน None ถ้าดึงราคาไม่สำเร็จ/ราคาไม่พอคำนวณ
    (< 5 แท่ง — เช่น ticker ผิด/เพิ่ง IPO)

    ticker: รหัสดิบไม่มี suffix เสมอ (เดียวกับ namespace 'FINN:{ex}:{ticker}' — ผู้เรียกต้อง
    ตัด '.HK' ออกก่อนแล้ว ดู _mirror_sym ใน app.py) — ฟังก์ชันนี้เติม suffix เองตอนยิง yfinance
    เท่านั้น (_yf_ticker) เก็บ/คืนค่าเป็นรหัสดิบเสมอให้ตรงกับ mirror_candidates/factor_snapshot"""
    ex = ex.upper()
    ticker = ticker.upper().strip()
    yf_ticker = _yf_ticker(ex, ticker)

    if not force:
        cached, stale = fs.get_mirror_ondemand(base_dir, ticker, ex, stale_days=1)
        if cached and not stale:
            return cached

    ohlc = yahoo_src.fetch_all_batch([yf_ticker], period="2y")
    rec = ohlc.get(yf_ticker)
    if rec is None or rec.get("close") is None or len(rec["close"]) < 5:
        return None

    info = yahoo_src.fetch_company_info(yf_ticker)
    name = info.get("long_name") or ticker
    sector = _normalize_sector(info.get("sector"))
    industry = info.get("industry")

    cfg = _cfg_for(ex)
    row = index_metrics_common.compute_ondemand_row(
        base_dir, cfg, ticker, rec["close"], rec["volume"], rec["high"], rec["low"],
        name, sector, industry)
    if row is None:
        return None

    row["mkt_cap"] = info.get("mkt_cap")
    row["pe"] = info.get("pe")
    row["pbv"] = info.get("pbv")
    row["div_yield"] = info.get("div_yield")
    row["pct_off_high52"] = (round((row["price"] / row["high_52w"] - 1) * 100, 2)
                              if row.get("price") and row.get("high_52w") else None)

    _ensure_factors(base_dir, ex, ticker)

    fs.save_mirror_ondemand(base_dir, ticker, ex, row)
    return row


def _ensure_factors(base_dir, ex, ticker):
    """sync งบ Yahoo annual ให้ ticker นี้ถ้ายังไม่เคยมี (ตัวเดียว เร็ว) แล้วคำนวณ factor
    (F-Score/Z-Score/FCF/CAGR ฯลฯ) เขียนเข้า factor_snapshot_mirror ทันที — ให้ Screener+/Peer
    Compare เห็นข้อมูลโดยไม่ต้องรอ batch sync (sources/financials_store.sync_mirror_yahoo_index)
    รอบถัดไป"""
    key = fs._mirror_key(ex, ticker)
    if not fs.get(base_dir, key, "yahoo", is_dr=False):
        try:
            payload = fs.fetch_yahoo_full(ticker, is_dr=True, market=ex)
            fs.upsert(base_dir, key, "yahoo", payload, is_dr=False)
        except Exception as e:
            print(f"[MirrorOndemand] sync งบ Yahoo {ex}:{ticker} ล้มเหลว: {str(e)[:80]}")
            return

    f = factor_snapshot._factors_for(base_dir, key, is_dr=False, z_variant="Z")
    if f is None:
        return
    f["div_cagr_5y"] = factor_snapshot._div_cagr_5y(base_dir, ticker, ex)
    factor_snapshot.upsert_mirror_row(base_dir, ticker, ex, f)
