# -*- coding: utf-8 -*-
"""sources/mirror_ondemand.py — header (ราคา/return/RS/stage/sector) + factor แบบ on-demand
สำหรับหุ้น mirror US/HK ที่ไม่ใช่สมาชิกดัชนีหลัก (~4,485 จาก ~5,108 ตัว) เปิด Tearsheet/Peer
Compare ของหุ้นกลุ่มนี้ได้โดยไม่ต้องมี pipeline ราคารายวันแบบ us_prices.db/hk_prices.db ครบทั้ง
universe — ดึงเฉพาะตัวที่ผู้ใช้เปิดดูจริง (period=2y ครั้งเดียว, cache ผลไว้ 1 วัน)

RS/stage คำนวณเทียบกับสมาชิกดัชนีหลักที่มีราคาอยู่แล้วใน <mkt>_prices.db (ไม่ fetch เพิ่มสำหรับ
สมาชิกเดิม — ดู index_metrics_common.compute_ondemand_row)"""
import threading
from datetime import datetime

from sources import financials_store as fs
from sources import factor_snapshot
from sources import index_metrics_common
from sources import yahoo as yahoo_src

_CFG_BY_EX = {}

# กัน request พร้อมกันหลายชุด (เปิดหลายแท็บ/หลายอุปกรณ์ไปที่หุ้นตัวเดียวกันตอน cache หมดอายุ)
# ยิง fetch Yahoo + เขียนทับ DB ซ้อนกัน — เดิมไม่มี lock เลย ทุก thread ที่เจอ cache miss/stale
# พร้อมกันจะยิง yfinance ซ้ำกันหมด (เปลือง quota โดยไม่จำเป็น + "ใครเขียนทีหลังชนะ") lock แยกต่อ
# (base_dir, kind, key1, key2) แบบ lazy สร้าง (pattern เดียวกับ _get_sector_trend_lock ใน app.py)
# ไม่ใช่ lock เดียวรวม เพราะหุ้นคนละตัวไม่ควรรอกัน
_fetch_locks: dict = {}
_fetch_locks_meta_lock = threading.Lock()


def _get_fetch_lock(key):
    with _fetch_locks_meta_lock:
        if key not in _fetch_locks:
            _fetch_locks[key] = threading.Lock()
        return _fetch_locks[key]


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

    with _get_fetch_lock((base_dir, "header", ex, ticker)):
        # เช็คซ้ำหลังได้ lock (double-checked) — ถ้า thread อื่นที่มาถึงก่อนเพิ่ง fetch+cache
        # เสร็จระหว่างที่เรารอ lock ก็ได้ค่าที่เพิ่งเขียนไปเลย ไม่ต้องยิง Yahoo ซ้ำ (ข้ามเช็คนี้
        # เมื่อ force=True — ผู้เรียกต้องการข้อมูลสดจริงๆ ไม่ใช่ของที่เพิ่งมีคนอื่น cache ไว้)
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


def fetch_header_lite(base_dir, yf_ticker, dr_sym, region, name_hint=None, force=False):
    """Header แบบ cohort-free สำหรับหุ้น DR ที่ underlying อยู่ตลาดยังไม่มี price cohort
    (JP/VN/SG/EU/TW/CN — ไม่มี jp_prices.db ฯลฯ ให้ rank RS เทียบ) — เรียก process_stock ต่อหุ้น
    ตัวเดียวตรงๆ ไม่ผ่าน compute_ondemand_row/rank_rs (จุดเดียวที่ผูกกับ cohort) ดังนั้น
    rs_score/rs_momentum = None เสมอ ส่วน stage/52W/EMA/return/ATR คำนวณต่อหุ้นได้ครบปกติ

    factor (F-Score/Z-Score/FCF/CAGR) มาจาก namespace DR: (is_dr=True) — 46/63 มีอยู่แล้วจาก
    DR pipeline เดิม ตัวที่เหลือ sync สดตอนเปิดครั้งแรก (_ensure_factors_dr) · cache 1 วันใน
    table mirror_ondemand (ex=region, ticker=dr_sym — คนละ namespace กับ US/HK on-demand)
    · คืน None ถ้าดึงราคาไม่สำเร็จ/ราคาไม่พอคำนวณ (< 5 แท่ง)"""
    from set_data_fetcher import process_stock, sanitize
    region = region.upper()
    dr_sym = dr_sym.upper().strip()
    yf_ticker = yf_ticker.upper().strip()

    if not force:
        cached, stale = fs.get_mirror_ondemand(base_dir, dr_sym, region, stale_days=1)
        if cached and not stale:
            return cached

    with _get_fetch_lock((base_dir, "lite", region, dr_sym)):
        if not force:
            cached, stale = fs.get_mirror_ondemand(base_dir, dr_sym, region, stale_days=1)
            if cached and not stale:
                return cached

        ohlc = yahoo_src.fetch_all_batch([yf_ticker], period="2y")
        rec = ohlc.get(yf_ticker)
        if rec is None or rec.get("close") is None or len(rec["close"]) < 5:
            return None

        info_co = yahoo_src.fetch_company_info(yf_ticker)
        name = name_hint or info_co.get("long_name") or dr_sym
        sector = _normalize_sector(info_co.get("sector"))
        industry = info_co.get("industry")

        info = {"symbol": dr_sym, "ticker": dr_sym, "name": name, "market": region,
                "industry": industry or sector or "Unknown", "sector": sector or "Unknown"}
        row = process_stock(info, rec["close"], rec["volume"], high=rec["high"], low=rec["low"])
        if row is None:
            return None
        row["rs_score"] = None       # ไม่มี cohort ให้ rank เทียบ
        row["rs_momentum"] = None
        # stage เป็นสูตรต่อหุ้นล้วน (EMA200 + slope) ไม่พึ่ง cohort — ปกติ rank_rs เซ็ตให้ แต่ path
        # lite ไม่ผ่าน rank_rs เลยต้องเรียก classify_stage เอง (single source of truth เดียวกัน)
        from core.metrics import classify_stage
        row["stage"] = classify_stage(row.get("above_ema200"), row.get("ema200_slope_pct"))
        row["mkt_cap"] = info_co.get("mkt_cap")
        row["pe"] = info_co.get("pe")
        row["pbv"] = info_co.get("pbv")
        row["div_yield"] = info_co.get("div_yield")
        row["pct_off_high52"] = (round((row["price"] / row["high_52w"] - 1) * 100, 2)
                                  if row.get("price") and row.get("high_52w") else None)

        _ensure_factors_dr(base_dir, dr_sym, region)

        row = sanitize([row])[0]
        fs.save_mirror_ondemand(base_dir, dr_sym, region, row)
        return row


def _ensure_factors_dr(base_dir, dr_sym, region):
    """sync งบ Yahoo เข้า namespace DR: (is_dr=True) ถ้ายังไม่มี — 46/63 ตัวมีอยู่แล้วจาก DR
    pipeline เดิม (update_financials.sync_all is_dr=True) ตัวที่เหลือดึงสดตอนเปิดครั้งแรก
    (fetch_yahoo_full resolve yf ticker จาก DR universe ให้เอง เช่น GSEMI -> 2243.T)"""
    try:
        if fs.get(base_dir, dr_sym, "yahoo", is_dr=True):
            return
        payload = fs.fetch_yahoo_full(dr_sym, is_dr=True, market=region)
        fs.upsert(base_dir, dr_sym, "yahoo", payload, is_dr=True)
    except Exception as e:
        print(f"[MirrorOndemandLite] sync งบ Yahoo DR:{dr_sym} ({region}) ล้มเหลว: {str(e)[:80]}")


def _ensure_factors(base_dir, ex, ticker):
    """sync งบ Yahoo annual ให้ ticker นี้ถ้ายังไม่เคยมี (ตัวเดียว เร็ว) แล้วคำนวณ factor
    (F-Score/Z-Score/FCF/CAGR ฯลฯ) เขียนเข้า factor_snapshot_mirror ทันที — ให้ Screener+/Peer
    Compare เห็นข้อมูลโดยไม่ต้องรอ batch sync (sources/financials_store.sync_mirror_yahoo_index)
    รอบถัดไป"""
    key = fs._mirror_key(ex, ticker)
    try:
        has_yahoo = fs.get(base_dir, key, "yahoo", is_dr=False)
    except Exception as e:
        print(f"[MirrorOndemand] เช็คงบ Yahoo {ex}:{ticker} ล้มเหลว: {str(e)[:80]}")
        return
    if not has_yahoo:
        try:
            payload = fs.fetch_yahoo_full(ticker, is_dr=True, market=ex)
            fs.upsert(base_dir, key, "yahoo", payload, is_dr=False)
        except Exception as e:
            print(f"[MirrorOndemand] sync งบ Yahoo {ex}:{ticker} ล้มเหลว: {str(e)[:80]}")
            return

    try:
        f = factor_snapshot._factors_for(base_dir, key, is_dr=False, z_variant="Z")
        if f is None:
            return
        f["div_cagr_5y"] = factor_snapshot._div_cagr_5y(base_dir, ticker, ex)
        factor_snapshot.upsert_mirror_row(base_dir, ticker, ex, f)
    except Exception as e:
        print(f"[MirrorOndemand] คำนวณ/บันทึก factor {ex}:{ticker} ล้มเหลว: {str(e)[:80]}")
