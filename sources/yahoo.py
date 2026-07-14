# -*- coding: utf-8 -*-
"""sources/yahoo.py — Yahoo Finance batch downloader"""
import time

import yfinance as yf


# ============================================================
# 3. Batch downloader — ดึง 100 ตัวต่อครั้ง
# ============================================================

BATCH_SIZE = 100


def _extract_ohlcav(df, min_bars=5):
    """แกะ OHLC + Adj Close + Volume จาก DataFrame ของหุ้นตัวเดียว (yf.download,
    auto_adjust=False) — จัดทุก series ให้ align กับ index ของ Close ที่ไม่เป็น null
    เพื่อให้ upsert zip ตามตำแหน่งได้ตรงกัน คืน None ถ้าแท่งน้อยกว่า min_bars

    เก็บทั้ง close (ราคาจริง — งาน PE/valuation เดิมใช้) และ adj_close (ปรับ
    split+ปันผล — ใช้คำนวณผลตอบแทน/CAGR/seasonality/drawdown ระยะยาวให้ถูก)"""
    if df is None or df.empty or "Close" not in df:
        return None
    close = df["Close"].dropna()
    if len(close) < min_bars:
        return None
    idx = close.index

    def _col(name):
        return df[name].reindex(idx) if name in df else None

    vol = df["Volume"].reindex(idx) if "Volume" in df else None
    return {
        "open":      _col("Open"),
        "high":      _col("High"),
        "low":       _col("Low"),
        "close":     close,
        "adj_close": _col("Adj Close"),
        "volume":    vol.fillna(0) if vol is not None else close * 0,
    }


def fetch_all_batch(tickers, callback=None, period="max"):
    """
    ดาวน์โหลดราคาทุกตัวด้วย yf.download() แบบ batch
    คืนค่า dict: ticker -> {'open','high','low','close','adj_close','volume': pd.Series}
    (ทุก series align index เดียวกับ close — ดู _extract_ohlcav)
    """
    chunks = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    n_chunks = len(chunks)
    all_data = {}

    for ci, chunk in enumerate(chunks):
        done_so_far = ci * BATCH_SIZE
        if callback:
            callback(done_so_far, len(tickers),
                     f"ดาวน์โหลด batch {ci + 1}/{n_chunks} ({len(chunk)} หุ้น, period={period})...")

        try:
            if len(chunk) == 1:
                raw = yf.download(
                    chunk[0], period=period, auto_adjust=False,
                    progress=False, threads=False,
                )
                rec = _extract_ohlcav(raw, min_bars=5)
                if rec is not None:
                    all_data[chunk[0]] = rec
            else:
                raw = yf.download(
                    chunk, period=period, auto_adjust=False,
                    progress=False, group_by="ticker", threads=True,
                )
                for tick in chunk:
                    try:
                        rec = _extract_ohlcav(raw[tick], min_bars=5)
                        if rec is not None:
                            all_data[tick] = rec
                    except Exception:
                        pass
        except Exception as e:
            print(f"  [batch {ci + 1}] error: {e}")

        time.sleep(0.3)

    return all_data


def fetch_gap_batch(tickers, start_date, callback=None):
    """
    ดาวน์โหลดเฉพาะวันใหม่ตั้งแต่ start_date (สำหรับ Quick Update)
    คืนค่า dict: ticker -> {'open','high','low','close','adj_close','volume': pd.Series}
    """
    chunks = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    n_chunks = len(chunks)
    all_data = {}

    for ci, chunk in enumerate(chunks):
        done_so_far = ci * BATCH_SIZE
        if callback:
            callback(done_so_far, len(tickers),
                     f"ดาวน์โหลด gap batch {ci + 1}/{n_chunks} ({len(chunk)} หุ้น)...")

        try:
            if len(chunk) == 1:
                raw = yf.download(
                    chunk[0], start=start_date, auto_adjust=False,
                    progress=False, threads=False,
                )
                rec = _extract_ohlcav(raw, min_bars=1)
                if rec is not None:
                    all_data[chunk[0]] = rec
            else:
                raw = yf.download(
                    chunk, start=start_date, auto_adjust=False,
                    progress=False, group_by="ticker", threads=True,
                )
                for tick in chunk:
                    try:
                        rec = _extract_ohlcav(raw[tick], min_bars=1)
                        if rec is not None:
                            all_data[tick] = rec
                    except Exception:
                        pass
        except Exception as e:
            print(f"  [gap batch {ci + 1}] error: {e}")

        time.sleep(0.3)

    return all_data


# ============================================================
# 4. Parallel fundamentals fetcher (market cap + P/E + P/BV + Div Yield)
# ============================================================

def fetch_market_caps_parallel(tickers, callback=None, workers=3):
    """ดึง market_cap + P/E + P/BV + Div Yield — sequential per-ticker เพื่อใช้ crumb เดียวกัน"""
    import random
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # สร้าง session เดียวร่วมกัน เพื่อให้ crumb ไม่หมดอายุระหว่างการดึง
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    results = {}

    def _get_fund(tick):
        time.sleep(random.uniform(0.3, 1.0))
        for attempt in range(4):
            try:
                t    = yf.Ticker(tick, session=session)
                info = t.info
                mc   = info.get("marketCap")
                pe   = info.get("trailingPE")
                pbv  = info.get("priceToBook")
                dy   = info.get("dividendYield")
                # เดิมมี heuristic "ค่า < 1.0 ถือว่าเป็น decimal → คูณ 100" เพราะ
                # yfinance รุ่นเก่าเคยคืนทศนิยม (0.0583) — ปัจจุบัน yfinance คืน
                # เป็น % ตรงๆ แล้ว (ยืนยันแล้วว่าตรงกับ SET API เช่น DELTA=0.19
                # หมายถึง 0.19% ไม่ใช่ 19%) heuristic เดิมเลยไปพองหุ้น yield ต่ำ
                # จริง (growth stock, high P/E) ให้ผิดเพี้ยน ×100 — เอาออก ใช้ค่าตรงๆ
                return tick, {
                    "mkt_cap":   int(mc)          if mc  is not None else None,
                    "pe":        round(float(pe),  2) if pe  is not None else None,
                    "pbv":       round(float(pbv), 2) if pbv is not None else None,
                    "div_yield": round(float(dy),  2) if dy  is not None else None,
                }
            except Exception as e:
                err = str(e).lower()
                if "rate" in err or "too many" in err or "429" in err or "401" in err or "crumb" in err:
                    wait = (2 ** attempt) + random.uniform(1, 3)
                    time.sleep(wait)
                else:
                    return tick, {}
        return tick, {}

    total = len(tickers)
    done  = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_get_fund, t): t for t in tickers}
        for f in as_completed(futures):
            tick, data = f.result()
            results[tick] = data
            done += 1
            if callback and done % 50 == 0:
                callback(done, total, f"Fundamentals {done}/{total}...")
    return results


