# -*- coding: utf-8 -*-
"""sources/yahoo.py — Yahoo Finance batch downloader"""
import time

import yfinance as yf


# ============================================================
# 3. Batch downloader — ดึง 100 ตัวต่อครั้ง
# ============================================================

BATCH_SIZE = 100


def fetch_all_batch(tickers, callback=None, period="max"):
    """
    ดาวน์โหลดราคาทุกตัวด้วย yf.download() แบบ batch
    คืนค่า dict: ticker -> {'close': pd.Series, 'volume': pd.Series}
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
                if not raw.empty and len(raw) >= 5:
                    close  = raw["Close"].dropna()
                    volume = raw["Volume"].dropna()
                    if len(close) >= 5:
                        all_data[chunk[0]] = {"close": close, "volume": volume}
            else:
                raw = yf.download(
                    chunk, period=period, auto_adjust=False,
                    progress=False, group_by="ticker", threads=True,
                )
                for tick in chunk:
                    try:
                        close  = raw[tick]["Close"].dropna()
                        volume = raw[tick]["Volume"].dropna()
                        if len(close) >= 5:
                            all_data[tick] = {"close": close, "volume": volume}
                    except Exception:
                        pass
        except Exception as e:
            print(f"  [batch {ci + 1}] error: {e}")

        time.sleep(0.3)

    return all_data


def fetch_gap_batch(tickers, start_date, callback=None):
    """
    ดาวน์โหลดเฉพาะวันใหม่ตั้งแต่ start_date (สำหรับ Quick Update)
    คืนค่า dict: ticker -> {'close': pd.Series, 'volume': pd.Series}
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
                if not raw.empty:
                    close  = raw["Close"].dropna()
                    volume = raw["Volume"].dropna()
                    if len(close) >= 1:
                        all_data[chunk[0]] = {"close": close, "volume": volume}
            else:
                raw = yf.download(
                    chunk, start=start_date, auto_adjust=False,
                    progress=False, group_by="ticker", threads=True,
                )
                for tick in chunk:
                    try:
                        close  = raw[tick]["Close"].dropna()
                        volume = raw[tick]["Volume"].dropna()
                        if len(close) >= 1:
                            all_data[tick] = {"close": close, "volume": volume}
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
                # yfinance .BK ปกติคืน % (เช่น 5.83) แต่บางครั้งคืน decimal (เช่น 0.0583)
                # ใช้ threshold 1.0: ค่า < 1.0 ถือว่าเป็น decimal format → คูณ 100
                # (SET stocks ที่มี div_yield จริงๆ < 1% มีน้อยมาก และ yfinance .BK ส่วนใหญ่คืน %)
                if dy is not None and 0 < float(dy) < 1.0:
                    dy = float(dy) * 100
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


