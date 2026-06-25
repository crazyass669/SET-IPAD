"""
SET Data Fetcher v3 — Batch Download + Flask-ready
ดึงหุ้น SET ทั้งหมดด้วย yf.download() batch (~7 นาที แทน 25 นาที)

ใช้เป็น library:
    from set_data_fetcher import run_with_progress
    run_with_progress(callback, base_dir)

รันตรง:
    python set_data_fetcher.py
"""

import os
import json
import time
import warnings
from datetime import datetime, timezone, timedelta

_ICT = timezone(timedelta(hours=7))

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    import pandas as pd
    from tqdm import tqdm
except ImportError as e:
    print(f"ติดตั้ง library ก่อน: pip install yfinance pandas openpyxl xlrd tqdm flask")
    print(f"Error: {e}")
    raise

XLS_FILE     = "listedCompanies_en_US.xls"
OUT_FILE     = "set_data.json"
HISTORY_FILE = "set_history.json"


# ============================================================
# 1. อ่านรายชื่อหุ้นจากไฟล์ SET
# ============================================================

def load_set_symbols(base_dir=None):
    path = os.path.join(base_dir, XLS_FILE) if base_dir else XLS_FILE
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"ไม่พบไฟล์ {path}\n"
            "โหลดจาก: https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls"
        )

    # ไฟล์จาก SET.or.th เป็น HTML table ที่ตั้งชื่อว่า .xls
    tables = pd.read_html(path, header=None)
    df = None
    for t in tables:
        for i, row in t.iterrows():
            row_str = " ".join(str(v).lower() for v in row.values)
            if "symbol" in row_str and ("market" in row_str or "company" in row_str):
                t.columns = t.iloc[i]
                t = t.iloc[i + 1:].reset_index(drop=True)
                t.columns = [str(c).strip() for c in t.columns]
                df = t
                break
        if df is not None:
            break
    if df is None:
        raise ValueError("ไม่พบตารางรายชื่อหุ้นในไฟล์ HTML")

    col_map = {}
    for col in df.columns:
        cl = col.lower()
        if "symbol"   in cl: col_map["symbol"]   = col
        elif "company" in cl or "name" in cl: col_map["name"] = col
        elif "market"  in cl: col_map["market"]   = col
        elif "industry" in cl: col_map["industry"] = col
        elif "sector"  in cl: col_map["sector"]   = col

    symbols = []
    for _, row in df.iterrows():
        sym = str(row.get(col_map.get("symbol", ""), "")).strip().upper()
        if not sym or sym in ("nan", "Symbol", "NAN"):
            continue
        market = str(row.get(col_map.get("market", ""), "")).strip()
        if market not in ("SET", "mai", ""):
            continue
        name     = str(row.get(col_map.get("name",     ""), "")).strip()
        industry = str(row.get(col_map.get("industry", ""), "")).strip()
        sector   = str(row.get(col_map.get("sector",   ""), "")).strip()
        _blank   = {"nan", "-", "", "N/A"}
        clean_industry = industry if industry not in _blank else "Unknown"
        clean_sector   = sector   if sector   not in _blank else None
        if clean_sector is None:
            clean_sector = (clean_industry + " -mai") if market == "mai" else "Unknown"
        symbols.append({
            "symbol":   sym,
            "ticker":   f"{sym}.BK",
            "name":     name     if name     not in _blank else sym,
            "market":   market,
            "industry": clean_industry,
            "sector":   clean_sector,
        })

    return symbols


# pattern สำหรับ กองทุนรวม / REIT / Infra Fund ใน SET
import re as _re
_FUND_PAT = _re.compile(
    r'(GIF|IF|REIT|PF|ARAF|BT|RT|MNIT\d*)$'   # ลงท้ายด้วย suffix กองทุน
    r'|^(CG)$'                                   # exact match เท่านั้น (ป้องกัน BCG ผิดพลาด)
    r'|^M-'
    r'|(LUXF|MNRF|WHAIR)$',
    _re.IGNORECASE
)

def _is_reit(symbol: str) -> bool:
    return bool(_FUND_PAT.search(symbol))


# ============================================================
# 1b. History helpers — load / merge / save set_history.json
# ============================================================

def load_history(base_dir):
    path = os.path.join(base_dir, HISTORY_FILE)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _merge_history(existing, new_dates, new_closes, new_volumes):
    """Merge new bars into existing, upsert by date (overwrite if exists), keep sorted."""
    if not existing or not existing.get("dates"):
        return new_dates, new_closes, new_volumes
    data_map = {d: (c, v) for d, c, v in zip(existing["dates"], existing["closes"], existing["volumes"])}
    for d, c, v in zip(new_dates, new_closes, new_volumes):
        data_map[d] = (c, v)  # overwrite ถ้ามีอยู่แล้ว
    triples = sorted((d, c, v) for d, (c, v) in data_map.items())
    if not triples:
        return [], [], []
    dates, closes, volumes = zip(*triples)
    return list(dates), list(closes), list(volumes)


def save_history(all_data_map, base_dir, existing_hist=None):
    """
    all_data_map: {ticker -> {close: pd.Series, volume: pd.Series}}
    Merges with existing_hist and writes set_history.json.
    Returns the new history dict.
    """
    stocks_hist = {}
    if existing_hist:
        stocks_hist = dict(existing_hist.get("stocks", {}))

    for ticker, data in all_data_map.items():
        close  = data["close"]
        volume = data["volume"]
        new_dates   = [d.strftime("%Y-%m-%d") for d in close.index]
        new_closes  = [round(float(c), 4) for c in close]
        new_volumes = [int(v) for v in volume]
        existing = stocks_hist.get(ticker)
        merged_d, merged_c, merged_v = _merge_history(
            existing, new_dates, new_closes, new_volumes
        )
        stocks_hist[ticker] = {
            "dates":   merged_d,
            "closes":  merged_c,
            "volumes": merged_v,
        }

    new_hist = {
        "updated_at": datetime.now(_ICT).strftime("%Y-%m-%d %H:%M:%S"),
        "stocks": stocks_hist,
    }
    path = os.path.join(base_dir, HISTORY_FILE)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(new_hist, f, ensure_ascii=False)
    return new_hist


# ============================================================
# 2. คำนวณ metrics จาก Series ที่ดาวน์โหลดมาแล้ว
# ============================================================

def _calc_return(series, days):
    if len(series) < days + 1:
        return None
    try:
        past = float(series.iloc[-(days + 1)])
        now  = float(series.iloc[-1])
        if past == 0:
            return None
        return round((now - past) / past * 100, 2)
    except Exception:
        return None


def _calc_ema(series, period):
    if len(series) < period:
        return None
    try:
        return round(float(series.ewm(span=period, adjust=False).mean().iloc[-1]), 4)
    except Exception:
        return None


def process_stock(info_dict, close, volume):
    """คำนวณ metrics จาก close/volume Series — ไม่ดึงข้อมูลเพิ่ม"""
    try:
        if close is None or len(close) < 5:
            return None

        dates = close.index
        price = round(float(close.iloc[-1]), 4)

        ema20  = _calc_ema(close, 20)
        ema50  = _calc_ema(close, 50)
        ema200 = _calc_ema(close, 200)

        ret_1d = _calc_return(close, 1)
        ret_1w = _calc_return(close, 5)
        ret_1m = _calc_return(close, 21)
        ret_3m = _calc_return(close, 63)
        ret_6m = _calc_return(close, 126)
        ret_1y = _calc_return(close, 250)

        current_year = datetime.now().year
        ytd_pairs = [(d, p) for d, p in zip(dates, close) if d.year == current_year]
        ret_ytd = None
        if ytd_pairs:
            first_price = float(ytd_pairs[0][1])
            if first_price > 0:
                ret_ytd = round((price - first_price) / first_price * 100, 2)

        above_ema20  = bool(price > ema20)  if ema20  is not None else None
        above_ema50  = bool(price > ema50)  if ema50  is not None else None
        above_ema200 = bool(price > ema200) if ema200 is not None else None

        parts = [(ret_1m, 2), (ret_3m, 1), (ret_6m, 1), (ret_1y, 1)]
        valid = [(v, w) for v, w in parts if v is not None]
        rs_raw = round(sum(v * w for v, w in valid) / sum(w for _, w in valid), 4) if valid else None

        # rs_raw คำนวณ ณ 4 สัปดาห์ก่อน (ใช้ rank ต่างหาก → rs_momentum)
        rs_raw_4w = None
        if len(close) >= 272:  # 21+21+63+126+250 min
            c4w = close.iloc[:-21]
            r1m = _calc_return(c4w, 21)
            r3m = _calc_return(c4w, 63)
            r6m = _calc_return(c4w, 126)
            r1y = _calc_return(c4w, 250)
            v4w = [(v, w) for v, w in [(r1m,2),(r3m,1),(r6m,1),(r1y,1)] if v is not None]
            if v4w:
                rs_raw_4w = round(sum(v*w for v,w in v4w)/sum(w for _,w in v4w), 4)

        # EMA200 slope: เทียบ EMA200 ปัจจุบัน vs 10 สัปดาห์ก่อน (50 วัน)
        ema200_slope_pct = None
        if ema200 is not None and len(close) >= 250:
            ema200_past = _calc_ema(close.iloc[:-50], 200)
            if ema200_past and ema200_past > 0:
                ema200_slope_pct = round((ema200 - ema200_past) / ema200_past * 100, 3)

        # ATR14 (close-only approximation): avg daily move % ช่วง 14 วัน
        atr14_pct = None
        if len(close) >= 15:
            daily_moves = close.pct_change().abs().tail(14).dropna()
            if len(daily_moves) >= 10:
                atr14_pct = round(float(daily_moves.mean()) * 100, 3)

        # RVOL: avg ไม่รวมวันนี้ (tail(21) ตัดวันสุดท้าย) — ป้องกัน self-reference
        vol_20    = int(volume.tail(21).iloc[:-1].mean()) if len(volume) >= 21 else None
        vol_today = int(volume.iloc[-1]) if len(volume) > 0 else None

        # price_history: เก็บ 1750 วันทำการ (~7 ปี) เพื่อให้ EMA200 converge ได้ถูกต้อง
        # (EMA200 ต้องการ warmup ~300 แท่งหลัง seed จึงจะ converge 97%)
        _hist_bars     = min(len(close), 1750)
        _display_bars  = min(len(close), 260)   # chart แสดง 1 ปี
        price_history  = [
            [d.strftime("%Y-%m-%d"), round(float(p), 2)]
            for d, p in zip(dates[-_hist_bars:], close.tail(_hist_bars))
        ]
        vol_history = [int(v) for v in volume.tail(_display_bars)]

        _ath = float(close.max())
        return {
            "symbol":           info_dict["symbol"],
            "ticker":           info_dict["ticker"],
            "name":             info_dict["name"],
            "market":           info_dict["market"],
            "industry":         info_dict["industry"],
            "sector":           info_dict["sector"],
            "price":            price,
            "mkt_cap":          None,
            "is_reit":          _is_reit(info_dict["symbol"]),
            "ret_1d":           ret_1d,
            "ret_1w":           ret_1w,
            "ret_1m":           ret_1m,
            "ret_3m":           ret_3m,
            "ret_6m":           ret_6m,
            "ret_1y":           ret_1y,
            "ret_ytd":          ret_ytd,
            "ema20":            ema20,
            "ema50":            ema50,
            "ema200":           ema200,
            "above_ema20":      above_ema20,
            "above_ema50":      above_ema50,
            "above_ema200":     above_ema200,
            "ema200_slope_pct": ema200_slope_pct,
            "rs_raw":           rs_raw,
            "rs_raw_4w":        rs_raw_4w,
            "rs_score":         None,
            "rs_score_4w":      None,
            "rs_momentum":      None,
            "stage":            None,
            "atr14_pct":        atr14_pct,
            "vol_avg20":        vol_20,
            "vol_today":        vol_today,
            "high_52w":         round(float(close.iloc[:-1].tail(252).max()), 2),
            "low_52w":          round(float(close.iloc[:-1].tail(252).min()), 2),
            "ath":              round(_ath, 2),
            "ath_pct":          round((price - _ath) / _ath * 100, 2) if _ath > 0 else None,
            "pe":               None,
            "pbv":              None,
            "div_yield":        None,
            "price_history":    price_history,
            "vol_history":      vol_history,
        }
    except Exception:
        return None


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


# ============================================================
# 5. RS Rank / Group summaries / Sanitize
# ============================================================

def rank_rs(stocks):
    # rank rs_score ปัจจุบัน
    valid = [s for s in stocks if s.get("rs_raw") is not None]
    valid.sort(key=lambda x: x["rs_raw"])
    n = len(valid)
    for i, s in enumerate(valid):
        s["rs_score"] = int(round(i / n * 99))

    # rank rs_score_4w และคำนวณ rs_momentum
    valid4w = [s for s in stocks if s.get("rs_raw_4w") is not None]
    valid4w.sort(key=lambda x: x["rs_raw_4w"])
    n4w = len(valid4w)
    for i, s in enumerate(valid4w):
        s["rs_score_4w"] = int(round(i / n4w * 99))
    for s in stocks:
        if s.get("rs_score") is not None and s.get("rs_score_4w") is not None:
            s["rs_momentum"] = s["rs_score"] - s["rs_score_4w"]

    # Weinstein Stage จาก above_ema200 + ema200_slope_pct
    for s in stocks:
        above = s.get("above_ema200")
        slope = s.get("ema200_slope_pct")
        if above is None or slope is None:
            s["stage"] = None
            continue
        if above and slope >= 0:
            s["stage"] = 2   # Advancing
        elif above and slope < 0:
            s["stage"] = 3   # Distribution / Topping
        elif not above and slope >= -1.5:
            s["stage"] = 1   # Basing
        else:
            s["stage"] = 4   # Declining

    return stocks


def summarize_groups(stocks, key):
    from collections import defaultdict
    groups = defaultdict(list)
    for s in stocks:
        groups[s.get(key, "Unknown")].append(s)

    result = []
    for name, members in groups.items():
        def avg(f):
            vals = [m[f] for m in members if m.get(f) is not None]
            return round(sum(vals) / len(vals), 2) if vals else None

        result.append({
            "name":             name,
            "count":            len(members),
            "ret_1d":           avg("ret_1d"),
            "ret_1w":           avg("ret_1w"),
            "ret_1m":           avg("ret_1m"),
            "ret_3m":           avg("ret_3m"),
            "ret_6m":           avg("ret_6m"),
            "ret_1y":           avg("ret_1y"),
            "avg_rs":           avg("rs_score"),
            "pct_above_ema50":  round(
                sum(1 for m in members if m.get("above_ema50")) / len(members) * 100
            ),
            "avg_pe":           avg("pe"),
            "avg_pbv":          avg("pbv"),
            "avg_div_yield":    avg("div_yield"),
        })
    return sorted(result, key=lambda x: x.get("ret_1m") or -999, reverse=True)


def sanitize(obj):
    import math
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(i) for i in obj]
    elif isinstance(obj, bool):
        return bool(obj)
    elif hasattr(obj, "item"):
        return obj.item()
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None  # Infinity/NaN ไม่ใช่ valid JSON
    elif obj is None or isinstance(obj, (int, float, str)):
        return obj
    else:
        return str(obj)


# ============================================================
# 5. _compute_market_internals + run_with_progress
# ============================================================

def _compute_market_internals(stocks, lookback_days=63, window_52w=252):
    """คำนวณ 52W New High/Low count ต่อวัน ย้อนหลัง lookback_days วัน
    ใช้ price_history จาก stocks list แทน set_history.json
    """
    try:
        series = {}
        for s in stocks:
            ph = s.get("price_history")
            if not ph or len(ph) < window_52w + lookback_days:
                continue
            dates  = [p[0] for p in ph]
            closes = [p[1] for p in ph]
            series[s.get("ticker", s.get("symbol", ""))] = (dates, closes)

        if not series:
            return {"dates": [], "new_highs": [], "new_lows": []}

        # หา trading dates ล่าสุด lookback_days วัน
        all_dates_set = set()
        for dates, _ in series.values():
            all_dates_set.update(dates[-lookback_days - 5:])
        recent_dates = sorted(all_dates_set)[-lookback_days:]

        new_highs, new_lows, date_labels = [], [], []
        for dt in recent_dates:
            nh = nl = 0
            for dates, closes in series.values():
                try:
                    idx = next(i for i, d in enumerate(dates) if d == dt)
                except StopIteration:
                    continue
                if idx < window_52w:
                    continue
                cur    = closes[idx]
                window = closes[idx - window_52w: idx]
                if cur >= max(window):
                    nh += 1
                elif cur <= min(window):
                    nl += 1
            new_highs.append(nh)
            new_lows.append(nl)
            date_labels.append(dt)

        return {"dates": date_labels, "new_highs": new_highs, "new_lows": new_lows}
    except Exception as e:
        print(f"[market_internals] คำนวณไม่สำเร็จ: {e}")
        return {"dates": [], "new_highs": [], "new_lows": []}


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

    callback(total, total, f"คำนวณ RS Rank ({len(stocks)} หุ้น)...")
    stocks = rank_rs(stocks)

    industries = summarize_groups(stocks, "industry")
    sectors    = summarize_groups(stocks, "sector")

    data_as_of = max(
        (d["close"].index[-1].strftime("%Y-%m-%d") for d in all_data.values() if len(d["close"]) > 0),
        default=None
    )

    # คำนวณ Market Internals: 52W New High/Low count ต่อวัน ย้อนหลัง 63 วัน
    market_internals = _compute_market_internals(stocks)

    output = {
        "updated_at":       datetime.now(_ICT).strftime("%Y-%m-%d %H:%M:%S"),
        "update_type":      "Full Refresh",
        "data_as_of":       data_as_of,
        "total":            len(stocks),
        "stocks":           stocks,
        "industries":       industries,
        "sectors":          sectors,
        "market_internals": market_internals,
    }

    out_path = os.path.join(base_dir, OUT_FILE)
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(sanitize(output), f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, out_path)

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

    callback(total, total, "คำนวณ RS Rank...")
    stocks     = rank_rs(stocks)
    industries = summarize_groups(stocks, "industry")
    sectors    = summarize_groups(stocks, "sector")

    data_as_of = max(
        (data["dates"][-1] for data in history["stocks"].values() if data.get("dates")),
        default=None
    )
    output = {
        "updated_at":  datetime.now(_ICT).strftime("%Y-%m-%d %H:%M:%S"),
        "update_type": "Quick Update",
        "data_as_of":  data_as_of,
        "total":       len(stocks),
        "stocks":      stocks,
        "industries":  industries,
        "sectors":     sectors,
    }
    out_path = os.path.join(base_dir, OUT_FILE)
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(sanitize(output), f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, out_path)

    callback(total, total,
             f"Quick Update เสร็จ! {len(stocks)} หุ้น (ดาวน์โหลดใหม่ {len(new_data)} หุ้น)")


# ============================================================
# 7. Standalone (python set_data_fetcher.py)
# ============================================================

def main():
    print("=" * 55)
    print("  SET Data Fetcher v3  (Batch Download)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55 + "\n")

    base_dir = os.path.dirname(os.path.abspath(__file__))

    def cb(current, total, msg):
        if total > 0:
            pct = int(current / total * 100)
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            print(f"\r  [{bar}] {pct:3d}%  {msg}          ", end="", flush=True)
        else:
            print(f"  {msg}")

    print()
    run_with_progress(cb, base_dir)
    print("\n\n✅ เสร็จแล้ว! ดู set_data.json")


if __name__ == "__main__":
    main()
