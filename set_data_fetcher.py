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
from datetime import datetime

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

# สูตรคำนวณทั้งหมดอยู่ที่ core/metrics.py (single source of truth)
# re-export ที่นี่เพื่อ backward compatibility กับ app.py และ tests
from core.metrics import (                       # noqa: E402
    calc_return as _calc_return,
    calc_ema as _calc_ema,
    calc_rs_raw,
    validate_stocks,
    rank_rs,
    summarize_groups,
)


# ============================================================
# 1. อ่านรายชื่อหุ้นจากไฟล์ SET
# ============================================================

def _try_download_xls(path):
    """ดาวน์โหลด XLS ใหม่จาก SET.or.th พร้อม backup/restore ถ้าไม่สำเร็จ"""
    import urllib.request, ssl, shutil
    url = "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls"
    backup = path + ".bak"
    # backup ไฟล์เดิม
    if os.path.exists(path):
        shutil.copy2(path, backup)
    try:
        ctx = ssl._create_unverified_context()
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Referer":    "https://www.set.or.th/en/market/product/stock/quote/",
            "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            data = r.read()
        # ตรวจสอบขนาด — ไฟล์จริงต้องใหญ่กว่า 50KB
        if len(data) < 50_000:
            raise ValueError(f"ไฟล์เล็กเกินไป ({len(data)} bytes) — น่าจะเป็น error page")
        # ตรวจสอบว่ามี table จริง
        import io
        test = pd.read_html(io.BytesIO(data), header=None)
        if not test:
            raise ValueError("ไม่พบตารางในไฟล์ที่ดาวน์โหลด")
        # ผ่านทุก check — บันทึกไฟล์ใหม่
        with open(path, "wb") as f:
            f.write(data)
        print(f"[XLS] อัพเดทสำเร็จ ({len(data):,} bytes)")
        if os.path.exists(backup):
            os.remove(backup)
    except Exception as e:
        print(f"[XLS] ดาวน์โหลดไม่สำเร็จ ({e}) — ใช้ไฟล์เดิม")
        # restore backup
        if os.path.exists(backup):
            shutil.copy2(backup, path)
            os.remove(backup)


def load_set_symbols(base_dir=None):
    path = os.path.join(base_dir, XLS_FILE) if base_dir else XLS_FILE
    # ลองดาวน์โหลดใหม่ทุกครั้ง (มี backup ป้องกัน)
    _try_download_xls(path)
    # fallback: ถ้าไม่มี .xls ให้หา .xlsx
    if not os.path.exists(path):
        alt = path.replace(".xls", ".xlsx")
        if os.path.exists(alt):
            path = alt
        else:
            raise FileNotFoundError(
                f"ไม่พบไฟล์ {path}\n"
                "โหลดจาก: https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls"
            )

    # ลอง read_html ก่อน (ไฟล์ .xls จาก SET.or.th เป็น HTML table)
    # ถ้าไม่ได้ให้ลอง .xlsx ด้วย read_excel
    tables = None
    try:
        tables = pd.read_html(path, header=None)
        if not tables:
            raise ValueError("no tables")
    except Exception:
        pass

    if tables is None:
        # ลอง xlsx fallback
        xlsx_path = path.replace(".xls", ".xlsx") if path.endswith(".xls") else None
        if xlsx_path and os.path.exists(xlsx_path):
            df_raw = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
        else:
            df_raw = pd.read_excel(path, header=None, engine="openpyxl")
        tables = [df_raw]

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

def _atomic_write_json(path, obj):
    """เขียน JSON แบบ atomic: เขียนลง .tmp ก่อนแล้ว os.replace ทับ
    ป้องกันไฟล์เสียหายถ้า process ตาย/ไฟดับกลางคัน"""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _check_stock_count(base_dir, new_count, min_ratio=0.8):
    """กันการเขียน set_data.json ทับด้วย universe ที่หดผิดปกติ
    (เช่น Yahoo ล่มทำให้ดึงได้ไม่กี่ตัว) — raise เพื่อให้ caller restore backup"""
    old_total = 0
    try:
        with open(os.path.join(base_dir, OUT_FILE), encoding="utf-8") as f:
            old_total = json.load(f).get("total", 0) or 0
    except Exception:
        return  # ไม่มีไฟล์เดิม/อ่านไม่ได้ — เขียนได้เลย
    if old_total > 0 and new_count < old_total * min_ratio:
        raise ValueError(
            f"ดึงข้อมูลได้แค่ {new_count}/{old_total} หุ้น (<{int(min_ratio*100)}%) — "
            f"อาจเกิดจากแหล่งข้อมูลล่ม จึงไม่บันทึกทับข้อมูลเดิม"
        )


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
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stocks": stocks_hist,
    }
    path = os.path.join(base_dir, HISTORY_FILE)
    _atomic_write_json(path, new_hist)
    return new_hist


# ============================================================
# 2. คำนวณ metrics จาก Series ที่ดาวน์โหลดมาแล้ว
# ============================================================

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

        _rs = calc_rs_raw(ret_1m, ret_3m, ret_6m, ret_1y)
        rs_raw = round(_rs, 4) if _rs is not None else None

        # rs_raw คำนวณ ณ 4 สัปดาห์ก่อน (ใช้ rank ต่างหาก → rs_momentum)
        rs_raw_4w = None
        if len(close) >= 272:  # 21+21+63+126+250 min
            c4w = close.iloc[:-21]
            _rs4 = calc_rs_raw(_calc_return(c4w, 21), _calc_return(c4w, 63),
                               _calc_return(c4w, 126), _calc_return(c4w, 250))
            if _rs4 is not None:
                rs_raw_4w = round(_rs4, 4)

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

        # price_history: เก็บ 500 วันทำการ (~2 ปี) เพื่อให้ EMA200 converge ได้ถูกต้อง
        # (EMA200 ต้องการ warmup ~300 แท่งหลัง seed จึงจะ converge 97%)
        _hist_bars     = min(len(close), 500)
        _display_bars  = min(len(close), 260)   # chart แสดง 1 ปี
        # หุ้นต่ำกว่า 1 บาท เก็บ 4 ทศนิยม — ปัด 2 ตำแหน่งทำให้ tick เดียวกลายเป็น
        # return ปลอม ±33% (เช่นหุ้น 0.03 บาท) แล้วลาก RS Score เพี้ยน
        price_history  = [
            [d.strftime("%Y-%m-%d"), round(float(p), 4 if p < 1 else 2)]
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
