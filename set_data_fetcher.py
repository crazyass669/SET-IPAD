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

# สูตรคำนวณทั้งหมดอยู่ที่ core/metrics.py (single source of truth)
# re-export ที่นี่เพื่อ backward compatibility กับ app.py และ tests
# Persistence — แยกไว้ที่ core/store.py (Phase 3 refactor)
from core.store import (OUT_FILE, HISTORY_FILE, _atomic_write_json,
                        _check_stock_count, load_history, _merge_history,
                        save_history)

# Yahoo batch downloader — แยกไว้ที่ sources/yahoo.py (Phase 2 refactor)
from sources.yahoo import (fetch_all_batch, fetch_gap_batch,
                           fetch_market_caps_parallel, BATCH_SIZE)

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
    from services.refresh import run_with_progress
    run_with_progress(cb, base_dir)
    print("\n\n✅ เสร็จแล้ว! ดู set_data.json")


if __name__ == "__main__":
    main()
