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
# ไฟล์ฝั่งไทยจาก SET.or.th — คอลัมน์เรียงเหมือนไฟล์อังกฤษเป๊ะ (symbol ตรงกัน 100%)
# ต่างแค่หัวตาราง/ชื่อบริษัท/กลุ่มอุตสาหกรรม/หมวดธุรกิจเป็นภาษาไทย + encoding TIS-620
# ใช้เสริม "ชื่อบริษัทภาษาไทย" (name_th) และเป็นแหล่งสำรองถ้าไฟล์อังกฤษโหลดไม่ได้
# หน้าเว็บ SET: https://www.set.or.th/th/market/information/securities-list/main
XLS_FILE_TH  = "listedCompanies_th_TH.xls"
_TH_ENCODING = "cp874"

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

def _try_download_xls(path, encoding=None):
    """ดาวน์โหลด XLS ใหม่จาก SET.or.th พร้อม backup/restore ถ้าไม่สำเร็จ
    ชื่อไฟล์ปลายทาง (basename ของ path) ใช้เป็นชื่อไฟล์บน SET ตรงๆ — รองรับทั้ง
    listedCompanies_en_US.xls (หลัก) และ listedCompanies_th_TH.xls (ชื่อบริษัทไทย)
    encoding: ไฟล์ฝั่งไทยเป็น TIS-620/cp874 ไม่ใช่ utf-8 — ต้องส่งมาตอน validate
    ไม่งั้น pandas เดาผิดแล้วได้ตัวอักษรเพี้ยน (ยังไม่ error แต่กันไว้ก่อน)"""
    import urllib.request, shutil
    fname = os.path.basename(path)
    lang  = "th" if "_th_TH" in fname else "en"
    from core.net import ssl_context
    url = f"https://www.set.or.th/dat/eod/listedcompany/static/{fname}"
    backup = path + ".bak"
    # backup ไฟล์เดิม
    if os.path.exists(path):
        shutil.copy2(path, backup)
    try:
        ctx = ssl_context()
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Referer":    f"https://www.set.or.th/{lang}/market/product/stock/quote/",
            "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            data = r.read()
        # ตรวจสอบขนาด — ไฟล์จริงต้องใหญ่กว่า 50KB
        if len(data) < 50_000:
            raise ValueError(f"ไฟล์เล็กเกินไป ({len(data)} bytes) — น่าจะเป็น error page")
        # ตรวจสอบว่ามี table จริง
        import io
        test = pd.read_html(io.BytesIO(data), header=None, encoding=encoding)
        if not test:
            raise ValueError("ไม่พบตารางในไฟล์ที่ดาวน์โหลด")
        # ผ่านทุก check — บันทึกไฟล์ใหม่
        with open(path, "wb") as f:
            f.write(data)
        print(f"[XLS] อัพเดทสำเร็จ: {fname} ({len(data):,} bytes)")
        if os.path.exists(backup):
            os.remove(backup)
        return True
    except Exception as e:
        print(f"[XLS] ดาวน์โหลดไม่สำเร็จ: {fname} ({e}) — ใช้ไฟล์เดิม")
        # restore backup
        if os.path.exists(backup):
            shutil.copy2(backup, path)
            os.remove(backup)
        return False


# หัวตารางไฟล์ไทยเป็นคนละคำกับไฟล์อังกฤษทั้งแถว — ใช้จับคอลัมน์ในโหมด fallback
# (คีย์ = ชื่อ field ในระบบ, ค่า = คำที่ต้องเจอในหัวคอลัมน์ไทย)
_TH_COL_KEYS = {
    "symbol":   "หลักทรัพย์",
    "name":     "บริษัท",
    "market":   "ตลาด",
    "industry": "กลุ่มอุตสาหกรรม",
    "sector":   "หมวดธุรกิจ",
}

# ไทย -> อังกฤษ สำหรับโหมด fallback (อ่านไฟล์ไทยแทนไฟล์อังกฤษที่โหลดไม่ได้)
# ต้องแปลงกลับเป็นอังกฤษเสมอ เพราะ industry/sector ถูกใช้เป็น "คีย์" จับคู่กับดัชนีกลุ่ม
# ^XXX.BK และ mapping อื่นทั่วระบบ (ปล่อยเป็นไทยจะทำให้กลุ่มแตกเป็นคนละก้อนเงียบๆ)
_TH_INDUSTRY_EN = {
    "เกษตรและอุตสาหกรรมอาหาร":  "Agro & Food Industry",
    "สินค้าอุปโภคบริโภค":        "Consumer Products",
    "ธุรกิจการเงิน":             "Financials",
    "สินค้าอุตสาหกรรม":          "Industrials",   # ฝั่ง mai ใช้ "Industrial" — ปรับตาม market ตอนแปลง
    "อสังหาริมทรัพย์และก่อสร้าง": "Property & Construction",
    "ทรัพยากร":                  "Resources",
    "บริการ":                    "Services",
    "เทคโนโลยี":                 "Technology",
}

_TH_SECTOR_EN = {
    "กระดาษและวัสดุการพิมพ์":     "Paper & Printing Materials",
    "กองทุนรวมอสังหาริมทรัพย์และกองทรัสต์เพื่อการลงทุนในอสังหาริมทรัพย์": "Property Fund & REITs",
    "การท่องเที่ยวและสันทนาการ":   "Tourism & Leisure",
    "การแพทย์":                   "Health Care Services",
    "ขนส่งและโลจิสติกส์":          "Transportation & Logistics",
    "ของใช้ส่วนตัวและเวชภัณฑ์":    "Personal Products & Pharmaceuticals",
    "ของใช้ในครัวเรือนและสำนักงาน": "Home & Office Products",
    "ชิ้นส่วนอิเล็กทรอนิกส์":       "Electronic Components",
    "ธนาคาร":                     "Banking",
    "ธุรกิจการเกษตร":              "Agribusiness",
    "บรรจุภัณฑ์":                  "Packaging",
    "บริการรับเหมาก่อสร้าง":       "Construction Services",
    "บริการเฉพาะกิจ":              "Professional Services",
    "ประกันภัยและประกันชีวิต":      "Insurance",
    "ปิโตรเคมีและเคมีภัณฑ์":        "Petrochemicals & Chemicals",
    "พลังงานและสาธารณูปโภค":       "Energy & Utilities",
    "พัฒนาอสังหาริมทรัพย์":        "Property Development",
    "พาณิชย์":                     "Commerce",
    "ยานยนต์":                     "Automotive",
    "วัสดุก่อสร้าง":                "Construction Materials",
    "วัสดุอุตสาหกรรมและเครื่องจักร": "Industrial Materials & Machinery",
    "สื่อและสิ่งพิมพ์":             "Media & Publishing",
    "อาหารและเครื่องดื่ม":          "Food & Beverage",
    "เงินทุนและหลักทรัพย์":         "Finance & Securities",
    "เทคโนโลยีสารสนเทศและการสื่อสาร": "Information & Communication Technology",
    "เหล็ก และ ผลิตภัณฑ์โลหะ":      "Steel and Metal Products",
    "แฟชั่น":                      "Fashion",
}


def _read_listed_table(path, encoding=None):
    """อ่านไฟล์ listedCompanies_*.xls -> DataFrame ที่ตั้งหัวคอลัมน์แล้ว
    ไฟล์จาก SET.or.th จริงๆ เป็น HTML table (ไม่ใช่ xls ไบนารี) — read_html ก่อน
    แล้วค่อย fallback ไป read_excel เผื่อผู้ใช้เอา .xlsx จริงมาวางเอง
    หาแถวหัวตารางจาก keyword ทั้งฝั่งอังกฤษ (symbol+market/company) และฝั่งไทย
    (หลักทรัพย์+ตลาด/บริษัท) — ไฟล์ไทยหัวตารางเป็นภาษาไทยทั้งแถว"""
    tables = None
    try:
        tables = pd.read_html(path, header=None, encoding=encoding)
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

    for t in tables:
        for i, row in t.iterrows():
            row_str = " ".join(str(v).lower() for v in row.values)
            is_en = "symbol" in row_str and ("market" in row_str or "company" in row_str)
            # ฝั่งไทยต้องเทียบทั้งเซลล์แบบเป๊ะ ไม่ใช่ substring ของทั้งแถว — แถวชื่อเรื่อง
            # ("รายชื่อบริษัทจดทะเบียนในตลาดหลักทรัพย์แห่งประเทศไทย") มีทั้งคำว่า "หลักทรัพย์"
            # และ "ตลาด" ซ่อนอยู่ในประโยค จะถูกจับเป็นหัวตารางแทนแถวหัวจริงที่อยู่ถัดลงมา
            cells = [str(v).strip() for v in row.values]
            is_th = ("หลักทรัพย์" in cells) and ("ตลาด" in cells or "บริษัท" in cells)
            if is_en or is_th:
                t.columns = t.iloc[i]
                t = t.iloc[i + 1:].reset_index(drop=True)
                t.columns = [str(c).strip() for c in t.columns]
                return t
    raise ValueError("ไม่พบตารางรายชื่อหุ้นในไฟล์ HTML")


def _load_thai_listed(base_dir=None, download=True):
    """อ่านไฟล์ไทย -> {symbol: {"name_th","industry_th","sector_th"}}
    ใช้ 2 ทาง: (ก) เสริมชื่อบริษัทไทยให้ไฟล์อังกฤษ (ปกติ) (ข) เป็นแหล่งหลักในโหมด
    fallback ตอนไฟล์อังกฤษโหลดไม่ได้เลย — ล้มเหลวได้ไม่กระทบงานหลัก (คืน {} เฉยๆ)"""
    path = os.path.join(base_dir, XLS_FILE_TH) if base_dir else XLS_FILE_TH
    try:
        if download:
            _try_download_xls(path, encoding=_TH_ENCODING)
        if not os.path.exists(path):
            return {}
        df = _read_listed_table(path, encoding=_TH_ENCODING)
        cols = {}
        for key, kw in _TH_COL_KEYS.items():
            for col in df.columns:
                if kw in col:
                    cols[key] = col
                    break
        if "symbol" not in cols:
            return {}
        out = {}
        for _, row in df.iterrows():
            sym = str(row.get(cols["symbol"], "")).strip().upper()
            if not sym or sym in ("NAN", "หลักทรัพย์"):
                continue
            def _get(k):
                v = str(row.get(cols.get(k, ""), "")).strip()
                return "" if v in ("nan", "-", "N/A") else v
            out[sym] = {"name_th":     _get("name"),
                        "industry_th": _get("industry"),
                        "sector_th":   _get("sector"),
                        "market":      _get("market")}
        return out
    except Exception as e:
        print(f"[XLS-TH] อ่านไฟล์ไทยไม่สำเร็จ ({e}) — ข้ามชื่อไทย ใช้ชื่ออังกฤษอย่างเดียว")
        return {}


def _symbols_from_thai(thai_rows):
    """สร้างรายชื่อหุ้นจากไฟล์ไทยล้วน — ใช้เฉพาะโหมด fallback (ไฟล์อังกฤษโหลดไม่ได้
    และไม่มีไฟล์เดิมในเครื่อง) แปลง industry/sector กลับเป็นอังกฤษเสมอเพื่อให้จับคู่
    ดัชนีกลุ่ม ^XXX.BK ได้เหมือนเดิม · ตัวที่แปลไม่ออก (SET เพิ่มหมวดใหม่) ตกเป็น
    "Unknown" ตามพฤติกรรมเดิมของช่องว่าง ไม่ปล่อยชื่อไทยหลุดไปทำให้กลุ่มแตก"""
    symbols = []
    for sym, d in thai_rows.items():
        market = d.get("market", "")
        if market not in ("SET", "mai", ""):
            continue
        ind_th, sec_th = d.get("industry_th", ""), d.get("sector_th", "")
        clean_industry = _TH_INDUSTRY_EN.get(ind_th, "Unknown")
        # ฝั่ง mai ใช้ "Industrial" (ไม่มี s) ต่างจาก SET ที่เป็น "Industrials"
        if market == "mai" and clean_industry == "Industrials":
            clean_industry = "Industrial"
        clean_sector = _TH_SECTOR_EN.get(sec_th) if sec_th else None
        if clean_sector is None:
            clean_sector = (clean_industry + " -mai") if market == "mai" else "Unknown"
        if market == "mai":
            clean_industry = clean_industry + " -mai"
        symbols.append({
            "symbol":   sym,
            "ticker":   f"{sym}.BK",
            "name":     d.get("name_th") or sym,   # ไม่มีชื่ออังกฤษให้ใช้ในโหมดนี้
            "name_th":  d.get("name_th", ""),
            "market":   market,
            "industry": clean_industry,
            "sector":   clean_sector,
        })
    return symbols


def load_set_symbols(base_dir=None):
    path = os.path.join(base_dir, XLS_FILE) if base_dir else XLS_FILE
    # ลองดาวน์โหลดใหม่ทุกครั้ง (มี backup ป้องกัน)
    _try_download_xls(path)
    # ไฟล์ไทย: ดึงคู่กันเสมอเพื่อเอา "ชื่อบริษัทภาษาไทย" มาเสริม (ไม่ critical —
    # ล้มเหลวได้โดยไม่กระทบ) และเป็นแหล่งสำรองถ้าไฟล์อังกฤษใช้ไม่ได้เลย
    thai_rows = _load_thai_listed(base_dir)

    # fallback: ถ้าไม่มี .xls ให้หา .xlsx
    if not os.path.exists(path):
        alt = path.replace(".xls", ".xlsx")
        if os.path.exists(alt):
            path = alt
        elif thai_rows:
            # ไฟล์อังกฤษหายทั้งคู่แต่ไฟล์ไทยยังโหลดได้ — เดินต่อด้วยข้อมูลไทย ดีกว่า
            # ล้มทั้งรอบ (แปลง industry/sector กลับเป็นอังกฤษให้แล้วใน _symbols_from_thai)
            print(f"[XLS] ไม่พบ {os.path.basename(path)} — ใช้ไฟล์ไทย {XLS_FILE_TH} แทนทั้งชุด")
            return _symbols_from_thai(thai_rows)
        else:
            raise FileNotFoundError(
                f"ไม่พบไฟล์ {path}\n"
                "โหลดจาก: https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls\n"
                "หรือหน้าเว็บ SET: https://www.set.or.th/th/market/information/securities-list/main"
            )

    try:
        df = _read_listed_table(path)
    except Exception as e:
        if thai_rows:
            print(f"[XLS] อ่าน {os.path.basename(path)} ไม่สำเร็จ ({e}) — ใช้ไฟล์ไทย {XLS_FILE_TH} แทนทั้งชุด")
            return _symbols_from_thai(thai_rows)
        raise

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
        # mai ใช้ชื่อ industry group เดียวกับ SET (เช่น "Technology" ทั้งคู่ ยกเว้น
        # "Industrial"/"Industrials" ที่ต่างกันโดยบังเอิญ) — ถ้าไม่ต่อ "-mai" เหมือน sector
        # ด้านบน หุ้น mai จะถูกนับรวมเข้ากลุ่ม industry ระดับ SET (ที่มีดัชนี ^XXX.BK ของตัวเอง
        # ซึ่งไม่รวมหุ้น mai จริง) ทำให้ count/return ของกลุ่ม industry เพี้ยนทั้งสองฝั่ง
        if market == "mai":
            clean_industry = clean_industry + " -mai"
        symbols.append({
            "symbol":   sym,
            "ticker":   f"{sym}.BK",
            "name":     name     if name     not in _blank else sym,
            # ชื่อไทยจากไฟล์ th_TH (symbol ตรงกัน 100% กับไฟล์อังกฤษ) — ค่าว่างถ้าโหลด
            # ไฟล์ไทยไม่ได้ ฝั่ง UI ตกกลับไปใช้ชื่ออังกฤษเองเมื่อไม่มีค่า
            "name_th":  (thai_rows.get(sym) or {}).get("name_th", ""),
            "market":   market,
            "industry": clean_industry,
            "sector":   clean_sector,
        })

    return symbols


# กองทุนรวม / REIT / Infra Fund ใน SET — จับจากชื่อบริษัท + sector
# (เดิมเดาจาก suffix ของ symbol อย่างเดียว ซึ่งโลภเกิน: `PF$` กิน CPF/UPF/PF,
#  `RT$` กิน JMART/KAMART/SAMART/DRT/TRT/PORT/RT, `BT$` กิน CIMBT, `^M-` กิน M-CHAI
#  — และยังพลาด REIT ที่ symbol ไม่ลงท้าย suffix เช่น AMATAR/BOFFICE/IMPACT/TPRIME)
import re as _re
_FUND_NAME_PAT = _re.compile(r'\b(REIT|FUND|TRUST)\b', _re.IGNORECASE)

def _is_reit(info_dict: dict) -> bool:
    if (info_dict.get("sector") or "") == "Property Fund & REITs":
        return True
    return bool(_FUND_NAME_PAT.search(info_dict.get("name") or ""))


# ============================================================
# 2. คำนวณ metrics จาก Series ที่ดาวน์โหลดมาแล้ว
# ============================================================

def process_stock(info_dict, close, volume, high=None, low=None):
    """คำนวณ metrics จาก close/volume Series — ไม่ดึงข้อมูลเพิ่ม
    high/low (optional): ถ้าให้มา คำนวณ ATR จริงจาก true range แทน close-only approximation"""
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

        # เดิมวนลูป Python (zip(dates, close) + list comp) ทั้งประวัติราคา — ช้ามาก
        # เพราะ boxing DatetimeIndex ทีละแท่ง (วัดจริง ~2 วิ ทั้งกระดาน) ใช้ dates.year
        # (vectorized ระดับ numpy) + boolean mask แทน ได้ผลเหมือนเดิมทุกกรณี เร็วกว่ามาก
        current_year = datetime.now().year
        ytd_mask = dates.year == current_year
        ret_ytd = None
        if ytd_mask.any():
            first_price = float(close.iloc[ytd_mask].iloc[0])
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

        # ATR14: ถ้ามี OHLC ใช้ true range จริง (max ของ high-low, |high-prevClose|, |low-prevClose|)
        # — จับ gap + ช่วงแกว่งระหว่างวันที่ close-to-close มองไม่เห็น · ถ้าไม่มี OHLC fallback
        # เป็น approximation เดิม (avg |%เปลี่ยนรายวัน|)
        atr14_pct = None
        _has_ohlc = (high is not None and low is not None
                     and len(high) == len(close) and high.notna().any() and low.notna().any())
        if _has_ohlc and len(close) >= 15:
            # true range ต้องการแค่ 14 แท่งท้าย (+1 แท่งก่อนหน้าไว้ shift) — ตัด slice
            # ก่อนคำนวณ แทนที่จะ concat/abs ทั้งประวัติ (หลายพันแท่ง) แล้วค่อย tail ทีหลัง
            # ผลลัพธ์เหมือนเดิมทุกกรณี (14 แท่งท้ายพึ่ง prev_c จาก 15 แท่งท้ายเท่านั้น)
            h15, l15, c15 = high.tail(15), low.tail(15), close.tail(15)
            prev_c = c15.shift(1)
            tr = pd.concat([(h15 - l15), (h15 - prev_c).abs(), (l15 - prev_c).abs()],
                           axis=1).max(axis=1)
            atr = tr.tail(14).mean()
            if atr == atr and price > 0:   # ไม่ใช่ NaN
                atr14_pct = round(float(atr / price * 100), 3)
        elif len(close) >= 15:
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
            # ชื่อบริษัทภาษาไทย (จาก listedCompanies_th_TH.xls) — ใช้ค้นหา/แสดงคู่ชื่ออังกฤษ
            # .get() เพราะ info_dict อาจมาจากแหล่งเก่าที่ยังไม่มีคีย์นี้ (cache/ทดสอบ)
            "name_th":          info_dict.get("name_th", ""),
            "market":           info_dict["market"],
            "industry":         info_dict["industry"],
            "sector":           info_dict["sector"],
            "price":            price,
            "mkt_cap":          None,
            "is_reit":          _is_reit(info_dict),
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
