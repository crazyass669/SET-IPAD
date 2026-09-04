# -*- coding: utf-8 -*-
"""
sources/set_market_stats_monthly.py

อ่าน "Market_Statistics_Month_th_TH.xls" (ไฟล์สรุปสถิติตลาดรายเดือน จาก
set.or.th -> Market Data -> Statistics -> Market Statistics) แล้ว merge เข้า
set_market_stats.json ที่มีอยู่แล้ว (upsert รายเดือน ไม่ rebuild ทับ)

ต่างจาก Table_PE.xls/Table_PBV.xls (ดู import_market_stats.py) ตรงที่ไฟล์นี้
เป็น "สะสมรายปี" — 1 ไฟล์ = 1 ปี พ.ศ., คอลัมน์ = ม.ค. ถึงเดือนล่าสุดของปีนั้น
เท่านั้น ไม่มีประวัติย้อนหลังยาวเหมือน Table_PE/PBV.xls (ย้อนได้ถึงปี 1975)
เพราะงั้นต้อง "เติม" เข้าประวัติเดิมทีละเดือน ไม่ใช่สร้างใหม่ทับหมด

ไฟล์นี้ไม่มี P/E-P/BV แยกรายกลุ่มดัชนีย่อย (SET50/SET100/sSET/...) เหมือน
Table_PE/PBV.xls มีแค่ P/E-P/BV ระดับตลาด SET รวม กับ mai รวม — ตรวจสอบแล้วว่า
หน้าเว็บ Valuation ใช้แค่ 2 ค่านี้จริง (key "SET"/"mai") ไม่เคยอ้างอิงกลุ่มย่อย
เลย จึงไม่กระทบของเดิม แถมไฟล์นี้มีข้อมูลเสริมที่ Table_PE/PBV.xls ไม่มี:
อัตราเงินปันผลตอบแทน, มูลค่าหลักทรัพย์ตามราคาตลาด, จำนวนบริษัทจดทะเบียน/
เข้าใหม่/ถูกเพิกถอน (breadth)
"""
import bisect
import re
from datetime import date as _date

import numpy as np
import pandas as pd

THAI_MONTH = {
    "ม.ค.": 1, "ก.พ.": 2, "มี.ค.": 3, "เม.ย.": 4, "พ.ค.": 5, "มิ.ย.": 6,
    "ก.ค.": 7, "ส.ค.": 8, "ก.ย.": 9, "ต.ค.": 10, "พ.ย.": 11, "ธ.ค.": 12,
}

# แถวในไฟล์มีเลขอ้างอิงเชิงอรรถติดท้าย label เช่น "P/E ของตลาด3,4/" — ตัดทิ้งก่อน match
_FOOTNOTE_RE = re.compile(r"[\d,]+/\s*$")

_ROW_LABELS = {
    "P/E ของตลาด": "pe",
    "P/BV ของตลาด": "pbv",
    "อัตราเงินปันผลตอบแทนของตลาด (%)": "div_yield",
    "จำนวนบริษัทจดทะเบียน": "listed",
    "บริษัทที่เข้าจดทะเบียนใหม่": "new_listed",
    "บริษัทจดทะเบียนที่ถูกเพิกถอน": "delisted",
}
# แถวลูกใต้หัวข้อ "มูลค่าหลักทรัพย์" — ไม่มีเชิงอรรถติดท้ายตัวเอง match ตรงๆ ได้เลย
_MKT_CAP_LABEL = "- ตามราคาตลาด (ล้านบาท)"


def _strip_footnote(label):
    return _FOOTNOTE_RE.sub("", str(label).strip()).strip()


def _num(cell):
    s = str(cell).strip()
    if s in ("", "-", "nan", "None"):
        return None
    s = s.replace(",", "")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _int(v):
    return None if v is None else int(round(v))


def parse_annual_market_statistics(path):
    """คืน (records, year_ad)
    records: {"YYYY-MM": {"SET": {pe,pbv,div_yield,listed,new_listed,delisted,mkt_cap}, "mai": {...}}}
    """
    tables = pd.read_html(path, header=None)
    title = str(tables[0].iloc[0, 0])
    m = re.search(r"(\d{4})", title)
    if not m:
        raise ValueError(f"หาปีจากหัวไฟล์ไม่เจอ: {title!r}")
    year_ad = int(m.group(1)) - 543  # พ.ศ. -> ค.ศ.
    # กันเคสโหลดไฟล์เวอร์ชันภาษาอังกฤษ (หัวไฟล์เป็น ค.ศ.) มาใส่ชื่อไทย — ลบ 543 แล้วจะได้
    # ปี 1483 แล้ว upsert เดือนขยะเข้าไปต้นประวัติแบบเงียบๆ (ค่า P/E จริงแต่ผิดปี)
    if not (1975 <= year_ad <= _date.today().year + 1):
        raise ValueError(
            f"ปีในหัวไฟล์ผิดปกติ: {title!r} -> ค.ศ. {year_ad} "
            "(ต้องเป็นไฟล์เวอร์ชันภาษาไทย th_TH ที่หัวไฟล์เป็น พ.ศ.)")

    df = tables[1]
    header = df.iloc[0]
    month_cols = {}  # col_idx -> month number (1-12)
    for col in range(2, df.shape[1]):
        mo = THAI_MONTH.get(str(header[col]).strip())
        if mo:
            month_cols[col] = mo
    if not month_cols:
        raise ValueError("หาคอลัมน์เดือน (ม.ค./ก.พ./...) ในไฟล์ไม่เจอ — รูปแบบไฟล์อาจเปลี่ยน")

    set_start = mai_start = None
    for i in range(1, len(df)):
        lbl = str(df.iloc[i, 1]).strip()
        if lbl == "ข้อมูลตลาด SET":
            set_start = i
        elif lbl == "ข้อมูลตลาด mai":
            mai_start = i
    if set_start is None or mai_start is None:
        raise ValueError("หาโซน 'ข้อมูลตลาด SET' / 'ข้อมูลตลาด mai' ในไฟล์ไม่เจอ — รูปแบบไฟล์อาจเปลี่ยน")

    def scan_section(start, end):
        out = {}
        for i in range(start, end):
            raw = str(df.iloc[i, 1]).strip()
            label = _strip_footnote(raw)
            key = _ROW_LABELS.get(label)
            # match บน label ที่ตัดเชิงอรรถแล้วเหมือนแถวอื่นๆ — ถ้า SET เติมเลขเชิงอรรถท้าย
            # "- ตามราคาตลาด (ล้านบาท)" (อย่างที่ทำกับ "P/E ของตลาด3,4/") การเทียบ raw ตรงๆ
            # จะพลาดเงียบๆ แล้ว mkt_cap ค้างค่าเดิมไปเรื่อยๆ โดยไม่มี error
            if key is None and label == _MKT_CAP_LABEL:
                key = "mkt_cap"
            if key is None:
                continue
            out[key] = {col: _num(df.iloc[i, col]) for col in month_cols}
        return out

    set_rows = scan_section(set_start, mai_start)
    mai_rows = scan_section(mai_start, len(df))

    records = {}
    for col, mo in month_cols.items():
        set_vals = {k: v.get(col) for k, v in set_rows.items()}
        mai_vals = {k: v.get(col) for k, v in mai_rows.items()}
        if set_vals.get("pe") is None and mai_vals.get("pe") is None:
            continue  # เดือนที่ยังไม่มีข้อมูลจริง (คอลัมน์ล่วงหน้าในอนาคต)
        ym = f"{year_ad:04d}-{mo:02d}"
        records[ym] = {"SET": set_vals, "mai": mai_vals}
    return records, year_ad


def calc_stats(values):
    v = [x for x in values if x is not None and not (isinstance(x, float) and np.isnan(x))]
    if not v:
        return {}
    arr = sorted(v)
    current = v[-1]
    avg = sum(v) / len(v)
    std = float(np.std(v))
    pct = round(sum(1 for x in arr if x <= current) / len(arr) * 100, 1)
    zscore = round((current - avg) / std, 2) if std else 0
    return {
        "current": round(current, 2), "min": round(min(arr), 2), "max": round(max(arr), 2),
        # np.median = เฉลี่ย 2 ตัวกลางเมื่อจำนวนงวดเป็นเลขคู่ (เดิม arr[len//2] คืนตัวบนเฉยๆ)
        "avg": round(avg, 2), "median": round(float(np.median(arr)), 2), "std": round(std, 2),
        "zscore": zscore, "percentile": pct,
        "bands": {
            "+3σ": round(avg + 3 * std, 2), "+2σ": round(avg + 2 * std, 2), "+1σ": round(avg + std, 2),
            "-1σ": round(avg - std, 2), "-2σ": round(avg - 2 * std, 2), "-3σ": round(avg - 3 * std, 2),
        },
    }


def _upsert_point(container, ym, values):
    dates = container["dates"]
    series = container["series"]
    if ym in dates:
        idx = dates.index(ym)
    else:
        # เดือนใหม่ที่ไม่มีค่าจริงเลย (ทุก field เป็น None) — อย่าแทรกแถวว่าง เดี๋ยว container นี้
        # จะงอกเดือน all-None ต่อท้ายเรื่อยๆ (เกิดเมื่อ SET เปลี่ยนชื่อ label เฉพาะแถวนี้ เช่น
        # div_yield แต่ pe/pbv ยัง parse ได้) แล้ว market-stats-meta จะเลื่อน monthly_date ไปเดือน
        # ที่ไม่มีข้อมูลปันผล/มูลค่าหลักทรัพย์จริง — UI โชว์ว่าข้อมูลสดทั้งที่เดือนท้ายๆ ว่าง
        if all(v is None for v in values.values()):
            return
        idx = bisect.bisect_left(dates, ym)
        dates.insert(idx, ym)
        for k in series:
            series[k].insert(idx, None)
    for k, v in values.items():
        if v is None:
            continue
        if k not in series:
            series[k] = [None] * len(dates)
        series[k][idx] = v


def merge_monthly(data, records):
    """อัพเดท data (โครงสร้าง set_market_stats.json) ด้วย records จาก
    parse_annual_market_statistics() — upsert ทีละเดือน ไม่แตะประวัติเก่าที่มาจาก
    Table_PE.xls/Table_PBV.xls (เก็บ key อื่นๆ เช่น SET50/SETCLMV ที่ไฟล์นี้ไม่มีไว้เหมือนเดิม)"""
    data.setdefault("pe", {"dates": [], "series": {"SET": [], "mai": []}, "stats": {}})
    data.setdefault("pbv", {"dates": [], "series": {"SET": [], "mai": []}, "stats": {}})
    data.setdefault("div_yield", {"dates": [], "series": {"SET": [], "mai": []}, "stats": {}})
    data.setdefault("mkt_cap", {"dates": [], "series": {"SET": [], "mai": []}, "stats": {}})
    data.setdefault("breadth", {"dates": [], "series": {
        "listed_SET": [], "new_listed_SET": [], "delisted_SET": [],
        "listed_mai": [], "new_listed_mai": [], "delisted_mai": [],
    }})

    for ym, rec in sorted(records.items()):
        s, mai = rec["SET"], rec["mai"]
        _upsert_point(data["pe"], ym, {"SET": s.get("pe"), "mai": mai.get("pe")})
        _upsert_point(data["pbv"], ym, {"SET": s.get("pbv"), "mai": mai.get("pbv")})
        _upsert_point(data["div_yield"], ym, {"SET": s.get("div_yield"), "mai": mai.get("div_yield")})
        _upsert_point(data["mkt_cap"], ym, {"SET": s.get("mkt_cap"), "mai": mai.get("mkt_cap")})
        _upsert_point(data["breadth"], ym, {
            # จำนวนบริษัทเป็นจำนวนนับ — เก็บเป็น int ไม่ใช่ 636.0 ที่ _num คืนมา
            "listed_SET": _int(s.get("listed")), "new_listed_SET": _int(s.get("new_listed")),
            "delisted_SET": _int(s.get("delisted")),
            "listed_mai": _int(mai.get("listed")), "new_listed_mai": _int(mai.get("new_listed")),
            "delisted_mai": _int(mai.get("delisted")),
        })

    for key in ("pe", "pbv", "div_yield", "mkt_cap"):
        data[key]["stats"] = {k: calc_stats(v) for k, v in data[key]["series"].items()}

    return data
