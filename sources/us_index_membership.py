# -*- coding: utf-8 -*-
"""sources/us_index_membership.py — ดึงรายชื่อ constituents ปัจจุบันของ S&P 500 / Dow Jones /
Nasdaq 100 ตรงจาก Wikipedia (wikitext ดิบ ไม่ผ่าน mirror บุคคลที่สาม) ใช้เทียบ/อัพเดทไฟล์
local data/us_index_membership.json — เป็นคู่ปุ่ม "เช็คหุ้นใหม่/ถูกถอด" + "ดึงเฉพาะที่ขาด/เก่า"
ของหน้า DR (ดู check_dr_diff ใน dr_universe.py) แต่ใช้กับ 3 ดัชนี US แทน underlying ของ DR"""
import json
import os
import re
import ssl
import urllib.request

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
INDEXES = ("SP500", "DOW", "NDX")
_INDEXES = INDEXES   # ชื่อเดิม — คงไว้กันโค้ดอื่นอ้างอิงพลาด


def _fetch_wikitext(title):
    url = f"https://en.wikipedia.org/w/index.php?title={title}&action=raw"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def _table_body(txt):
    # ตัดเอาเฉพาะตารางแรก (id="constituents") — ตารางถัดไปในหน้าเดียวกันมักเป็นประวัติ
    # การเปลี่ยนแปลง (เพิ่ม/ถอด) ไม่ใช่รายชื่อปัจจุบัน จะพาร์สปนกันถ้าไม่ตัดก่อน
    m = re.search(r'id="constituents".*?\n(.*?)\n\|\}', txt, re.DOTALL)
    return m.group(1) if m else txt


def _parse_ndx(txt):
    return re.findall(r'^\| ([A-Z.]+) \|\|', _table_body(txt), re.MULTILINE)


def _norm(sym):
    return sym.replace(".", "-")   # BRK.B -> BRK-B ให้ตรง yfinance/mirror


def _parse_ndx_sectors(txt):
    # แต่ละแถวอยู่บรรทัดเดียว: "| TICKER || [[Company]] || Sector || Subsector"
    # ช่องว่างรอบ "||" ไม่คงเส้นคงวาเสมอ (เช่นแถว FER: "...|| Industrials|| Military...”
    # ไม่มีช่องว่างก่อน "||" ตัวที่สาม) — ใช้ \s* แทนช่องว่างตายตัว ไม่งั้น parse ตกหล่น
    # กลายเป็น sector "Unknown" ทั้งที่ Wikipedia มีข้อมูลอยู่จริง
    out = {}
    for m in re.finditer(r'^\| ([A-Z.]+) \|\|\s*.*?\s*\|\|\s*([^|]+?)\s*\|\|', _table_body(txt), re.MULTILINE):
        out[_norm(m.group(1))] = m.group(2).strip()
    return out


def _parse_sp500_sectors(txt):
    # แต่ละแถวคั่นด้วย "|-" แล้วบรรทัดแรกเป็น symbol ({{NyseSymbol|X}} ฯลฯ) บรรทัดถัดไป
    # เป็น "|[[Security]]|| Sector || Sub-Industry || ..." — ต้องจับคู่ตามลำดับแถว
    out = {}
    for row in _table_body(txt).split("|-"):
        sm = re.search(r'\{\{(?:NyseSymbol|NasdaqSymbol|BZX link)\|([A-Z.]+)\}\}', row)
        if not sm:
            continue
        secm = re.search(r'\|\|\s*([^|]+?)\s*\|\|', row)
        if secm:
            out[_norm(sm.group(1))] = secm.group(1).strip()
    return out


def _parse_dow_sectors(txt):
    # แต่ละแถว: symbol บรรทัด "| {{NYSE link|X}}" ตามด้วย sector บรรทัดถัดไปทันที "| Sector"
    out = {}
    for row in _table_body(txt).split("|-"):
        sm = re.search(r'\{\{(?:NYSE|NASDAQ) link\|([A-Z.]+)\}\}', row)
        if not sm:
            continue
        rest = row[sm.end():]
        secm = re.search(r'\|\s*([A-Za-z][A-Za-z &]+?)\s*\n', rest)
        if secm:
            out[_norm(sm.group(1))] = secm.group(1).strip()
    return out


def fetch_live_membership():
    """คืน {SP500:[...], DOW:[...], NDX:[...], SP500_sector:{...}, DOW_sector:{...}, NDX_sector:{...}}
    สดจาก Wikipedia (ticker แบบ yfinance เช่น BRK-B ไม่ใช่ BRK.B) — sector ใช้จัดกลุ่ม heatmap
    (ดู GICS Sector ในหน้า SP500/DOW, ICB Industry ในหน้า NDX) โยน exception ถ้าเน็ตพัง/
    หน้า Wikipedia เปลี่ยนโครงสร้างจนพาร์สไม่ได้"""
    out = {}
    sp_txt = _fetch_wikitext("List_of_S%26P_500_companies")
    # ส่วนใหญ่ใช้ {{NyseSymbol|X}} / {{NasdaqSymbol|X}} แต่มีบางตัว (เช่น CBOE) เทรดบน
    # Cboe BZX ที่ใช้ template {{BZX link|X}} แยกต่างหาก — ต้องจับให้ครบไม่งั้นตกหล่น
    # กลายเป็น "false removed" ทั้งที่จริงยังอยู่ในดัชนี (ตรวจแล้ว 342+160+1 = 503 ตรงเป๊ะ)
    out["SP500"] = re.findall(r'\{\{(?:NyseSymbol|NasdaqSymbol|BZX link)\|([A-Z.]+)\}\}', sp_txt)
    out["SP500_sector"] = _parse_sp500_sectors(sp_txt)

    dow_txt = _fetch_wikitext("Dow_Jones_Industrial_Average")
    out["DOW"] = re.findall(r'\{\{(?:NYSE|NASDAQ) link\|([A-Z.]+)\}\}', dow_txt)
    out["DOW_sector"] = _parse_dow_sectors(dow_txt)

    ndx_txt = _fetch_wikitext("List_of_NASDAQ-100_companies")
    out["NDX"] = _parse_ndx(ndx_txt)
    out["NDX_sector"] = _parse_ndx_sectors(ndx_txt)

    for k in _INDEXES:
        v = out[k]
        if len(v) < 10:
            raise ValueError(f"parse {k} ได้แค่ {len(v)} ตัว — หน้า Wikipedia อาจเปลี่ยนโครงสร้างตาราง")
        out[k] = sorted({_norm(s) for s in v})
    return out


def _local_path(base_dir):
    return os.path.join(base_dir, "data", "us_index_membership.json")


def load_local(base_dir):
    p = _local_path(base_dir)
    if not os.path.exists(p):
        return {"SP500": [], "DOW": [], "NDX": [], "extra_names": {},
                "SP500_sector": {}, "DOW_sector": {}, "NDX_sector": {}}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def save_local(base_dir, data):
    p = _local_path(base_dir)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, p)


def all_tickers(base_dir):
    """union ticker ของทั้ง 3 ดัชนี (ไม่ซ้ำ) จากไฟล์ local — เดิมแต่ละที่ที่ต้องการ
    union นี้ (app.py, backfill script, metrics builder) เขียน sorted(set(...)|...)
    ซ้ำมือเอง กระจาย ~6 จุด แก้ยากถ้า INDEXES เปลี่ยน"""
    local = load_local(base_dir)
    return sorted(set().union(*(local.get(k, []) for k in INDEXES)))


def diff_membership(base_dir, mirror_us=None):
    """เทียบ live (Wikipedia) กับไฟล์ local — รายงานอย่างเดียว ไม่แก้ไฟล์
    mirror_us: set/list ของ ticker ที่มีอยู่ใน mirror list หลัก (factor_mirror) — ใส่มาด้วย
    จะได้รายงาน 'mirror_gap' (กี่ตัวที่ต้องพึ่ง Yahoo แทน Finnomena) ไปด้วยในตัว"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    mirror_set = set(mirror_us or [])
    result = {"new": {}, "removed": {}, "live_counts": {}, "local_counts": {}, "mirror_gap": {}}
    for k in _INDEXES:
        live_set = set(live.get(k, []))
        local_set = set(local.get(k, []))
        result["new"][k] = sorted(live_set - local_set)
        result["removed"][k] = sorted(local_set - live_set)
        result["live_counts"][k] = len(live_set)
        result["local_counts"][k] = len(local_set)
        result["mirror_gap"][k] = len(live_set - mirror_set) if mirror_us is not None else None
    return result, live


def sync_membership(base_dir):
    """ดึง live list ใหม่จาก Wikipedia แล้วเขียนทับไฟล์ local ให้ตรง (คง extra_names/source/note
    เดิมไว้ — ไม่แตะชื่อที่เคย backfill มือ) คืน (diff_summary, live_membership)"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    diff = {}
    for k in _INDEXES:
        live_set, local_set = set(live[k]), set(local.get(k, []))
        diff[k] = {"new": sorted(live_set - local_set), "removed": sorted(local_set - live_set)}
    updated = dict(local)
    updated.update(live)
    updated.setdefault("extra_names", {})
    updated["note"] = ("รายชื่อ constituents ปัจจุบัน ณ วันที่ดึงข้อมูล — ตัวที่ไม่อยู่ใน mirror list "
                        "หลัก (factor_mirror) จะดึงงบผ่าน Yahoo Finance โดยตรงแทน Finnomena เมื่อกรองด้วยปุ่มดัชนีนี้")
    updated["source"] = ("Wikipedia (List of S&P 500 companies / Dow Jones Industrial Average / "
                          "List of NASDAQ-100 companies) — ดึงสดตรงจาก Wikipedia")
    save_local(base_dir, updated)
    return diff, live
