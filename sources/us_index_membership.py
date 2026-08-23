# -*- coding: utf-8 -*-
"""sources/us_index_membership.py — ดึงรายชื่อ constituents ปัจจุบันของ S&P 500 / Dow Jones /
Nasdaq 100 ตรงจาก Wikipedia (wikitext ดิบ ไม่ผ่าน mirror บุคคลที่สาม) ใช้เทียบ/อัพเดทไฟล์
local data/us_index_membership.json — เป็นคู่ปุ่ม "เช็คหุ้นใหม่/ถูกถอด" + "ดึงเฉพาะที่ขาด/เก่า"
ของหน้า DR (ดู check_dr_diff ใน dr_universe.py) แต่ใช้กับ 3 ดัชนี US แทน underlying ของ DR"""
import json
import os
import re
import urllib.request

from core.net import ssl_context

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
INDEXES = ("SP500", "DOW", "NDX")
_INDEXES = INDEXES   # ชื่อเดิม — คงไว้กันโค้ดอื่นอ้างอิงพลาด


def _fetch_wikitext(title):
    url = f"https://en.wikipedia.org/w/index.php?title={title}&action=raw"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    ctx = ssl_context()
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


_SECTOR_JUNK_RE = re.compile(r'[\[\]{}|]')


def _looks_like_sector(s):
    """เดาว่าค่าที่ parse ได้ "ดูเป็น sector" จริงไหม (สั้น ไม่มีร่องรอย wiki markup [[ ]] {{ }} |
    หลงเหลือ) — กันกรณี regex/split จับผิดคอลัมน์แล้วได้ชื่อบริษัท/เทมเพลตดิบมาแทน sector จริง
    (บั๊กเดิมที่ SP500_sector ได้ "[[3M]]" แทน "Industrials" — ดู _parse_sp500_sectors) ใช้ก่อน
    เก็บค่าเข้า out ทุกจุดที่ parse จาก wikitext กันเขียนทับข้อมูลดีด้วยขยะแบบเงียบๆ"""
    return bool(s) and len(s) <= 40 and not _SECTOR_JUNK_RE.search(s)


def _parse_ndx_sectors(txt):
    # แต่ละแถวอยู่บรรทัดเดียว: "| TICKER || [[Company]] || Sector || Subsector"
    # ช่องว่างรอบ "||" ไม่คงเส้นคงวาเสมอ (เช่นแถว FER: "...|| Industrials|| Military...”
    # ไม่มีช่องว่างก่อน "||" ตัวที่สาม) — ใช้ \s* แทนช่องว่างตายตัว ไม่งั้น parse ตกหล่น
    # กลายเป็น sector "Unknown" ทั้งที่ Wikipedia มีข้อมูลอยู่จริง
    out = {}
    for m in re.finditer(r'^\| ([A-Z.]+) \|\|\s*.*?\s*\|\|\s*([^|]+?)\s*\|\|', _table_body(txt), re.MULTILINE):
        sec = m.group(2).strip()
        if _looks_like_sector(sec):
            out[_norm(m.group(1))] = sec
    return out


def _parse_sp500_sectors(txt):
    # แต่ละแถวคั่นด้วย "|-" แต่ละคอลัมน์ขึ้นบรรทัดใหม่ด้วย "||": symbol, Security,
    # GICS Sector, Sub-Industry, ... — เดิม regex \|\|\s*([^|]+?)\s*\|\| หา "||...||"
    # แรกที่เจอในแถว ซึ่งพังเวลา symbol template มี "|" ภายใน (เช่น {{NyseSymbol|MMM}})
    # ทำให้ match ข้ามไปแมตช์คู่ Security-Sector แทนแล้วดึงชื่อบริษัท [[Security]] ออกมา
    # ผิดที่ (เช่น MMM -> "[[3M]]") แทนที่จะเป็น sector จริง — แก้ด้วยการ split("||") แล้ว
    # นับตำแหน่งคอลัมน์เทียบกับ field ที่เจอ symbol แทน (symbol+1=Security, symbol+2=Sector)
    out = {}
    for row in _table_body(txt).split("|-"):
        fields = row.split("||")
        for i, f in enumerate(fields):
            sm = re.search(r'\{\{(?:NyseSymbol|NasdaqSymbol|BZX link)\|([A-Z.]+)\}\}', f)
            if sm and i + 2 < len(fields):
                sec = fields[i + 2].strip()
                if _looks_like_sector(sec):
                    out[_norm(sm.group(1))] = sec
                break
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
            sec = secm.group(1).strip()
            if _looks_like_sector(sec):
                out[_norm(sm.group(1))] = sec
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

    # 2026-08-20: Wikipedia ย้ายตาราง constituents ออกจากหน้า "Dow_Jones_Industrial_Average"
    # ไปอยู่หน้าแยก "List_of_Dow_Jones_Industrial_Average_companies" แล้ว (หน้าเดิมเหลือแค่
    # infobox "constituents = 30" ไม่มีตารางจริงให้ parse — ทำให้ได้ 0 ตัวเงียบๆ จน guard ด้านล่าง
    # จับได้ว่า < 10 ตัว) เช็คสด 2026-08-20 หน้าใหม่มีตาราง {{NYSE/NASDAQ link|X}} ครบ 30 ตัวถูกต้อง
    dow_txt = _fetch_wikitext("List_of_Dow_Jones_Industrial_Average_companies")
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


def _yfinance_sector_fallback(tickers):
    """safety net รองสุดท้าย — ดึง sector จาก yfinance เฉพาะ ticker ที่ไม่มีค่าที่ใช้ได้เลยทั้ง
    ข้อมูลเก่าในเครื่องและรอบ parse ใหม่ (เช่น Wikipedia เปลี่ยนโครงสร้างตารางทั้งยวงจน parse
    ไม่ได้เลย) ไม่ใช่แหล่งหลักเพราะ Yahoo ใช้ taxonomy คนละแบบกับ GICS ที่ใช้จัดกลุ่ม Sector
    Ranking/Rotation อยู่ (ดู sector_keys_order ใน us_index_metrics.py) — ยิงทีละ ticker ช้า
    จึงเรียกเฉพาะตัวที่ขาดจริงๆ เท่านั้น (ปกติควรมี 0 ตัว ถ้า parser ทำงานถูก)"""
    if not tickers:
        return {}
    import yfinance as yf
    out = {}
    for t in tickers:
        try:
            sec = yf.Ticker(t).get_info().get("sector")
        except Exception:
            continue
        if sec:
            out[t] = sec
    return out


def _local_path(base_dir):
    return os.path.join(base_dir, "data", "us_index_membership.json")


def load_local(base_dir):
    p = _local_path(base_dir)
    default = {"SP500": [], "DOW": [], "NDX": [], "extra_names": {},
               "SP500_sector": {}, "DOW_sector": {}, "NDX_sector": {}}
    if not os.path.exists(p):
        return default
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


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
    เดิมไว้ — ไม่แตะชื่อที่เคย backfill มือ) คืน (diff_summary, live_membership)

    sector map (SP500_sector/DOW_sector/NDX_sector) merge ทีละ ticker แทนเขียนทับทั้งก้อน —
    ตัวที่ parse รอบใหม่ไม่ได้ค่า (ตกหล่น/ไม่ผ่าน _looks_like_sector) จะคงค่าเก่าไว้ ไม่หาย
    (กันบั๊กเดือน 2026-08 ที่ SP500_sector parse ผิดคอลัมน์ทั้งไฟล์แล้วเขียนทับข้อมูลดีเดิม)
    ตัวที่ไม่เคยมี sector เลยทั้งเก่า/ใหม่ (เช่น Wikipedia เปลี่ยนโครงสร้างตารางทั้งยวง) จะลอง
    เติมจาก yfinance เป็น safety net รองสุดท้าย — ดู _yfinance_sector_fallback"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    diff = {}
    for k in _INDEXES:
        live_set, local_set = set(live[k]), set(local.get(k, []))
        diff[k] = {"new": sorted(live_set - local_set), "removed": sorted(local_set - live_set)}
    updated = dict(local)
    updated.update(live)
    updated.setdefault("extra_names", {})

    sector_keys = ("SP500_sector", "DOW_sector", "NDX_sector")
    for key in sector_keys:
        merged = dict(local.get(key) or {})
        merged.update(live.get(key) or {})
        updated[key] = merged

    all_syms = sorted(set().union(*(live[k] for k in _INDEXES)))
    combined_sector = {}
    for key in sector_keys:
        combined_sector.update(updated.get(key) or {})
    missing = [s for s in all_syms if s not in combined_sector]
    if missing:
        fallback = _yfinance_sector_fallback(missing)
        if fallback:
            updated["SP500_sector"] = {**updated.get("SP500_sector", {}), **fallback}

    updated["note"] = ("รายชื่อ constituents ปัจจุบัน ณ วันที่ดึงข้อมูล — ตัวที่ไม่อยู่ใน mirror list "
                        "หลัก (factor_mirror) จะดึงงบผ่าน Yahoo Finance โดยตรงแทน Finnomena เมื่อกรองด้วยปุ่มดัชนีนี้")
    updated["source"] = ("Wikipedia (List of S&P 500 companies / Dow Jones Industrial Average / "
                          "List of NASDAQ-100 companies) — ดึงสดตรงจาก Wikipedia")
    save_local(base_dir, updated)
    return diff, live
