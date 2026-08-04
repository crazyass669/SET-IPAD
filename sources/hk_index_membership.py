# -*- coding: utf-8 -*-
"""sources/hk_index_membership.py — ดึงรายชื่อ constituents ปัจจุบันของ HSI / HSCEI / HSTECH
ตรงจาก Wikipedia (wikitext ดิบ) ใช้เทียบ/อัพเดทไฟล์ local data/hk_index_membership.json
เลียนแบบ sources/us_index_membership.py แต่ต้องรองรับทั้ง en.wikipedia (HSI/HSCEI) และ
zh.wikipedia (HSTECH ไม่มีหน้า en แยก) และ parse โครงตารางที่ต่างกัน 3 แบบ:
  - HSI:    wikitable id="constituents", คอลัมน์ Ticker|Name|Sub-index
  - HSCEI:  wikitable ไม่มี id, คอลัมน์ Ticker|Name|Weighting(%)|Industry
  - HSTECH: ไม่ใช่ wikitable — bullet list ใต้ {{Col-begin}}...{{Col-end}}, รูปแบบ
            "*รหัส [[ชื่อจีน]]" ไม่มี sector

sector/industry ไม่ได้ parse จากตาราง Wikipedia อีกต่อไป (เดิม HSI มีแค่คอลัมน์ Sub-index
หยาบ 4 กลุ่ม ทำให้หุ้น HSI-only 29/85 ตัวกองรวมเป็น "Commerce & Industry" ก้อนเดียว, HSCEI
มี Industry ละเอียดกว่าแต่ไม่ครอบคลุมตัวที่อยู่แค่ HSTECH — ผสมกันแล้วได้ taxonomy ปนกัน
~20 แบบ ทั้ง "Finance"/"Financials" ซ้ำกัน) — ใช้ yfinance sector/industry (Yahoo taxonomy
เดียวกันทั้งก้อน ครบ 105/105 ตัวตอนทดสอบ) แทน ดู _fetch_sector_map()"""
import json
import os
import re
import urllib.request

from core.net import ssl_context

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
INDEXES = ("HSI", "HSCEI", "HSTECH")
_INDEXES = INDEXES   # ชื่อเดิม — คงไว้กันโค้ดอื่นอ้างอิงพลาด


def _fetch_wikitext(title, lang="en"):
    url = f"https://{lang}.wikipedia.org/w/index.php?title={title}&action=raw"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    ctx = ssl_context()
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def _norm(code):
    return f"{int(code):04d}.HK"


def _parse_hsi(txt):
    # ตาราง id="constituents" — แถวคั่นด้วย "|-" รูปแบบ "|{{SEHK|N}}\n|[[Name]]\n|Sub-index"
    # เอาแค่ ticker (ไม่ parse คอลัมน์ Sub-index อีกต่อไป — ดู docstring หัวไฟล์)
    m = re.search(r'id="constituents".*?\n(.*?)\n\|\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    return [_norm(tm.group(1)) for tm in re.finditer(r'\{\{SEHK\|(\d+)\}\}', body)]


def _parse_hscei(txt):
    # ไม่มี id="constituents" — หา header "!Ticker" ... "!Industry" แล้วตัดถึง "|}"
    # เอาแค่ ticker (ไม่ parse คอลัมน์ Industry อีกต่อไป — ดู docstring หัวไฟล์)
    m = re.search(r'(\{\|.*?!Ticker.*?!Industry.*?)\n\|\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    return [_norm(tm.group(1)) for tm in re.finditer(r'\{\{SEHK\|(\d+)\}\}', body)]


def _parse_hstech(txt):
    # bullet list ใต้ {{Col-begin}}...{{Col-end}}: "*รหัส [[ชื่อจีน]]" ไม่มี sector
    m = re.search(r'\{\{Col-begin\}\}(.*?)\{\{Col-end\}\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    return [_norm(c) for c in re.findall(r'^\*(\d{4,5})\s+\[\[', body, re.MULTILINE)]


def fetch_live_membership():
    """คืน {HSI:[...], HSCEI:[...], HSTECH:[...]} สดจาก Wikipedia (ticker normalize เป็น
    NNNN.HK ให้ตรง yfinance) — sector/industry ไม่ได้ดึงตรงนี้ (ดู _fetch_sector_map,
    เรียกแยกใน sync_membership เพราะยิง yfinance ทีละตัว ~105 ตัว ช้ากว่าพาร์ส Wikipedia มาก
    ไม่อยากให้ diff_membership/check-updates ที่ต้องการแค่ ticker ช้าตามไปด้วย)
    โยน exception ถ้าเน็ตพัง/หน้า Wikipedia เปลี่ยนโครงสร้างจนพาร์สไม่ได้"""
    out = {}
    out["HSI"] = _parse_hsi(_fetch_wikitext("Hang_Seng_Index"))
    out["HSCEI"] = _parse_hscei(_fetch_wikitext("Hang_Seng_China_Enterprises_Index"))
    out["HSTECH"] = _parse_hstech(
        _fetch_wikitext("%E6%81%92%E7%94%9F%E7%A7%91%E6%8A%80%E6%8C%87%E6%95%B0", lang="zh"))

    for k in _INDEXES:
        v = out[k]
        if len(v) < 20:
            raise ValueError(f"parse {k} ได้แค่ {len(v)} ตัว — หน้า Wikipedia อาจเปลี่ยนโครงสร้างตาราง")
        out[k] = sorted(set(v))
    return out


def _fetch_sector_map(tickers, progress_cb=None):
    """ดึง sector/industry จริงจาก yfinance (Yahoo taxonomy — เช่น "Technology"/
    "Consumer Cyclical"/"Financial Services"/"Real Estate" ฯลฯ) คืน (sector_map, industry_map)
    ทั้งคู่ {ticker: str} — ใช้แทน Wikipedia HSI Sub-index/HSCEI Industry ที่ครอบคลุมไม่ครบ
    (ดู docstring หัวไฟล์) ข้ามตัวที่ดึงไม่ได้เงียบๆ (rate-limit/delisted ชั่วคราว) ให้
    caller (sync_membership) เก็บค่าเดิมของ ticker นั้นไว้แทนไม่ใช่หายไปเป็น Unknown"""
    import yfinance as yf
    sector_map, industry_map = {}, {}
    for i, t in enumerate(tickers):
        if progress_cb and i % 10 == 0:
            progress_cb(i, len(tickers), f"กำลังดึง sector จาก Yahoo Finance {i}/{len(tickers)}...")
        try:
            info = yf.Ticker(t).get_info()
        except Exception:
            continue
        sec, ind = info.get("sector"), info.get("industry")
        if sec:
            sector_map[t] = sec
        if ind:
            industry_map[t] = ind
    return sector_map, industry_map


def _local_path(base_dir):
    return os.path.join(base_dir, "data", "hk_index_membership.json")


def load_local(base_dir):
    p = _local_path(base_dir)
    if not os.path.exists(p):
        return {"HSI": [], "HSCEI": [], "HSTECH": [], "extra_names": {},
                "sector": {}, "industry": {}}
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
    """union ticker ของทั้ง 3 ดัชนี (ไม่ซ้ำ) จากไฟล์ local — ดู us_index_membership.all_tickers"""
    local = load_local(base_dir)
    return sorted(set().union(*(local.get(k, []) for k in INDEXES)))


def diff_membership(base_dir, mirror_hk=None):
    """เทียบ live (Wikipedia) กับไฟล์ local — รายงานอย่างเดียว ไม่แก้ไฟล์
    mirror_hk: set/list ของ ticker ที่มีอยู่ใน mirror list หลัก (financials.db FINN:HK:)
    ใส่มาด้วยจะได้รายงาน 'mirror_gap' (กี่ตัวที่ต้องพึ่ง Yahoo แทน Finnomena) ไปด้วยในตัว"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    mirror_set = set(mirror_hk or [])
    result = {"new": {}, "removed": {}, "live_counts": {}, "local_counts": {}, "mirror_gap": {}}
    for k in _INDEXES:
        live_set = set(live.get(k, []))
        local_set = set(local.get(k, []))
        result["new"][k] = sorted(live_set - local_set)
        result["removed"][k] = sorted(local_set - live_set)
        result["live_counts"][k] = len(live_set)
        result["local_counts"][k] = len(local_set)
        result["mirror_gap"][k] = len(live_set - mirror_set) if mirror_hk is not None else None
    return result, live


def sync_membership(base_dir, progress_cb=None):
    """ดึง live list ใหม่จาก Wikipedia แล้วเขียนทับไฟล์ local ให้ตรง (คง extra_names เดิมไว้)
    จากนั้นดึง sector/industry จริงจาก yfinance ให้ครบทุกตัวในดัชนี (merge ทับของเดิม —
    ตัวที่ดึงพลาดรอบนี้ยังคงค่าเก่าไว้ ไม่หายไปเป็น Unknown) คืน (diff_summary, live_membership)
    progress_cb(current,total,msg) — optional ใช้โชว์ progress ระหว่างดึง sector (~105 ตัว
    ยิง yfinance ทีละตัว ช้ากว่าพาร์ส Wikipedia ~15-20 วิ)"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    diff = {}
    for k in _INDEXES:
        live_set, local_set = set(live[k]), set(local.get(k, []))
        diff[k] = {"new": sorted(live_set - local_set), "removed": sorted(local_set - live_set)}
    updated = dict(local)
    updated.update(live)
    updated.setdefault("extra_names", {})

    all_syms = sorted(set().union(*(live[k] for k in _INDEXES)))
    sector_map, industry_map = _fetch_sector_map(all_syms, progress_cb=progress_cb)
    merged_sector = dict(local.get("sector") or {})
    merged_sector.update(sector_map)
    merged_industry = dict(local.get("industry") or {})
    merged_industry.update(industry_map)
    updated["sector"] = merged_sector
    updated["industry"] = merged_industry
    # เลิกใช้ HSI_sector/HSCEI_sector (Wikipedia Sub-index/Industry หยาบ — ดู docstring หัวไฟล์)
    updated.pop("HSI_sector", None)
    updated.pop("HSCEI_sector", None)

    updated["note"] = ("รายชื่อ constituents ปัจจุบัน ณ วันที่ดึงข้อมูล — sector/industry มาจาก "
                        "yfinance (Yahoo taxonomy) ไม่ใช่ Wikipedia")
    updated["source"] = ("Wikipedia (Hang Seng Index / Hang Seng China Enterprises Index [en] + "
                          "恒生科技指数 [zh]) สำหรับ ticker, yfinance สำหรับ sector/industry")
    save_local(base_dir, updated)
    return diff, live
