# -*- coding: utf-8 -*-
"""sources/hk_index_membership.py — ดึงรายชื่อ constituents ปัจจุบันของ HSI / HSCEI / HSTECH
ตรงจาก Wikipedia (wikitext ดิบ) ใช้เทียบ/อัพเดทไฟล์ local data/hk_index_membership.json
เลียนแบบ sources/us_index_membership.py แต่ต้องรองรับทั้ง en.wikipedia (HSI/HSCEI) และ
zh.wikipedia (HSTECH ไม่มีหน้า en แยก) และ parse โครงตารางที่ต่างกัน 3 แบบ:
  - HSI:    wikitable id="constituents", คอลัมน์ Ticker|Name|Sub-index
  - HSCEI:  wikitable ไม่มี id, คอลัมน์ Ticker|Name|Weighting(%)|Industry
  - HSTECH: ไม่ใช่ wikitable — bullet list ใต้ {{Col-begin}}...{{Col-end}}, รูปแบบ
            "*รหัส [[ชื่อจีน]]" ไม่มี sector"""
import json
import os
import re
import ssl
import urllib.request

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
INDEXES = ("HSI", "HSCEI", "HSTECH")
_INDEXES = INDEXES   # ชื่อเดิม — คงไว้กันโค้ดอื่นอ้างอิงพลาด


def _fetch_wikitext(title, lang="en"):
    url = f"https://{lang}.wikipedia.org/w/index.php?title={title}&action=raw"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def _norm(code):
    return f"{int(code):04d}.HK"


def _parse_hsi(txt):
    # ตาราง id="constituents" — แถวคั่นด้วย "|-" รูปแบบ
    # "|{{SEHK|N}}\n|[[Name]]\n|Sub-index"
    m = re.search(r'id="constituents".*?\n(.*?)\n\|\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    tickers, sectors = [], {}
    for row in body.split("|-"):
        tm = re.search(r'\{\{SEHK\|(\d+)\}\}', row)
        if not tm:
            continue
        code = _norm(tm.group(1))
        tickers.append(code)
        lines = [ln.strip() for ln in row.strip().splitlines() if ln.strip().startswith("|")]
        if len(lines) >= 3:
            sectors[code] = lines[-1].lstrip("|").strip()
    return tickers, sectors


def _parse_hscei(txt):
    # ไม่มี id="constituents" — หา header "!Ticker" ... "!Industry" แล้วตัดถึง "|}"
    m = re.search(r'(\{\|.*?!Ticker.*?!Industry.*?)\n\|\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    tickers, sectors = [], {}
    for row in body.split("|-"):
        tm = re.search(r'\{\{SEHK\|(\d+)\}\}', row)
        if not tm:
            continue
        code = _norm(tm.group(1))
        tickers.append(code)
        lines = [ln.strip() for ln in row.strip().splitlines() if ln.strip().startswith("|")]
        if len(lines) >= 4:
            sectors[code] = lines[-1].lstrip("|").strip()
    return tickers, sectors


def _parse_hstech(txt):
    # bullet list ใต้ {{Col-begin}}...{{Col-end}}: "*รหัส [[ชื่อจีน]]" ไม่มี sector
    m = re.search(r'\{\{Col-begin\}\}(.*?)\{\{Col-end\}\}', txt, re.DOTALL)
    body = m.group(1) if m else txt
    return [_norm(c) for c in re.findall(r'^\*(\d{4,5})\s+\[\[', body, re.MULTILINE)]


def fetch_live_membership():
    """คืน {HSI:[...], HSCEI:[...], HSTECH:[...], HSI_sector:{...}, HSCEI_sector:{...}}
    สดจาก Wikipedia (ticker normalize เป็น NNNN.HK ให้ตรง yfinance) HSTECH ไม่มี sector
    จาก Wikipedia (ปล่อยว่าง เติมทีหลังจาก HSI/HSCEI sector หรือ dr_universe.py)
    โยน exception ถ้าเน็ตพัง/หน้า Wikipedia เปลี่ยนโครงสร้างจนพาร์สไม่ได้"""
    out = {}

    hsi_txt = _fetch_wikitext("Hang_Seng_Index")
    hsi_tickers, hsi_sectors = _parse_hsi(hsi_txt)
    out["HSI"] = hsi_tickers
    out["HSI_sector"] = hsi_sectors

    hscei_txt = _fetch_wikitext("Hang_Seng_China_Enterprises_Index")
    hscei_tickers, hscei_sectors = _parse_hscei(hscei_txt)
    out["HSCEI"] = hscei_tickers
    out["HSCEI_sector"] = hscei_sectors

    hstech_txt = _fetch_wikitext("%E6%81%92%E7%94%9F%E7%A7%91%E6%8A%80%E6%8C%87%E6%95%B0", lang="zh")
    out["HSTECH"] = _parse_hstech(hstech_txt)

    for k in _INDEXES:
        v = out[k]
        if len(v) < 20:
            raise ValueError(f"parse {k} ได้แค่ {len(v)} ตัว — หน้า Wikipedia อาจเปลี่ยนโครงสร้างตาราง")
        out[k] = sorted(set(v))
    return out


def _local_path(base_dir):
    return os.path.join(base_dir, "data", "hk_index_membership.json")


def load_local(base_dir):
    p = _local_path(base_dir)
    if not os.path.exists(p):
        return {"HSI": [], "HSCEI": [], "HSTECH": [], "extra_names": {},
                "HSI_sector": {}, "HSCEI_sector": {}}
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


def sync_membership(base_dir):
    """ดึง live list ใหม่จาก Wikipedia แล้วเขียนทับไฟล์ local ให้ตรง (คง extra_names เดิมไว้)
    คืน (diff_summary, live_membership)"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    diff = {}
    for k in _INDEXES:
        live_set, local_set = set(live[k]), set(local.get(k, []))
        diff[k] = {"new": sorted(live_set - local_set), "removed": sorted(local_set - live_set)}
    updated = dict(local)
    updated.update(live)
    updated.setdefault("extra_names", {})
    updated["note"] = ("รายชื่อ constituents ปัจจุบัน ณ วันที่ดึงข้อมูล — HSTECH ไม่มี sector จาก "
                        "Wikipedia (bullet list ไม่มีคอลัมน์ industry)")
    updated["source"] = ("Wikipedia (Hang Seng Index / Hang Seng China Enterprises Index [en] + "
                          "恒生科技指数 [zh]) — ดึงสดตรงจาก Wikipedia")
    save_local(base_dir, updated)
    return diff, live
