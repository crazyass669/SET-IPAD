# -*- coding: utf-8 -*-
"""sources/jp_index_membership.py — ดึงรายชื่อ constituents ปัจจุบันของ Nikkei 225
ตรงจาก ja.wikipedia (wikitext ดิบ) ใช้เทียบ/อัพเดทไฟล์ local data/jp_index_membership.json
เลียนแบบ sources/hk_index_membership.py แต่มีจุดต่าง 2 อย่าง:
  - en.wikipedia หน้า "Nikkei 225" ไม่มีตาราง constituents เลย (มีแค่ annual return
    ย้อนหลัง) ต้องใช้ ja.wikipedia หน้า "日経平均株価" แทน (section "構成銘柄一覧")
  - โครงสร้างต่างจาก HK: sector ฝังอยู่ใน header ของ sub-section (=== ชื่อ（N銘柄） ===)
    ไม่ใช่คอลัมน์ในตาราง แบ่งเป็น ~34 ตารางย่อยตามหมวดของนิเคอิเอง (ไม่ใช่ TSE 33-sector
    มาตรฐานเป๊ะ ๆ — ใกล้เคียงแต่ไม่เหมือน) รหัสหุ้นบางตัวเป็นตัวอักษรผสมแล้ว (เช่น "285A"
    รูปแบบใหม่ TSE เริ่มใช้) ห้ามแปลงเป็น int()"""
import json
import os
import re
import urllib.parse
import urllib.request

from core.net import ssl_context

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"
INDEXES = ("NIKKEI225",)

# ชื่อ sector ของนิเคอิ (ภาษาญี่ปุ่น ตามที่ปรากฏจริงในหน้า ja.wikipedia) -> ชื่ออังกฤษ
# แปลง 4 กลุ่มการเงิน (銀行/証券/保険/その他金融) ให้ตรงกับ "Financial Services" ที่
# sources/factor_snapshot.py::FINANCIAL_SECTOR_NAMES เช็คอยู่แล้ว (ไม่ต้องแก้ไฟล์นั้นเพิ่ม)
_SECTOR_JA_TO_EN = {
    "食品": "Foods", "繊維": "Textiles & Apparel", "パルプ・紙": "Pulp & Paper",
    "化学": "Chemicals", "医薬品": "Pharmaceuticals", "石油": "Oil & Coal Products",
    "ゴム": "Rubber Products", "窯業": "Glass & Ceramics", "鉄鋼": "Steel",
    "非鉄・金属": "Nonferrous Metals", "機械": "Machinery", "電気機器": "Electric Appliances",
    "造船": "Shipbuilding", "自動車": "Automobiles", "精密機器": "Precision Instruments",
    "その他製造": "Other Manufacturing", "水産": "Fishery", "鉱業": "Mining",
    "建設": "Construction", "商社": "Trading Companies", "小売業": "Retail Trade",
    "銀行": "Financial Services", "証券": "Financial Services", "保険": "Financial Services",
    "その他金融": "Financial Services", "不動産": "Real Estate",
    "鉄道・バス": "Railway & Bus", "陸運": "Land Transportation",
    "海運": "Marine Transportation", "空運": "Air Transportation",
    "通信": "Telecommunications", "電力": "Electric Power", "ガス": "Gas",
    "サービス": "Services",
}


def _fetch_wikitext(title, lang="ja"):
    url = f"https://{lang}.wikipedia.org/w/index.php?title={urllib.parse.quote(title)}&action=raw"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    ctx = ssl_context()
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


_SECTION_RE = re.compile(r'==\s*構成銘柄一覧\s*==\n(.*?)\n==[^=]', re.DOTALL)
_SUBHEAD_RE = re.compile(r'^===\s*(.+?)（\d+銘柄）\s*===$')
_ROW_RE = re.compile(r'^\|([0-9A-Za-z]{3,6})\|\|\[\[([^\]]+)\]\]')

# นอกจาก wiki markup ([[ ]] {{ }} |) ยังกัน < > " ' = ด้วย — เหมือน
# sources/us_index_membership.py::_SECTOR_JUNK_RE (ดูคอมเมนต์ที่นั่น): ชื่อ sector จริง
# ไม่มีตัวอักษรพวกนี้ ปล่อยผ่านทำให้ header ที่ถูก vandalize บน ja.wikipedia (เช่น
# "=== Tech<svg onload=...>（5銘柄） ===") หลุดเข้า jp_index_membership.json / jp_index_metrics.json
# แล้ว render เป็น HTML สดในหน้า "หุ้น JP"
_SECTOR_JUNK_RE = re.compile(r'''[\[\]{}|<>"'=]''')


def _looks_like_sector(s):
    """เดาว่าค่าที่ parse ได้ "ดูเป็น sector" จริงไหม — เทียบเคียง _looks_like_sector ใน
    sources/us_index_membership.py (ดูที่นั่นสำหรับที่มา/บั๊กที่กันอยู่) กันกรณี _SECTION_RE
    หาไม่เจอแล้ว fallback ไป parse ทั้งหน้า (body = txt) จนหยิบข้อความปนจากที่อื่นมาเป็น sector"""
    return bool(s) and len(s) <= 40 and not _SECTOR_JUNK_RE.search(s)


def _parse_nikkei225(txt):
    """คืน (tickers, sector_map) จาก wikitext ของ ja.wikipedia 日経平均株価
    ticker normalize เป็น "CODE.T" (CODE คงรูปเดิม ไม่แปลง int — บางตัวมีตัวอักษรปนแล้ว)"""
    m = _SECTION_RE.search(txt)
    body = m.group(1) if m else txt
    tickers, sectors = [], {}
    current_sector_en = None
    for line in body.splitlines():
        hm = _SUBHEAD_RE.match(line.strip())
        if hm:
            ja_name = hm.group(1).strip()
            current_sector_en = _SECTOR_JA_TO_EN.get(ja_name, ja_name)
            continue
        rm = _ROW_RE.match(line.strip())
        if rm and current_sector_en:
            code = rm.group(1)
            name_raw = rm.group(2)
            ticker = f"{code}.T"
            tickers.append(ticker)
            if _looks_like_sector(current_sector_en):
                sectors[ticker] = current_sector_en
    return tickers, sectors


def fetch_live_membership():
    """คืน {NIKKEI225:[...], NIKKEI225_sector:{...}} สดจาก ja.wikipedia (ticker normalize
    เป็น CODE.T ให้ตรง yfinance) โยน exception ถ้าเน็ตพัง/หน้า Wikipedia เปลี่ยนโครงสร้างจนพาร์สไม่ได้"""
    txt = _fetch_wikitext("日経平均株価")
    tickers, sectors = _parse_nikkei225(txt)
    if len(tickers) < 150:
        raise ValueError(f"parse NIKKEI225 ได้แค่ {len(tickers)} ตัว (คาดหวัง ~225) — "
                          f"หน้า ja.wikipedia อาจเปลี่ยนโครงสร้างตาราง")
    return {"NIKKEI225": sorted(set(tickers)), "NIKKEI225_sector": sectors}


def _local_path(base_dir):
    return os.path.join(base_dir, "data", "jp_index_membership.json")


def load_local(base_dir):
    p = _local_path(base_dir)
    default = {"NIKKEI225": [], "extra_names": {}, "NIKKEI225_sector": {}}
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
    """union ticker ของทั้งดัชนี (ตอนนี้มีแค่ NIKKEI225 ตัวเดียว) จากไฟล์ local
    ดู us_index_membership.all_tickers / hk_index_membership.all_tickers"""
    local = load_local(base_dir)
    return sorted(set().union(*(local.get(k, []) for k in INDEXES)))


def diff_membership(base_dir, mirror_jp=None):
    """เทียบ live (ja.wikipedia) กับไฟล์ local — รายงานอย่างเดียว ไม่แก้ไฟล์
    mirror_jp: set/list ของ ticker ที่มีอยู่ใน mirror list หลัก (financials.db FINN:JP:)"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    mirror_set = set(mirror_jp or [])
    result = {"new": {}, "removed": {}, "live_counts": {}, "local_counts": {}, "mirror_gap": {}}
    for k in INDEXES:
        live_set = set(live.get(k, []))
        local_set = set(local.get(k, []))
        result["new"][k] = sorted(live_set - local_set)
        result["removed"][k] = sorted(local_set - live_set)
        result["live_counts"][k] = len(live_set)
        result["local_counts"][k] = len(local_set)
        result["mirror_gap"][k] = len(live_set - mirror_set) if mirror_jp is not None else None
    return result, live


def sync_membership(base_dir):
    """ดึง live list ใหม่จาก ja.wikipedia แล้วเขียนทับไฟล์ local ให้ตรง (คง extra_names เดิมไว้)
    คืน (diff_summary, live_membership)

    NIKKEI225_sector merge ทีละ ticker แทนเขียนทับทั้งก้อน — ตัวที่ parse รอบใหม่ไม่ได้ค่า
    (หน้า ja.wikipedia เปลี่ยนโครงสร้างจน _SECTION_RE หาไม่เจอ ฯลฯ) จะคงค่าเก่าไว้ ไม่หาย
    เทียบเคียงมาตรการเดียวกับ us_index_membership.sync_membership (ดูที่นั่นสำหรับที่มา)"""
    live = fetch_live_membership()
    local = load_local(base_dir)
    diff = {}
    for k in INDEXES:
        live_set, local_set = set(live[k]), set(local.get(k, []))
        diff[k] = {"new": sorted(live_set - local_set), "removed": sorted(local_set - live_set)}
    updated = dict(local)
    updated.update(live)
    updated.setdefault("extra_names", {})

    merged_sector = dict(local.get("NIKKEI225_sector") or {})
    merged_sector.update(live.get("NIKKEI225_sector") or {})
    updated["NIKKEI225_sector"] = merged_sector

    updated["note"] = "รายชื่อ constituents ปัจจุบันของ Nikkei 225 ณ วันที่ดึงข้อมูล"
    updated["source"] = "ja.wikipedia (日経平均株価 — section 構成銘柄一覧) — ดึงสดตรงจาก Wikipedia"
    save_local(base_dir, updated)
    return diff, live
