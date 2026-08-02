# -*- coding: utf-8 -*-
"""sources/etf_universe.py — ETF ที่จดทะเบียนบน SET โดยตรง (ต่างจาก DR ที่เป็น
underlying หุ้นต่างประเทศ) ดึงรายชื่อ+metadata สดจาก SET internal API ทุกครั้ง —
ไม่ต้อง curate มือแบบ dr_universe.py เพราะ /api/set/etf/list ให้ข้อมูลครบอยู่แล้ว
(nav/premium-discount/mgmt fee/div yield/mkt cap/investment policy ภาษาไทย)"""
import re

from sources.set_api import _bootstrap_headers, _get_json

_CATEGORY_MAP = {
    "ดัชนีในประเทศ": "TH_EQ",
    "ดัชนีต่างประเทศ": "FOREIGN",
    "ตราสารหนี้": "BOND",
    "สินค้าโภคภัณฑ์": "COMMODITY",
}
# คำสำคัญ (เช็คก่อน exact map ด้านบนถ้าไม่เจอ) กัน SET ตั้งชื่อ underlyingClassName ผัน
# คำนำหน้าต่าง (เช่น "หุ้นต่างประเทศ" แทน "ดัชนีต่างประเทศ") แล้วหลุดไป OTHER เงียบๆ —
# เรียงลำดับให้ "ต่างประเทศ"/"ในประเทศ" ไม่ทับกัน (คนละคำ ไม่ใช่ substring ของกัน)
_CATEGORY_KEYWORDS = [
    ("ต่างประเทศ", "FOREIGN"),
    ("ในประเทศ", "TH_EQ"),
    ("ตราสารหนี้", "BOND"),
    ("โภคภัณฑ์", "COMMODITY"),
]

_LNI_PAT    = re.compile(r"Invers|Leverag", re.I)
_LNI_TH_PAT = re.compile(r"อินเวิร์ส|เลเวอเรจ|ทวี|ผกผัน")
# สำรองจากชื่อ — ชื่อกองทุนมาจาก endpoint stock/list ที่ครอบ except: pass ไว้ (ถ้าล่ม
# name_en จะ fallback เป็นชื่อ underlying เฉยๆ ไม่มีคำว่า Invers/Leverag ให้ match) ใช้
# สัญลักษณ์แทนกัน — ETF ทวี/ผกผันของไทยตั้งชื่อ <จำนวนเท่า><I|X><รหัสอ้างอิง> เช่น
# 1I01BSET50 (Inverse 1X), 2X01BSET50 (Leverage 2X) ไม่ตรงกับ pattern สัญลักษณ์ ETF ปกติ
_LNI_SYM_PAT = re.compile(r"^\d+[IX]\d")


def classify_category(underlying_class_name):
    """exact match ก่อน (เร็ว/แม่นสุด) ตกไปเช็คคำสำคัญเป็น fallback — กัน ETF ประเภทใหม่ที่
    SET อาจตั้งชื่อ underlyingClassName ผันคำนำหน้าต่าง (เช่น 'หุ้นต่างประเทศ' แทน
    'ดัชนีต่างประเทศ') ไม่ให้หลุดไป OTHER เงียบๆ จนหายจากทุกแท็บยกเว้น 'ทั้งหมด'"""
    name = (underlying_class_name or "").strip()
    if name in _CATEGORY_MAP:
        return _CATEGORY_MAP[name]
    for keyword, code in _CATEGORY_KEYWORDS:
        if keyword in name:
            return code
    return "OTHER"


def is_leveraged_inverse(name_en, name_th=None, symbol=None):
    """เช็คชื่ออังกฤษก่อน (หลัก) ตกมาที่ชื่อไทย/รูปแบบ symbol เป็น fallback — กันกรณี
    endpoint stock/list (แหล่งชื่อ) ล่มแล้ว name_en fallback เป็นชื่อ underlying เฉยๆ
    ซึ่งจะทำให้ L&I หลุดเข้า RS ranking/RRG cohort เงียบๆ (beta ผิดธรรมชาติเทียบกันไม่ได้)"""
    if _LNI_PAT.search(name_en or ""):
        return True
    if name_th and _LNI_TH_PAT.search(name_th):
        return True
    if symbol and _LNI_SYM_PAT.match(symbol):
        return True
    return False


def fetch_etf_list_live():
    """ดึงรายชื่อ+metadata ETF ทั้งหมดสดจาก SET — raise ถ้าเรียกไม่สำเร็จ
    (caller fallback ไปใช้ symbol list จาก cache รอบก่อนเอง)

    รวม 2 endpoint: etf/list (nav/premium-discount/mgmt fee/policy) +
    stock/list?securityType=L (ชื่อกองทุนไทย/อังกฤษทางการ — etf/list ไม่มี field ชื่อ)"""
    ctx, hdr = _bootstrap_headers("/th/market/product/etf/overview")
    raw = _get_json(ctx, hdr, "/api/set/etf/list?lang=th", timeout=15)
    names = {}
    try:
        sec = _get_json(ctx, hdr, "/api/set/stock/list?securityType=L&lang=th", timeout=15)
        for s in sec.get("securitySymbols", []):
            names[s.get("symbol")] = (s.get("nameTH"), s.get("nameEN"))
    except Exception:
        pass  # ไม่มีชื่อก็ยัง fallback ใช้ underlying แทนได้ (ดูด้านล่าง)

    out = []
    for e in raw:
        sym = e.get("symbol")
        if not sym:
            continue
        name_th, name_en = names.get(sym, (None, None))
        name_en = name_en or e.get("underlying") or sym
        out.append({
            "symbol": sym,
            "name_th": name_th or name_en,
            "name_en": name_en,
            "underlying": e.get("underlying"),
            "underlying_class": e.get("underlyingClassName"),
            "category": classify_category(e.get("underlyingClassName")),
            "issuer": e.get("issuerName"),
            "mgmt_fee": e.get("managementFee"),
            "investment_policy": e.get("investmentPolicy"),
            "div_yield": e.get("dividendYield"),
            "nav": e.get("nav"),
            "nav_date": e.get("navDate"),
            "pnav_ratio": e.get("pnavRatio"),  # P/NAV (เท่า) ของ SET — ไม่ใช่ % premium ห้ามสับสน
            "mkt_cap": e.get("marketCap"),  # หน่วยจดทะเบียน x ราคา — ไม่ใช่ AUM จริง ใช้ aum แทนตอนแสดง/เรียง
            "aum": e.get("totalNav"),  # มูลค่าทรัพย์สินสุทธิกองทุนจริง (หน่วยบาท ไม่ใช่ล้านบาท)
            "value_traded": e.get("totalValue"),
            "dividend": e.get("dividend"),  # เงินปันผลล่าสุด (บาท/หน่วย)
            "xd_date": e.get("xdDate"),
            "market_maker": e.get("marketMaker"),  # รายชื่อ market maker — ตัวชี้คุณภาพสภาพคล่อง ETF ที่นักลงทุนใช้จริง
            "is_lna": is_leveraged_inverse(name_en, name_th, sym),
        })
    return out
