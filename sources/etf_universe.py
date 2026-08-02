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

_LNI_PAT = re.compile(r"Invers|Leverag", re.I)


def classify_category(underlying_class_name):
    return _CATEGORY_MAP.get((underlying_class_name or "").strip(), "OTHER")


def is_leveraged_inverse(name_en):
    return bool(_LNI_PAT.search(name_en or ""))


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
            "pnav_ratio": e.get("pnavRatio"),
            "mkt_cap": e.get("marketCap"),
            "is_lna": is_leveraged_inverse(name_en),
        })
    return out
