# -*- coding: utf-8 -*-
"""
sources/set_daily_valuation.py — P/E, P/BV, Div Yield, EPS รายวันของตลาด SET/mai
จากหน้า https://www.set.or.th/th/market/product/stock/overview

หน้านี้เป็น server-side rendered (Nuxt.js) — ตัวเลขฝังอยู่ในตัว HTML โดยตรงทุก
request ไม่มี JSON API แยกต่างหาก (ต่างจาก endpoint อื่นของ SET ที่ scrape ผ่าน
sources/set_api.py) ต่างจาก Table_PE.xls/Table_PBV.xls ที่เป็นรายเดือนต้องโหลด
เอง — อันนี้เป็นรายวัน ดึงอัตโนมัติได้เลย

ข้อควรระวัง: เป็นการ parse HTML โดยตรง ไม่ใช่ JSON schema ที่แน่นอน — ถ้า SET
renovate หน้าเว็บ โค้ดนี้อาจพัง ฟังก์ชัน fetch() คืน None แทนการ throw กัน
ทำให้หน้าเว็บพังไปด้วย (caller ควร fallback ไปใช้ cache เก่า)
"""
import re
import urllib.request as ur

from core.net import ssl_context

URL = "https://www.set.or.th/th/market/product/stock/overview"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"

_LABEL_MAP = {
    "มูลค่าหลักทรัพย์ตามราคาตลาด (ลบ.)": "mkt_cap",
    "อัตราหมุนเวียนปริมาณการซื้อขาย (YTD) (%)": "turnover_ytd",
    "P/E (เท่า)": "pe",
    "P/BV (เท่า)": "pbv",
    "อัตราเงินปันผลตอบแทน (%)": "div_yield",
    "กำไรสุทธิต่อหุ้น": "eps",
}


def _num(s):
    s = s.strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def fetch():
    """คืน {"SET": {...}, "mai": {...}, "as_of": "13 ก.ค. 2569"} หรือ None ถ้าดึง/parse ไม่ได้"""
    try:
        ctx = ssl_context()
        req = ur.Request(URL, headers={"User-Agent": _UA})
        with ur.urlopen(req, context=ctx, timeout=20) as r:
            html = r.read().decode("utf-8", "ignore")
    except Exception:
        return None

    # เดิม anchor ด้วย html.find(label) ตัวแรกที่เจอ แล้วตัดหน้าต่าง ±1500 ตัวอักษร —
    # เปราะ: หน้า Nuxt.js SSR อาจฝัง label เดียวกันซ้ำในก้อน state/JSON ก่อนตาราง
    # จริง ทำให้ anchor ผิดตำแหน่งได้ เปลี่ยนมา scan ทั้งหน้าหา <td>×3 ที่ label ตรงกับ
    # _LABEL_MAP โดยตรงแทน — ทนต่อ label ซ้ำที่ไหนก็ได้ในหน้า เพราะกรองด้วยโครงสร้าง td จริง
    pattern = r"<td[^>]*>\s*([^<]+?)\s*</td>\s*<td[^>]*>\s*([^<]+?)\s*</td>\s*<td[^>]*>\s*([^<]+?)\s*</td>"
    out = {"SET": {}, "mai": {}}
    pe_pos = None
    for m in re.finditer(pattern, html, re.DOTALL):
        label, set_v, mai_v = m.groups()
        key = _LABEL_MAP.get(label.strip())
        if not key:
            continue
        out["SET"][key] = _num(set_v)
        out["mai"][key] = _num(mai_v)
        if key == "pe" and pe_pos is None:
            pe_pos = m.start()

    if not out["SET"].get("pe"):
        return None   # parse ล้มเหลว — โครงสร้างหน้าเว็บอาจเปลี่ยนไปแล้ว

    m = re.search(r"ข้อมูล ณ วันที่\s*([^<]+)", html[max(0, pe_pos - 6000):pe_pos + 500])
    out["as_of"] = m.group(1).strip() if m else None
    return out
