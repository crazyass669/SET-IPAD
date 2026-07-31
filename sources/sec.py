# -*- coding: utf-8 -*-
"""sources/sec.py — SEC (ก.ล.ต.) scraping helpers: แบบ 59 / แบบ 246-2"""
from core.net import ssl_context

def _sec_viewstate(url, ua):
    import urllib.request as _ur, re as _re
    ctx = ssl_context()
    req = _ur.Request(url, headers={"User-Agent": ua})
    with _ur.urlopen(req, context=ctx, timeout=20) as r:
        html = r.read().decode("utf-8", errors="ignore")
    def _v(pat): m = _re.search(pat, html); return m.group(1) if m else ""
    return (
        _v(r'id="__VIEWSTATE"\s+value="([^"]*)"'),
        _v(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"'),
        _v(r'id="__EVENTVALIDATION"\s+value="([^"]*)"'),
    )

def _sec_post(url, payload, ua):
    import urllib.request as _ur, urllib.parse as _up, io, pandas as _pd
    ctx = ssl_context()
    data = _up.urlencode(payload, encoding="utf-8").encode("utf-8")
    req = _ur.Request(url, data=data, headers={
        "User-Agent": ua,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    })
    with _ur.urlopen(req, context=ctx, timeout=30) as r:
        html = r.read().decode("utf-8", errors="ignore")
    try:
        tables = _pd.read_html(io.StringIO(html))
    except ValueError:
        # ช่วงวันที่ไม่มีรายการเลย -> หน้าเว็บไม่มี <table> ใดๆ เลย
        # (read_html โยน ValueError แทนที่จะคืน list ว่าง)
        return _pd.DataFrame()
    return tables[0] if tables else _pd.DataFrame()

def _fetch_window(url, ua, build_payload, d_from, d_to, cap, depth, max_depth):
    import pandas as _pd
    vs, vsg, ev = _sec_viewstate(url, ua)
    df = _sec_post(url, build_payload(d_from, d_to, vs, vsg, ev), ua)
    n = len(df)
    span_days = (d_to - d_from).days + 1
    if n < cap or span_days <= 1 or depth >= max_depth:
        # ต่ำกว่า cap (ไม่โดนตัด) หรือแบ่งต่อไม่ได้แล้ว (เหลือ 1 วัน / ลึกเกินกำหนด)
        return [df]
    mid = d_from + (d_to - d_from) // 2
    left  = _fetch_window(url, ua, build_payload, d_from, mid, cap, depth + 1, max_depth)
    right = _fetch_window(url, ua, build_payload, mid + __import__("datetime").timedelta(days=1), d_to,
                           cap, depth + 1, max_depth)
    return left + right


def sec_fetch_chunked(url, ua, date_from, date_to, build_payload, cap=95, max_depth=8):
    """ดึงผลลัพธ์ทั้งช่วง date_from..date_to โดยแบ่งครึ่งช่วงย้อนกลับ (recursive)
    เฉพาะตอนที่ผลลัพธ์ก้อนนั้น "อิ่มตัว" (>= cap แถว) ซึ่งบ่งชี้ว่าอาจโดนเว็บ SEC
    ตัดผลลัพธ์ทิ้ง (เว็บจำกัดไว้ ~100 แถว/การค้นหา ไม่ว่าขอช่วงกว้างแค่ไหน)
    ช่วงที่ข้อมูลน้อยอยู่แล้วจะเสร็จในคำขอเดียว ไม่ต้องแบ่งเพิ่ม
    """
    import pandas as _pd
    frames = _fetch_window(url, ua, build_payload, date_from, date_to, cap, 0, max_depth)
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return _pd.DataFrame()
    return _pd.concat(frames, ignore_index=True)


def _thai_date(d):
    return d.strftime(f"%d/%m/{d.year + 543}")

def _extract_symbol(company_str):
    import re as _re
    m = _re.search(r'\(([A-Z0-9\-]+)\)', str(company_str))
    return m.group(1) if m else None


