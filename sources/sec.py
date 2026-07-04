# -*- coding: utf-8 -*-
"""sources/sec.py — SEC (ก.ล.ต.) scraping helpers: แบบ 59 / แบบ 246-2"""

def _sec_viewstate(url, ua):
    import urllib.request as _ur, ssl as _ssl, re as _re
    ctx = _ssl._create_unverified_context()
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
    import urllib.request as _ur, urllib.parse as _up, ssl as _ssl, io, pandas as _pd
    ctx = _ssl._create_unverified_context()
    data = _up.urlencode(payload, encoding="utf-8").encode("utf-8")
    req = _ur.Request(url, data=data, headers={
        "User-Agent": ua,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    })
    with _ur.urlopen(req, context=ctx, timeout=30) as r:
        html = r.read().decode("utf-8", errors="ignore")
    tables = _pd.read_html(io.StringIO(html))
    return tables[0] if tables else _pd.DataFrame()

def _thai_date(d):
    return d.strftime(f"%d/%m/{d.year + 543}")

def _extract_symbol(company_str):
    import re as _re
    m = _re.search(r'\(([A-Z0-9\-]+)\)', str(company_str))
    return m.group(1) if m else None


