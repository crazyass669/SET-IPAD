# -*- coding: utf-8 -*-
"""
sources/set_api.py — client สำหรับ SET internal API (www.set.or.th)

ใช้เป็น primary ของ fundamentals (mkt_cap/PE/PBV/DivYield) แทน Yahoo ที่โดน
rate limit บ่อย — และเป็นโครงสำหรับ backup ราคาในอนาคต (chart-quotation)

หมายเหตุ: เป็น internal API ของเว็บ SET (ไม่มีสัญญา) — ทุกฟังก์ชันมี
validation gate: ถ้าผลได้ต่ำกว่าเกณฑ์ให้ raise เพื่อให้ caller fallback
ไปแหล่งอื่นแทนที่จะรับข้อมูลไม่ครบเข้า pipeline เงียบๆ
"""
import json
import ssl
import time
import urllib.parse
import urllib.request as ur
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://www.set.or.th"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"


def _bootstrap_headers(referer_path="/th/market/product/stock/quote/PTT/price"):
    """เปิดหน้าเว็บหนึ่งครั้งเพื่อรับ cookie — internal API ตอบ 403 ถ้าไม่มี"""
    ctx = ssl._create_unverified_context()
    req = ur.Request(BASE + referer_path, headers={"User-Agent": _UA})
    with ur.urlopen(req, context=ctx, timeout=20) as r:
        cookie = r.getheader("Set-Cookie", "") or ""
    return ctx, {
        "User-Agent": _UA,
        "Accept": "application/json",
        "Referer": BASE + referer_path,
        "Cookie": cookie,
    }


def _get_json(ctx, hdr, path, timeout=12):
    req = ur.Request(BASE + path, headers=hdr)
    with ur.urlopen(req, context=ctx, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def fetch_fundamentals(tickers, callback=None, workers=8, min_ratio=0.5):
    """ดึง mkt_cap/PE/PBV/DivYield จาก /api/set/stock/<sym>/highlight-data

    tickers: list ของ "PTT.BK" (รูปแบบเดียวกับ fetch_market_caps_parallel)
    คืน {ticker: {"mkt_cap": int|None, "pe": float|None,
                  "pbv": float|None, "div_yield": float|None}}
    raise ValueError ถ้าดึงสำเร็จน้อยกว่า min_ratio (ให้ caller fallback Yahoo)
    """
    ctx, hdr = _bootstrap_headers()
    results = {}

    def _one(tick):
        sym = tick[:-3] if tick.endswith(".BK") else tick
        path = f"/api/set/stock/{urllib.parse.quote(sym)}/highlight-data?lang=th"
        for attempt in range(2):
            try:
                d = _get_json(ctx, hdr, path)
                mc  = d.get("marketCap")
                pe  = d.get("peRatio")
                pbv = d.get("pbRatio")
                dy  = d.get("dividendYield")
                return tick, {
                    "mkt_cap":   int(mc)            if mc  is not None else None,
                    "pe":        round(float(pe), 2)  if pe  is not None else None,
                    "pbv":       round(float(pbv), 2) if pbv is not None else None,
                    "div_yield": round(float(dy), 2)  if dy  is not None else None,
                }
            except Exception:
                if attempt == 0:
                    time.sleep(0.5)
        return tick, None

    total = len(tickers)
    done = ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, t) for t in tickers]
        for f in as_completed(futures):
            tick, data = f.result()
            done += 1
            if data is not None:
                results[tick] = data
                ok += 1
            if callback and done % 100 == 0:
                callback(done, total, f"Fundamentals (SET API) {done}/{total}...")

    if total > 0 and ok < total * min_ratio:
        raise ValueError(f"SET API fundamentals ได้แค่ {ok}/{total} "
                         f"(<{int(min_ratio*100)}%) — ควร fallback")
    return results


def fetch_daily_bars(symbol, period="1M"):
    """สำรองราคา: ราคา+volume รายวันของหุ้นหนึ่งตัวจาก chart-quotation
    คืน list ของ (date_str, close, volume) — สำหรับ emergency backup ของ Yahoo"""
    ctx, hdr = _bootstrap_headers()
    sym = symbol[:-3] if symbol.endswith(".BK") else symbol
    d = _get_json(ctx, hdr,
                  f"/api/set/stock/{urllib.parse.quote(sym)}/chart-quotation"
                  f"?period={period}&accumulated=false")
    out = []
    for q in d.get("quotations", []):
        dt = (q.get("localDatetime") or q.get("datetime") or "")[:10]
        px = q.get("price")
        if dt and px is not None:
            out.append((dt, float(px), int(q.get("volume") or 0)))
    return out
