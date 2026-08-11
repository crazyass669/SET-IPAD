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
import time
import urllib.parse
import urllib.request as ur
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from core.net import ssl_context

BASE = "https://www.set.or.th"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0"


def _bootstrap_headers(referer_path="/th/market/product/stock/quote/PTT/price"):
    """เปิดหน้าเว็บหนึ่งครั้งเพื่อรับ cookie — internal API ตอบ 403 ถ้าไม่มี"""
    ctx = ssl_context()
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


_SUSPEND_SIGNS = {"SP", "NC", "H"}


def fetch_quotes_batch(tickers, callback=None, workers=6, batch_size=30, min_ratio=0.5):
    """ดึงราคาล่าสุด (OHLCV วันเดียว) ของทั้งกระดานเร็ว จาก
    /api/set/stock-info/list-by-symbols — primary ของ Quick Update แทน Yahoo
    (เร็วกว่ามาก + ไม่ lag เหมือน Yahoo ที่ปล่อยแท่งปิดหุ้นไทยช้า — ดู
    services/refresh.py::run_quick_update สำหรับ fast-path logic เต็ม)

    tickers: list ของ "PTT.BK" -> คืน {ticker: {"open","high","low","close",
             "volume","prior"}} เฉพาะตัวที่มีราคาจริง (ข้าม sign SP/NC/H หรือ
             OHLC เป็น null — หุ้นพักเทรด/non-compliant)
    รับสูงสุด 30 ตัว/request (เกินกว่านั้นตอบ 400)
    raise ValueError ถ้าดึงสำเร็จน้อยกว่า min_ratio (ให้ caller fallback Yahoo)
    """
    ctx, hdr = _bootstrap_headers()
    sym_map = {(t[:-3] if t.endswith(".BK") else t): t for t in tickers}
    chunks = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    results = {}

    def _one(chunk):
        syms = [t[:-3] if t.endswith(".BK") else t for t in chunk]
        q = urllib.parse.quote(",".join(syms))
        for attempt in range(2):
            try:
                return _get_json(ctx, hdr,
                                  f"/api/set/stock-info/list-by-symbols?symbols={q}&lang=th")
            except Exception:
                if attempt == 0:
                    time.sleep(0.5)
        return None

    total = len(tickers)
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, c) for c in chunks]
        for f in as_completed(futures):
            rows = f.result()
            if not rows:
                continue
            for r in rows:
                tick = sym_map.get(r.get("symbol"))
                if not tick:
                    continue
                sign_codes = {c.strip() for c in (r.get("sign") or "").split(",") if c.strip()}
                o, h, l, c = r.get("open"), r.get("high"), r.get("low"), r.get("last")
                if sign_codes & _SUSPEND_SIGNS or None in (o, h, l, c):
                    continue
                prior = r.get("prior")
                results[tick] = {
                    "open": float(o), "high": float(h), "low": float(l), "close": float(c),
                    "volume": int(r.get("totalVolume") or 0),
                    "prior": float(prior) if prior is not None else None,
                }
                ok += 1
            if callback:
                callback(ok, total, f"ราคาล่าสุด (SET API) {ok}/{total}...")

    if total > 0 and ok < total * min_ratio:
        raise ValueError(f"SET API quotes ได้แค่ {ok}/{total} "
                         f"(<{int(min_ratio*100)}%) — ควร fallback")
    return results


def fetch_trading_calendar_tail(ref_symbol="PTT", n=5):
    """คืนวันเทรดล่าสุด n วัน ('YYYY-MM-DD', เรียงเก่า->ใหม่) จาก chart-quotation ของ
    หุ้นอ้างอิงตัวเดียว (blue chip สภาพคล่องสูง แทบไม่มีพักเทรด) — list-by-symbols
    (fetch_quotes_batch) ไม่มี field วันที่ตรงๆ ในตัวเอง (marketDateTime คือเวลาที่
    query ไม่ใช่วันเทรด — ยืนยันแล้วว่าต่างกันได้เวลา query ก่อนตลาดเปิด/วันหยุด)

    เอา n>=2 เพื่อให้ caller เช็คได้ว่า 'วันก่อนวันล่าสุด' ตรงกับวันที่เก็บไว้จริงไหม —
    กัน Quick Update fast path เข้าใจผิดว่ามีช่องว่างแค่ 1 วัน ทั้งที่จริงขาดหลายวัน
    (ดู services/refresh.py::run_quick_update) raise ถ้าดึงไม่ได้"""
    bars = fetch_daily_bars(ref_symbol, period="1M")
    if not bars:
        raise ValueError("ดึงปฏิทินวันเทรดล่าสุดไม่ได้ (chart-quotation ว่างเปล่า)")
    return [b[0] for b in bars[-n:]]


def fetch_daily_bars(symbol, period="1M", ctx=None, hdr=None):
    """สำรองราคา: ราคา+volume รายวันของหุ้นหนึ่งตัวจาก chart-quotation
    คืน list ของ (date_str, close, volume) — สำหรับ emergency backup ของ Yahoo

    ctx/hdr: ส่งมาเองได้เพื่อ reuse cookie เดียวข้ามหลายเรียก (ดู
    fetch_price_history_batch ที่เรียกฟังก์ชันนี้เป็นร้อยครั้งแบบ threaded — ไม่งั้น
    จะ bootstrap คุกกี้ใหม่ทุกครั้งซึ่งช้าเกินจำเป็น) ไม่ส่งมาก็ bootstrap ให้เองเหมือนเดิม"""
    if ctx is None or hdr is None:
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


_PERIOD_TIERS = (("1M", 28), ("3M", 88), ("6M", 178), ("1Y", 360))


def fetch_price_history_batch(tickers, start_date, callback=None, workers=6):
    """สำรองราคาย้อนหลังหลายวันจาก chart-quotation ทั้งกระดาน — fallback ชั้นสุดท้าย
    ของ Quick Update เมื่อ Yahoo ล่ม/ไม่ตอบหลายวันติด (ปกติ Yahoo เป็นตัวเติม gap
    หลายวัน — ดู services/refresh.py::run_quick_update)

    ต่างจาก fetch_quotes_batch (list-by-symbols) ตรงที่ได้ "ประวัติย้อนหลัง" จริงหลายวัน
    (ไม่ใช่แค่วันล่าสุดวันเดียว) แต่ chart-quotation ไม่มี field OHLC เลย ได้แค่
    close+volume — พอให้ราคา/RS/เทคนิคขยับต่อได้ระหว่างรอ Yahoo ฟื้น พอ Yahoo ตอบปกติ
    Quick Update รอบถัดไปจะเติม OHLC ให้เองผ่าน Yahoo (upsert_bars รองรับ OHLC เป็น
    NULL อยู่แล้ว เหมือนแถวเก่าก่อน migration — ดู core/store.py init_db)

    tickers: list ของ "PTT.BK", start_date: "YYYY-MM-DD"
    คืน {ticker: {"close": pd.Series, "volume": pd.Series}} เฉพาะตัวที่ดึงได้ — ไม่ raise
    ถ้าบางตัวพลาด (เป็น fallback ชั้นสุดท้ายอยู่แล้ว ได้เท่าไหร่เอาเท่านั้น)"""
    days_needed = max((pd.Timestamp.now().normalize() - pd.to_datetime(start_date)).days, 1)
    period = "1Y"
    for p, max_days in _PERIOD_TIERS:
        if days_needed <= max_days:
            period = p
            break

    ctx, hdr = _bootstrap_headers()
    results = {}

    def _one(tick):
        sym = tick[:-3] if tick.endswith(".BK") else tick
        try:
            bars = fetch_daily_bars(sym, period=period, ctx=ctx, hdr=hdr)
        except Exception:
            return tick, None
        keep = [(d, c, v) for d, c, v in bars if d >= start_date]
        if not keep:
            return tick, None
        idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _, _ in keep])
        close  = pd.Series([c for _, c, _ in keep], index=idx, dtype=float)
        volume = pd.Series([v for _, _, v in keep], index=idx, dtype=float)
        return tick, {"close": close, "volume": volume}

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
                callback(done, total, f"สำรองราคา (SET API chart-quotation) {done}/{total}...")

    return results
