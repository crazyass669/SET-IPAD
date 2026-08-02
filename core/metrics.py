# -*- coding: utf-8 -*-
"""
core/metrics.py — สูตรคำนวณทั้งหมดของระบบ (single source of truth)

pure functions ไม่มี I/O / ไม่รู้จัก Flask — ใช้ร่วมกันโดย:
  - set_data_fetcher.py  (SET pipeline: process_stock, Full/Quick update)
  - app.py               (DR route, indices RS vs SET)
  - tests/               (golden-file regression)

ห้าม copy สูตรเหล่านี้ไปวางที่อื่น — import จากที่นี่เท่านั้น
"""
from datetime import datetime, timedelta
from collections import Counter, defaultdict

# น้ำหนัก RS: 40% เดือนล่าสุด + 20% ต่อช่วง 3/6/12 เดือน
RS_WEIGHTS = ((21, 2), (63, 1), (126, 1), (250, 1))  # (bars, weight) — อ้างอิง


def calc_return(series, days):
    """% return จาก bar offset (pandas Series ของ close)"""
    if len(series) < days + 1:
        return None
    try:
        past = float(series.iloc[-(days + 1)])
        now  = float(series.iloc[-1])
        if past == 0:
            return None
        return round((now - past) / past * 100, 2)
    except Exception:
        return None


def calc_return_calendar(series, calendar_days):
    """% return ย้อนหลังตาม 'วันปฏิทิน' จริง (อิง index วันที่ของ series) แทน bar offset —
    ใช้กับตราสารที่เทรดไม่ครบทุกวันทำการ (เช่น ETF ไทยบางตัวที่เบามาก) ที่ calc_return
    (นับ bar offset ตรงๆ) จะได้หน้าต่างเวลายาวกว่าที่ตั้งใจอย่างเงียบๆ (เช่น "1M" กลาย
    เป็นย้อนหลังจริง 1.5 เดือน) ทำให้เทียบกับตราสารอื่นที่เทรดครบทุกวันไม่ตรงกัน"""
    if series.empty:
        return None
    try:
        last_date = series.index[-1]
        target_date = last_date - timedelta(days=calendar_days)
        past_series = series[series.index <= target_date]
        if past_series.empty:
            return None
        past = float(past_series.iloc[-1])
        now  = float(series.iloc[-1])
        if past == 0:
            return None
        return round((now - past) / past * 100, 2)
    except Exception:
        return None


def calc_ema(series, period):
    if len(series) < period:
        return None
    try:
        return round(float(series.ewm(span=period, adjust=False).mean().iloc[-1]), 4)
    except Exception:
        return None


def calc_rs_raw(ret_1m, ret_3m, ret_6m, ret_1y):
    """RS raw score = weighted avg ของ returns: (2×1m + 3m + 6m + 1y) / น้ำหนักที่มีจริง
    คืน float (ไม่ปัด — caller ปัดเองตาม format ที่ต้องการ) หรือ None ถ้าไม่มี component เลย"""
    parts = [(ret_1m, 2), (ret_3m, 1), (ret_6m, 1), (ret_1y, 1)]
    valid = [(v, w) for v, w in parts if v is not None]
    if not valid:
        return None
    return sum(v * w for v, w in valid) / sum(w for _, w in valid)


def calc_cagr(first, last, years):
    """Compound Annual Growth Rate % — คืน None ถ้าคำนวณไม่ได้ (ค่าติดลบ/ศูนย์/years<=0)
    first/last ต้องเป็นบวกทั้งคู่ (ขาดทุน/ติดลบทำ CAGR ไม่มีความหมายทางคณิตศาสตร์)"""
    if first is None or last is None or years is None or years <= 0:
        return None
    if first <= 0 or last <= 0:
        return None
    try:
        return round(((last / first) ** (1 / years) - 1) * 100, 2)
    except Exception:
        return None


def calc_growth_score(rev_cagr, profit_cagr, margin_trend, roe_trend, de_trend):
    """รวม CAGR รายได้/กำไร + เทรนด์ margin/ROE/D-E เป็นคะแนนเดียว (raw, ไม่ normalize)
    น้ำหนัก: revenue/profit CAGR สำคัญสุด, margin/ROE trend รอง, D-E trend เป็น penalty
    (ยิ่งหนี้โตเร็วกว่าทุน ยิ่งหักคะแนน) — ข้าม component ที่เป็น None, คืน None ถ้าไม่มีเลย"""
    parts = [
        (rev_cagr,     3),
        (profit_cagr,  3),
        (margin_trend, 2),
        (roe_trend,    2),
        (-de_trend if de_trend is not None else None, 1),
    ]
    valid = [(v, w) for v, w in parts if v is not None]
    if not valid:
        return None
    return sum(v * w for v, w in valid) / sum(w for _, w in valid)


def rank_percentile(values_by_symbol):
    """แปลง {symbol: raw_value} เป็น {symbol: percentile 0-99} (pattern เดียวกับ rank_rs())
    ข้าม symbol ที่ value เป็น None"""
    items = [(sym, v) for sym, v in values_by_symbol.items() if v is not None]
    items.sort(key=lambda x: x[1])
    n = len(items)
    if n == 0:
        return {}
    return {sym: int(round(i / n * 99)) for i, (sym, v) in enumerate(items)}


def validate_stocks(stocks, data_as_of=None):
    """Data Validation Layer — ติด flag คุณภาพข้อมูลต่อหุ้น ก่อนเข้า RS/Sector calc
    Flags:
      no_data    : ขาด field สำคัญ (price/ret_1m/rs_raw)         → ออกทุกการคำนวณ
      stale      : แท่งสุดท้ายเก่ากว่า data_as_of > 7 วัน (พักเทรด) → ออกจาก RS + sector avg
      thin       : 21 แท่งล่าสุดกินเวลา > 45 วันปฏิทิน (เทรดเบาบาง) → ออกจาก RS + ret เฉลี่ยระยะยาว
      suspect_ca : |ret_1d| > 31% เกิน ceiling SET (สงสัย corporate action) → พักออกจาก RS รอบนี้
      penny      : ราคา < 0.10 บาท (tick 0.01 = ±10%+)            → rank ได้ แต่ UI ติด badge/กันออก leader list
      short_hist : ไม่มี ret_1y (IPO ใหม่)                          → rank ได้ แต่ UI แจ้ง
      no_trade   : volume = 0 ติดกัน >= 5 แท่งท้าย (SP ที่ Yahoo ยังส่ง
                   แท่ง carry-forward มา — "zombie bars" ดูสดแต่ไม่มีเทรดจริง)
                   → ออกจาก RS + sector avg เหมือน stale
    คืน dq_summary dict สำหรับใส่ใน output"""
    as_of = None
    if data_as_of:
        try:
            as_of = datetime.strptime(data_as_of, "%Y-%m-%d")
        except Exception:
            pass

    counts = Counter()
    for s in stocks:
        flags = []

        if s.get("price") is None or s.get("ret_1m") is None or s.get("rs_raw") is None:
            flags.append("no_data")

        ph = s.get("price_history") or []
        if as_of and ph:
            try:
                gap = (as_of - datetime.strptime(ph[-1][0], "%Y-%m-%d")).days
                if gap > 7:
                    flags.append("stale")
                    s["dq_stale_days"] = gap
            except Exception:
                pass
        if len(ph) >= 22:
            try:
                span = (datetime.strptime(ph[-1][0], "%Y-%m-%d")
                        - datetime.strptime(ph[-22][0], "%Y-%m-%d")).days
                if span > 45:
                    flags.append("thin")
            except Exception:
                pass

        if s.get("ret_1d") is not None and abs(s["ret_1d"]) > 31:
            flags.append("suspect_ca")
        if s.get("price") is not None and s["price"] < 0.10:
            flags.append("penny")
        if s.get("ret_1y") is None and "no_data" not in flags:
            flags.append("short_hist")

        # V7 no_trade: หุ้น SP ที่แท่งดูสดแต่ volume = 0 ต่อเนื่อง — return
        # ทุกช่วงเป็น 0% ปลอมๆ ถ้าปล่อยเข้า rank จะกอง percentile กลางตาราง
        vh = s.get("vol_history") or []
        if len(vh) >= 5 and all(v == 0 for v in vh[-5:]):
            flags.append("no_trade")

        rs_eligible    = not any(f in flags for f in
                                 ("no_data", "stale", "thin", "suspect_ca", "no_trade"))
        group_eligible = not any(f in flags for f in ("no_data", "stale", "no_trade"))
        s["dq"] = {"flags": flags, "rs_eligible": rs_eligible, "group_eligible": group_eligible}
        for f in flags:
            counts[f] += 1

    n_excl = sum(1 for s in stocks if not s["dq"]["rs_eligible"])
    return {
        "counts":      dict(counts),
        "rs_excluded": n_excl,
        "rs_universe": len(stocks) - n_excl,
    }


def classify_stage(above_ema200, ema200_slope_pct):
    """Weinstein Stage (1 Basing / 2 Advancing / 3 Distribution / 4 Declining) จาก
    ตำแหน่งเทียบ EMA200 + ความชัน EMA200 — เป็นสูตรต่อหุ้นตัวเดียวล้วน ไม่พึ่ง cohort/RS
    (ใช้ทั้งใน rank_rs และใน mirror_ondemand.fetch_header_lite ที่ไม่ได้ผ่าน rank_rs)"""
    if above_ema200 is None or ema200_slope_pct is None:
        return None
    if above_ema200 and ema200_slope_pct >= 0:
        return 2   # Advancing
    if above_ema200 and ema200_slope_pct < 0:
        return 3   # Distribution / Topping
    if (not above_ema200) and ema200_slope_pct >= -1.5:
        return 1   # Basing
    return 4       # Declining


def rank_rs(stocks):
    def _ok(s):
        return s.get("dq", {}).get("rs_eligible", True)

    # rank rs_score ปัจจุบัน — เฉพาะหุ้นที่ผ่าน validation (ตัวที่ไม่ผ่านคง rs_score=None)
    valid = [s for s in stocks if s.get("rs_raw") is not None and _ok(s)]
    valid.sort(key=lambda x: x["rs_raw"])
    n = len(valid)
    for i, s in enumerate(valid):
        s["rs_score"] = int(round(i / n * 99))

    # rank rs_score_4w และคำนวณ rs_momentum
    valid4w = [s for s in stocks if s.get("rs_raw_4w") is not None and _ok(s)]
    valid4w.sort(key=lambda x: x["rs_raw_4w"])
    n4w = len(valid4w)
    for i, s in enumerate(valid4w):
        s["rs_score_4w"] = int(round(i / n4w * 99))
    for s in stocks:
        if s.get("rs_score") is not None and s.get("rs_score_4w") is not None:
            s["rs_momentum"] = s["rs_score"] - s["rs_score_4w"]

    # Weinstein Stage จาก above_ema200 + ema200_slope_pct
    for s in stocks:
        s["stage"] = classify_stage(s.get("above_ema200"), s.get("ema200_slope_pct"))

    return stocks


def summarize_groups(stocks, key):
    groups = defaultdict(list)
    for s in stocks:
        groups[s.get(key, "Unknown")].append(s)

    # ret ระยะยาวของหุ้น thin (เทรดเบาบาง) หน่วยเวลาเพี้ยน — ไม่เอาเข้าเฉลี่ย
    _LONG_RETS = {"ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y"}

    result = []
    for name, members in groups.items():
        eligible = [m for m in members
                    if m.get("dq", {}).get("group_eligible", True)]

        def _vals(f):
            pool = eligible
            if f in _LONG_RETS:
                pool = [m for m in pool if "thin" not in m.get("dq", {}).get("flags", [])]
            return [m[f] for m in pool if m.get(f) is not None]

        def avg(f, min_n=3):
            vals = _vals(f)
            return round(sum(vals) / len(vals), 2) if len(vals) >= min_n else None

        def med(f, min_n=3):
            # median สำหรับ valuation fields — กัน outlier (เช่น PE 710) ลากค่าเฉลี่ย
            vals = sorted(_vals(f))
            n = len(vals)
            if n < min_n:
                return None
            mid = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
            return round(mid, 2)

        result.append({
            "name":             name,
            "count":            len(members),
            "n_valid":          len(_vals("ret_1m")),
            "ret_1d":           avg("ret_1d"),
            "ret_1w":           avg("ret_1w"),
            "ret_1m":           avg("ret_1m"),
            "ret_3m":           avg("ret_3m"),
            "ret_6m":           avg("ret_6m"),
            "ret_1y":           avg("ret_1y"),
            "avg_rs":           avg("rs_score"),
            "pct_above_ema50":  round(
                sum(1 for m in eligible if m.get("above_ema50")) / len(eligible) * 100
            ) if eligible else None,
            "avg_pe":           med("pe"),
            "avg_pbv":          med("pbv"),
            "avg_div_yield":    med("div_yield"),
        })
    return sorted(result, key=lambda x: x.get("ret_1m") or -999, reverse=True)
