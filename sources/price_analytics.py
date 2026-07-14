# -*- coding: utf-8 -*-
"""sources/price_analytics.py — วิเคราะห์ราคาระยะยาวจาก set_prices.db (ย้อนถึง 1983)

ใช้ Adj Close (ปรับ split+ปันผล) เป็นหลักในการคำนวณผลตอบแทน/ฤดูกาล/drawdown ให้ถูก
— ราคาปิดดิบ (close) ใช้เฉพาะ "ระดับราคาจริง" (ATH ที่คนเห็นบนกราฟ) เท่านั้น

ฟังก์ชันคำนวณเป็น pure (รับ list dates + values) เพื่อทดสอบง่าย; ตัว build_* ต่อกับ store
"""
from datetime import date as _date


def _to_date(s):
    return _date.fromisoformat(s[:10])


def _clean_adj_series(ohlc):
    """คืน (dates, adj_values) ที่ตัด None/<=0 ออก — ถ้า adj_close ว่าง (หุ้น delisted
    ที่ backfill ไม่ได้) fallback ใช้ close ดิบแทน (แม่นน้อยกว่าแต่ดีกว่าไม่มี)"""
    dates = ohlc.get("dates") or []
    adj = ohlc.get("adj_closes") or []
    close = ohlc.get("closes") or []
    out_d, out_v = [], []
    use_adj = any(v is not None for v in adj)
    src = adj if use_adj else close
    for d, v in zip(dates, src):
        if v is not None and v > 0:
            out_d.append(d)
            out_v.append(float(v))
    return out_d, out_v, use_adj


# ── Seasonality รายเดือน ─────────────────────────────────────
def monthly_seasonality(dates, values, min_years=5):
    """ผลตอบแทนเฉลี่ยรายเดือนปฏิทิน (ม.ค.–ธ.ค.) จากผลตอบแทนรายเดือน (สิ้นเดือนต่อสิ้นเดือน)

    คืน {'months': [{month, avg_return_pct, hit_rate_pct, n}], 'years_span', 'enough_data'}
    — enough_data=False ถ้าข้อมูลน้อยกว่า min_years (seasonality ไม่มีนัย)"""
    if len(dates) < 60:
        return {"months": [], "years_span": 0, "enough_data": False}

    # เก็บราคาปิด "สิ้นเดือน" (แท่งสุดท้ายของแต่ละเดือน)
    month_last = {}   # "YYYY-MM" -> value (แท่งสุดท้ายของเดือนนั้น)
    for d, v in zip(dates, values):
        ym = d[:7]
        month_last[ym] = v   # dates เรียงอยู่แล้ว -> ค่าสุดท้ายคือสิ้นเดือน

    keys = sorted(month_last)
    if len(keys) < 24:
        return {"months": [], "years_span": 0, "enough_data": False}

    # ผลตอบแทนรายเดือน = สิ้นเดือนนี้ / สิ้นเดือนก่อน - 1 (เฉพาะเดือนติดกันจริง)
    from collections import defaultdict
    by_cal_month = defaultdict(list)   # 1..12 -> [returns]
    for i in range(1, len(keys)):
        prev_k, cur_k = keys[i - 1], keys[i]
        py, pm = int(prev_k[:4]), int(prev_k[5:7])
        cy, cm = int(cur_k[:4]), int(cur_k[5:7])
        # ต้องเป็นเดือนถัดไปติดกัน (กัน gap ตอนหุ้นหยุดเทรดยาว)
        if (cy - py) * 12 + (cm - pm) != 1:
            continue
        prev_v = month_last[prev_k]
        if prev_v > 0:
            ret = month_last[cur_k] / prev_v - 1
            by_cal_month[cm].append(ret)

    years_span = (_to_date(dates[-1]) - _to_date(dates[0])).days / 365.25
    months = []
    for m in range(1, 13):
        rets = by_cal_month.get(m, [])
        if rets:
            avg = sum(rets) / len(rets)
            hit = sum(1 for r in rets if r > 0) / len(rets)
            months.append({"month": m, "avg_return_pct": round(avg * 100, 2),
                           "hit_rate_pct": round(hit * 100, 1), "n": len(rets)})
        else:
            months.append({"month": m, "avg_return_pct": None, "hit_rate_pct": None, "n": 0})

    return {"months": months, "years_span": round(years_span, 1),
            "enough_data": years_span >= min_years}


# ── Drawdown ─────────────────────────────────────────────────
def drawdown_stats(dates, closes):
    """สถิติ drawdown จาก "ราคาปิดดิบ" (ระดับราคาจริงที่คนเห็นบนกราฟ ตรงกับ '% จาก ATH'
    ในแอป) — ไม่ใช้ adj เพราะ drawdown = 'ราคาตกจากยอดกี่ %' ตามความรู้สึกผู้ถือ

    คืน ATH + วันที่, current_drawdown_pct (จาก ATH), max_drawdown_pct (peak→trough ลึกสุด)
    + วันยอด/ก้น, recovered"""
    # ตัด None ออก (หุ้น delisted บางแท่งอาจว่าง)
    pairs = [(d, c) for d, c in zip(dates, closes) if c is not None and c > 0]
    if len(pairs) < 30:
        return None
    dts = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]

    max_dd = 0.0
    max_dd_peak_date = dts[0]
    max_dd_trough_date = dts[0]
    cur_peak = vals[0]
    peak_date = dts[0]
    for d, v in zip(dts, vals):
        if v > cur_peak:
            cur_peak = v
            peak_date = d
        dd = v / cur_peak - 1
        if dd < max_dd:
            max_dd = dd
            max_dd_peak_date = peak_date
            max_dd_trough_date = d

    ath = max(vals)
    ath_date = dts[vals.index(ath)]
    cur_dd = vals[-1] / ath - 1

    return {
        "ath_price": round(ath, 4),
        "ath_date": ath_date,
        "current_price": round(vals[-1], 4),
        "current_drawdown_pct": round(cur_dd * 100, 1),
        "max_drawdown_pct": round(max_dd * 100, 1),
        "max_dd_peak_date": max_dd_peak_date,
        "max_dd_trough_date": max_dd_trough_date,
        "recovered": cur_dd > max_dd + 1e-9,   # ปัจจุบันตื้นกว่าก้นที่เคยลึกสุด = ฟื้นแล้ว
    }


# ── ผลตอบแทนระยะยาว ──────────────────────────────────────────
def long_term_returns(dates, values):
    """CAGR ตลอดช่วง + ผลตอบแทนย้อน 1/3/5/10 ปี + ปีที่ดี/แย่สุด (จาก adj close)"""
    if len(values) < 250:
        return None
    d0, dN = _to_date(dates[0]), _to_date(dates[-1])
    span_years = (dN - d0).days / 365.25
    if span_years < 1:
        return None
    cagr = (values[-1] / values[0]) ** (1 / span_years) - 1

    # trailing returns: หา index แรกที่ห่างจากวันสุดท้าย >= n ปี
    def _trailing(n_years):
        target = dN.toordinal() - int(n_years * 365.25)
        for i in range(len(dates)):
            if _to_date(dates[i]).toordinal() >= target:
                if values[i] > 0:
                    return round((values[-1] / values[i] - 1) * 100, 1)
                return None
        return None

    # ผลตอบแทนรายปีปฏิทิน (สิ้นปีต่อสิ้นปี)
    year_last = {}
    for d, v in zip(dates, values):
        year_last[d[:4]] = v
    yrs = sorted(year_last)
    yearly = []
    for i in range(1, len(yrs)):
        if int(yrs[i]) - int(yrs[i - 1]) == 1 and year_last[yrs[i - 1]] > 0:
            yearly.append((yrs[i], year_last[yrs[i]] / year_last[yrs[i - 1]] - 1))
    best = max(yearly, key=lambda x: x[1]) if yearly else None
    worst = min(yearly, key=lambda x: x[1]) if yearly else None

    return {
        "cagr_pct": round(cagr * 100, 1),
        "span_years": round(span_years, 1),
        "ret_1y_pct": _trailing(1), "ret_3y_pct": _trailing(3),
        "ret_5y_pct": _trailing(5), "ret_10y_pct": _trailing(10),
        "best_year": best[0] if best else None,
        "best_year_pct": round(best[1] * 100, 1) if best else None,
        "worst_year": worst[0] if worst else None,
        "worst_year_pct": round(worst[1] * 100, 1) if worst else None,
    }


# ── รวมทั้งหมดต่อหุ้น ─────────────────────────────────────────
def analyze(ohlc, min_years=5):
    """รับผลจาก store.get_ohlc_series() คืน dict analytics รวม (หรือ None ถ้าข้อมูลไม่พอ)"""
    if not ohlc or not ohlc.get("dates"):
        return None
    dates, values, used_adj = _clean_adj_series(ohlc)
    if len(dates) < 250:
        return None
    return {
        # seasonality/returns ใช้ adj close (total return ถูกต้อง); drawdown ใช้ราคาดิบ
        "source": "adj_close" if used_adj else "close",
        "seasonality": monthly_seasonality(dates, values, min_years=min_years),
        "drawdown": drawdown_stats(ohlc.get("dates"), ohlc.get("closes")),
        "returns": long_term_returns(dates, values),
        "as_of": dates[-1],
    }


def build_for_symbol(base_dir, ticker, min_years=5):
    """โหลด OHLC จาก store แล้ววิเคราะห์ — ticker ควรมี .BK ต่อท้ายแล้ว"""
    from core import store
    ohlc = store.get_ohlc_series(base_dir, ticker)
    if not ohlc:
        return None
    return analyze(ohlc, min_years=min_years)
