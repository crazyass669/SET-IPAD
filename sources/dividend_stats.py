# -*- coding: utf-8 -*-
"""sources/dividend_stats.py — คำนวณสถิติปันผลจากประวัติ ex-date/DPS (financials_store.get_dividends)

Phase A (เริ่มแรก): DPS รายปี, streak, CAGR, YoY, ความถี่การจ่าย/ปี, yield รายปี (ต้องมี
price_series จาก local price store — TH/US/HK รองรับแล้ว ดู /api/dividends ใน app.py, DR ยังไม่)
ยังไม่ทำ (phase B): payout ratio จาก EPS Finnomena + ปรับ split (เสี่ยงผิดสุด ต้องมี
split log ก่อน — ดู PLAN_stock_study_suite.txt งาน #5)
"""
from collections import defaultdict
from datetime import date


def _annual_from_rows(rows):
    """rows: [{ex_date, dps}] -> {year: {"dps": sum, "count": n, "events": [...]}}"""
    by_year = defaultdict(lambda: {"dps": 0.0, "count": 0, "events": []})
    for r in rows:
        y = int(r["ex_date"][:4])
        by_year[y]["dps"] += r["dps"]
        by_year[y]["count"] += 1
        by_year[y]["events"].append(r)
    return dict(by_year)


def _near_zero_base(value, others, ratio=0.15):
    """value บวกจริงแต่จิ๋วผิดปกติเทียบ median ของ DPS ปีอื่นในประวัติเดียวกัน (pattern เดียวกับ
    scale_ref ของ compute_cash_cycle/financials_store.py และ _latest_eps ของ factor_snapshot.py
    — CALC_RISK_AUDIT_ROUND4) กัน yoy_growth_pct/cagr_Ny_pct ระเบิดจากปีฐาน DPS จิ๋วผิดปกติ
    (เช่น ปันผลค้างท่อ/ปีที่จ่ายแค่บางส่วนก่อนหยุดจ่ายชั่วคราว) แทนที่จะเป็นธุรกิจลด DPS ถาวรจริง"""
    pos = sorted(v for v in others if v and v > 0)
    if not pos:
        return False
    scale_ref = pos[len(pos) // 2]
    return value < scale_ref * ratio


def _nearest_close_on_or_before(dates, closes, target_iso):
    """หาราคาปิดล่าสุดที่ <= target_iso จาก series ที่เรียงตามวันที่แล้ว (binary search มือ)"""
    lo, hi, best = 0, len(dates) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] <= target_iso:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return closes[best] if best is not None else None


def compute_payout_ratio(rows, eps_value, eps_date):
    """Payout Ratio (%) = DPS ของปีเดียวกับปีที่ปิดงวด EPS ล่าสุด ÷ EPS ล่าสุด × 100

    ตั้งใจ **ไม่ปรับ split เอง** ต่างจากแผนเดิม (PLAN_stock_study_suite.txt งาน #5 เฟส B บอกว่า
    เสี่ยงผิดสุดถ้าไม่มี split log) — ใช้ EPS จากงบ Yahoo (sources.factor_snapshot._latest_eps)
    ที่รายงาน ณ จำนวนหุ้นของงวดนั้นอยู่แล้วเสมอ เทียบกับ DPS ปีปฏิทินเดียวกับวันที่ปิดงวด (ไม่ใช่
    ปีปัจจุบัน — กันหุ้นที่ปิดงวดการเงินไม่ตรง ธ.ค. เช่น AAPL ปิดงวด ก.ย.) ทั้งคู่จึงอยู่ในช่วง
    เวลาเดียวกัน ไม่มีปัญหา split-adjustment ต่างกันแบบที่แผนเดิมกังวล (เทียบข้อมูลย้อนหลังหลายปี
    ถึงจะมีความเสี่ยงนั้น ตัวนี้เทียบแค่งวดล่าสุดงวดเดียว)

    คืน None ถ้าไม่มี EPS, EPS<=0 (ขาดทุน — payout ratio ไม่มีความหมาย), หรือไม่มี DPS ปีนั้นเลย"""
    if not rows or eps_value is None or eps_value <= 0 or not eps_date:
        return None
    year = str(eps_date)[:4]
    dps_year = sum(r["dps"] for r in rows if r["ex_date"][:4] == year)
    if dps_year <= 0:
        return None
    return round(dps_year / eps_value * 100, 1)


def compute_dividend_stats(rows, price_series=None, current_year=None):
    """rows: [{ex_date, dps}] เรียงตามวันที่ (ผลจาก financials_store.get_dividends)
    price_series: {"dates": [...], "closes": [...]} จาก core.store.get_series (optional —
    ไม่มีก็ข้าม yield รายปี)
    คืน None ถ้าไม่มีประวัติเลย"""
    if not rows:
        return None

    annual = _annual_from_rows(rows)
    years = sorted(annual.keys())
    cy = current_year or date.today().year
    # ปีปัจจุบันอาจจ่ายไม่ครบรอบ (ปันผลระหว่างกาลยังไม่ออกงวดสุดท้าย) — ไม่นับใน streak/CAGR
    # แต่ยังโชว์เป็นแถวข้อมูลดิบได้
    complete_years = [y for y in years if y < cy]

    # นับย้อนจากปีล่าสุดที่ "ครบปี" กลับไปทีละปี — หยุดทันทีที่เจอปีขาดหาย (ไม่ต่อเนื่อง)
    # ถ้าปีครบปีล่าสุดไม่ใช่ cy-1 (หุ้นหยุดจ่ายไปแล้วหลายปี) ถือว่า streak = 0 ทันที — ไม่งั้นจะนับ
    # ย้อนจากปีที่หยุดจ่ายไปแล้วทำให้ดูเหมือนยังจ่ายต่อเนื่องอยู่ทั้งที่หยุดไปนานแล้ว
    still_paying = bool(complete_years) and complete_years[-1] == cy - 1

    # streak_paid: จ่าย > 0 ต่อเนื่อง (แยกอิสระจาก nondecreasing — ปีที่จ่ายลดลงยังนับต่อได้)
    streak_paid = 0
    if still_paying:
        for i in range(len(complete_years) - 1, -1, -1):
            y = complete_years[i]
            if i < len(complete_years) - 1 and y != complete_years[i + 1] - 1:
                break
            if annual[y]["dps"] <= 0:
                break
            streak_paid += 1

    # streak_nondecreasing: จ่ายเท่าเดิมหรือมากขึ้นต่อเนื่อง (หยุดทันทีที่ลดลงหรือขาดหาย)
    streak_nondecreasing = 0
    if still_paying:
        for i in range(len(complete_years) - 1, -1, -1):
            y = complete_years[i]
            if i < len(complete_years) - 1 and y != complete_years[i + 1] - 1:
                break
            if annual[y]["dps"] <= 0:
                break
            if i < len(complete_years) - 1 and annual[y]["dps"] > annual[complete_years[i + 1]]["dps"]:
                break
            streak_nondecreasing += 1

    def _cagr(n_years):
        pts = [y for y in complete_years if y > cy - 1 - n_years]
        if len(pts) < 2:
            return None
        first_y, last_y = pts[0], pts[-1]
        first, last = annual[first_y]["dps"], annual[last_y]["dps"]
        span = last_y - first_y
        if first <= 0 or span <= 0:
            return None
        if _near_zero_base(first, (annual[y]["dps"] for y in complete_years if y != first_y)):
            return None
        return round(((last / first) ** (1 / span) - 1) * 100, 2)

    yoy = None
    if len(complete_years) >= 2:
        y0, y1 = complete_years[-1], complete_years[-2]
        # y0/y1 ต้องเป็นปีติดกันจริงถึงจะเรียกว่า "YoY" ได้ — หุ้นที่หยุดจ่ายไปหลายปีแล้ว
        # กลับมาจ่ายใหม่ (เช่น INSURE: 2008 -> ว่าง 16 ปี -> 2025) ปี 2 ตัวท้ายสุดที่ "มี
        # ข้อมูล" ไม่ใช่ปีติดกัน ถ้าไม่กันไว้จะได้ growth ข้ามหลายปีแต่โชว์เป็น YoY หลอก
        # (เคยเจอจริง INSURE ได้ 900% ทั้งที่จริงคือ growth ตลอด 17 ปี ไม่ใช่ปีเดียว)
        if y0 == y1 + 1:
            prev = annual[y1]["dps"]
            if prev > 0 and not _near_zero_base(prev, (annual[y]["dps"] for y in complete_years if y != y1)):
                yoy = round((annual[y0]["dps"] / prev - 1) * 100, 2)

    freq = None
    if complete_years:
        recent = complete_years[-3:]
        counts = [annual[y]["count"] for y in recent]
        freq = round(sum(counts) / len(counts))

    yearly = []
    d_dates = (price_series or {}).get("dates")
    d_closes = (price_series or {}).get("closes")
    for y in years:
        row = {"year": y, "dps": round(annual[y]["dps"], 4), "count": annual[y]["count"],
               "complete": y < cy, "yield_pct": None}
        if d_dates and d_closes:
            px = _nearest_close_on_or_before(d_dates, d_closes, f"{y}-12-31")
            if px:
                row["yield_pct"] = round(annual[y]["dps"] / px * 100, 2)
        yearly.append(row)

    return {
        "years": yearly,
        "streak_paid_years": streak_paid,
        "streak_nondecreasing_years": streak_nondecreasing,
        "cagr_5y_pct": _cagr(5),
        "cagr_10y_pct": _cagr(10),
        "yoy_growth_pct": yoy,
        "events_per_year": freq,
        "events": rows,
    }
