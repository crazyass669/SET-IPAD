# -*- coding: utf-8 -*-
"""Backtest เงื่อนไข Screener+ ย้อนหลัง ~15 ปี จากงบ+ราคารายไตรมาส Finnomena ใน financials.db

คำถามที่ตอบ: "ถ้ากรองหุ้นด้วยเงื่อนไขนี้ ณ สิ้นแต่ละไตรมาสในอดีต แล้วถือ 1 ไตรมาส
ผลตอบแทนเฉลี่ยเป็นเท่าไหร่ เทียบค่าเฉลี่ยตลาด (equal-weight) ในไตรมาสเดียวกัน"

วิธีวัด (กัน look-ahead bias):
  - สัญญาณจากงบไตรมาส t (วันสิ้นงวดสังเคราะห์ของ Finnomena)
  - "ซื้อ" ณ สิ้นไตรมาส t+1 (งบไตรมาส t ประกาศแล้วแน่นอน — งบไทยออกภายใน 45-60 วัน)
  - "ขาย" ณ สิ้นไตรมาส t+2  ->  ผลตอบแทน = Close[t+2]/Close[t+1] - 1
  - benchmark = ค่าเฉลี่ยผลตอบแทนช่วงเดียวกันของ "ทุกหุ้นที่มีราคา" ใน universe

ข้อจำกัด (ต้องรู้ก่อนตีความ):
  - เป็น price return จาก Close รายไตรมาส — ไม่รวมปันผล (screen สายปันผลจะดูแย่กว่าจริง)
  - survivorship bias บางส่วน: ใช้หุ้นทั้งหมดใน mirror รวมตัวที่ delist ไปแล้ว
    (ประวัติก่อน delist ยังอยู่ในการทดสอบ) แต่ตัวที่ Finnomena ไม่เคยเก็บจะไม่มี
  - Close เป็นราคา ณ วันสิ้นงวดสังเคราะห์ ไม่ใช่ราคาซื้อขายจริงวันประกาศงบ
  - ตัด outlier: ผลตอบแทนต่อไตรมาสถูก clip ที่ ±80% กันหุ้นเก็งกำไร/ข้อมูลเพี้ยนลากค่าเฉลี่ย

ใช้:
    python backtest_screener.py            หุ้นไทยทั้งตลาด (FINN:TH — ~1,000 ตัว)
    python backtest_screener.py US         หุ้น US (ช้ากว่า ~24k ตัว)
    python backtest_screener.py TH 2018    เริ่มนับตั้งแต่ปี 2018

ผลบันทึกที่ backtest_screener_results.json (local-only เหมือน financials.db)
"""
import io
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))

RET_CLIP = 0.80          # clip ผลตอบแทนต่อไตรมาส ±80% กัน outlier ลากค่าเฉลี่ย
MAX_TRADE_GAP = 120      # วันสูงสุดระหว่างไตรมาสซื้อ-ขาย (ราย Q ~91 วัน — เกิน = งวดขาด ข้าม)
MIN_PICKS = 3            # ไตรมาสที่ preset เจอหุ้นน้อยกว่านี้ ไม่นับ (ค่าเฉลี่ยไม่มีนัยยะ)


def _days(a, b):
    try:
        return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days
    except Exception:
        return None


def _load_payloads(ex):
    con = sqlite3.connect(os.path.join(BASE, "financials.db"))
    try:
        rows = con.execute(
            "SELECT symbol, payload FROM financials WHERE source='finnomena_q' "
            "AND symbol LIKE ? AND payload NOT LIKE '%\"empty\": true%'",
            (f"FINN:{ex}:%",)).fetchall()
    finally:
        con.close()
    return [(k.split(":", 2)[2], json.loads(p)) for k, p in rows]


def _sorted_items(row):
    return sorted((d, v) for d, v in (row or {}).items() if v is not None)


def _yoy_at(vals):
    """คืน {date: %YoY} ของ series รายงวด (หา 'งวดเดียวกันปีก่อน' ด้วยหน้าต่าง 330-400 วัน)"""
    out = {}
    for i in range(1, len(vals)):
        base = None
        for j in range(i - 1, -1, -1):
            g = _days(vals[j][0], vals[i][0])
            if g is None or g > 400:
                break
            if 330 <= g <= 400:
                base = vals[j][1]
                break
        if base and base > 0:
            out[vals[i][0]] = (vals[i][1] - base) / base * 100
    return out


def _accel_at(vals, yoy):
    """คืน {date: จำนวนก้าวติดกันที่ %YoY สูงขึ้น} (นับถึงงวดนั้น)"""
    ds = [d for d, _ in vals if d in yoy]
    out = {}
    for i, d in enumerate(ds):
        n = 0
        for k in range(i, 0, -1):
            if yoy[ds[k]] > yoy[ds[k - 1]]:
                n += 1
            else:
                break
        out[d] = n
    return out


def _ttm_at(vals):
    """คืน {date: ยอดรวม 4 งวดติดกันจบที่งวดนั้น} — เฉพาะจุดที่ 4 งวด span <= 340 วัน (ราย Q ครบ)"""
    out = {}
    for i in range(3, len(vals)):
        span = _days(vals[i - 3][0], vals[i][0])
        if span is not None and span <= 340:
            out[vals[i][0]] = sum(v for _, v in vals[i - 3:i + 1])
    return out


def _streak_ge_at(items, threshold):
    """คืน {date: จำนวนงวดติดกันที่ค่า >= threshold} (นับถึงงวดนั้น)"""
    out = {}
    run = 0
    for d, v in items:
        run = run + 1 if v >= threshold else 0
        out[d] = run
    return out


def _prep_stock(p):
    """precompute factor ต่อไตรมาสของหุ้น 1 ตัว — คืน dict ของ series {date: val} หรือ None"""
    inc = p.get("income", {})
    cf = p.get("cashflow", {})
    rat = p.get("ratios", {})
    val = p.get("valuation", {})

    close = _sorted_items(val.get("Close"))
    ni = _sorted_items(inc.get("Net Income"))
    rev = _sorted_items(inc.get("Total Revenue"))
    if len(close) < 8 or len(ni) < 8:
        return None

    ni_yoy = _yoy_at(ni)
    rev_yoy = _yoy_at(rev)
    ni_ttm = _ttm_at(ni)

    # กำไรโต TTM YoY (ตัวหาร PEG)
    ttm_items = sorted(ni_ttm.items())
    ni_ttm_yoy = _yoy_at(ttm_items)

    # OCF/NI TTM
    ocf = _sorted_items(cf.get("Operating Cash Flow"))
    ocf_ttm = _ttm_at(ocf)
    ocf_ni = {}
    for d, v in ni_ttm.items():
        o = ocf_ttm.get(d)
        if o is not None and v > 0:
            ocf_ni[d] = o / v

    roe = dict(_sorted_items(rat.get("ROE")))
    nm = dict(_sorted_items(rat.get("Net Margin")))
    de = dict(_sorted_items(rat.get("Debt To Equity")))
    roe15 = _streak_ge_at([(d, v) for d, v in sorted(roe.items()) if -300 <= v <= 300], 15.0)

    # PE percentile แบบ expanding (เทียบเฉพาะประวัติ 'ก่อนหน้า' ถึงงวดนั้น — ไม่แอบเห็นอนาคต)
    pe_pts = [(d, v) for d, v in _sorted_items(val.get("PE")) if v > 0]
    pe_pct = {}
    pe_val = {}
    for i, (d, v) in enumerate(pe_pts):
        pe_val[d] = v
        if i + 1 >= 12:
            hist = [x for _, x in pe_pts[:i + 1]]
            pe_pct[d] = sum(1 for x in hist if x < v) / len(hist) * 100

    dy = dict((d, v) for d, v in _sorted_items(val.get("Dividend Yield")) if v > 0)

    return {"close": close, "ni_yoy": ni_yoy, "rev_yoy": rev_yoy,
            "ni_accel": _accel_at(ni, ni_yoy), "ni_ttm_yoy": ni_ttm_yoy,
            "ocf_ni": ocf_ni, "roe": roe, "nm": nm, "de": de, "roe15": roe15,
            "pe_pct": pe_pct, "pe_val": pe_val, "dy": dy,
            "signal_dates": [d for d, _ in ni]}


def _fwd_return(stock, signal_d):
    """ผลตอบแทนถือ 1 ไตรมาส: ซื้อสิ้นไตรมาสถัดจาก signal (งบประกาศแล้ว) ขายอีกไตรมาสถัดไป"""
    close = stock["close"]
    ds = [d for d, _ in close]
    import bisect
    i = bisect.bisect_right(ds, signal_d)          # งวดแรกหลัง signal = จุดซื้อ
    if i + 1 >= len(ds):
        return None, None
    buy_d, sell_d = ds[i], ds[i + 1]
    g1, g2 = _days(signal_d, buy_d), _days(buy_d, sell_d)
    if g1 is None or g2 is None or g1 > MAX_TRADE_GAP or g2 > MAX_TRADE_GAP:
        return None, None
    buy, sell = close[i][1], close[i + 1][1]
    if not buy or buy <= 0 or not sell or sell <= 0:
        return None, None
    r = sell / buy - 1
    return buy_d, max(-RET_CLIP, min(RET_CLIP, r))


# ---- เงื่อนไข preset (mirror ของ FS_PRESETS ที่คำนวณย้อนอดีตได้จาก Finnomena) ----

def _g(series, d):
    return series.get(d)


PRESETS = {
    "CANSLIM (พื้นฐาน)": lambda s, d: (
        (_g(s["ni_yoy"], d) or -1e9) >= 25 and (_g(s["rev_yoy"], d) or -1e9) >= 0
        and (_g(s["ni_accel"], d) or 0) >= 2),
    "Quality Compounder": lambda s, d: (
        (_g(s["roe15"], d) or 0) >= 8 and (_g(s["nm"], d) or -1e9) >= 10
        and (_g(s["de"], d) if _g(s["de"], d) is not None else 1e9) <= 1
        and (_g(s["ocf_ni"], d) or -1e9) >= 0.8),
    "คุณภาพกำไร (OCF/NI>=0.8)": lambda s, d: (
        (_g(s["ocf_ni"], d) or -1e9) >= 0.8 and (_g(s["ni_yoy"], d) or -1e9) >= 0),
    "Value ในอดีตตัวเอง (PE%<=25)": lambda s, d: (
        (_g(s["pe_pct"], d) if _g(s["pe_pct"], d) is not None else 1e9) <= 25
        and (_g(s["ni_yoy"], d) or -1e9) >= 0),
    "GARP (PEG<=1)": lambda s, d: (
        _g(s["pe_val"], d) is not None and _g(s["ni_ttm_yoy"], d) is not None
        and 1 <= s["ni_ttm_yoy"][d] <= 200
        and s["pe_val"][d] / s["ni_ttm_yoy"][d] <= 1
        and (_g(s["roe"], d) or -1e9) >= 12),
    "Dividend >=4%": lambda s, d: (_g(s["dy"], d) or 0) >= 4,
    "Turnaround (YoY>=50 x2Q)": lambda s, d: (
        (_g(s["ni_yoy"], d) or -1e9) >= 50 and (_g(s["ni_accel"], d) or 0) >= 1
        and (_g(s["ni_yoy"], d) or 0) <= 3000),
}


def run(ex="TH", start_year=2011):
    t0 = time.time()
    print(f"[Backtest] โหลดงบ {ex} ...", flush=True)
    payloads = _load_payloads(ex)
    stocks = {}
    for name, p in payloads:
        s = _prep_stock(p)
        if s:
            stocks[name] = s
    print(f"[Backtest] ใช้ได้ {len(stocks)}/{len(payloads)} ตัว ({time.time()-t0:.0f} วิ)", flush=True)

    # benchmark: ผลตอบแทนทุกหุ้นทุกไตรมาสซื้อขาย (จัดกลุ่มด้วยวันซื้อ)
    bench = defaultdict(list)                       # buy_date -> [ret, ...]
    picks = {k: defaultdict(list) for k in PRESETS}  # preset -> buy_date -> [ret]
    for name, s in stocks.items():
        for sig_d in s["signal_dates"]:
            if int(sig_d[:4]) < start_year:
                continue
            buy_d, r = _fwd_return(s, sig_d)
            if r is None:
                continue
            bench[buy_d].append(r)
            for pname, cond in PRESETS.items():
                try:
                    if cond(s, sig_d):
                        picks[pname][buy_d].append(r)
                except Exception:
                    pass

    bench_avg = {d: sum(v) / len(v) for d, v in bench.items() if len(v) >= 30}
    results = {}
    print(f"\n{'เงื่อนไข':<34}{'ไตรมาส':>7}{'หุ้น/Q':>8}{'ผลตอบแทน/Q':>12}{'ตลาด/Q':>9}{'ส่วนต่าง':>10}{'ชนะตลาด':>9}")
    print("-" * 92)
    for pname, by_date in picks.items():
        rows = []
        for d, rets in sorted(by_date.items()):
            if d not in bench_avg or len(rets) < MIN_PICKS:
                continue
            rows.append((d, sum(rets) / len(rets), bench_avg[d], len(rets)))
        if len(rows) < 8:
            print(f"{pname:<34}{'—':>7}   (ไตรมาสที่ทดสอบได้ไม่พอ)")
            continue
        avg_r = sum(r for _, r, _, _ in rows) / len(rows) * 100
        avg_b = sum(b for _, _, b, _ in rows) / len(rows) * 100
        excess = avg_r - avg_b
        win = sum(1 for _, r, b, _ in rows if r > b) / len(rows) * 100
        n_avg = sum(n for _, _, _, n in rows) / len(rows)
        results[pname] = {"quarters": len(rows), "avg_picks": round(n_avg, 1),
                          "ret_q_pct": round(avg_r, 2), "bench_q_pct": round(avg_b, 2),
                          "excess_q_pct": round(excess, 2), "win_rate_pct": round(win, 1),
                          "by_quarter": [{"buy": d, "ret": round(r * 100, 2),
                                          "bench": round(b * 100, 2), "n": n} for d, r, b, n in rows]}
        print(f"{pname:<34}{len(rows):>7}{n_avg:>8.0f}{avg_r:>11.2f}%{avg_b:>8.2f}%{excess:>+9.2f}%{win:>8.0f}%")

    out = {"exchange": ex, "start_year": start_year, "stocks_used": len(stocks),
           "note": "price return ไม่รวมปันผล · clip ±80%/Q · ซื้อสิ้นไตรมาสถัดจากงบ ถือ 1 ไตรมาส",
           "benchmark_quarters": len(bench_avg), "results": results,
           "ran_at": time.strftime("%Y-%m-%d %H:%M")}
    path = os.path.join(BASE, f"backtest_screener_results_{ex}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"\n[Backtest] เสร็จใน {time.time()-t0:.0f} วิ — รายไตรมาสเต็มอยู่ที่ {os.path.basename(path)}")
    print("[Backtest] คำเตือน: price return ไม่รวมปันผล — screen สายปันผลถูกกดต่ำกว่าจริง ~1-2%/Q")
    return out


if __name__ == "__main__":
    args = sys.argv[1:]
    ex = (args[0].upper() if args else "TH")
    yr = int(args[1]) if len(args) > 1 else 2011
    run(ex, yr)
