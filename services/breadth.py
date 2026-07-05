# -*- coding: utf-8 -*-
"""
services/breadth.py — Market Breadth Indicators รายวัน ย้อนหลัง ~1 ปี

คำนวณจาก SQLite price store แบบ vectorized (pandas wide DataFrame):
  - pct_above_ema50 / pct_above_ema200 : % ของหุ้นที่ราคาปิด > EMA
  - nh / nl / nhnl_ratio               : 52W New High/Low counts + NH/(NH+NL)
  - adv / dec                          : จำนวนหุ้นขึ้น/ลงรายวัน
  - mcclellan_osc                      : EMA19 - EMA39 ของ RANA
                                         (ratio-adjusted net advances = 1000*(A-D)/(A+D))
  - mcclellan_sum                      : summation ของ oscillator (เริ่ม 0 ที่ต้น window)

Performance: คำนวณครั้งเดียว ~วินาทีระดับเดียวกับ market-internals แล้ว cache
ฝั่ง app.py (memory, event invalidation หลัง Quick Update) — ไม่กระทบ request อื่น
"""
import numpy as np
import pandas as pd

from core.store import iter_all_series

DISPLAY_DAYS = 252   # ~1 ปีซื้อขาย
WARMUP_BARS  = 800   # ต่อหุ้น: 252 แสดง + 252 rolling 52W + ~300 EMA warmup


def compute_breadth(base_dir, days=DISPLAY_DAYS):
    series = {}
    for tick, d in iter_all_series(base_dir):
        if len(d["dates"]) < 60:          # ข้อมูลน้อยเกินกว่าจะมีความหมาย
            continue
        idx = pd.to_datetime(d["dates"][-WARMUP_BARS:])
        series[tick] = pd.Series(d["closes"][-WARMUP_BARS:], index=idx, dtype="float64")

    if not series:
        return None

    close = pd.DataFrame(series).sort_index()
    valid = close.notna()
    n_universe = valid.sum(axis=1)

    # ---- % above EMA (per-stock EMA ตามแนวเดียวกับ metrics หลัก) ----
    ema50  = close.ewm(span=50,  adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    pct50  = (close > ema50).sum(axis=1)  / n_universe * 100
    pct200 = (close > ema200).sum(axis=1) / n_universe * 100

    # ---- Advances / Declines + McClellan ----
    chg = close.diff()
    adv = (chg > 0).sum(axis=1)
    dec = (chg < 0).sum(axis=1)
    denom = (adv + dec).replace(0, np.nan)
    rana = 1000.0 * (adv - dec) / denom          # ratio-adjusted net advances
    rana = rana.ffill()
    mcc_osc = rana.ewm(span=19, adjust=False).mean() - rana.ewm(span=39, adjust=False).mean()

    # ---- 52W New High / New Low (เทียบ max/min 252 แท่งก่อนหน้า) ----
    prior_max = close.shift(1).rolling(252, min_periods=100).max()
    prior_min = close.shift(1).rolling(252, min_periods=100).min()
    nh = (close >= prior_max).sum(axis=1)
    nl = (close <= prior_min).sum(axis=1)

    # ---- ตัดเหลือ window แสดงผล ----
    tail = close.index[-days:]
    mcc_win = mcc_osc.loc[tail]
    out = {
        "dates":            [d.strftime("%Y-%m-%d") for d in tail],
        "n_universe":       [int(x) for x in n_universe.loc[tail]],
        "pct_above_ema50":  [round(float(x), 1) if np.isfinite(x) else None for x in pct50.loc[tail]],
        "pct_above_ema200": [round(float(x), 1) if np.isfinite(x) else None for x in pct200.loc[tail]],
        "adv":              [int(x) for x in adv.loc[tail]],
        "dec":              [int(x) for x in dec.loc[tail]],
        "nh":               [int(x) for x in nh.loc[tail]],
        "nl":               [int(x) for x in nl.loc[tail]],
        "nhnl_ratio":       [round(float(h / (h + l)), 3) if (h + l) > 0 else None
                             for h, l in zip(nh.loc[tail], nl.loc[tail])],
        "mcclellan_osc":    [round(float(x), 1) if np.isfinite(x) else None for x in mcc_win],
        # summation เริ่มนับ 0 ที่ต้น window (ระดับ absolute ขึ้นกับจุดเริ่ม —
        # ใช้ดู "ทิศ" ของ breadth สะสม ไม่ใช่ค่ามาตรฐานข้ามระบบ)
        "mcclellan_sum":    [round(float(x), 1) if np.isfinite(x) else None
                             for x in mcc_win.fillna(0).cumsum()],
    }
    return out
