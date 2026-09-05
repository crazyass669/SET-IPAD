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

from core import store as _default_store

DISPLAY_DAYS = 252   # ~1 ปีซื้อขาย (ค่า default เมื่อไม่ระบุ days)
WARMUP_EXTRA = 550   # buffer เพิ่มจาก days ที่แสดง: 252 rolling 52W + ~300 EMA warmup

# ตัวเลือกช่วงเวลาที่หน้า UI เรียกใช้ได้ (~จำนวนวันซื้อขาย, None = ทั้งหมดที่มี)
RANGE_DAYS = {"1y": 252, "3y": 756, "5y": 1260, "all": None}


def compute_breadth(base_dir, days=DISPLAY_DAYS, store=None):
    """days=None → ใช้ข้อมูลทั้งหมดที่มี (ไม่ตัด warmup ต่อหุ้น)
    store=None → core.store (หุ้นไทย, set_prices.db) — ส่ง core.us_store เข้ามาแทน
    เพื่อคำนวณ breadth ของหุ้น US จาก us_prices.db (โครง schema/ฟังก์ชันเลียนแบบกัน)"""
    store = store or _default_store
    warmup = None if days is None else days + WARMUP_EXTRA

    # days=None (range "all") ต้อง full history; ถ้ามี warmup จำกัดแล้ว ใช้
    # iter_recent_series ที่ query ทีละหุ้นด้วย LIMIT — เร็วกว่า full-scan มาก
    # เมื่อตาราง prices โตขึ้น (วัดจริง ~2 วิ เทียบกับ ~35 วิ)
    src = store.iter_all_series(base_dir) if warmup is None else store.iter_recent_series(base_dir, warmup)

    series = {}
    for tick, d in src:
        if len(d["dates"]) < 60:          # ข้อมูลน้อยเกินกว่าจะมีความหมาย
            continue
        dates  = d["dates"][-warmup:]  if warmup else d["dates"]
        closes = d["closes"][-warmup:] if warmup else d["closes"]
        idx = pd.to_datetime(dates)
        series[tick] = pd.Series(closes, index=idx, dtype="float64")

    if not series:
        return None

    close = pd.DataFrame(series).sort_index()
    valid = close.notna()
    n_universe = valid.sum(axis=1)

    # thin-universe guard: n_universe ต่ำกว่านี้ (เช่น HK/JP ช่วงต้นข้อมูลที่มีแค่
    # 1-2 ตัว) ทำให้ % สั่นแรงโดยไม่มีความหมายทางสถิติ — เกณฑ์เดียวกับ
    # summarize_groups.avg() min_n=3 (core/metrics.py:250)
    MIN_UNIVERSE = 3
    thin = n_universe < MIN_UNIVERSE

    # ---- % above EMA (per-stock EMA ตามแนวเดียวกับ metrics หลัก) ----
    ema50  = close.ewm(span=50,  adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    pct50  = (close > ema50).sum(axis=1)  / n_universe * 100
    pct200 = (close > ema200).sum(axis=1) / n_universe * 100
    pct50[thin]  = np.nan
    pct200[thin] = np.nan

    # ---- Advances / Declines + McClellan ----
    # ffill ก่อน diff — กันหุ้นที่กลับมาเทรดหลังหยุดพัก (SP/H) หายไปจาก adv/dec วันแรกที่กลับมา:
    # close.diff() เทียบกับแถวก่อนหน้าตรงๆ ซึ่งเป็น NaN ช่วงหยุดพัก ทำให้ diff ของวันกลับมาเทรด
    # เป็น NaN ไปด้วย (ทั้งที่ราคาวันนั้นเปลี่ยนจริงเทียบกับราคาก่อนหยุดพัก) ffill ทำให้ diff วันที่
    # หยุดพักเป็น 0 (ถูกต้อง ไม่ใช่ adv/dec เพราะไม่ได้เทรด) แล้ววันกลับมาเทรดได้ diff จริง
    chg = close.ffill().diff()
    adv = (chg > 0).sum(axis=1)
    dec = (chg < 0).sum(axis=1)
    denom = (adv + dec).replace(0, np.nan)
    rana = 1000.0 * (adv - dec) / denom          # ratio-adjusted net advances
    rana[thin] = np.nan   # กัน universe จิ๋วป้อน noise เข้า EMA/summation ด้านล่าง
    rana = rana.ffill()
    mcc_osc = rana.ewm(span=19, adjust=False).mean() - rana.ewm(span=39, adjust=False).mean()

    # ---- 52W New High / New Low (เทียบ max/min 252 แท่งก่อนหน้า) ----
    # min_periods=252 (เท่ากับ window เต็ม) — หุ้นที่มีประวัติสั้นกว่า 1 ปียังไม่เข้าเงื่อนไข
    # NH/NL เลย กัน false positive ที่ราคาทำ "จุดสูงสุด/ต่ำสุดใหม่" เทียบกับ window สั้นกว่า 52
    # สัปดาห์จริง (เช่นหุ้นเพิ่งเข้าใหม่มีประวัติแค่ 100-251 วัน)
    prior_max = close.shift(1).rolling(252, min_periods=252).max()
    prior_min = close.shift(1).rolling(252, min_periods=252).min()
    nh = (close >= prior_max).sum(axis=1)
    nl = (close <= prior_min).sum(axis=1)

    # ---- ตัดเหลือ window แสดงผล ----
    tail = close.index[-days:] if days is not None else close.index
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
