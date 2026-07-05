# -*- coding: utf-8 -*-
"""
Unit tests ของฟังก์ชันคำนวณหลัก — ค่าคาดหวังคำนวณมือทั้งหมด ไม่พึ่งข้อมูลจริง

รัน:  python tests/test_units.py        (exit 0 = ผ่าน, 1 = พัง)
ครอบคลุม: core/metrics (RS/EMA/rank/validation/sector), services/rotation
(RRG quadrant state machine), backtest_rs_rrg (breadth + engine)

ใช้คู่กับ tests/test_golden.py (pipeline-level) ก่อน commit ทุกครั้ง
"""
import os
import sys
from datetime import date, timedelta

import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

T = 0
F = 0


def check(name, cond, detail=""):
    global T, F
    print(("✅" if cond else f"❌ {detail}"), name)
    T += 1
    F += 0 if cond else 1


# ============================================================
# 1. core/metrics — RS Score
# ============================================================
from core.metrics import (calc_ema, calc_return, calc_rs_raw, rank_rs,
                          summarize_groups, validate_stocks)

print("── calc_rs_raw ──")
check("น้ำหนักเต็ม (2×1M+3M+6M+1Y)/5", calc_rs_raw(10, 20, 30, 40) == 22.0)
check("ขาด 1Y -> reweight /4", calc_rs_raw(10, 20, 30, None) == 17.5)
check("มีแค่ 1M -> /2", calc_rs_raw(10, None, None, None) == 10.0)
check("ไม่มีเลย -> None", calc_rs_raw(None, None, None, None) is None)
check("ค่าติดลบ", calc_rs_raw(-10, -20, -30, -40) == -22.0)

print("── calc_return / calc_ema ──")
s = pd.Series([float(i) for i in range(1, 13)])          # 1..12
check("return 1 วัน = (12-11)/11", calc_return(s, 1) == round((12-11)/11*100, 2))
check("return 11 วัน = 1100%", calc_return(s, 11) == 1100.0)
check("แท่งไม่พอ -> None", calc_return(s, 12) is None)
check("ราคาอดีต = 0 -> None", calc_return(pd.Series([0.0, 5.0]), 1) is None)
# EMA span=3 (alpha=0.5): [2,4,8] -> 2, 3, 5.5
check("EMA(3) ของ [2,4,8] = 5.5", calc_ema(pd.Series([2.0, 4.0, 8.0]), 3) == 5.5)
check("EMA แท่งไม่พอ -> None", calc_ema(pd.Series([2.0, 4.0]), 3) is None)

print("── rank_rs (percentile + eligibility + stage) ──")
def mk(sym, raw, elig=True, **kw):
    d = {"symbol": sym, "rs_raw": raw, "rs_raw_4w": None, "rs_score": None,
         "rs_score_4w": None, "rs_momentum": None, "stage": None,
         "above_ema200": None, "ema200_slope_pct": None,
         "dq": {"flags": [], "rs_eligible": elig, "group_eligible": True}}
    d.update(kw)
    return d

stocks = [mk(f"S{i}", raw) for i, raw in enumerate([10, 20, 30, 40, 50])]
rank_rs(stocks)
check("universe 5 ตัว -> scores [0,20,40,59,79]",
      [s["rs_score"] for s in stocks] == [0, 20, 40, 59, 79])

stocks = [mk("A", 10), mk("B", 20), mk("X", 999, elig=False), mk("C", 30), mk("D", 40)]
rank_rs(stocks)
check("ตัว ineligible ไม่ถูก rank (None) และไม่เบียด universe",
      stocks[2]["rs_score"] is None
      and [s["rs_score"] for s in stocks if s["symbol"] != "X"] == [0, 25, 50, 74])

st = [mk("UP", 1, above_ema200=True,  ema200_slope_pct=1.0),
      mk("TOP", 2, above_ema200=True,  ema200_slope_pct=-0.5),
      mk("BASE", 3, above_ema200=False, ema200_slope_pct=-1.0),
      mk("DOWN", 4, above_ema200=False, ema200_slope_pct=-5.0),
      mk("NA", 5)]
rank_rs(st)
check("Weinstein stage 2/3/1/4/None",
      [x["stage"] for x in st] == [2, 3, 1, 4, None])

# rs_momentum = rs_score - rs_score_4w
st = [mk(f"M{i}", raw, rs_raw_4w=raw2)
      for i, (raw, raw2) in enumerate([(10, 30), (20, 20), (30, 10)])]
rank_rs(st)
check("rs_momentum = score - score_4w",
      all(x["rs_momentum"] == x["rs_score"] - x["rs_score_4w"] for x in st))

# ============================================================
# 2. core/metrics — Data Validation (V1-V6)
# ============================================================
print("── validate_stocks ──")
def ph(end, n, step_days=1, price=5.0):
    """สร้าง price_history n แท่ง จบที่ end (YYYY-MM-DD)"""
    e = date.fromisoformat(end)
    return [[(e - timedelta(days=(n - 1 - i) * step_days)).isoformat(), price]
            for i in range(n)]

AS_OF = "2026-01-30"
def base(sym, **kw):
    d = {"symbol": sym, "price": 5.0, "ret_1d": 1.0, "ret_1m": 2.0,
         "ret_1y": 10.0, "rs_raw": 5.0, "price_history": ph(AS_OF, 60)}
    d.update(kw)
    return d

vs = [
    base("NORM",  vol_history=[100, 100, 100, 100, 100]),
    base("STALE", price_history=ph("2025-12-01", 60)),          # ห่าง 60 วัน
    base("THIN",  price_history=ph(AS_OF, 30, step_days=4)),    # 21 แท่ง = 80 วัน
    base("CA",    ret_1d=35.0),
    base("PENNY", price=0.05),
    base("IPO",   ret_1y=None),
    base("NODATA", ret_1m=None),
    # V7 zombie bars: แท่งสด (ph ถึง as_of) แต่ volume = 0 ต่อเนื่อง
    base("ZOMBIE", vol_history=[0] * 10),
    # volume 0 แค่ 4 วันท้าย (มีเทรดวันที่ 5 นับถอยหลัง) -> ไม่ flag
    base("THINVOL", vol_history=[100, 0, 0, 0, 0][::-1] + [0, 0, 0, 0]),
]
summ = validate_stocks(vs, AS_OF)
flags = {s["symbol"]: set(s["dq"]["flags"]) for s in vs}
check("ปกติ: ไม่มี flag", flags["NORM"] == set())
check("stale ถูก flag + ออกจาก RS", "stale" in flags["STALE"]
      and not vs[1]["dq"]["rs_eligible"] and not vs[1]["dq"]["group_eligible"])
check("thin ถูก flag + ออกจาก RS แต่อยู่ในกลุ่ม", "thin" in flags["THIN"]
      and not vs[2]["dq"]["rs_eligible"] and vs[2]["dq"]["group_eligible"])
check("suspect_ca (|1D|>31)", "suspect_ca" in flags["CA"] and not vs[3]["dq"]["rs_eligible"])
check("penny (<0.10) ยัง rank ได้", "penny" in flags["PENNY"] and vs[4]["dq"]["rs_eligible"])
check("short_hist ยัง rank ได้", "short_hist" in flags["IPO"] and vs[5]["dq"]["rs_eligible"])
check("no_data ออกทุกอย่าง", "no_data" in flags["NODATA"]
      and not vs[6]["dq"]["rs_eligible"] and not vs[6]["dq"]["group_eligible"])
check("V7 zombie (vol=0 >=5 แท่ง) ออกจาก RS + กลุ่ม", "no_trade" in flags["ZOMBIE"]
      and not vs[7]["dq"]["rs_eligible"] and not vs[7]["dq"]["group_eligible"])
check("vol=0 แค่ 4 แท่งท้าย -> ไม่ flag", "no_trade" not in flags["THINVOL"])
check("dq_summary นับถูก", summ["rs_excluded"] == 5 and summ["rs_universe"] == 4)

# ============================================================
# 3. core/metrics — Sector Return (summarize_groups)
# ============================================================
print("── summarize_groups ──")
def gs(sym, sector, ret_1m=None, ret_1d=None, pe=None, thin=False, elig=True, ema50=None):
    return {"symbol": sym, "sector": sector, "ret_1m": ret_1m, "ret_1d": ret_1d,
            "pe": pe, "above_ema50": ema50,
            "dq": {"flags": (["thin"] if thin else []),
                   "rs_eligible": True, "group_eligible": elig}}

grp = [
    gs("A1", "SecA", ret_1m=10, ret_1d=1, pe=5,    ema50=True),
    gs("A2", "SecA", ret_1m=20, ret_1d=2, pe=10,   ema50=True),
    gs("A3", "SecA", ret_1m=30, ret_1d=3, pe=15,   ema50=False),
    gs("A4", "SecA", ret_1m=40, ret_1d=4, pe=1000, ema50=False),
    gs("AT", "SecA", ret_1m=999, ret_1d=5, thin=True),        # thin: ห้ามเข้า ret_1m
    gs("AX", "SecA", ret_1m=888, elig=False),                 # stale: ห้ามเข้าทุกอย่าง
    gs("B1", "SecB", ret_1m=7), gs("B2", "SecB", ret_1m=9),   # 2 ตัว < min_n=3
]
res = {g["name"]: g for g in summarize_groups(grp, "sector")}
a, b = res["SecA"], res["SecB"]
check("avg ret_1m ไม่รวม thin/stale = 25", a["ret_1m"] == 25.0)
check("thin เข้า ret_1d ได้ (avg = 3)", a["ret_1d"] == 3.0)
check("avg_pe เป็น median กัน outlier 1000 -> 12.5", a["avg_pe"] == 12.5)
check("n_valid = 4", a["n_valid"] == 4)
check("กลุ่ม < 3 ตัว -> ret_1m = None (RRG ไม่ plot)", b["ret_1m"] is None)
check("count นับสมาชิกทั้งหมด", a["count"] == 6 and b["count"] == 2)

# ============================================================
# 4. RRG — quadrant + state machine (services/rotation)
# ============================================================
print("── RRG quadrant state machine ──")
from services.rotation import _advance, quadrant_of

check("Leading (+,+)", quadrant_of(5, 2) == "Leading")
check("Weakening (+,-)", quadrant_of(5, -2) == "Weakening")
check("Lagging (-,-)", quadrant_of(-5, -2) == "Lagging")
check("Improving (-,+)", quadrant_of(-5, 2) == "Improving")
check("dead zone ±0.3", quadrant_of(0.2, 5) is None and quadrant_of(5, -0.29) is None)

def run_days(seq):
    entry, trans = None, []
    for i, (r3, r1) in enumerate(seq):
        entry, t = _advance(entry, quadrant_of(r3, r1), f"D{i+1:02d}")
        if t:
            trans.append((t["from"], t["to"]))
    return entry, trans

e, tr = run_days([(5, 2)])
check("seed เงียบ", e["confirmed"] == "Leading" and tr == [])
e, tr = run_days([(5, 2), (5, -1), (5, -1), (5, 2)])
check("2 วันไม่พอ + กลับ quadrant เดิม = reset", e["confirmed"] == "Leading" and tr == [])
e, tr = run_days([(5, 2), (5, -1), (5, -1), (5, -1)])
check("ครบ 3 วัน -> ยืนยัน", tr == [("Leading", "Weakening")])
e, tr = run_days([(5, 2), (5, -1), (-5, -1), (5, -1), (5, -1)])
check("แวะ quadrant อื่น = เริ่มนับใหม่", tr == [] and e["pending"]["days"] == 2)
e, tr = run_days([(5, 2), (5, -1), (5, 0.1), (5, -1), (5, -1)])
check("dead zone ไม่นับต่อและไม่ reset", tr == [("Leading", "Weakening")])
e, tr = run_days([(5, 2)] + [(5, -1)] * 3 + [(-5, -1)] * 3)
check("เปลี่ยน 2 ครั้งติด = 2 alerts",
      tr == [("Leading", "Weakening"), ("Weakening", "Lagging")])

# ============================================================
# 5. Backtest engine (backtest_rs_rrg)
# ============================================================
print("── backtest engine ──")
import backtest_rs_rrg as bt

# breadth: X คงที่ (close > ema เป็น False), Y ขาขึ้น (True) -> 50%
idx = pd.bdate_range("2015-01-01", periods=300)
close = pd.DataFrame({
    "X.BK": [10.0] * 300,
    "Y.BK": [10.0 + 0.05 * i for i in range(300)],
}, index=idx)
pct = bt.breadth_pct200(close)
check("breadth %>EMA200 = 50% (1/2 ตัว)", abs(float(pct.iloc[-1]) - 50.0) < 1e-9)

# engine: สร้าง 12 หุ้นขาขึ้นแรงต่างกัน — ผ่าน filter สภาพคล่อง/ราคา
n_days = 700
idx = pd.bdate_range("2014-06-01", periods=n_days)         # ครอบ 2016+ พร้อม warmup
data, vols = {}, {}
for i in range(12):
    drift = 0.0005 * (i + 1)
    data[f"T{i}.BK"] = [10.0 * (1 + drift) ** d for d in range(n_days)]
    vols[f"T{i}.BK"] = [1_000_000.0] * n_days
close = pd.DataFrame(data, index=idx).astype("float32")
vol = pd.DataFrame(vols, index=idx).astype("float32")
sec = {t: "S1" for t in close.columns}

eq, stt = bt.run(close, vol, sec)
check("ขาขึ้นทุกตัว -> equity โต, ถือหุ้นทุกงวด",
      eq.iloc[-1] > 1.0 and stt["avg_n"] >= 1, f"eq={eq.iloc[-1]:.3f}")

# ขาลงทุกตัว -> sector momentum ติดลบ -> RRG กันหมด -> ถือเงินสดตลอด
close_dn = pd.DataFrame(
    {f"T{i}.BK": [100.0 * (1 - 0.001) ** d for d in range(n_days)] for i in range(12)},
    index=idx).astype("float32")
eq_dn, stt_dn = bt.run(close_dn, vol, sec)
check("RRG filter กันหมด -> ถือเงินสด equity = 1.0 เป๊ะ (regression: "
      "ห้ามหัก cost ตอนไม่มีการซื้อขาย)",
      abs(eq_dn.iloc[-1] - 1.0) < 1e-12 and stt_dn["avg_n"] == 0,
      f"eq={eq_dn.iloc[-1]:.6f}")

# regime ต่ำกว่าเกณฑ์ตลอด -> เงินสดทุกงวด
regime = pd.Series(10.0, index=idx)
eq_rg, stt_rg = bt.run(close, vol, sec, regime=regime, regime_min=30)
check("regime < เกณฑ์ตลอด -> เงินสดทุกงวด equity = 1.0",
      abs(eq_rg.iloc[-1] - 1.0) < 1e-12 and stt_rg["n_cash"] == stt_rg["periods"])

print(f"\n{'✅ ALL PASS' if F == 0 else f'❌ {F} FAILED'} ({T} tests)")
sys.exit(0 if F == 0 else 1)
