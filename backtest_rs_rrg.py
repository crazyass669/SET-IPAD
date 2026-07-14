# -*- coding: utf-8 -*-
"""
backtest_rs_rrg.py — Simple backtest ของ strategy RS Score + RRG sector filter

รัน:  python backtest_rs_rrg.py [ปีเริ่ม เช่น 2016]

กฎ (fix ไว้ก่อนรัน — ไม่ optimize):
  Universe   : มีข้อมูล >= 250 แท่ง, ราคา >= 1 บาท, มูลค่าซื้อขายเฉลี่ย 20 วัน >= 5 ล้านบาท
  Entry      : rs_score >= 90 (percentile ณ วันนั้น) และ sector 1M momentum > 0
               (= quadrant Leading/Improving ตามตรรกะ RRG)
  Portfolio  : top 10 ตาม rs_raw, equal weight
  Rebalance  : ทุก 21 วันทำการ, ขายตัวที่หลุดเงื่อนไข
  ต้นทุน      : 0.25% ต่อข้าง เฉพาะสัดส่วนที่เปลี่ยนตัว
  Look-ahead : สัญญาณจาก close วัน t -> เข้า/ออกที่ close วัน t+1

ข้อจำกัดสำคัญพิมพ์ท้ายรายงาน — อ่านก่อนเชื่อตัวเลข
"""
import json
import os
import sys

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

import sqlite3

REB_BARS    = 21
TOP_N       = 10
RS_MIN      = 90
PRICE_MIN   = 1.0
VALUE_MIN   = 5e6
COST_SIDE   = 0.0025
START_YEAR  = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def load_frames():
    """คืน (close_adj, close_raw, vol)
      close_adj = Adj Close (ปรับ split+ปันผล) -> ใช้คำนวณ signal RS + ผลตอบแทน entry/exit
      close_raw = ราคาปิดจริง            -> ใช้ filter ราคา (>=1 บาท) + สภาพคล่อง (close×vol)
    แยกสองฐานให้ถูกหลัก: total return ต้องรวมปันผล แต่มูลค่าซื้อขาย/ราคาต้องเป็นราคาจริง
    (adj_close เป็น NULL สำหรับหุ้น delisted บางตัว -> fallback ใช้ close ดิบ)"""
    con = sqlite3.connect(os.path.join(BASE, "set_prices.db"))
    adjs, raws, vols = {}, {}, {}
    cur_t, dts, ca, cr, vv = None, [], [], [], []

    def _flush():
        if cur_t is not None and len(dts) >= 300:
            idx = pd.to_datetime(dts)
            adjs[cur_t] = pd.Series(ca, index=idx, dtype="float32")
            raws[cur_t] = pd.Series(cr, index=idx, dtype="float32")
            vols[cur_t] = pd.Series(vv, index=idx, dtype="float32")

    for t, d, c, a, v in con.execute(
            "SELECT ticker,date,close,adj_close,volume FROM prices ORDER BY ticker,date"):
        if t != cur_t:
            _flush()
            cur_t, dts, ca, cr, vv = t, [], [], [], []
        dts.append(d); cr.append(c); ca.append(a if a is not None else c); vv.append(v)
    _flush()
    con.close()

    close_adj = pd.DataFrame(adjs).sort_index()
    close_raw = pd.DataFrame(raws).sort_index()
    vol       = pd.DataFrame(vols).sort_index()
    return close_adj, close_raw, vol


def sector_map():
    d = json.load(open(os.path.join(BASE, "set_data.json"), encoding="utf-8"))
    return {s["ticker"]: s["sector"] for s in d["stocks"]}


def set_index_series():
    try:
        idx = json.load(open(os.path.join(BASE, "indices_cache.json"), encoding="utf-8"))
        e = idx["data"]["^SET.BK"]
        return pd.Series(e["closes"], index=pd.to_datetime(e["dates"]), dtype="float64")
    except Exception:
        return None


def breadth_pct200(close):
    """% ของหุ้นที่ปิดเหนือ EMA200 ต่อวัน (ตรรกะเดียวกับ services/breadth.py)"""
    ema200 = close.ewm(span=200, adjust=False).mean()
    return (close > ema200).sum(axis=1) / close.notna().sum(axis=1) * 100


def run(close, close_raw, vol, sec_of, use_rrg=True, regime=None, regime_min=None):
    """คืน (dates, equity_curve, stats_dict)
    close = Adj Close (signal RS + ผลตอบแทน total return รวมปันผล)
    close_raw = ราคาจริง (filter ราคา >=1 บาท + สภาพคล่อง close×vol)
    regime/regime_min: ถ้า breadth ณ วัน rebalance < regime_min -> ถือเงินสดงวดนั้น"""
    # ---- สัญญาณ (คำนวณจาก adj close — โมเมนตัม/RS รวมผลปันผลถูกต้อง) ----
    r21  = close.pct_change(21,  fill_method=None)
    r63  = close.pct_change(63,  fill_method=None)
    r126 = close.pct_change(126, fill_method=None)
    r250 = close.pct_change(250, fill_method=None)
    rs_raw = (2 * r21 + r63 + r126 + r250) / 5 * 100          # ต้องครบทั้ง 4 ช่วง
    rs_pct = rs_raw.rank(axis=1, pct=True) * 99               # percentile 0-99 ต่อวัน

    # สภาพคล่อง = ราคาจริง × ปริมาณ (มูลค่าซื้อขายจริงเป็นบาท ไม่ใช่ adj)
    value20 = (close_raw * vol).rolling(20).mean()

    # sector 1M momentum (equal-weight ของสมาชิก) -> RRG filter
    sec_ser = pd.Series({t: sec_of.get(t) for t in close.columns})
    sec_mom = r21.T.groupby(sec_ser).mean().T                 # dates × sectors

    # ---- เดินพอร์ตทีละ rebalance ----
    dates = close.index
    start_i = max(260, int(np.searchsorted(dates, pd.Timestamp(f"{START_YEAR}-01-01"))))
    reb_idx = list(range(start_i, len(dates) - REB_BARS - 1, REB_BARS))

    equity, eq_dates = [1.0], [dates[reb_idx[0]]]
    prev_hold = set()
    per_period = []
    ffilled = close.ffill()

    n_cash = 0
    for t in reb_idx:
        dt = dates[t]
        # Regime filter: breadth ต่ำกว่าเกณฑ์ -> ถือเงินสดทั้งงวด
        if regime is not None and regime_min is not None and regime.iloc[t] < regime_min:
            picks = []
            n_cash += 1
        else:
            row_rs, row_pct = rs_raw.iloc[t], rs_pct.iloc[t]
            # ราคาขั้นต่ำใช้ราคาจริง (raw) ไม่ใช่ adj (หุ้นเก่าปันผลเยอะ adj อาจต่ำกว่า 1 บาททั้งที่ราคาจริงสูง)
            ok = (row_pct >= RS_MIN) & (close_raw.iloc[t] >= PRICE_MIN) & (value20.iloc[t] >= VALUE_MIN)
            if use_rrg:
                mom = sec_mom.iloc[t]
                ok &= sec_ser.map(lambda s: bool(mom.get(s, np.nan) > 0)).values
            picks = row_rs[ok].nlargest(TOP_N).index.tolist()

        if picks:
            entry = ffilled.iloc[t + 1][picks]
            exit_ = ffilled.iloc[min(t + 1 + REB_BARS, len(dates) - 1)][picks]
            ret = float((exit_ / entry - 1).mean())
        else:
            ret = 0.0                                          # ไม่มีตัวผ่านเกณฑ์ -> ถือเงินสด

        # สัดส่วนพอร์ตที่เปลี่ยน (สำหรับคิดต้นทุน) — ถือเงินสดต่อเนื่อง = ไม่มี
        # การซื้อขาย ห้ามหัก cost (บั๊กเดิม: prev_hold ว่าง -> changed=1.0 เสมอ
        # ทำให้งวดเงินสดโดนหัก 0.5% ฟรี ผล regime variant ต่ำกว่าจริง)
        if not picks and not prev_hold:
            changed = 0.0
        elif not prev_hold or not picks:
            changed = 1.0
        else:
            changed = len(set(picks) ^ prev_hold) / max(len(picks) + len(prev_hold), 1)
        ret -= changed * COST_SIDE * 2
        prev_hold = set(picks)

        equity.append(equity[-1] * (1 + ret))
        eq_dates.append(dates[min(t + 1 + REB_BARS, len(dates) - 1)])
        per_period.append({"date": str(dt.date()), "n": len(picks), "ret": ret})

    eq = pd.Series(equity, index=pd.DatetimeIndex(eq_dates))
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr  = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0
    dd    = float((eq / eq.cummax() - 1).min())
    wins  = sum(1 for p in per_period if p["ret"] > 0)
    return eq, {
        "total":    eq.iloc[-1] - 1,
        "cagr":     cagr,
        "max_dd":   dd,
        "win_rate": wins / len(per_period) if per_period else 0,
        "periods":  len(per_period),
        "avg_n":    np.mean([p["n"] for p in per_period]),
        "n_cash":   n_cash,
        "per_period": per_period,
    }


def main():
    print(f"โหลดข้อมูลจาก SQLite... (backtest ตั้งแต่ {START_YEAR})")
    close, close_raw, vol = load_frames()   # close = adj_close (total return)
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วันทำการ "
          f"({close.index[0].date()} → {close.index[-1].date()}) · ผลตอบแทน=Adj Close (รวมปันผล)")
    sec_of = sector_map()

    print("คำนวณ breadth %>EMA200 สำหรับ regime filter ...")
    pct200 = breadth_pct200(close_raw)   # breadth = แนวโน้มราคาจริง

    print("รัน strategy RS>=90 + RRG sector filter ...")
    eq_rrg, s_rrg = run(close, close_raw, vol, sec_of, use_rrg=True)
    print("รัน variant RS-only (ไม่มี RRG filter) ...")
    eq_rs, s_rs = run(close, close_raw, vol, sec_of, use_rrg=False)
    print("รัน variant RS+RRG + Regime (ถือเงินสดเมื่อ %>EMA200 < 30) ...")
    eq_rg, s_rg = run(close, close_raw, vol, sec_of, use_rrg=True, regime=pct200, regime_min=30)

    # ---- benchmarks ----
    win = eq_rrg.index
    set_idx = set_index_series()
    bench = {}
    if set_idx is not None:
        s = set_idx.reindex(win, method="ffill").dropna()
        bench["SET Index"] = s / s.iloc[0]
    # equal-weight universe (ใช้ adj close ให้เทียบกับ strategy อย่างเป็นธรรม — รวมปันผลทั้งคู่)
    monthly = close.ffill().reindex(win)
    uni_ret = (monthly.pct_change(fill_method=None)).mean(axis=1).fillna(0)
    bench["Universe EW"] = (1 + uni_ret).cumprod()

    def fmt(eq, st=None):
        years = (eq.index[-1] - eq.index[0]).days / 365.25
        cagr = eq.iloc[-1] ** (1 / years) - 1
        dd = float((eq / eq.cummax() - 1).min())
        base = f"total {eq.iloc[-1]-1:+8.0%} | CAGR {cagr:+6.1%} | MaxDD {dd:6.1%}"
        if st:
            base += f" | win {st['win_rate']:.0%} ของ {st['periods']} งวด | ถือเฉลี่ย {st['avg_n']:.1f} ตัว"
        return base

    print("\n" + "=" * 78)
    print(f"ผล Backtest {START_YEAR} → {win[-1].date()}  (rebalance ทุก {REB_BARS} วันทำการ)")
    print("=" * 78)
    print(f"RS+RRG+Regime<30    : {fmt(eq_rg, s_rg)} | ถือเงินสด {s_rg['n_cash']} งวด")
    print(f"RS>=90 + RRG filter : {fmt(eq_rrg, s_rrg)}")
    print(f"RS>=90 อย่างเดียว    : {fmt(eq_rs, s_rs)}")
    for name, b in bench.items():
        print(f"{name:<19} : {fmt(b)}")

    # sensitivity ของ regime threshold — ดูความทนทาน ไม่ใช่หาค่าที่สวยสุด
    print("\nSensitivity ของ regime threshold (RS+RRG + ถือเงินสดเมื่อ breadth < X):")
    for th in (25, 35, 40, 50):
        e, s = run(close, close_raw, vol, sec_of, use_rrg=True, regime=pct200, regime_min=th)
        print(f"  X={th:>2}: {fmt(e, s)} | เงินสด {s['n_cash']} งวด")

    # รายปี
    print("\nผลตอบแทนรายปี (%):")
    rows = {"RS+RRG+Rgm": eq_rg, "RS+RRG": eq_rrg, "RS-only": eq_rs, **bench}
    yr_table = {}
    for name, e in rows.items():
        yearly = e.resample("YE").last().pct_change(fill_method=None)
        first_year = e.resample("YE").last().iloc[0] / e.iloc[0] - 1
        yearly.iloc[0] = first_year
        yr_table[name] = {d.year: v for d, v in yearly.items()}
    years_all = sorted({y for v in yr_table.values() for y in v})
    hdr = "ปี     " + "".join(f"{n:>12}" for n in rows)
    print(hdr)
    for y in years_all:
        line = f"{y}   "
        for n in rows:
            v = yr_table[n].get(y)
            line += f"{v:>11.1%} " if v is not None and np.isfinite(v) else f"{'—':>11} "
        print(line)


if __name__ == "__main__":
    main()
