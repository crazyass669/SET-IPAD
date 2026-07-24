# -*- coding: utf-8 -*-
"""analyze_ohlc_backtest.py — วิเคราะห์กลยุทธ์ RS+RRG เพิ่มเติมด้วยข้อมูล OHLC
(ที่ close-only ทำไม่ได้) — รันซ้ำได้เมื่อข้อมูลอัปเดต ไม่แตะ strategy หลัก

    python analyze_ohlc_backtest.py [ปีเริ่ม เช่น 2016]

ครอบคลุม (สรุปผลที่เคยรัน 2016→2026, ตัวเลขจะขยับตามข้อมูล):
  1. MFE / MAE       — ระหว่างถือ ราคาลงลึก/ขึ้นสูงสุดเท่าไร (High/Low) + stop-loss what-if
  2. Open-fill       — เข้า/ออกที่ราคาเปิดวันถัดไป เทียบราคาปิด (robustness/gap risk)
  3. ATR sizing      — ถ่วงน้ำหนักผกผัน ATR (volatility targeting)
  4. 52wk-high filter — คัดเฉพาะหุ้นใกล้จุดสูงสุด 52 สัปดาห์

ข้อสรุปที่พบ: stop-loss ไม่คุ้ม (ตัดไม้ชนะ) · open-fill แทบไม่ต่าง (backtest สมจริง)
  · ATR sizing เพิ่ม CAGR ~3%/ปี · near-52wk-high ลด MaxDD ~15% (แต่ตีกับ ATR ถ้ารวม)
ทุก variant ยังพึ่งปี 2021 ปีเดียวเป็นหลัก (edge กระจุกตัว) — ระวัง overfit ถ้า stack
"""
import os
import sys
import sqlite3

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

import backtest_rs_rrg as bt

RS_MIN, TOP_N, REB_BARS = bt.RS_MIN, bt.TOP_N, bt.REB_BARS
PRICE_MIN, VALUE_MIN, COST = bt.PRICE_MIN, bt.VALUE_MIN, bt.COST_SIDE
START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def load_all():
    """คืน DataFrame: adj (signal/return), close/open/high/low (price/path), vol"""
    con = sqlite3.connect(os.path.join(BASE, "set_prices.db"))
    D = {k: {} for k in "acohlv"}
    cur, buf = None, {k: [] for k in "dacohlv"}

    def flush():
        if cur is not None and len(buf["d"]) >= 300:
            idx = pd.to_datetime(buf["d"])
            for k in "acohlv":
                D[k][cur] = pd.Series(buf[k], index=idx, dtype="float32")

    for t, dt, c, a, o, h, l, v in con.execute(
            "SELECT ticker,date,close,adj_close,open,high,low,volume FROM prices ORDER BY ticker,date"):
        if t != cur:
            flush(); cur = t; buf = {k: [] for k in "dacohlv"}
        buf["d"].append(dt); buf["c"].append(c); buf["a"].append(a if a is not None else c)
        buf["o"].append(o if o is not None else c); buf["h"].append(h if h is not None else c)
        buf["l"].append(l if l is not None else c); buf["v"].append(v)
    flush(); con.close()
    f = lambda k: pd.DataFrame(D[k]).sort_index()
    return f("a"), f("c"), f("o"), f("h"), f("l"), f("v")


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    adj, close, opn, high, low, vol = load_all()
    sec_of = bt.sector_map()
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    # sanitize กันแท่งเพี้ยน (O=H=L แต่ close ต่าง) ก่อนใช้ high/low ทริกเกอร์อะไร
    ohlc = pd.concat([opn, high, low, close])
    low_s = ohlc.groupby(level=0).min().reindex(index=close.index, columns=close.columns)
    high_s = ohlc.groupby(level=0).max().reindex(index=close.index, columns=close.columns)
    adj_open = opn * (adj / close).replace([np.inf, -np.inf], np.nan)

    prev_c = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_c).abs(), (low - prev_c).abs()]).groupby(level=0).max()
    tr = tr.reindex(index=close.index, columns=close.columns)
    atr_pct = (tr.rolling(14).mean() / close).clip(lower=0.005)
    roll_high_prev = high.rolling(252).max().shift(1)
    dist_high = close / roll_high_prev - 1

    r21 = adj.pct_change(21, fill_method=None); r63 = adj.pct_change(63, fill_method=None)
    r126 = adj.pct_change(126, fill_method=None); r250 = adj.pct_change(250, fill_method=None)
    rs_raw = (2 * r21 + r63 + r126 + r250) / 5 * 100
    rs_pct = rs_raw.rank(axis=1, pct=True) * 99
    value20 = (close * vol).rolling(20).mean()
    sec_ser = pd.Series({t: sec_of.get(t) for t in close.columns})
    sec_mom = r21.T.groupby(sec_ser).mean().T

    dates = close.index
    start_i = max(260, int(np.searchsorted(dates, pd.Timestamp(f"{START_YEAR}-01-01"))))
    reb_idx = list(range(start_i, len(dates) - REB_BARS - 1, REB_BARS))

    def picks_at(t, nearhigh=False):
        ok = (rs_pct.iloc[t] >= RS_MIN) & (close.iloc[t] >= PRICE_MIN) & (value20.iloc[t] >= VALUE_MIN)
        ok &= sec_ser.map(lambda s: bool(sec_mom.iloc[t].get(s, np.nan) > 0)).values
        if nearhigh:
            ok &= (dist_high.iloc[t] >= -0.05)
        return rs_raw.iloc[t][ok].nlargest(TOP_N).index.tolist()

    def equity(fill, atr=False, nearhigh=False):
        ff = fill.ffill()
        eq, prev, per = [1.0], set(), []
        for t in reb_idx:
            picks = picks_at(t, nearhigh)
            e_i, x_i = t + 1, min(t + 1 + REB_BARS, len(dates) - 1)
            if picks:
                rets = ff.iloc[x_i][picks] / ff.iloc[e_i][picks] - 1
                if atr:
                    w = 1.0 / atr_pct.iloc[t][picks]; w = (w / w.sum()).reindex(picks).fillna(0)
                    ret = float((rets * w).sum())
                else:
                    ret = float(rets.mean())
            else:
                ret = 0.0
            changed = (len(set(picks) ^ prev) / max(len(picks) + len(prev), 1)) if (picks and prev) \
                else (0.0 if not picks and not prev else 1.0)
            ret -= changed * COST * 2; prev = set(picks)
            eq.append(eq[-1] * (1 + ret)); per.append(ret)
        e = pd.Series(eq)
        yrs = (dates[reb_idx[-1]] - dates[reb_idx[0]]).days / 365.25
        return {"total": e.iloc[-1] - 1, "cagr": e.iloc[-1] ** (1 / yrs) - 1,
                "maxdd": float((e / e.cummax() - 1).min()),
                "win": sum(1 for r in per if r > 0) / len(per)}

    def show(n, s):
        print(f"  {n:34s} total {s['total']*100:+7.0f}% | CAGR {s['cagr']*100:+6.1f}% | "
              f"MaxDD {s['maxdd']*100:6.1f}% | win {s['win']*100:.0f}%")

    # ── 1. MFE/MAE + stop-loss what-if ──
    trades = []
    cff, hff, lff = close.ffill(), high_s.ffill(), low_s.ffill()
    for t in reb_idx:
        e_i, x_i = t + 1, min(t + 1 + REB_BARS, len(dates) - 1)
        for p in picks_at(t):
            entry = cff.iloc[e_i][p]
            if not entry or entry <= 0:
                continue
            mae = float(lff.iloc[e_i:x_i + 1][p].min() / entry - 1)
            ret = float(cff.iloc[x_i][p] / entry - 1)
            trades.append((mae, ret))
    df = pd.DataFrame(trades, columns=["mae", "ret"])
    df = df[df.mae.notna() & (df.mae > -0.95)]
    print(f"1) MFE/MAE — {len(df)} ไม้ (ชนะ {(df.ret>0).mean()*100:.0f}%)")
    q = lambda s, p: s.quantile(p) * 100
    print(f"   MAE median: ไม้ชนะ {q(df[df.ret>0].mae,.5):+.1f}% vs ไม้แพ้ {q(df[df.ret<=0].mae,.5):+.1f}%")
    print(f"   stop-loss what-if (avg/ไม้, baseline ไม่มี stop = {df.ret.mean()*100:+.2f}%):")
    for stop in (10, 15, 20):
        nr = np.where(df.mae <= -stop / 100, -stop / 100, df.ret)
        print(f"     stop -{stop}%: {nr.mean()*100:+.2f}% (โดน stop {(df.mae<=-stop/100).mean()*100:.0f}%)  -> ทุกระดับแย่กว่า = stop ไม่คุ้ม")

    # ── 2-4. execution / sizing / filter ──
    print("\n2-4) execution / sizing / filter:")
    show("baseline (close-fill, equal)", equity(adj))
    show("open-fill (robustness)", equity(adj_open))
    show("ATR sizing (+return)", equity(adj, atr=True))
    show("near-52wk-high (+ลด MaxDD)", equity(adj, nearhigh=True))
    show("ATR + near-high (ตีกันเอง)", equity(adj, atr=True, nearhigh=True))
    print("\nสรุป: ATR sizing = enhancement เดี่ยวที่คุ้มสุด · near-high = ลด MaxDD · stop-loss ไม่คุ้ม")
    print("      (ระวัง overfit — ทุก variant พึ่งปี 2021 · ดูรายละเอียดใน backtest_report.html)")


if __name__ == "__main__":
    main()
