# -*- coding: utf-8 -*-
"""backtest_breakout.py — ทดสอบกลุ่ม Breakout ด้วย OHLC
  (Donchian Breakout: High>Highest High 20วัน ซื้อ / Low<Lowest Low 10วัน ขาย, ATR Breakout)

รัน:  python backtest_breakout.py [ปีเริ่ม เช่น 2016]

ถือจนกว่าจะมีสัญญาณขาย (เหมือน backtest_mean_reversion.py — run_stateful_signal)
High/Low ปรับสัดส่วนด้วย adj/close ratio ก่อนใช้ (กัน split/ปันผลใหญ่ทำให้ high/low ในอดีตเพี้ยนเทียบราคาปัจจุบัน)
วิธีวัดอื่นๆ (universe/ต้นทุน/rebalance ทุก 21 วัน) เหมือน backtest_trend_following.py
"""
import sys

import numpy as np

import bt_lib as L

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    d = L.load_ohlc()
    adj, close, high, low, vol = d["a"], d["c"], d["h"], d["l"], d["v"]
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    _, base_ok = L.universe_ok(close, vol)

    # ปรับ high/low ด้วยสัดส่วน adj/close (เหมือน analyze_ohlc_backtest.py) กัน split/ปันผลใหญ่ทำให้ระดับราคาย้อนหลังเพี้ยน
    ratio = (adj / close).replace([np.inf, -np.inf], np.nan)
    adj_high, adj_low = high * ratio, low * ratio

    donchian_high20 = adj_high.rolling(20).max().shift(1)   # highest high 20 วันก่อนหน้า (ไม่รวมวันนี้)
    donchian_low10  = adj_low.rolling(10).min().shift(1)    # lowest low 10 วันก่อนหน้า
    sma20 = adj.rolling(20).mean()
    atr14 = L.atr(adj_high, adj_low, adj, 14)

    strategies = {
        "Donchian 20/10 Breakout": (adj_high > donchian_high20, adj_low < donchian_low10),
        "ATR Breakout (SMA20+1ATR)": (adj > sma20 + atr14, adj < sma20),
    }

    print(L.header_row())
    results = {}
    for name, (enter_b, exit_b) in strategies.items():
        eq, s = L.run_stateful_signal(adj, enter_b.fillna(False), exit_b.fillna(True), base_ok, START_YEAR)
        results[name] = (eq, s)
        print(L.fmt_row(name, s))

    win = next(iter(results.values()))[0].index
    set_idx = L.set_index_series()
    if set_idx is not None:
        b = set_idx.reindex(win, method="ffill").dropna()
        b = b / b.iloc[0]
        yrs = (b.index[-1] - b.index[0]).days / 365.25
        cagr = b.iloc[-1] ** (1 / yrs) - 1
        dd = float((b / b.cummax() - 1).min())
        print(f"{'SET Index':<22} {(b.iloc[-1]-1)*100:+8.0f}% {cagr*100:+7.1f}% {dd*100:7.1f}%")

    monthly = adj.ffill().reindex(win)
    uni_ret = monthly.pct_change(fill_method=None).mean(axis=1).fillna(0)
    uni_eq = (1 + uni_ret).cumprod()
    yrs = (uni_eq.index[-1] - uni_eq.index[0]).days / 365.25
    cagr = uni_eq.iloc[-1] ** (1 / yrs) - 1
    dd = float((uni_eq / uni_eq.cummax() - 1).min())
    print(f"{'Universe EW':<22} {(uni_eq.iloc[-1]-1)*100:+8.0f}% {cagr*100:+7.1f}% {dd*100:7.1f}%")

    print("\n⚠ ข้อจำกัด:")
    print("  - เช็ค breakout ทุก 21 วันทำการเท่านั้น ไม่ใช่ทุกวัน (Turtle system จริงเช็ครายวัน — ที่นี่อาจพลาด/เข้าช้ากว่าจริงมาก)")
    print("  - high/low ปรับด้วย adj/close ratio (ประมาณการ ไม่ใช่ adjusted high/low จริงจาก data vendor)")
    print("  - ถือจนกว่าจะมีสัญญาณขาย ไม่เช็ค liquidity ซ้ำระหว่างถือ")


if __name__ == "__main__":
    main()
