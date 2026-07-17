# -*- coding: utf-8 -*-
"""backtest_pure_momentum.py — ทดสอบ 12-Month Momentum แบบล้วน (single factor)
  ต่างจาก backtest_rs_rrg.py ตรงที่ตัวนี้ใช้ผลตอบแทน 252 วันเดี่ยวๆ ไม่ผสม 21/63/126 วัน
  และไม่มี sector filter (RRG) — ซื้อ top decile ตาม return 252 วัน ขายเมื่อหลุด top decile

รัน:  python backtest_pure_momentum.py [ปีเริ่ม เช่น 2016]

วิธีวัด (เหมือน backtest_trend_following.py): universe/ต้นทุน/rebalance ทุก 21 วัน/adj close
"""
import sys

import bt_lib as L

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    d = L.load_ohlc()
    adj, close, vol = d["a"], d["c"], d["v"]
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    _, base_ok = L.universe_ok(close, vol)

    r252 = adj.pct_change(252, fill_method=None)
    r252_pct = r252.rank(axis=1, pct=True)

    strategies = {
        "12M Mom top decile (>=90pct)": r252_pct >= 0.90,
        "12M Mom top quartile (>=75pct)": r252_pct >= 0.75,
    }

    print(L.header_row())
    results = {}
    for name, sig in strategies.items():
        eq, s = L.run_periodic_signal(adj, sig.fillna(False), base_ok, START_YEAR)
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

    print("\n⚠ ข้อจำกัด: ไม่มี sector filter (RRG) ไม่มี liquidity weighting — เทียบเพื่อดูว่า blend "
          "4 timeframe ของ backtest_rs_rrg.py ดีกว่า 252 วันเดี่ยวๆ จริงไหมเท่านั้น")


if __name__ == "__main__":
    main()
