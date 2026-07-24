# -*- coding: utf-8 -*-
"""backtest_bollinger_stochastic.py — ทดสอบ Mean Reversion เพิ่มเติม
  (Bollinger Bands: ซื้อหลุด Lower Band ขายแตะ Basis, Stochastic Oscillator: %K<20 ซื้อ />80 ขาย)

รัน:  python backtest_bollinger_stochastic.py [ปีเริ่ม เช่น 2016]

ใช้ engine เดียวกับ backtest_mean_reversion.py (run_stateful_signal — ถือจนกว่าจะมีสัญญาณขาย)
วิธีวัดอื่นๆ (universe/ต้นทุน/rebalance ทุก 21 วัน/adj close) เหมือนสคริปต์อื่นในชุดนี้ทุกอย่าง
"""
import sys

import bt_lib as L

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def bollinger(close, period=20, mult=2.0):
    basis = close.rolling(period).mean()
    std = close.rolling(period).std()
    return basis, basis + mult * std, basis - mult * std


def stochastic(close, high, low, k_period=14, d_period=3):
    hh = high.rolling(k_period).max()
    ll = low.rolling(k_period).min()
    k = (close - ll) / (hh - ll).replace(0, float("nan")) * 100
    d = k.rolling(d_period).mean()
    return k, d


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    d = L.load_ohlc()
    adj, close, high, low, vol = d["a"], d["c"], d["h"], d["l"], d["v"]
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    _, base_ok = L.universe_ok(close, vol)

    basis, upper, lower = bollinger(adj)
    stoch_k, stoch_d = stochastic(adj, high, low)

    strategies = {
        "Bollinger <Lower buy / >=Basis sell": (adj < lower, adj >= basis),
        "Stochastic %K<20 buy / >80 sell":     (stoch_k < 20, stoch_k > 80),
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

    print("\n⚠ ข้อจำกัด: เหมือน backtest_mean_reversion.py — ถือจนกว่าจะมีสัญญาณขาย ไม่เช็ค liquidity ซ้ำ,")
    print("  เช็คทุก 21 วันทำการเท่านั้น ไม่ใช่ threshold ตัดรายวัน (อาจแตะแล้วเด้งกลับก่อนเช็ครอบถัดไป)")


if __name__ == "__main__":
    main()
