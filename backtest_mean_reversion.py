# -*- coding: utf-8 -*-
"""backtest_mean_reversion.py — ทดสอบกลุ่ม Mean Reversion ด้วย OHLC
  (RSI<30 ซื้อ/>70 ขาย, Close ต่ำกว่า SMA20 10% ซื้อ/กลับมายืน SMA20 ขาย)

รัน:  python backtest_mean_reversion.py [ปีเริ่ม เช่น 2016]

ต่างจาก backtest_trend_following.py ตรงที่กลยุทธ์กลุ่มนี้เป็น "ซื้อแล้วถือจนกว่าจะมีสัญญาณขาย"
(ไม่ใช่เช็คเงื่อนไขซ้ำทุกจุด) จึงใช้ engine คนละตัว (run_stateful_signal แทน run_periodic_signal)
วิธีวัดอื่นๆ (universe/ต้นทุน/rebalance ทุก 21 วัน/adj close) เหมือน backtest_trend_following.py ทุกอย่าง
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

    rsi14 = L.rsi(adj, 14)
    sma20 = adj.rolling(20).mean()

    strategies = {
        "RSI<30 buy />70 sell": (rsi14 < 30, rsi14 > 70),
        "Close<SMA20-10% buy":  (adj < sma20 * 0.90, adj >= sma20),
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
    print("  - ถือจนกว่าจะมีสัญญาณขาย (ไม่เช็ค liquidity ซ้ำระหว่างถือ) — อาจค้างหุ้นสภาพคล่องต่ำนานผิดปกติ")
    print("  - เช็ค entry/exit ทุก 21 วันทำการเท่านั้น ไม่ใช่ราคาตัด threshold ระหว่างวัน (RSI/SMA อาจแตะแล้วเด้งกลับก่อนเช็ครอบถัดไป)")
    print("  - RSI<30/>70 แบบไม่มี exit สำรอง อาจถือยาวมากถ้าหุ้นไม่เด้งกลับ (ดู AvgN สูง = ค้างหลายตัว)")


if __name__ == "__main__":
    main()
