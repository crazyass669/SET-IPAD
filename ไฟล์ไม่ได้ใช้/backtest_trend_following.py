# -*- coding: utf-8 -*-
"""backtest_trend_following.py — ทดสอบกลุ่ม Trend Following ด้วย OHLC
  (SMA20/50 Cross, SMA50/200 Golden Cross, EMA20/50 Cross, MACD, ADX+DI Trend Filter,
   SuperTrend(10,3), Ichimoku close>cloud + Tenkan>Kijun)

รัน:  python backtest_trend_following.py [ปีเริ่ม เช่น 2016]

วิธีวัด (เหมือน backtest_rs_rrg.py ให้เทียบกันตรงๆ ได้):
  Universe   : ราคาจริง >= 1 บาท, มูลค่าซื้อขายเฉลี่ย 20 วัน >= 5 ล้านบาท
  Signal     : คำนวณจาก Adj Close (รวมปันผล — เหมือน momentum strategy หลัก)
  Portfolio  : equal-weight ทุกหุ้นที่ signal=True ณ จุดเช็ค (ไม่ใช่ top-N — ถือทุกตัวที่เข้าเกณฑ์)
  Rebalance  : เช็คทุก 21 วันทำการ (ไม่ใช่ intrabar cross จริง)
  ต้นทุน      : 0.25% ต่อข้าง เฉพาะสัดส่วนที่เปลี่ยน
  Look-ahead : สัญญาณจาก close วัน t -> เข้า/ออกที่ close วัน t+1
"""
import sys

import numpy as np

import bt_lib as L

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    d = L.load_ohlc()
    adj, close, high, low, vol = d["a"], d["c"], d["h"], d["l"], d["v"]
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    _, base_ok = L.universe_ok(close, vol)

    sma20, sma50, sma200 = adj.rolling(20).mean(), adj.rolling(50).mean(), adj.rolling(200).mean()
    ema20, ema50 = adj.ewm(span=20, adjust=False).mean(), adj.ewm(span=50, adjust=False).mean()
    macd_line, macd_sig = macd(adj)
    adx_v, plus_di, minus_di = L.adx(high, low, adj)

    # high/low ปรับสัดส่วนด้วย adj/close ratio ก่อนใช้ (เหมือน backtest_breakout.py) กัน split/ปันผลใหญ่ทำให้ระดับราคาย้อนหลังเพี้ยน
    ratio = (adj / close).replace([np.inf, -np.inf], np.nan)
    adj_high, adj_low = high * ratio, low * ratio
    print("คำนวณ SuperTrend (loop ตามเวลา ใช้เวลาสักครู่)...")
    supertrend_up = L.supertrend_uptrend(adj_high, adj_low, adj)
    ichimoku_bull = L.ichimoku_signal(adj_high, adj_low, adj)

    strategies = {
        "SMA20>50 Cross":  sma20 > sma50,
        "SMA50>200 Cross": sma50 > sma200,
        "EMA20>50 Cross":  ema20 > ema50,
        "MACD>Signal":     macd_line > macd_sig,
        "ADX>25 + DI+":    (adx_v > 25) & (plus_di > minus_di),
        "SuperTrend(10,3)": supertrend_up,
        "Ichimoku (close>cloud + Tenkan>Kijun)": ichimoku_bull,
    }

    print(L.header_row())
    results = {}
    for name, sig in strategies.items():
        eq, s = L.run_periodic_signal(adj, sig.fillna(False), base_ok, START_YEAR)
        results[name] = (eq, s)
        print(L.fmt_row(name, s))

    # benchmark: SET Index + Universe equal-weight (ช่วงเวลาเดียวกับ strategy แรก)
    win = results["SMA20>50 Cross"][0].index
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
    print("  - signal คำนวณจาก Adj Close (รวมปันผล) เหมือน momentum strategy หลัก ไม่ใช่ราคาปิดจริงล้วน")
    print("  - เช็คทุก 21 วันทำการเท่านั้น ไม่ใช่ cross ที่ตรวจทุกวัน (อาจพลาด cross ที่กลับตัวไวในเดือนเดียว)")
    print("  - ถือ equal-weight ทุกตัวที่เข้าเกณฑ์ ไม่ได้จำกัด top-N — ดูคอลัมน์ AvgN")
    print("    (บางช่วงตลาด sideways จะแทบไม่มีหุ้นเข้าเกณฑ์ บางช่วง trending แรงจะถือหลายร้อยตัว)")
    print("  - ยังไม่รวมต้นทุน slippage/market impact จากการถือพอร์ตกระจายมาก")


if __name__ == "__main__":
    main()
