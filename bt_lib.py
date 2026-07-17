# -*- coding: utf-8 -*-
"""bt_lib.py — เครื่องมือกลางสำหรับ backtest strategy กลุ่ม OHLC (Trend/Mean-Reversion/Breakout)
ให้ script ทดสอบ strategy ใหม่แต่ละตัว import ใช้ร่วมกัน (ไม่ต้อง copy โค้ดโหลดข้อมูล/เดินพอร์ตซ้ำ)
ใช้ universe filter, ต้นทุน, จุดเช็ค signal เดียวกับ backtest_rs_rrg.py (momentum ที่ทดสอบแล้ว)
เพื่อให้ strategy ใหม่เทียบกับของเดิมได้แบบ apples-to-apples
"""
import json
import os
import sqlite3

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))

PRICE_MIN = 1.0
VALUE_MIN = 5e6
COST_SIDE = 0.0025
REB_BARS  = 21   # เช็ค signal ทุก 21 วันทำการ (ประมาณเดือนละครั้ง) — เหมือน backtest_rs_rrg ให้เทียบกันตรงๆ ได้


def load_ohlc():
    """คืน dict {'a':adj_close, 'c':close, 'o':open, 'h':high, 'l':low, 'v':volume}
    แต่ละตัวเป็น DataFrame (date index × ticker columns)"""
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
    return {k: f(k) for k in "acohlv"}


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


def true_range(high, low, close):
    prev_c = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_c).abs(), (low - prev_c).abs()]).groupby(level=0).max()
    return tr.reindex(index=close.index, columns=close.columns)


def atr(high, low, close, period=14):
    return true_range(high, low, close).rolling(period).mean()


def rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def adx(high, low, close, period=14):
    """คืน (adx, plus_di, minus_di) — Wilder's smoothing"""
    up = high.diff()
    down = -low.diff()
    plus_dm = ((up > down) & (up > 0)).astype(float) * up.clip(lower=0)
    minus_dm = ((down > up) & (down > 0)).astype(float) * down.clip(lower=0)
    atr_n = true_range(high, low, close).ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_n
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_n
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx_v = dx.ewm(alpha=1 / period, adjust=False).mean()
    return adx_v, plus_di, minus_di


def supertrend_uptrend(high, low, close, period=10, mult=3.0):
    """คืน DataFrame bool: True = อยู่ในขาขึ้นตาม SuperTrend (close > final_upper band)
    ต้อง loop ตามเวลา (recursive: final band วันนี้ขึ้นกับ band เมื่อวาน) — vectorize เฉพาะข้ามหุ้น (คอลัมน์)
    ต่อรอบเวลา ให้เร็วพอ (913 หุ้น × ~10,800 วัน ใช้เวลาไม่กี่วินาที)"""
    atr_ = atr(high, low, close, period)
    hl2 = (high + low) / 2
    basic_upper = (hl2 + mult * atr_).values
    basic_lower = (hl2 - mult * atr_).values
    c = close.values
    hl2v = hl2.values
    n_days, n_stocks = c.shape

    final_upper = np.full((n_days, n_stocks), np.nan)
    final_lower = np.full((n_days, n_stocks), np.nan)
    trend = np.zeros((n_days, n_stocks))   # 1 = ขาขึ้น, -1 = ขาลง, 0 = ยังไม่เริ่ม (ATR ไม่พอ)

    for t in range(n_days):
        bu_t, bl_t = basic_upper[t], basic_lower[t]
        if t == 0:
            final_upper[t], final_lower[t] = bu_t, bl_t
            continue
        prev_fu, prev_fl, prev_c, prev_trend = final_upper[t - 1], final_lower[t - 1], c[t - 1], trend[t - 1]
        fu = np.where((bu_t < prev_fu) | (prev_c > prev_fu), bu_t, prev_fu)
        fl = np.where((bl_t > prev_fl) | (prev_c < prev_fl), bl_t, prev_fl)
        fu = np.where(np.isnan(prev_fu), bu_t, fu)
        fl = np.where(np.isnan(prev_fl), bl_t, fl)
        final_upper[t], final_lower[t] = fu, fl
        cur_c = c[t]
        new_trend = np.where(cur_c > fu, 1, np.where(cur_c < fl, -1, prev_trend))
        new_trend = np.where(prev_trend == 0, np.where(cur_c >= hl2v[t], 1, -1), new_trend)
        trend[t] = new_trend

    return pd.DataFrame(trend > 0, index=close.index, columns=close.columns)


def ichimoku_signal(high, low, close):
    """คืน DataFrame bool: True = close อยู่เหนือ cloud (Senkou A/B ที่ project มาถึงวันนี้) และ Tenkan > Kijun"""
    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    senkou_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    cloud_top = np.maximum(senkou_a, senkou_b)
    return (close > cloud_top) & (tenkan > kijun)


def universe_ok(close_raw, vol, price_min=PRICE_MIN, value_min=VALUE_MIN):
    """เงื่อนไข universe เดียวกับ backtest_rs_rrg: ราคาจริง>=1บาท, มูลค่าเทรดเฉลี่ย20วัน>=5ล้านบาท
    คืน (value20, ok_bool_df) — ราคาต้องเป็นราคาจริง (close_raw) ไม่ใช่ adj (หุ้นปันผลเยอะ adj อาจต่ำกว่า 1 บาททั้งที่ราคาจริงสูง)"""
    value20 = (close_raw * vol).rolling(20).mean()
    return value20, (close_raw >= price_min) & (value20 >= value_min)


def run_periodic_signal(adj, signal_bool, base_ok, start_year,
                         cost_side=COST_SIDE, reb_bars=REB_BARS, top_n=None):
    """Engine กลาง: เช็ค signal ทุก reb_bars วันทำการ (เหมือน backtest_rs_rrg.run())
    ถือ equal-weight ทุกหุ้นที่ signal=True และผ่าน universe filter (base_ok) ณ จุดเช็ค
    entry ที่ close วัน t+1, exit ที่ close วัน t+1+reb_bars (สัญญาณจาก close วัน t เท่านั้น — กัน look-ahead)
    top_n: ถ้าระบุ = ตัดพอร์ตให้เหลือ top_n ตัวแรกตามลำดับ column (ไม่ได้จัดอันดับ) default = ถือทุกตัวที่เข้าเกณฑ์"""
    dates = adj.index
    start_i = max(260, int(np.searchsorted(dates, pd.Timestamp(f"{start_year}-01-01"))))
    reb_idx = list(range(start_i, len(dates) - reb_bars - 1, reb_bars))
    if not reb_idx:
        raise ValueError("ช่วงข้อมูลสั้นเกินไปสำหรับ start_year ที่ระบุ")
    ffilled = adj.ffill()

    equity, eq_dates, per_period = [1.0], [dates[reb_idx[0]]], []
    prev_hold = set()
    for t in reb_idx:
        ok = signal_bool.iloc[t] & base_ok.iloc[t]
        picks = ok[ok].index.tolist()
        if top_n and len(picks) > top_n:
            picks = picks[:top_n]
        e_i, x_i = t + 1, min(t + 1 + reb_bars, len(dates) - 1)
        if picks:
            rets = ffilled.iloc[x_i][picks] / ffilled.iloc[e_i][picks] - 1
            ret = float(rets.mean())
        else:
            ret = 0.0
        if not picks and not prev_hold:
            changed = 0.0
        elif not prev_hold or not picks:
            changed = 1.0
        else:
            changed = len(set(picks) ^ prev_hold) / max(len(picks) + len(prev_hold), 1)
        ret -= changed * cost_side * 2
        prev_hold = set(picks)
        equity.append(equity[-1] * (1 + ret))
        eq_dates.append(dates[x_i])
        per_period.append({"n": len(picks), "ret": ret})

    eq = pd.Series(equity, index=pd.DatetimeIndex(eq_dates))
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    per = pd.Series([p["ret"] for p in per_period])
    wins = int((per > 0).sum())
    gross_win = per[per > 0].sum()
    gross_loss = -per[per < 0].sum()
    return eq, {
        "total":         eq.iloc[-1] - 1,
        "cagr":          eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0.0,
        "max_dd":        float((eq / eq.cummax() - 1).min()),
        "win_rate":      wins / len(per) if len(per) else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else np.nan,
        "sharpe":        (per.mean() / per.std() * np.sqrt(252 / reb_bars)) if per.std() > 0 else np.nan,
        "periods":       len(per),
        "avg_n":         float(np.mean([p["n"] for p in per_period])),
    }


def run_stateful_signal(adj, enter_bool, exit_bool, base_ok, start_year,
                         cost_side=COST_SIDE, reb_bars=REB_BARS):
    """Engine สำหรับกลยุทธ์ที่ต้อง 'ถือจนกว่าจะมีสัญญาณขาย' (เช่น RSI<30 ซื้อ / RSI>70 ขาย)
    ต่างจาก run_periodic_signal ตรงที่หุ้นที่ถืออยู่แล้วจะไม่ขายทิ้งแค่เพราะ enter_bool กลายเป็น False —
    ขายเมื่อ exit_bool=True เท่านั้น (เข้าใหม่ต้องผ่าน base_ok ด้วย แต่ที่ถืออยู่แล้วไม่เช็ค base_ok ซ้ำ
    เพราะ mean-reversion อาจ hold ผ่านช่วงสภาพคล่องต่ำชั่วคราว)"""
    dates = adj.index
    start_i = max(260, int(np.searchsorted(dates, pd.Timestamp(f"{start_year}-01-01"))))
    reb_idx = list(range(start_i, len(dates) - reb_bars - 1, reb_bars))
    if not reb_idx:
        raise ValueError("ช่วงข้อมูลสั้นเกินไปสำหรับ start_year ที่ระบุ")
    ffilled = adj.ffill()

    equity, eq_dates, per_period = [1.0], [dates[reb_idx[0]]], []
    holdings, prev_hold = set(), set()
    for t in reb_idx:
        exit_row = exit_bool.iloc[t]
        holdings = {s for s in holdings if not bool(exit_row.get(s, False))}
        enter_row = enter_bool.iloc[t] & base_ok.iloc[t]
        new_entries = set(enter_row[enter_row].index) - holdings
        holdings |= new_entries
        picks = list(holdings)

        e_i, x_i = t + 1, min(t + 1 + reb_bars, len(dates) - 1)
        if picks:
            rets = ffilled.iloc[x_i][picks] / ffilled.iloc[e_i][picks] - 1
            ret = float(rets.mean())
        else:
            ret = 0.0
        if not picks and not prev_hold:
            changed = 0.0
        elif not prev_hold or not picks:
            changed = 1.0
        else:
            changed = len(set(picks) ^ prev_hold) / max(len(picks) + len(prev_hold), 1)
        ret -= changed * cost_side * 2
        prev_hold = set(picks)
        equity.append(equity[-1] * (1 + ret))
        eq_dates.append(dates[x_i])
        per_period.append({"n": len(picks), "ret": ret})

    eq = pd.Series(equity, index=pd.DatetimeIndex(eq_dates))
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    per = pd.Series([p["ret"] for p in per_period])
    wins = int((per > 0).sum())
    gross_win = per[per > 0].sum()
    gross_loss = -per[per < 0].sum()
    return eq, {
        "total":         eq.iloc[-1] - 1,
        "cagr":          eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0.0,
        "max_dd":        float((eq / eq.cummax() - 1).min()),
        "win_rate":      wins / len(per) if len(per) else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else np.nan,
        "sharpe":        (per.mean() / per.std() * np.sqrt(252 / reb_bars)) if per.std() > 0 else np.nan,
        "periods":       len(per),
        "avg_n":         float(np.mean([p["n"] for p in per_period])),
    }


def run_calendar_overlay(daily_ret, in_market_mask, cost_side=COST_SIDE):
    """สำหรับกลยุทธ์ตามปฏิทิน (Sell in May, January Effect) — ถือพอร์ตเดียว (เช่น Universe EW)
    เฉพาะเดือนที่ in_market_mask=True เท่านั้น นอกนั้นถือเงินสด (return=0)
    หัก cost ทุกครั้งที่สลับสถานะเข้า/ออกตลาด (นับเป็น 1 รอบ ไม่ใช่ต้นทุนรายหุ้นเหมือน engine อื่น
    เพราะนี่คือบัญชีเดียวสลับ all-in/all-out ไม่ใช่พอร์ตหลายหุ้น)
    daily_ret, in_market_mask: pandas Series (index=dates) ความยาวเท่ากัน"""
    ret = daily_ret.copy().fillna(0.0)
    mask = in_market_mask.reindex(ret.index).fillna(False)
    switch = mask.astype(int).diff().abs().fillna(0) > 0
    strat_ret = ret.where(mask, 0.0) - switch.astype(float) * cost_side * 2
    eq = (1 + strat_ret).cumprod()
    eq.iloc[0] = 1.0
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0.0
    dd = float((eq / eq.cummax() - 1).min())
    yearly = eq.resample("YE").last().pct_change(fill_method=None)
    yearly.iloc[0] = eq.resample("YE").last().iloc[0] / eq.iloc[0] - 1
    wins = int((yearly > 0).sum())
    return eq, {
        "total": eq.iloc[-1] - 1, "cagr": cagr, "max_dd": dd,
        "win_rate": wins / len(yearly) if len(yearly) else 0.0,
        "years_n": len(yearly),
    }


def fmt_row(name, s):
    pf = f"{s['profit_factor']:.2f}" if np.isfinite(s.get('profit_factor', np.nan)) else "—"
    sh = f"{s['sharpe']:.2f}" if np.isfinite(s.get('sharpe', np.nan)) else "—"
    return (f"{name:<22} {s['total']*100:+8.0f}% {s['cagr']*100:+7.1f}% {s['max_dd']*100:7.1f}% "
            f"{s['win_rate']*100:6.0f}% {pf:>6} {sh:>7} {s['avg_n']:6.1f} {s['periods']:7d}")


def header_row():
    return f"{'Strategy':<22} {'Total':>9} {'CAGR':>8} {'MaxDD':>8} {'Win%':>7} {'PF':>6} {'Sharpe':>7} {'AvgN':>6} {'Periods':>8}"
