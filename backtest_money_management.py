# -*- coding: utf-8 -*-
"""backtest_money_management.py — ทดสอบวิธีจัดสรรเงินทุน (ไม่ใช่การเลือกหุ้น)
  ส่วน A: DCA (ถัวเฉลี่ยทุกเดือน) เทียบ Grid/Laddered buying (ซื้อเพิ่มเมื่อราคาย่อ 5%/10%) บน SET Index
  ส่วน B: Pyramiding proxy — ถ่วงน้ำหนักตาม momentum score (RS) แทน equal-weight ในพอร์ต RS+RRG เดิม

รัน:  python backtest_money_management.py [ปีเริ่ม เช่น 2016]

หมายเหตุ: ATR position sizing (ถ่วงน้ำหนักผกผัน volatility) ทดสอบไปแล้วใน analyze_ohlc_backtest.py
(+CAGR ~3%/ปี) จึงไม่ทำซ้ำในนี้
"""
import sys

import numpy as np
import pandas as pd

import bt_lib as L
import backtest_rs_rrg as rrg

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


# ── ส่วน A: DCA vs Grid บน SET Index ──────────────────────────────────────
def part_a():
    print("=" * 70)
    print("ส่วน A: DCA vs Grid/Laddered Buying (SET Index, ลงทุนงวดละ 100 บาทเทียบเท่า)")
    print("=" * 70)
    set_idx = L.set_index_series()
    if set_idx is None:
        print("ไม่มี indices_cache.json — ข้าม"); return
    monthly = set_idx.resample("ME").last().dropna()
    monthly = monthly[monthly.index >= pd.Timestamp(f"{START_YEAR}-01-01")]
    TRANCHE = 100.0

    # DCA: ซื้อทุกเดือน ไม่สนราคา
    units_dca = (TRANCHE / monthly).sum()
    invested_dca = TRANCHE * len(monthly)
    final_dca = units_dca * monthly.iloc[-1]
    avg_cost_dca = invested_dca / units_dca
    simple_avg_price = monthly.mean()

    def grid(drop_pct):
        """ซื้อเพิ่ม 1 ไม้ทุกครั้งที่ราคาย่อลงมาอีก drop_pct จากจุดสูงสุดล่าสุด (rolling high) —
        พีคขยับขึ้นเรื่อยๆ เมื่อราคาทำจุดสูงใหม่ (ฐานการย่อ "รีเซ็ต" ตามพีคใหม่เสมอ ไม่ใช่ยึดติดจุดเริ่มต้นเดิม)"""
        peak, last_step = monthly.iloc[0], 0
        units, invested = TRANCHE / peak, TRANCHE   # ซื้อไม้แรกเสมอที่จุดเริ่ม
        for p in monthly.iloc[1:]:
            if p > peak:
                peak, last_step = p, 0
            step = int((1 - p / peak) // drop_pct)
            if step > last_step:
                n_new = step - last_step
                units += n_new * TRANCHE / p; invested += n_new * TRANCHE
                last_step = step
        final_val = units * monthly.iloc[-1]
        idle_tranches = len(monthly) - (invested / TRANCHE)
        return units, invested, final_val, idle_tranches

    print(f"\n{'วิธี':<26}{'ลงทุนจริง':>12}{'มูลค่าสุดท้าย':>16}{'Return%':>10}{'ต้นทุนเฉลี่ย/หน่วย':>20}")
    ret_dca = (final_dca / invested_dca - 1) * 100
    print(f"{'DCA รายเดือน':<26}{invested_dca:>12,.0f}{final_dca:>16,.0f}{ret_dca:>9.1f}%{avg_cost_dca:>19.2f}")
    for dp in (0.05, 0.10):
        u, inv, fv, idle = grid(dp)
        ret = (fv / inv - 1) * 100 if inv else float("nan")
        cost = inv / u if u else float("nan")
        n_tranches = inv / TRANCHE
        print(f"{'Grid ย่อ ' + f'{int(dp*100)}%':<26}{inv:>12,.0f}{fv:>16,.0f}{ret:>9.1f}%{cost:>19.2f} "
              f"(ซื้อ {n_tranches:.0f} ไม้ จาก {len(monthly)} เดือนที่เช็ค)")
    print(f"\nราคาเฉลี่ยง่ายๆ (simple average, ไม่ถ่วงน้ำหนัก): {simple_avg_price:.2f}")
    print(f"ต้นทุนเฉลี่ยของ DCA ({avg_cost_dca:.2f}) ต่ำกว่าราคาเฉลี่ยง่ายๆ = DCA ได้ซื้อหน่วยเยอะกว่าตอนราคาถูก (ตามทฤษฎี)")
    print("⚠ Grid ลงทุนเงินน้อยกว่า DCA มาก (ซื้อเฉพาะตอนย่อ ไม่ใช่ทุกเดือน) — Return% ของ Grid สูงกว่าดูน่าตื่นเต้น")
    print("  แต่เป็น 'ผลตอบแทนต่อเงินที่ลงจริง' เท่านั้น เงินส่วนที่เหลือ (ไม่ได้ลงทุน) ไม่ได้ทำงานอะไร — ถ้าเทียบต่อ")
    print("  'เงินทั้งก้อนที่เตรียมไว้เท่า DCA' Grid จะแพ้ DCA ชัดเจน เพราะเงินส่วนใหญ่นอนเฉยๆ รอจังหวะ")


# ── ส่วน B: Pyramiding proxy (ถ่วงน้ำหนักตาม RS momentum แทน equal-weight) ──
def part_b():
    print("\n" + "=" * 70)
    print("ส่วน B: Pyramiding proxy — ถ่วงน้ำหนักตาม RS (แรงกว่าได้น้ำหนักมากกว่า) vs equal-weight")
    print("=" * 70)
    close, close_raw, vol = rrg.load_frames()
    sec_of = rrg.sector_map()
    r21 = close.pct_change(21, fill_method=None); r63 = close.pct_change(63, fill_method=None)
    r126 = close.pct_change(126, fill_method=None); r250 = close.pct_change(250, fill_method=None)
    rs_raw = (2 * r21 + r63 + r126 + r250) / 5 * 100
    rs_pct = rs_raw.rank(axis=1, pct=True) * 99
    value20 = (close_raw * vol).rolling(20).mean()
    sec_ser = pd.Series({t: sec_of.get(t) for t in close.columns})
    sec_mom = r21.T.groupby(sec_ser).mean().T

    dates = close.index
    start_i = max(260, int(np.searchsorted(dates, pd.Timestamp(f"{START_YEAR}-01-01"))))
    reb_idx = list(range(start_i, len(dates) - rrg.REB_BARS - 1, rrg.REB_BARS))
    ffilled = close.ffill()

    def picks_at(t):
        ok = (rs_pct.iloc[t] >= rrg.RS_MIN) & (close_raw.iloc[t] >= rrg.PRICE_MIN) & (value20.iloc[t] >= rrg.VALUE_MIN)
        ok &= sec_ser.map(lambda s: bool(sec_mom.iloc[t].get(s, np.nan) > 0)).values
        return rs_raw.iloc[t][ok].nlargest(rrg.TOP_N)   # Series: index=ticker, value=rs_raw (ไว้ถ่วงน้ำหนัก)

    def run(weighted):
        eq, prev = [1.0], set()
        for t in reb_idx:
            picks = picks_at(t)
            if len(picks):
                e_i, x_i = t + 1, min(t + 1 + rrg.REB_BARS, len(dates) - 1)
                rets = ffilled.iloc[x_i][picks.index] / ffilled.iloc[e_i][picks.index] - 1
                if weighted:
                    w = picks.clip(lower=0.01); w = w / w.sum()   # น้ำหนักตาม RS raw (แรงกว่า = ถือมากกว่า)
                    ret = float((rets * w).sum())
                else:
                    ret = float(rets.mean())
            else:
                ret = 0.0
            cur = set(picks.index)
            changed = (len(cur ^ prev) / max(len(cur) + len(prev), 1)) if (cur and prev) \
                else (0.0 if not cur and not prev else 1.0)
            ret -= changed * rrg.COST_SIDE * 2
            prev = cur
            eq.append(eq[-1] * (1 + ret))
        e = pd.Series(eq)
        yrs = (dates[reb_idx[-1]] - dates[reb_idx[0]]).days / 365.25
        return {"total": e.iloc[-1] - 1, "cagr": e.iloc[-1] ** (1 / yrs) - 1,
                "maxdd": float((e / e.cummax() - 1).min())}

    s_eq = run(weighted=False)
    s_w = run(weighted=True)
    print(f"{'วิธี':<28}{'Total':>10}{'CAGR':>9}{'MaxDD':>9}")
    print(f"{'Equal-weight (top 10)':<28}{s_eq['total']*100:>9.0f}%{s_eq['cagr']*100:>8.1f}%{s_eq['maxdd']*100:>8.1f}%")
    print(f"{'RS-weighted (pyramid proxy)':<28}{s_w['total']*100:>9.0f}%{s_w['cagr']*100:>8.1f}%{s_w['maxdd']*100:>8.1f}%")
    print("\n⚠ นี่คือ proxy ของ pyramiding (ถ่วงน้ำหนักตามความแรง ไม่ใช่การซื้อเพิ่มจริงระหว่างถือ)")
    print("  เพราะ engine เช็คทุก 21 วัน ไม่สามารถจำลอง 'ซื้อเพิ่มเมื่อราคาทำจุดสูงใหม่ระหว่างถือ' ได้แม่นยำ")


if __name__ == "__main__":
    part_a()
    part_b()
