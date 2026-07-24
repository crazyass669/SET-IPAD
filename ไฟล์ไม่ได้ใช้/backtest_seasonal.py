# -*- coding: utf-8 -*-
"""backtest_seasonal.py — ทดสอบกลยุทธ์เชิงฤดูกาล/ปฏิทิน
  (Sell in May and Go Away, January Effect) เทียบกับ Buy & Hold ตลอดปี

รัน:  python backtest_seasonal.py [ปีเริ่ม เช่น 2016]

ต่างจากสคริปต์อื่นทั้งหมด — นี่คือ "บัญชีเดียว" สลับ all-in/all-out ตามปฏิทิน ไม่ใช่คัดหุ้นรายตัว
ทดสอบทั้งบน Universe EW (equal-weight ทุกหุ้นในฐาน) และ SET Index (ราคาจริงของตลาด) คู่กัน
เพื่อกัน bias จากพอร์ต synthetic (Universe EW คำนวณจาก adj close หุ้นรายตัว SET Index มาจาก
ราคาดัชนีจริง — ถ้าสอง benchmark นี้ให้ข้อสรุปตรงกัน มั่นใจได้มากขึ้นว่าไม่ใช่ artifact ของวิธีคำนวณ)
"""
import sys

import numpy as np
import pandas as pd

import bt_lib as L

START_YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2016


def run_both(name_prefix, ret, mask_sellinmay, mask_jan_only, mask_avoid_jan):
    rows = {}
    rows["Buy & Hold"] = L.run_calendar_overlay(ret, pd.Series(True, index=ret.index))
    rows["Sell in May (คืน พ.ค.-ต.ค.)"] = L.run_calendar_overlay(ret, mask_sellinmay)
    rows["ถือเฉพาะเดือน ม.ค."] = L.run_calendar_overlay(ret, mask_jan_only)
    rows["หลีกเลี่ยงเดือน ม.ค."] = L.run_calendar_overlay(ret, mask_avoid_jan)
    print(f"\n== {name_prefix} ==")
    print(f"{'กลยุทธ์':<28} {'Total':>9} {'CAGR':>8} {'MaxDD':>8} {'WinYr%':>8}")
    for n, (eq, s) in rows.items():
        print(f"{n:<28} {s['total']*100:+8.0f}% {s['cagr']*100:+7.1f}% {s['max_dd']*100:7.1f}% {s['win_rate']*100:7.0f}%")
    return rows


def main():
    print(f"โหลด OHLC (backtest ตั้งแต่ {START_YEAR})...")
    d = L.load_ohlc()
    adj, close, vol = d["a"], d["c"], d["v"]
    print(f"  {close.shape[1]} หุ้น × {close.shape[0]} วัน\n")

    start_dt = pd.Timestamp(f"{START_YEAR}-01-01")
    dates = adj.index[adj.index >= start_dt]

    # Universe EW: ผลตอบแทนเฉลี่ยรายวันของทุกหุ้น (adj close) — เหมือน benchmark ในสคริปต์อื่นๆ
    uni_ret = adj.ffill().pct_change(fill_method=None).mean(axis=1).reindex(dates).fillna(0)

    months = pd.Series(dates.month, index=dates)
    sellinmay = ~months.isin([5, 6, 7, 8, 9, 10])   # in-market พ.ย.-เม.ย., คืนเงินสด พ.ค.-ต.ค.
    jan_only = months == 1
    avoid_jan = months != 1

    run_both("Universe EW (equal-weight ทุกหุ้นในฐาน)", uni_ret, sellinmay, jan_only, avoid_jan)

    set_idx = L.set_index_series()
    if set_idx is not None:
        idx_ret = set_idx.pct_change(fill_method=None).reindex(dates).fillna(0)
        months2 = pd.Series(idx_ret.index.month, index=idx_ret.index)
        sellinmay2 = ~months2.isin([5, 6, 7, 8, 9, 10])
        jan_only2 = months2 == 1
        avoid_jan2 = months2 != 1
        run_both("SET Index (ราคาจริง)", idx_ret, sellinmay2, jan_only2, avoid_jan2)

        # January Effect — ดูสัดส่วนผลตอบแทนรวมที่มาจากเดือน ม.ค. อย่างเดียว เทียบทั้งปี
        yearly_ret = (1 + idx_ret).groupby(idx_ret.index.year).apply(lambda s: s.prod() - 1)
        jan_ret = (1 + idx_ret[months2 == 1]).groupby(idx_ret.index[months2 == 1].year).apply(lambda s: s.prod() - 1)
        print("\n== January Effect: ผลตอบแทน ม.ค. เทียบทั้งปี (SET Index) ==")
        print(f"{'ปี':<6}{'ทั้งปี':>10}{'ม.ค.':>10}")
        for y in yearly_ret.index:
            j = jan_ret.get(y)
            print(f"{y:<6}{yearly_ret[y]*100:+9.1f}%{(j*100 if j is not None else float('nan')):+9.1f}%" if j is not None
                  else f"{y:<6}{yearly_ret[y]*100:+9.1f}%{'—':>10}")
        pos_jan = (jan_ret > 0).sum()
        print(f"ม.ค. บวก {pos_jan}/{len(jan_ret)} ปี ({pos_jan/len(jan_ret)*100:.0f}%) · "
              f"ผลเฉลี่ย ม.ค. {jan_ret.mean()*100:+.2f}% เทียบเฉลี่ยเดือนอื่นๆ")

    print("\n⚠ ข้อจำกัด:")
    print("  - สลับ all-in/all-out ตามปฏิทินตรงๆ ไม่มี signal ยืนยัน (ไม่เช็คเทรนด์/momentum ประกอบ)")
    print("  - ต้นทุนคิดแบบง่าย (0.25%×2 ต่อครั้งที่สลับสถานะ) ไม่ใช่ต้นทุนจริงของพอร์ตหลายหุ้น")
    print("  - จำนวนรอบปีที่ทดสอบมีจำกัด (~10 รอบ) โดยเฉพาะ 'มกราคม effect' นับได้แค่ ~10 จุดข้อมูล")
    print("    ยังไม่พอสรุปทางสถิติแน่นหนา (ต้องการอย่างน้อย 20-30 ปีถึงจะมั่นใจได้มากขึ้น)")


if __name__ == "__main__":
    main()
