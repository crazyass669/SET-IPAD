# -*- coding: utf-8 -*-
"""sources/flow_signals.py — รวมสัญญาณ "เงินทุน" 3 ชั้นต่อหุ้น: insider + short + NVDR

แต่ละสัญญาณให้ทิศทาง +1 (bullish) / -1 (bearish) / 0 (กลาง/ไม่มีข้อมูล) แล้วรวมเป็น
score เพื่อหาหุ้นที่สัญญาณ "ตรงกันหลายชั้น" (confluence) — ข้อมูลทั้งหมดเป็นสาธารณะ
(SET short/NVDR + SEC insider) เผยแพร่ได้ ไม่ใช่ข้อมูล local-only

ทิศทาง:
  insider: ซื้อสุทธิ (มูลค่า buy - sell > 0) = bullish
  short  : short position % "ลดลง" (คลุมสถานะ) = bullish ; เพิ่ม = bearish
  nvdr   : สัดส่วนถือครองต่างชาติ "เพิ่ม" = bullish ; ลด = bearish

หน้าต่างเวลา: insider ใช้ 90 วัน (insider_days) · short/NVDR ใช้ TREND_WINDOW
snapshots ล่าสุด (~1 เดือนทำการ) ไม่ใช่ daily ทั้งชุดที่เก็บยาวถึง 365 วัน
"""
import json
import os
import sqlite3
from datetime import datetime, timedelta


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── สัญญาณย่อย ───────────────────────────────────────────────
MIN_INSIDER_NET_BAHT = 1e6   # สุทธิขั้นต่ำ 1 ล้านบาท ถึงนับเป็นทิศทาง


def insider_signal(rows):
    """rows: list ของ insider_trades (dict มี action, qty, price) ในหน้าต่างเวลา
    คืน (dir, detail) — dir +1/-1/0 จากมูลค่าซื้อ-ขายสุทธิ

    เกณฑ์นับทิศทาง (ต้องผ่านทั้งสองข้อ):
      1. สุทธิ >10% ของมูลค่ารวม (ไม่ก้ำกึ่ง)
      2. สุทธิ >= 1 ล้านบาท (เดิมไม่มีขั้นต่ำ — ซื้อ 2,500 บาทก็ได้ dir=+1
         น้ำหนักเท่าซื้อ 100 ล้าน ทำให้ score confluence เฟ้อจากรายการจิ๋ว)"""
    buy_val = sell_val = 0.0
    n_buy = n_sell = 0
    for r in rows:
        qty = r.get("qty") or 0
        price = r.get("price") or 0
        val = qty * price
        if r.get("action") == "buy":
            buy_val += val; n_buy += 1
        elif r.get("action") == "sell":
            sell_val += val; n_sell += 1
    if n_buy == 0 and n_sell == 0:
        return 0, None
    net = buy_val - sell_val
    total = buy_val + sell_val
    d = 0
    if total > 0 and abs(net) >= MIN_INSIDER_NET_BAHT:
        if net > 0.1 * total:
            d = 1
        elif net < -0.1 * total:
            d = -1
    return d, {"net_value_mbaht": round(net / 1e6, 2), "n_buy": n_buy, "n_sell": n_sell}


TREND_WINDOW = 21   # snapshots ล่าสุดที่ใช้คิดเทรนด์ (~1 เดือนทำการ)


def _trend_dir(daily, key, min_points=3, window=TREND_WINDOW):
    """เทรนด์ของ key ใน daily array: เทียบค่าเฉลี่ย 'ช่วงท้าย' vs 'ช่วงต้น'
    คืน (change_pct_of_series, avg_recent, avg_early) หรือ None ถ้าข้อมูลไม่พอ

    ตัดเหลือ window snapshots ล่าสุดก่อนคำนวณเสมอ — daily เก็บยาวถึง 365 วัน ถ้าใช้
    ทั้งชุดจะกลายเป็นเทียบ "ค่าเฉลี่ย 4 เดือนแรกของปี vs 4 เดือนท้าย" ซึ่งไม่ใช่
    สัญญาณ flow ระยะใกล้อีกต่อไป (ตอนเขียนมีข้อมูลแค่ ~24 วันเลยยังไม่เห็นอาการ)"""
    vals = [(d.get("date"), d.get(key)) for d in (daily or []) if d.get(key) is not None]
    if len(vals) < min_points:
        return None
    vals.sort()
    vals = vals[-window:]
    k = max(1, len(vals) // 3)
    early = sum(v for _, v in vals[:k]) / k
    recent = sum(v for _, v in vals[-k:]) / k
    return recent - early, recent, early


def short_signal(entry):
    """entry: dict หุ้นจาก short_sales_data.json (มี daily[].short_pos_pct)
    short ลดลง = bullish (คลุมสถานะ) / เพิ่ม = bearish"""
    t = _trend_dir(entry.get("daily"), "short_pos_pct")
    if t is None:
        return 0, None
    chg, recent, early = t
    lvl = entry.get("short_pos_pct")
    d = 0
    # เกณฑ์: เปลี่ยน >= 0.1 percentage-point ของหุ้นชำระแล้ว ถึงนับ
    if chg <= -0.1:
        d = 1
    elif chg >= 0.1:
        d = -1
    return d, {"short_pos_pct": round(lvl, 3) if lvl is not None else None,
               "trend_pp": round(chg, 3)}


def nvdr_signal(entry):
    """entry: dict หุ้นจาก nvdr_data.json (มี daily[].nvdr_pct)
    NVDR% เพิ่ม = ต่างชาติเก็บ (bullish) / ลด = ขาย (bearish)"""
    t = _trend_dir(entry.get("daily"), "nvdr_pct")
    if t is None:
        return 0, None
    chg, recent, early = t
    d = 0
    if chg >= 0.1:
        d = 1
    elif chg <= -0.1:
        d = -1
    return d, {"nvdr_pct": round(recent, 3), "trend_pp": round(chg, 3)}


# ── รวมทั้งตลาด ──────────────────────────────────────────────
def build_flow_signals(base_dir, insider_days=90):
    """คืน list ของ dict ต่อหุ้น: {symbol, score, n_signals, insider/short/nvdr: {dir, ...}}
    เรียงตาม score จากมากไปน้อย (bullish confluence ก่อน)"""
    short_data = _load_json(os.path.join(base_dir, "short_sales_data.json")) or {"stocks": {}}
    nvdr_data = _load_json(os.path.join(base_dir, "nvdr_data.json")) or {"stocks": {}}

    # insider: ดึงรายการในหน้าต่างเวลา แล้ว group ตาม symbol
    insider_by_sym = {}
    db = os.path.join(base_dir, "sec_filings.db")
    if os.path.exists(db):
        con = sqlite3.connect(db)
        try:
            cutoff = (datetime.now() - timedelta(days=insider_days)).strftime("%Y-%m-%d")
            for sym, action, qty, price in con.execute(
                    "SELECT symbol, action, qty, price FROM insider_trades WHERE trade_date>=?",
                    (cutoff,)):
                insider_by_sym.setdefault(sym, []).append(
                    {"action": action, "qty": qty, "price": price})
        finally:
            con.close()

    # universe = ทุก symbol ที่ปรากฏในอย่างน้อย 1 แหล่ง
    syms = set(short_data.get("stocks", {})) | set(nvdr_data.get("stocks", {})) | set(insider_by_sym)
    out = []
    for sym in syms:
        i_dir, i_det = insider_signal(insider_by_sym.get(sym, []))
        s_dir, s_det = short_signal(short_data.get("stocks", {}).get(sym, {}))
        n_dir, n_det = nvdr_signal(nvdr_data.get("stocks", {}).get(sym, {}))
        n_signals = sum(1 for d in (i_dir, s_dir, n_dir) if d != 0)
        if n_signals == 0:
            continue
        score = i_dir + s_dir + n_dir
        out.append({
            "symbol": sym, "score": score, "n_signals": n_signals,
            "insider": {"dir": i_dir, **(i_det or {})},
            "short": {"dir": s_dir, **(s_det or {})},
            "nvdr": {"dir": n_dir, **(n_det or {})},
        })
    # เรียง: score มากก่อน, ตามด้วยจำนวนสัญญาณที่ตรงกัน
    out.sort(key=lambda r: (r["score"], r["n_signals"]), reverse=True)
    return out
