# -*- coding: utf-8 -*-
"""sources/factor_snapshot.py — precompute 'ตารางปัจจัย' ต่อหุ้น 1 แถว

รวมทุก factor ที่ Screener ต้องใช้ (growth streak, กำไรเร่งตัว, ratio, valuation
percentile, cash cycle, คุณภาพงบดุล, risk flag) มาคำนวณ 'ครั้งเดียว' หลัง mirror
งบ Finnomena จบ แล้วเก็บลงตาราง factor_snapshot ใน financials.db — Screener แค่
อ่านตารางนี้มา filter/sort ไม่ต้องเปิด JSON blob ทั้งตลาดคำนวณสดทุกครั้งที่ค้นหา

รันซ้ำได้เสมอ (idempotent) — เขียนทับทั้งตาราง คำนวณจากงบล่าสุดใน DB
เป็น local-only เหมือน financials.db (ห้าม push ขึ้น GitHub)

หมายเหตุ: PEG / PE-live ที่อิงราคาปัจจุบันรายวัน 'ไม่' เก็บในนี้ — Screener
overlay จาก set_data.json ตอน query เอง (ราคาเปลี่ยนทุกวัน ไม่ควร freeze)
valuation percentile ในนี้อิงราคางวดล่าสุดของ Finnomena (อัพเดตตาม mirror)
"""
import json
import os
import re
import sqlite3
import time
from datetime import datetime

from sources import financials_store as fs
from core import delisted_log

TABLE = "factor_snapshot"

# ชื่อ GICS sector ที่ถือเป็น "สถาบันการเงิน" สำหรับ US/HK (us/hk_index_metrics.json ใช้ชื่อ
# sector ไม่ตรงกันเป๊ะระหว่างไฟล์ — HK มีทั้ง "Financials" (แบงก์จีน) และ "Finance" (HSBC/HKEX/
# AIA/BOCHK) — เจอจากรีวิวโค้ด 2026-07-20 ว่าเช็คแค่ "Financials" ตัวเดียวหลุด 4 ตัวนี้ไป)
FINANCIAL_SECTOR_NAMES = {"Financials", "Finance", "Financial Services"}

_financial_sector_cache = None


def _financial_sector_symbols(base_dir):
    """สัญลักษณ์หุ้นไทยกลุ่มการเงิน (ธนาคาร/เงินทุน-หลักทรัพย์/ประกัน) จาก set_data.json —
    industry='Financials' ครอบคลุมทั้ง 3 กลุ่มนี้ (ไม่รวม Property Fund & REITs ซึ่งอยู่คนละ
    industry) ใช้กัน Z-Score ที่ตีความไม่ได้กับงบดุลสถาบันการเงิน (ไม่มี current asset/liability
    ปกติ) แคชไว้ระดับ process เพราะ set_data.json ไม่เปลี่ยนบ่อยเท่าการ build snapshot
    ไม่แคชถ้าอ่านไฟล์ล้มเหลว — กัน error ชั่วคราวครั้งแรกทำให้ผลเป็น set ว่างตลอดไปทั้ง process"""
    global _financial_sector_cache
    if _financial_sector_cache is not None:
        return _financial_sector_cache
    path = os.path.join(base_dir, "set_data.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        out = {s["symbol"] for s in d.get("stocks", []) if s.get("industry") == "Financials"}
    except (OSError, json.JSONDecodeError, KeyError):
        return set()
    _financial_sector_cache = out
    return out


def _connect(base_dir):
    con = sqlite3.connect(fs._db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_table(base_dir):
    con = _connect(base_dir)
    try:
        con.executescript(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}(
              symbol TEXT, is_dr INTEGER, market TEXT, name TEXT,
              factors TEXT, computed_at TEXT,
              PRIMARY KEY(symbol, is_dr)
            );
        """)
        con.commit()
    finally:
        con.close()


def _factors_for(base_dir, sym, is_dr, z_variant=None):
    """คำนวณ factor ทั้งหมดของหุ้น 1 ตัวจากงบใน DB — คืน dict แบน หรือ None ถ้าไม่มีงบ Yahoo
    (Yahoo รายปีเป็น field หลักของ growth/ratio/FCF — ไม่มีก็คำนวณตัวหลักไม่ได้)

    z_variant: บังคับ Z-Score variant ('Z'|'Z2') แทน default ที่ผูกกับ is_dr — ใช้ตอนเรียกกับ
    หุ้น mirror US/HK ดัชนีหลัก (key 'FINN:{ex}:{name}', is_dr=False สำหรับ storage) ที่ควรได้ Z'
    (ต้นฉบับ) เหมือน DR ไม่ใช่ Z'' (emerging market) ที่ default ให้เมื่อ is_dr=False"""
    y = fs.get(base_dir, sym, "yahoo", is_dr=is_dr)
    if not y:
        return None

    gs = fs.compute_growth_score(y)
    streaks = fs.compute_growth_streaks(y)
    ratios = fs.compute_ratio_trends(y)
    fcf = fs.compute_fcf_metrics(y, None)          # fcf_yield ต้องใช้ mkt_cap live -> เว้นไว้ overlay
    ccc = fs.compute_cash_cycle(y)
    bq = fs.compute_balance_quality(y)

    # การเติบโตรายไตรมาส — ใช้แหล่งที่ลึกกว่า (finnomena_q 65 ไตรมาส vs yahoo_q ~6)
    fq = fs.get(base_dir, sym, "finnomena_q", is_dr=is_dr)
    yq = fs.get(base_dir, sym, "yahoo_q", is_dr=is_dr)
    qg_f = fs.compute_quarterly_growth(fq)
    qg_y = fs.compute_quarterly_growth(yq)
    qg = qg_f if qg_f["quarters_available"] >= qg_y["quarters_available"] else qg_y
    q_src = "finnomena_q" if qg is qg_f else "yahoo_q"

    valp = fs.compute_valuation_percentile(fq) if fq else fs.compute_valuation_percentile(None)
    eq = fs.compute_earnings_quality(fq if qg is qg_f else yq)   # คุณภาพกำไรจากแหล่งไตรมาสเดียวกับ qg
    seas = fs.compute_seasonality(fq) if fq else None   # ฤดูกาลใช้ประวัติยาว -> Finnomena
    ps = fs.compute_ps(fq) if fq else {"value": None, "percentile": None}
    qs = fs.compute_quality_streaks(fq)   # ROE>=15 ต่อเนื่อง / NM ไม่ลด (Finnomena 65 งวด)
    tg = fs.compute_ttm_growth(fq if qg is qg_f else yq)   # growth TTM (เรียบกว่า YoY งวดเดียว)
    dg = fs.compute_dividend_growth(fq)   # ปันผลโตติดกันกี่ปี (DPS = yield x close)
    pq = fs.compute_positive_streaks(fq)  # รายได้/กำไร/EBITDA/OCF เป็นบวกติดกันกี่ไตรมาส

    # interest coverage ปีล่าสุด (มาจาก ratio trends แล้ว) + rule of 40
    rule40 = None
    if gs.get("rev_cagr") is not None and ratios.get("net_margin") is not None:
        rule40 = round(gs["rev_cagr"] + ratios["net_margin"], 1)

    # PEG จาก Finnomena ล้วน (PE งวดล่าสุด ÷ กำไรโต TTM) — นิยามเดียวกันทุก universe
    # (endpoint จะ refine ด้วย pe_live สดสำหรับหุ้นไทยอีกชั้น)
    peg = _calc_peg(valp["pe"]["value"], tg["profit_ttm_yoy"])

    # F-Score / Z-Score — ต้องใช้งบ Yahoo รายปี (มีอยู่แล้วจาก y ด้านบน)
    fscore = fs.compute_fscore(y)
    # mkt_cap สดใช้ราคางวดล่าสุดของ Finnomena (เหมือน valuation percentile ด้านบน)
    # -> ค้าง (refresh ตอน rebuild snapshot ครั้งถัดไป) ไม่ใช่ราคาสดรายวินาที
    mc_row = (fq or {}).get("valuation", {}).get("Market Cap", {})
    mkt_cap = mc_row[max(mc_row)] if mc_row else None
    # Z' (ต้นฉบับ) สำหรับ DR/ต่างประเทศ, Z'' (emerging market) สำหรับหุ้นไทย
    z_variant = z_variant or ("Z" if is_dr else "Z2")
    if (not is_dr) and sym in _financial_sector_symbols(base_dir):
        # สถาบันการเงิน (ธนาคาร/เงินทุน/ประกัน) — งบดุลไม่มี current asset/liability
        # ปกติ ทำให้ working capital ตีความไม่ได้ -> ไม่แสดง Z-Score
        zscore = {"z_score": None, "z_variant": None, "z_zone": None, "z_as_of": None,
                  "z_excluded_reason": "สถาบันการเงิน — สูตร Altman ไม่ valid กับงบดุลกลุ่มนี้"}
    else:
        zscore = fs.compute_zscore(y, mkt_cap, variant=z_variant)

    f = {
        # --- growth (รายปี) ---
        "growth_score": gs["growth_score"], "rev_cagr": gs["rev_cagr"],
        "profit_cagr": gs["profit_cagr"], "years_span": gs["years_span"],
        "revenue_streak": streaks["revenue_streak"], "profit_streak": streaks["profit_streak"],
        "years_available": streaks["years_available"], "rule_of_40": rule40,
        # --- ratio ปีล่าสุด ---
        "gross_margin": ratios["gross_margin"], "net_margin": ratios["net_margin"],
        "roe": ratios["roe"], "de_ratio": ratios["de_ratio"],
        "roa": _sane(_last_ratio((fq or {}).get("ratios", {}).get("ROA")), -100, 100),   # ROA จาก Finnomena
        "interest_coverage": ratios["interest_coverage"],
        "roe15_streak_q": qs["roe15_streak_q"], "nm_hold_streak_q": qs["nm_hold_streak_q"],
        "rev_ttm_yoy": tg["rev_ttm_yoy"], "profit_ttm_yoy": tg["profit_ttm_yoy"],
        "peg": peg, "div_growth_streak_y": dg["div_growth_streak_y"],
        # --- growth รายไตรมาส (แหล่งลึกสุด) ---
        "q_source": q_src, "quarters_available": qg["quarters_available"],
        "latest_quarter": qg["latest_quarter"],
        "rev_qoq": qg["rev_qoq"], "profit_qoq": qg["profit_qoq"],
        "rev_yoy_q": qg["rev_yoy_q"], "profit_yoy_q": qg["profit_yoy_q"],
        "rev_qoq_streak": qg["rev_qoq_streak"], "profit_qoq_streak": qg["profit_qoq_streak"],
        "margin_qoq_streak": qg["margin_qoq_streak"],
        "rev_accel_streak": qg["rev_accel_streak"], "profit_accel_streak": qg["profit_accel_streak"],
        "margin_yoyq_delta": qg["margin_yoyq_delta"], "ttm_margin_delta": qg["ttm_margin_delta"],
        "rev_pos_streak_q": pq["rev_pos_streak_q"], "profit_pos_streak_q": pq["profit_pos_streak_q"],
        "ebitda_pos_streak_q": pq["ebitda_pos_streak_q"], "ocf_pos_streak_q": pq["ocf_pos_streak_q"],
        "ocf_gt_ni_latest": pq["ocf_gt_ni_latest"],
        # --- FCF (fcf_yield/dividend เว้น mkt_cap -> overlay) ---
        "fcf": fcf["fcf"], "dividend_coverage": fcf["dividend_coverage"],
        "ocf_ni_ratio": eq["ocf_ni_ratio"],   # คุณภาพกำไร TTM (จับหุ้นแต่งบัญชี)
        # --- cash cycle (คำนวณจาก Yahoo) ---
        "cash_cycle": ccc["cash_cycle"], "dio": ccc["dio"], "dso": ccc["dso"], "dpo": ccc["dpo"],
        # --- คุณภาพงบดุล / risk ---
        "net_cash": bq["net_cash"], "net_cash_positive": bq["net_cash_positive"],
        "goodwill_ratio": bq["goodwill_ratio"], "shares_chg_yoy": bq["shares_chg_yoy"],
        "buyback": bq["buyback"], "ocf_neg_years": bq["ocf_neg_years"], "shares_out": bq["shares_out"],
        # --- valuation percentile (อิงราคางวดล่าสุดของ Finnomena) ---
        "pe_value": valp["pe"]["value"], "pe_percentile": valp["pe"]["percentile"],
        "pe_median": valp["pe"]["median"],
        "pbv_value": valp["pbv"]["value"], "pbv_percentile": valp["pbv"]["percentile"],
        "ps_value": ps["value"], "ps_percentile": ps["percentile"],
        "div_yield": valp["div_yield"]["value"],
        # --- ฤดูกาล (จากรายได้ Finnomena) ---
        "high_season_q": seas["high_q"] if seas else None,
        "low_season_q": seas["low_q"] if seas else None,
        "season_swing": seas["swing_pct"] if seas else None,
        "entering_high_season": (_entering_high_season(qg.get("latest_quarter"), seas["high_q"]) if seas else None),
        # --- Piotroski F-Score / Altman Z-Score ---
        "f_score": fscore["f_score"], "f_score_max": fscore["f_score_max"],
        "f_score_detail": fscore["f_score_detail"], "f_score_as_of": fscore["f_score_as_of"],
        "z_score": zscore["z_score"], "z_variant": zscore["z_variant"],
        "z_zone": zscore["z_zone"], "z_as_of": zscore["z_as_of"],
        "z_excluded_reason": zscore.get("z_excluded_reason"),
    }
    return f


def build_snapshot(base_dir, callback=None):
    """คำนวณ factor snapshot ของทั้ง universe (หุ้นไทย + DR ที่มีงบ Yahoo) เก็บลงตาราง
    คืน {"set": n, "dr": n, "total": n} — set/dr = จำนวนแถวที่เขียนสำเร็จ

    growth_percentile จัดอันดับ 'แยก universe' (หุ้นไทย vs DR) — ไม่งั้นหุ้นไทยจะถูก
    เทียบ growth กับหุ้นเทคยักษ์ระดับโลกที่ปนใน DR (เหมือน _compute_fin_analytics_for เดิม)"""
    from core.metrics import rank_percentile
    init_table(base_dir)

    set_syms = sorted(fs.get_synced_symbols(base_dir, "yahoo", is_dr=False))
    dr_syms = sorted(fs.get_synced_symbols(base_dir, "yahoo", is_dr=True))
    total = len(set_syms) + len(dr_syms)
    done = 0
    rows = []          # (symbol, is_dr, market, name, factors_dict)

    for syms, is_dr in ((set_syms, False), (dr_syms, True)):
        computed = {}
        growth_raw = {}
        for sym in syms:
            done += 1
            f = _factors_for(base_dir, sym, is_dr)
            if f is not None:
                computed[sym] = f
                growth_raw[sym] = f["growth_score"]
            if callback and done % 50 == 0:
                callback(done, total, f"factor snapshot {done}/{total} ({sym})")
        # percentile ภายใน universe เดียวกัน
        pct = rank_percentile(growth_raw)
        for sym, f in computed.items():
            f["growth_percentile"] = pct.get(sym)
            # market + ชื่อ (จาก DR universe ถ้าเป็น DR)
            if is_dr:
                e = next((s for s in fs.load_dr_universe(fs._PROJECT_ROOT) if s["sym"] == sym), None)
                market = (e.get("region") if e else None) or "DR"
                name = (e.get("name") if e else None) or sym
            else:
                market, name = "TH", sym
            rows.append((sym, 1 if is_dr else 0, market, name, f))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = _connect(base_dir)
    try:
        con.execute(f"DELETE FROM {TABLE}")
        con.executemany(
            f"INSERT INTO {TABLE}(symbol, is_dr, market, name, factors, computed_at) VALUES (?,?,?,?,?,?)",
            [(s, d, m, n, json.dumps(f, ensure_ascii=False), now) for s, d, m, n, f in rows])
        con.commit()
    finally:
        con.close()
    fs._set_meta(base_dir, "factor_snapshot_at", now)

    n_set = sum(1 for r in rows if r[1] == 0)
    n_dr = sum(1 for r in rows if r[1] == 1)
    return {"set": n_set, "dr": n_dr, "total": len(rows), "at": now}


MIRROR_TABLE = "factor_snapshot_mirror"


def init_mirror_table(base_dir):
    con = _connect(base_dir)
    try:
        con.executescript(f"""
            CREATE TABLE IF NOT EXISTS {MIRROR_TABLE}(
              symbol TEXT, market TEXT, factors TEXT, computed_at TEXT,
              PRIMARY KEY(symbol, market)
            );
        """)
        con.commit()
    finally:
        con.close()


def _entering_high_season(latest_q_date, high_q):
    """ไตรมาสถัดจากงวดล่าสุดที่รายงาน = ไฮซีซั่นไหม (ใช้หาหุ้นที่ 'กำลังจะเข้าไฮซีซั่น')"""
    if not latest_q_date or not high_q:
        return None
    lq = {"03": 1, "06": 2, "09": 3, "12": 4}.get(latest_q_date[5:7])
    if not lq:
        return None
    return (lq % 4 + 1) == high_q


def _last_ratio(series):
    """ค่าล่าสุดของ series {date: val} — ratios/valuation ของ Finnomena เก็บเป็นรายไตรมาส"""
    if not series:
        return None
    return series[max(series)]


def _calc_peg(pe, growth_pct):
    """PEG = PE ÷ %โตของกำไร (TTM YoY) — มีความหมายเฉพาะเมื่อทั้งคู่เป็นบวก
    growth ต่ำมาก (<1%) ทำ PEG ระเบิด · growth สูงเกิน (>200%) แทบทั้งหมดคือ
    base effect (พลิกจากกำไรจิ๋ว/turnaround) — PEG จะจิ๋วหลอกตาว่า 'ถูกมาก'
    ทั้งที่การโตระดับนั้นไม่ยั่งยืน -> ทั้งสองกรณีคืน None"""
    if pe is None or growth_pct is None or pe <= 0 or not (1 <= growth_pct <= 200):
        return None
    return round(pe / growth_pct, 2)


def _sane(v, lo, hi):
    """winsorize ratio สำเร็จรูปของ Finnomena — ค่าเกินช่วงสมเหตุผล = ข้อมูลเพี้ยน ให้ None
    (วัดจริงใน mirror: net_margin |>100%| ~358 ตัว, ROE |>300%| ~106 ตัว จาก 5,571 —
    ส่วนใหญ่คือ OTC/holding/one-off ที่ค่าดิบใช้กรองไม่ได้ ถ้าปล่อยไว้ default sort
    ROE มากไปน้อยจะดันขยะขึ้นหัวตาราง)"""
    return v if (v is not None and lo <= v <= hi) else None


def _factors_from_finn(fq):
    """คำนวณ factor จากงบ Finnomena 'ล้วน' (ไม่มี Yahoo) — ใช้กับหุ้น mirror US/HK ที่ไม่อยู่
    ใน universe หลัก จึงไม่มีงบ Yahoo. ได้: growth รายไตรมาส + ratio ล่าสุด (จาก Finnomena
    ratios) + valuation percentile + OCF/NI. ส่วนที่ต้องใช้งบดุลละเอียด Yahoo (cash cycle,
    net cash, goodwill, buyback, FCF, CAGR รายปี) จะเป็น None"""
    qg = fs.compute_quarterly_growth(fq)
    eq = fs.compute_earnings_quality(fq)
    valp = fs.compute_valuation_percentile(fq)
    seas = fs.compute_seasonality(fq)
    ps = fs.compute_ps(fq)
    qs = fs.compute_quality_streaks(fq)
    tg = fs.compute_ttm_growth(fq)
    dg = fs.compute_dividend_growth(fq)
    pq = fs.compute_positive_streaks(fq)
    valp_pe = valp["pe"]["value"]
    rat = (fq or {}).get("ratios", {})
    return {
        "roe15_streak_q": qs["roe15_streak_q"], "nm_hold_streak_q": qs["nm_hold_streak_q"],
        "rev_pos_streak_q": pq["rev_pos_streak_q"], "profit_pos_streak_q": pq["profit_pos_streak_q"],
        "ebitda_pos_streak_q": pq["ebitda_pos_streak_q"], "ocf_pos_streak_q": pq["ocf_pos_streak_q"],
        "ocf_gt_ni_latest": pq["ocf_gt_ni_latest"],
        "rev_ttm_yoy": tg["rev_ttm_yoy"], "profit_ttm_yoy": tg["profit_ttm_yoy"],
        "peg": _calc_peg(valp_pe, tg["profit_ttm_yoy"]),
        "div_growth_streak_y": dg["div_growth_streak_y"],
        # ratio ล่าสุดจาก Finnomena (winsorize กันค่าดิบเพี้ยน — ดู _sane)
        "roe": _sane(_last_ratio(rat.get("ROE")), -300, 300),
        "net_margin": _sane(_last_ratio(rat.get("Net Margin")), -200, 200),
        "gross_margin": _sane(_last_ratio(rat.get("Gross Margin")), -100, 100),
        "de_ratio": _sane(_last_ratio(rat.get("Debt To Equity")), 0, 50),
        "roa": _sane(_last_ratio(rat.get("ROA")), -100, 100),
        # cash_cycle ของ Finnomena เชื่อไม่ได้ (ทดสอบแล้วเพี้ยน) — mirror ไม่มี Yahoo
        # ให้คำนวณจริง จึงเว้น None (ตรงกับ tip 'US/HK นอกพอร์ตไม่มี')
        "cash_cycle": None,
        # growth รายไตรมาส
        "q_source": "finnomena_q", "quarters_available": qg["quarters_available"],
        "latest_quarter": qg["latest_quarter"],
        "rev_qoq": qg["rev_qoq"], "profit_qoq": qg["profit_qoq"],
        "rev_yoy_q": qg["rev_yoy_q"], "profit_yoy_q": qg["profit_yoy_q"],
        "rev_qoq_streak": qg["rev_qoq_streak"], "profit_qoq_streak": qg["profit_qoq_streak"],
        "margin_qoq_streak": qg["margin_qoq_streak"],
        "rev_accel_streak": qg["rev_accel_streak"], "profit_accel_streak": qg["profit_accel_streak"],
        "margin_yoyq_delta": qg["margin_yoyq_delta"], "ttm_margin_delta": qg["ttm_margin_delta"],
        "ocf_ni_ratio": eq["ocf_ni_ratio"],
        # valuation percentile (เทียบอดีตตัวเอง)
        "pe_value": valp["pe"]["value"], "pe_percentile": valp["pe"]["percentile"], "pe_median": valp["pe"]["median"],
        "pbv_value": valp["pbv"]["value"], "pbv_percentile": valp["pbv"]["percentile"],
        "ps_value": ps["value"], "ps_percentile": ps["percentile"],
        "div_yield": valp["div_yield"]["value"],
        "high_season_q": seas["high_q"] if seas else None,
        "low_season_q": seas["low_q"] if seas else None,
        "season_swing": seas["swing_pct"] if seas else None,
        "entering_high_season": (_entering_high_season(qg.get("latest_quarter"), seas["high_q"]) if seas else None),
        # Yahoo-only -> None (บอกชัดว่าหุ้น mirror ไม่มีข้อมูลกลุ่มนี้)
        "growth_score": None, "growth_percentile": None, "rev_cagr": None, "profit_cagr": None,
        "revenue_streak": None, "profit_streak": None, "rule_of_40": None,
        "interest_coverage": None, "net_cash": None, "net_cash_positive": None,
        "goodwill_ratio": None, "shares_chg_yoy": None, "buyback": None, "ocf_neg_years": None,
        "shares_out": None,
        "fcf": None, "dividend_coverage": None, "dio": None, "dso": None, "dpo": None,
        "f_score": None, "f_score_max": None, "f_score_detail": None, "f_score_as_of": None,
        "z_score": None, "z_variant": None, "z_zone": None, "z_as_of": None, "z_excluded_reason": None,
    }


def build_mirror_snapshot(base_dir, exchanges=("US", "HK"), min_quarters=12, min_pe=4, callback=None):
    """สร้าง snapshot ของหุ้น mirror US/HK (นอก universe หลัก) ที่ 'ใช้งานได้จริง'
    เก็บตาราง factor_snapshot_mirror แยกจากตัวหลัก — Screener โหลดแยกตามตลาด (opt-in)

    เกณฑ์คัดหุ้นจริง (ตัด OTC/foreign shares ขยะที่ข้อมูลบางหรือเพี้ยน):
      - งบ >= min_quarters ไตรมาส (มีประวัติดำเนินงานจริง)
      - มี PE ในประวัติ >= min_pe งวด (หุ้นจดทะเบียนซื้อขายจริง มีราคาตลาด —
        OTC/delisted ส่วนใหญ่ไม่มี valuation เลย)
    คืน {US: n, HK: n}"""
    import sqlite3
    init_mirror_table(base_dir)
    con = _connect(base_dir)
    out = {}
    try:
        for ex in exchanges:
            rows = con.execute(
                "SELECT symbol, payload FROM financials WHERE source='finnomena_q' "
                "AND symbol LIKE ? AND payload NOT LIKE '%\"empty\": true%'", (f"FINN:{ex}:%",)).fetchall()
            recs = []
            for key, pl in rows:
                name = key.split(":", 2)[2]
                # ตัด OTC/foreign ADR ที่ข้อมูลมักเพี้ยน (ROE/growth ผิดปกติ):
                # US ticker 5 ตัวลงท้าย F (foreign ordinary) หรือ Y (ADR) = ซื้อขาย OTC
                if ex == "US" and re.fullmatch(r"[A-Z]{4}[FY]", name):
                    continue
                fq = json.loads(pl)
                if len(fq.get("income", {}).get("Total Revenue", {})) < min_quarters:
                    continue
                pe_series = fq.get("valuation", {}).get("PE", {})
                if len([v for v in pe_series.values() if v and v > 0]) < min_pe:
                    continue   # ไม่มีราคาตลาดในประวัติ = OTC/delisted ข้อมูลไม่น่าเชื่อถือ ตัดทิ้ง
                # หุ้นตาย (delisted/ถูกซื้อกิจการ เช่น SBNY) ทุก series หยุดพร้อมกัน — เช็ค
                # stale เทียบ Close ตัวเองจับไม่ได้ ต้องเทียบกับ 'วันนี้': ไม่มีงวด valuation
                # ใหม่เกิน ~400 วัน = ไม่ได้ซื้อขายแล้ว ตัดออกจาก screener (งวด Finnomena
                # เป็นวันสังเคราะห์รายไตรมาส + มีงวดปัจจุบันเสมอถ้ายังเทรด — 400 วันเหลือเฟือ)
                # เช็คทั้ง valuation และ 'งบ' — บางตัว (เช่น SBNY ธนาคารล้ม 2023) Finnomena
                # ยังปล่อยราคา OTC ต่อ แต่งบหยุดตั้งแต่ปีที่ล้ม ถ้าดูราคาอย่างเดียวจะรอด
                # ดูเฉพาะรายได้/กำไร — field รอง (เช่น Basic EPS ของ SBNY) Finnomena
                # ยังเติมงวดใหม่ต่อทั้งที่งบหลักหยุดแล้ว ใช้ทุก field จะหลุด
                val_dates = [d for row in fq.get("valuation", {}).values() for d in row]
                inc_dates = [d for f_ in ("Total Revenue", "Net Income")
                             for d in (fq.get("income", {}).get(f_) or {})]
                if not val_dates or not inc_dates:
                    continue
                try:
                    v_age = (datetime.now() - datetime.strptime(max(val_dates)[:10], "%Y-%m-%d")).days
                    i_age = (datetime.now() - datetime.strptime(max(inc_dates)[:10], "%Y-%m-%d")).days
                except ValueError:
                    continue
                if v_age > 400 or i_age > 400:
                    # บันทึกไว้ให้ backtest รุ่นถัดไปรู้ว่าหุ้นนี้ "ยังอยู่จริง" ถึงวันไหน
                    # (upsert — เก็บวันที่ตรวจพบครั้งแรก ไม่ทับทุกรอบที่ snapshot กรองซ้ำ)
                    last_seen = max(val_dates + inc_dates)[:10]
                    delisted_log.record_delisted(
                        base_dir, name, ex,
                        f"ไม่มีงวด valuation/งบใหม่เกิน 400 วัน (v_age={v_age}, i_age={i_age})",
                        last_seen=last_seen)
                    continue
                fq["synced_at"] = None
                # ถ้ามีงบ Yahoo annual sync ไว้แล้วใช้ _factors_for เดียวกับ TH/DR ได้ FCF/
                # net_cash/CAGR/F-Score/Z-Score ครบ แทนที่จะเป็น None ทั้งหมดแบบ _factors_from_finn
                # — เช็ค 2 namespace: FINN:{ex}:{name} (จาก sync_mirror_yahoo_index จำกัดแค่หุ้น
                # ดัชนีหลัก) ก่อน แล้ว fallback ไป DR:{name} (หุ้นที่บังเอิญอยู่ใน DR portfolio
                # ที่ curate ไว้อยู่แล้ว ~104 ตัว sync ไว้ก่อนงานนี้ทั้งคู่ finnomena_q+yahoo)
                # ห้ามลืมเช็ค DR: — พลาดจุดนี้ตอนแรกทำให้หุ้นดังๆ อย่าง AAPL/CAT ที่อยู่ใน DR
                # portfolio อยู่แล้วไม่ได้ full factors ทั้งที่มีงบ Yahoo พร้อมใช้จริง
                f = None
                if fs.get(base_dir, key, "yahoo", is_dr=False):
                    f = _factors_for(base_dir, key, is_dr=False, z_variant="Z")
                elif fs.get(base_dir, name, "yahoo", is_dr=True):
                    f = _factors_for(base_dir, name, is_dr=True)
                if f is None:
                    f = _factors_from_finn(fq)
                recs.append((name, ex, f))
                if callback and len(recs) % 200 == 0:
                    callback(ex, len(recs), name)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            con.execute(f"DELETE FROM {MIRROR_TABLE} WHERE market=?", (ex,))
            con.executemany(
                f"INSERT INTO {MIRROR_TABLE}(symbol, market, factors, computed_at) VALUES (?,?,?,?)",
                [(n, m, json.dumps(f, ensure_ascii=False), now) for n, m, f in recs])
            con.commit()
            out[ex] = len(recs)
    finally:
        con.close()
    fs._set_meta(base_dir, "factor_mirror_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return out


def get_mirror_snapshot(base_dir, market):
    """อ่าน mirror snapshot ของตลาดเดียว (US หรือ HK) — คืน list ของ dict"""
    if not fs.db_exists(base_dir):
        return []
    con = _connect(base_dir)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {MIRROR_TABLE}(symbol TEXT, market TEXT, factors TEXT, computed_at TEXT, PRIMARY KEY(symbol, market))")
        cur = con.execute(f"SELECT symbol, market, factors, computed_at FROM {MIRROR_TABLE} WHERE market=?", (market,))
        out = []
        for sym, mkt, factors, at in cur.fetchall():
            row = {"symbol": sym, "is_dr": True, "market": mkt, "name": sym, "computed_at": at}
            row.update(json.loads(factors))
            out.append(row)
    finally:
        con.close()
    return out


def get_mirror_symbols(base_dir):
    """คืน {US: [sym,...], HK: [sym,...]} ของหุ้น mirror ที่ผ่านเกณฑ์ (สำหรับ datalist ค้นหา)
    — เบา ไม่ parse factors"""
    if not fs.db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {MIRROR_TABLE}(symbol TEXT, market TEXT, factors TEXT, computed_at TEXT, PRIMARY KEY(symbol, market))")
        rows = con.execute(f"SELECT market, symbol FROM {MIRROR_TABLE} ORDER BY symbol").fetchall()
    finally:
        con.close()
    out = {}
    for m, s in rows:
        out.setdefault(m, []).append(s)
    return out


def mirror_snapshot_meta(base_dir):
    if not fs.db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {MIRROR_TABLE}(symbol TEXT, market TEXT, factors TEXT, computed_at TEXT, PRIMARY KEY(symbol, market))")
        counts = {m: n for m, n in con.execute(f"SELECT market, COUNT(*) FROM {MIRROR_TABLE} GROUP BY market").fetchall()}
    finally:
        con.close()
    return {"computed_at": fs._get_meta(base_dir, "factor_mirror_at"), "counts": counts}


def get_snapshot(base_dir, is_dr=None):
    """อ่าน snapshot ทั้งหมด (หรือเฉพาะ universe) — คืน list ของ dict พร้อม factors แตกออกมา
    is_dr=None คืนทั้งคู่, False เฉพาะหุ้นไทย, True เฉพาะ DR"""
    if not fs.db_exists(base_dir):
        return []
    con = _connect(base_dir)
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE}(symbol TEXT, is_dr INTEGER, market TEXT, name TEXT, factors TEXT, computed_at TEXT, PRIMARY KEY(symbol, is_dr))")
        if is_dr is None:
            cur = con.execute(f"SELECT symbol, is_dr, market, name, factors, computed_at FROM {TABLE}")
        else:
            cur = con.execute(f"SELECT symbol, is_dr, market, name, factors, computed_at FROM {TABLE} WHERE is_dr=?", (1 if is_dr else 0,))
        out = []
        for sym, dr, market, name, factors, at in cur.fetchall():
            row = {"symbol": sym, "is_dr": bool(dr), "market": market, "name": name, "computed_at": at}
            row.update(json.loads(factors))
            out.append(row)
    finally:
        con.close()
    return out


def snapshot_meta(base_dir):
    return {"computed_at": fs._get_meta(base_dir, "factor_snapshot_at"),
            "count": len(get_snapshot(base_dir))}
