# -*- coding: utf-8 -*-
"""sources/dcf_screener.py — DCF Model (พยากรณ์เต็มรูปแบบ) รันวนทุกหุ้นไทยที่คำนวณได้

Port สูตรเดียวกับ "DCF Model — พยากรณ์เต็มรูปแบบ" ในหน้า Tearsheet (ดู _tsDcfModelRecalc ใน
static/dashboard.js) จาก JS มาเป็น Python แล้วรันเป็น batch แทนที่จะเปิดทีละหุ้น ค่าเริ่มต้น
เดียวกับ Tearsheet ทั้งหมด (Rf 2.5%/Beta 1.00/ERP 5.5%/Terminal Growth 2.5%) แต่ทุก field
(Revenue Growth ปี1-3/ปี4-5, EBIT Margin, Tax Rate, D&A/CapEx/ΔNWC %Revenue, WACC) กรอก
override ทั้งตลาดได้จาก UI — เว้นว่างช่องไหน = ใช้ 'ค่าจริงของหุ้นนั้น' จากงบ/factor_snapshot
(พฤติกรรมเดิม), กรอกช่องไหน = บังคับใช้ตัวเลขเดียวกันกับหุ้นทุกตัว (ดู resolve_assumptions/
compute_dcf_for_symbol) — เป็นการคัดกรองเบื้องต้นเท่านั้น ไม่ใช่คำแนะนำซื้อ/ขาย เพราะ Beta
ค่าเริ่มต้นไม่ได้ปรับตาม risk จริงรายตัว (ปรับเองได้ผ่านช่อง WACC override)

ไม่ยิง network เพิ่มเลย — อ่านจาก financials.db (งบ Yahoo ที่ sync ไว้แล้ว) + factor_snapshot
(rev_cagr/net_cash ที่คำนวณไว้แล้ว) + set_data.json (price/mkt_cap สด) ล้วนๆ จึงรันได้เร็ว
(ไม่กี่วินาทีทั้งตลาด) เป็น local-only เหมือน financials.db (ห้าม push ขึ้น GitHub)
"""
import json
import os
import sqlite3
from datetime import datetime

from sources import financials_store as fs
from sources import factor_snapshot
from sources import analyst_consensus

TABLE = "dcf_screener"

# ค่าเริ่มต้นเดียวกับ dcf.discount_rate_default/terminal_growth_default + ค่าตั้งต้นในฟอร์ม
# WACC ของหน้า Tearsheet (_tsDcfModelHtml/_tsDcfModelRecalc) — Beta=1.00 คงที่ทุกตัว (ไม่ได้ปรับ
# ตาม risk จริงรายหุ้น) ตามที่เลือกไว้ตอนออกแบบ (เร็ว, ไม่ต้องคำนวณ regression ราคาเพิ่ม)
RISK_FREE_PCT = 2.5
BETA = 1.00
ERP_PCT = 5.5
TERMINAL_GROWTH_PCT = 2.5
FORECAST_YEARS = 5
DEFAULT_COST_OF_DEBT_PRETAX_PCT = 4.5   # fallback เมื่องบไม่มีดอกเบี้ยจ่าย/หนี้ให้คำนวณ
DEFAULT_G13_PCT = 5.0                   # fallback เมื่อ factor snapshot ไม่มี rev_cagr


def _connect(base_dir):
    con = sqlite3.connect(fs._db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_table(base_dir):
    con = _connect(base_dir)
    try:
        con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE}(
            symbol TEXT PRIMARY KEY, ok INTEGER, result TEXT, computed_at TEXT
        )""")
        con.commit()
    finally:
        con.close()


def _load_set_data_map(base_dir):
    """{symbol: entry} จาก set_data.json ที่ root โปรเจกต์ (price/mkt_cap/sector สด) —
    เหมือน _tearsheet_universe_map(mkt='TH') ใน app.py แต่ทำเองตรงนี้กันวน import app.py"""
    path = os.path.join(base_dir, "set_data.json")
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for s in json.load(f).get("stocks", []):
                out[s["symbol"]] = s
    except (OSError, json.JSONDecodeError, KeyError):
        pass
    return out


def _analyst_growth_pct(ac):
    """ประมาณการอัตราโตจากนักวิเคราะห์ (Yahoo) สำหรับใช้เป็น g ปี1-3 แทน CAGR อดีต —
    เลือก long-term growth (LTG) ก่อนเพราะเหมาะกับ DCF หลายปีที่สุด แต่ coverage แทบ
    เป็นศูนย์ (Yahoo แทบไม่ส่ง) → fallback ประมาณการโต EPS ปีหน้า · clamp [-50,200]
    เท่าขอบเขต g13 override · คืน None ถ้าไม่มีข้อมูลนักวิเคราะห์เลย"""
    if not ac:
        return None
    g = ac.get("ltg_pct")
    if g is None:
        g = ac.get("eps_growth_next_y")
    if g is None:
        return None
    return max(-50.0, min(200.0, float(g)))


def compute_dcf_for_symbol(sym, entry, y_payload, snap_factors, is_financial,
                           assumptions=None, analyst_growth=None):
    """คำนวณ DCF Model เต็มรูปแบบ 1 ตัว — คืน (result_dict, error_reason)
    error_reason ไม่ None แปลว่าคำนวณไม่ได้ (result_dict มีแค่ symbol/name/sector ให้ UI แสดง)

    analyst_growth: อัตราโต %/ปี จากนักวิเคราะห์ (ดู _analyst_growth_pct) — ใช้แทน rev_cagr
    สำหรับ g ปี1-3 เฉพาะเมื่อ assumptions['use_analyst_growth'] เป็น True และช่อง g13
    ไม่ได้ override · None = หุ้นตัวนี้ไม่มีนักวิเคราะห์ตาม (ตกกลับไป rev_cagr/default ตามเดิม)

    assumptions: dict จาก resolve_assumptions() เสมอ — แบ่ง 2 กลุ่ม:
    - rf_pct/beta/erp_pct/terminal_growth_pct/years: มีค่าเริ่มต้นเสมอ (ไม่มี "ค่าจริงของหุ้น"
      ให้ fallback เพราะเป็นสมมติฐานตลาด ไม่ใช่ตัวเลขในงบ)
    - g13_pct/g45_pct/ebit_margin_pct/tax_rate_pct/da_pct/capex_pct/nwc_pct/wacc_pct: None ได้ —
      None = ใช้ 'ค่าจริงของหุ้นนั้น' จากงบ/factor_snapshot (พฤติกรรมเดิมก่อนมี override),
      ไม่ None = บังคับใช้ตัวเลขเดียวกันนี้กับหุ้นทุกตัวในตลาด (ตาราง input แบบหน้า reference
      ที่ผู้ใช้ส่งมา — เว้นว่างในฟอร์ม = ปล่อยให้ระบบไปหาค่าประวัติศาสตร์ของหุ้นนั้นเอง)"""
    a = assumptions or {}
    rf_pct = a.get("rf_pct", RISK_FREE_PCT)
    beta = a.get("beta", BETA)
    erp_pct = a.get("erp_pct", ERP_PCT)
    tg_pct = a.get("terminal_growth_pct", TERMINAL_GROWTH_PCT)
    years = a.get("years", FORECAST_YEARS)
    ov_g13 = a.get("g13_pct")
    ov_g45 = a.get("g45_pct")
    ov_ebit_m = a.get("ebit_margin_pct")
    ov_tax = a.get("tax_rate_pct")
    ov_da = a.get("da_pct")
    ov_capex = a.get("capex_pct")
    ov_nwc = a.get("nwc_pct")
    ov_wacc = a.get("wacc_pct")

    name = entry.get("name") if entry else None
    sector = entry.get("sector") if entry else None
    base = {"symbol": sym, "name": name, "sector": sector}

    if is_financial:
        return base, "กลุ่มการเงิน — สูตร DCF ใช้ไม่ได้"
    if not entry:
        return base, "ไม่พบใน set_data.json"
    price = entry.get("price")
    mkt_cap = entry.get("mkt_cap")
    if not price or not mkt_cap:
        return base, "ไม่มีราคา/มูลค่าตลาดสด"
    if not y_payload:
        return base, "ยังไม่ sync งบ Yahoo"
    forecast = fs.compute_dcf_forecast_inputs(y_payload)
    if not forecast:
        return base, "ไม่มี Revenue/EBIT ปีล่าสุดในงบ"

    snap_factors = snap_factors or {}
    rev_cagr = snap_factors.get("rev_cagr")
    use_analyst = bool(a.get("use_analyst_growth"))
    if ov_g13 is not None:
        g13_pct_used, g_source = ov_g13, "override"
    elif use_analyst and analyst_growth is not None:
        g13_pct_used, g_source = analyst_growth, "analyst"
    elif rev_cagr is not None:
        g13_pct_used, g_source = rev_cagr, "rev_cagr"
    else:
        g13_pct_used, g_source = DEFAULT_G13_PCT, "default"
    g45_pct_used = ov_g45 if ov_g45 is not None else g13_pct_used * 0.6
    g13 = g13_pct_used / 100
    g45 = g45_pct_used / 100
    ebit_m_pct_used = ov_ebit_m if ov_ebit_m is not None else forecast["ebit_margin"]
    ebit_m = ebit_m_pct_used / 100
    tax_pct_used = ov_tax if ov_tax is not None else forecast["tax_rate"]
    tax = tax_pct_used / 100
    da_pct = (ov_da if ov_da is not None else (forecast.get("da_pct_revenue") or 0.0)) / 100
    capex_pct = (ov_capex if ov_capex is not None else (forecast.get("capex_pct_revenue") or 0.0)) / 100
    nwc_pct = (ov_nwc if ov_nwc is not None else (forecast.get("nwc_pct_revenue") or 0.0)) / 100
    tg = tg_pct / 100

    if ov_wacc is not None:
        wacc = ov_wacc / 100   # กรอก WACC ตรง = ข้ามการ build CAPM/capital structure ทั้งหมด
    else:
        cost_equity = rf_pct / 100 + beta * (erp_pct / 100)
        kd_pretax = (forecast.get("cost_of_debt_pretax") or DEFAULT_COST_OF_DEBT_PRETAX_PCT) / 100
        cost_debt_after_tax = kd_pretax * (1 - tax)
        debt_val = forecast.get("total_debt") or 0
        eq_val = mkt_cap
        total_v = eq_val + debt_val
        e_over_v = eq_val / total_v if total_v > 0 else 1.0
        d_over_v = debt_val / total_v if total_v > 0 else 0.0
        wacc = e_over_v * cost_equity + d_over_v * cost_debt_after_tax
    if wacc <= tg:
        return base, "WACC ต้องมากกว่า Terminal Growth"

    revenue = forecast["revenue"]
    pv_sum = 0.0
    last_fcff = 0.0
    for t in range(1, years + 1):
        revenue_prev = revenue
        g = g13 if t <= 3 else g45
        revenue = revenue_prev * (1 + g)
        ebit = revenue * ebit_m
        noplat = ebit * (1 - tax)
        da = revenue * da_pct
        capex = revenue * capex_pct
        # nwc_pct = ระดับ Working Capital ÷ Revenue (ไม่ใช่ 'การเปลี่ยนแปลง') — รายได้โตขึ้นเท่าไหร่
        # ต้องใช้เงินทุนหมุนเวียนเพิ่มตามอัตราส่วนนี้ ผลกระทบต่อกระแสเงินสด = −(nwc_pct × ΔRevenue)
        # ไม่ใช่ nwc_pct × Revenue เต็มปี (สูตรเดิมก่อนเปลี่ยนนิยาม 2026-08-21)
        nwc_impact = -(nwc_pct * (revenue - revenue_prev))
        fcff = noplat + da - capex + nwc_impact
        pv_sum += fcff / (1 + wacc) ** t
        last_fcff = fcff
    tv = last_fcff * (1 + tg) / (wacc - tg)
    pv_tv = tv / (1 + wacc) ** years

    net_cash = snap_factors.get("net_cash") or 0
    net_debt = -net_cash
    ev = pv_sum + pv_tv
    equity_value = ev - net_debt
    shares = mkt_cap / price
    intrinsic = equity_value / shares
    upside = (intrinsic / price - 1) * 100

    base.update({
        "price": price, "intrinsic": round(intrinsic, 2), "upside_pct": round(upside, 2),
        "wacc_pct": round(wacc * 100, 2), "g13_pct": round(g13 * 100, 2),
        "g_source": g_source, "as_of": forecast.get("as_of"),
    })
    return base, None


def resolve_assumptions(raw):
    """แปลง/clamp ค่าที่ผู้ใช้กรอกจากฟอร์ม (dict ดิบจาก JSON body, ค่าอาจเป็น str/None/ขาดหาย)
    ให้เป็นตัวเลขปลอดภัยเสมอ — ผิดพลาด/ว่างเปล่า -> กลับไปใช้ค่าเริ่มต้นของโมดูล (RISK_FREE_PCT ฯลฯ)
    ไม่ปล่อยให้ WACC/ปีพยากรณ์เพี้ยนจนคำนวณพัง (เช่น years=0, beta ติดลบสุดโต่ง)

    2 กลุ่ม field: rf_pct/beta/erp_pct/terminal_growth_pct/years มีค่าเริ่มต้นเสมอ (ไม่เว้นว่างได้)
    ส่วน g13_pct/g45_pct/ebit_margin_pct/tax_rate_pct/da_pct/capex_pct/nwc_pct/wacc_pct เว้นว่างได้
    จริงๆ (คืน None) เพื่อสื่อว่า 'ไม่ override ทั้งตลาด ใช้ค่าจริงของหุ้นนั้น' ดู compute_dcf_for_symbol"""
    raw = raw or {}

    def _f(key, lo, hi, default):
        try:
            v = float(raw.get(key))
        except (TypeError, ValueError):
            return default
        if v != v:  # NaN
            return default
        return max(lo, min(hi, v))

    def _opt_f(key, lo, hi):
        v = raw.get(key)
        if v is None or v == "":
            return None
        try:
            v = float(v)
        except (TypeError, ValueError):
            return None
        if v != v:  # NaN
            return None
        return max(lo, min(hi, v))

    try:
        years = int(float(raw.get("years")))
    except (TypeError, ValueError):
        years = FORECAST_YEARS
    years = max(1, min(15, years))

    return {
        "rf_pct": _f("rf_pct", -5, 20, RISK_FREE_PCT),
        "beta": _f("beta", -5, 10, BETA),
        "erp_pct": _f("erp_pct", 0, 20, ERP_PCT),
        "terminal_growth_pct": _f("terminal_growth_pct", -5, 10, TERMINAL_GROWTH_PCT),
        "years": years,
        # ใช้ประมาณการโตของนักวิเคราะห์ (Yahoo) แทน CAGR อดีต สำหรับ g ปี1-3 — เฉพาะหุ้น
        # ที่มีนักวิเคราะห์ตาม (~40%) ตัวอื่นตกกลับไป rev_cagr/default ตามเดิม · ช่อง g13
        # ที่กรอกเอง (override ทั้งตลาด) ยังชนะเสมอ
        "use_analyst_growth": bool(raw.get("use_analyst_growth")),
        # override ทั้งตลาด — None = ใช้ค่าจริงของหุ้นนั้น
        "g13_pct": _opt_f("g13_pct", -50, 200),
        "g45_pct": _opt_f("g45_pct", -50, 200),
        "ebit_margin_pct": _opt_f("ebit_margin_pct", -100, 100),
        "tax_rate_pct": _opt_f("tax_rate_pct", 0, 60),
        "da_pct": _opt_f("da_pct", 0, 100),
        "capex_pct": _opt_f("capex_pct", 0, 100),
        "nwc_pct": _opt_f("nwc_pct", -100, 100),
        "wacc_pct": _opt_f("wacc_pct", 0.1, 50),
    }


def build_snapshot(base_dir, callback=None, assumptions=None):
    """คำนวณ DCF ใหม่ทั้งตลาดหุ้นไทย เขียนทับตาราง dcf_screener — รันซ้ำได้เสมอ (idempotent)
    assumptions: ผ่าน resolve_assumptions() แล้วเสมอ (ไม่ใส่ = ใช้ค่าเริ่มต้นทั้งหมด)
    คืน {"total": ..., "ok": ..., "at": ..., "assumptions": ...}"""
    init_table(base_dir)
    assumptions = resolve_assumptions(assumptions)
    set_map = _load_set_data_map(base_dir)
    financial_syms = factor_snapshot._financial_sector_symbols(base_dir)
    snap_map = {r["symbol"]: r for r in factor_snapshot.get_snapshot(base_dir, is_dr=False)}
    # ประมาณการโตนักวิเคราะห์ — โหลดครั้งเดียวเฉพาะเมื่อเปิดใช้ (อ่านจาก analyst_consensus
    # ในตาราง financials.db ที่ sync ไว้แล้ว ไม่ยิง network) · {} ถ้ายังไม่เคย sync
    ac_map = analyst_consensus.get_map(base_dir, "TH") if assumptions.get("use_analyst_growth") else {}

    syms = sorted(set_map.keys())
    total = len(syms)
    rows = []
    con = fs._connect(base_dir) if fs.db_exists(base_dir) else None
    try:
        for i, sym in enumerate(syms):
            entry = set_map.get(sym)
            y_payload = fs.get(base_dir, sym, "yahoo", is_dr=False, con=con) if con else None
            result, error = compute_dcf_for_symbol(
                sym, entry, y_payload, snap_map.get(sym), sym in financial_syms, assumptions,
                analyst_growth=_analyst_growth_pct(ac_map.get(sym)))
            rows.append((sym, 0 if error else 1,
                         json.dumps({**result, "error": error}, ensure_ascii=False)))
            if callback and (i + 1) % 100 == 0:
                callback(i + 1, total, f"DCF screener {i + 1}/{total} ({sym})")
    finally:
        if con:
            con.close()

    if not rows:
        print(f"[dcf_screener] rows ว่าง (0 ตัว) — ข้ามการเขียนทับ {TABLE} เก็บผลรอบก่อนหน้าไว้")
        return {"total": 0, "ok": 0, "at": None, "skipped_empty": True}

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    wcon = _connect(base_dir)
    try:
        wcon.execute(f"DELETE FROM {TABLE}")
        wcon.executemany(
            f"INSERT INTO {TABLE}(symbol, ok, result, computed_at) VALUES (?,?,?,?)",
            [(s, ok, r, now) for s, ok, r in rows])
        wcon.commit()
    finally:
        wcon.close()
    fs._set_meta(base_dir, "dcf_screener_at", now)
    fs._set_meta(base_dir, "dcf_screener_assumptions", json.dumps(assumptions, ensure_ascii=False))

    ok_count = sum(1 for _, ok, _ in rows if ok)
    return {"total": len(rows), "ok": ok_count, "at": now, "assumptions": assumptions}


def get_snapshot(base_dir):
    """อ่านผลลัพธ์ทั้งหมดที่คำนวณไว้แล้ว — คืน list ของ dict (แต่ละตัวมี symbol/name/sector
    เสมอ + field ผลคำนวณถ้า error เป็น None)"""
    init_table(base_dir)
    con = _connect(base_dir)
    try:
        cur = con.execute(f"SELECT symbol, result, computed_at FROM {TABLE}")
        out = []
        for sym, result, at in cur.fetchall():
            try:
                row = json.loads(result)
            except (TypeError, ValueError):
                continue
            row["computed_at"] = at
            out.append(row)
    finally:
        con.close()
    return out


def snapshot_meta(base_dir):
    rows = get_snapshot(base_dir)
    ok_count = sum(1 for r in rows if not r.get("error"))
    at = fs._get_meta(base_dir, "dcf_screener_at")
    raw_assumptions = fs._get_meta(base_dir, "dcf_screener_assumptions")
    try:
        assumptions = json.loads(raw_assumptions) if raw_assumptions else resolve_assumptions(None)
    except (TypeError, ValueError):
        assumptions = resolve_assumptions(None)
    return {"computed_at": at, "count": len(rows), "ok_count": ok_count, "assumptions": assumptions}
