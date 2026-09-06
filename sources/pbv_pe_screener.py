# -*- coding: utf-8 -*-
"""sources/pbv_pe_screener.py — Fair Value จาก Justified P/B + Justified P/E (ROE-based)
รันวนทุกหุ้นไทย เรียงตาม upside% เหมือน 🎯 DCF Screener (ดู sources/dcf_screener.py)

เติมช่องว่างที่ DCF Screener ทำไม่ได้: DCF ตัดกลุ่มการเงิน (ธนาคาร/เงินทุน/ประกัน ~69 ตัว)
ทิ้งเพราะ FCF ไม่มีความหมาย — แต่ Justified P/B อิง ROE/ส่วนของผู้ถือหุ้น ใช้ได้กับกลุ่มนี้
โดยเฉพาะ **จึงไม่ตัดกลุ่มการเงินทิ้ง** (ตรงข้ามกับ DCF Screener)

สูตร (สองอันสอดคล้องกันเป๊ะ: Justified P/B = Justified P/E × ROE):
- Justified P/B  = (ROE − g) / (r − g)              → mirror ของ _altJustifiedPb() ใน
                                                       static/dashboard.js (Tearsheet)
- Justified P/E  = (1 − g/ROE) / (r − g)   [leading, ROE-based]
  Fair Value(P/B) = Justified P/B × BVPS  ·  Fair Value(P/E) = Justified P/E × EPS(รายปีล่าสุด)
  r = Cost of Equity (CAPM: Rf + β·ERP, β=1.00 คงที่เหมือน DCF Screener — ปรับ/กรอกตรงได้)
  g = อัตราโตระยะยาว "ถาวร" ค่าเดียวทั้งตลาด (แนวเดียวกับ Terminal Growth ของ DCF) — ปรับได้

กฎที่พลาดไม่ได้ (ดู memory pbv-pe-screener-plan / dcf-screener-batch):
- r > g เสมอ (guard เดียวกับ WACC > Terminal Growth) → ไม่ผ่าน = คำนวณไม่ได้
- (r − g) ต้องห่างกันอย่างน้อย MIN_R_G_SPREAD_PCT (ไม่ใช่แค่ r > g) — ตัวส่วนของ jpb_x/jpe_x
  ทั้งคู่ ถ้า r ใกล้ g มากๆ (เช่นกรอก coe_pct เองให้ห่าง g แค่ 0.5pp) ตัวคูณระเบิดได้เหมือน
  ROE ที่ไม่ถูก cap แม้ ROE จะดูสมเหตุสมผลก็ตาม (near-singularity ที่ตัวส่วน ไม่ใช่ตัวเศษ)
- ROE > g เสมอ (ไม่งั้น retention ratio > 1 / มูลค่าติดลบ) → ไม่ผ่าน = คำนวณไม่ได้
- BVPS > 0 ถึงคำนวณ P/B ได้ · EPS > 0 ถึงคำนวณ P/E ได้ (คำนวณได้ข้างเดียวก็ถือว่า ok)
- ROE ที่ป้อนเข้าสูตรจริง (จุด jpb_x/jpe_x) ถูก cap เพิ่มที่ ROE_VALUATION_CAP_PCT — ค่า winsorize
  ±300% ของ factor_snapshot._sane() เป็นแค่ display-safe (กันขยะดันขึ้นหัวตาราง sort ธรรมดา)
  ไม่ใช่ valuation-safe ROE ที่พุ่งเกินนี้มักมาจากทุนบางผิดปกติ (buyback หนัก/ตัดขาดทุนก้อนใหญ่
  ปีก่อนกดฐาน equity ต่ำ) ป้อนตรงเข้าสูตร perpetuity แบบไม่มี cap จะพองมูลค่าเกินจริงหลายสิบเท่า

ไม่ยิง network เลย — อ่านจาก factor_snapshot (roe/bvps/eps_latest/pe_value/pbv_value ที่
คำนวณไว้แล้ว) + set_data.json (price/mkt_cap/sector สด) ล้วนๆ จึงรันได้เร็ว (ไม่กี่วินาที
ทั้งตลาด) เก็บผลในตาราง pbv_pe_screener (additive ใน financials.db — ไม่แตะตารางเดิม)
เป็น local-only เหมือน financials.db (ห้าม push ขึ้น GitHub)
"""
import json
import os
import sqlite3
from datetime import datetime

from sources import financials_store as fs
from sources import factor_snapshot

TABLE = "pbv_pe_screener"

# ค่าเริ่มต้น — ต้องแก้คู่กับ PBV_PE_SCR_DEFAULTS ใน static/dashboard.js ถ้าเปลี่ยน
RISK_FREE_PCT = 2.5
BETA = 1.00
ERP_PCT = 5.5
LONG_TERM_GROWTH_PCT = 3.0   # g "ถาวร" ทั้งตลาด (แนวเดียวกับ Terminal Growth 2.5% ของ DCF
                             # แต่ตั้งสูงกว่าเล็กน้อย = โต book value/กำไร nominal ระยะยาว)
# หมายเหตุ: threshold "|upside| เกิน 300% = noise" เป็นแค่ไอคอนเตือน ⚠️ ฝั่ง static/dashboard.js
# (PBV_PE_SCR_EXTREME_UPSIDE_PCT) ไม่ได้กรองแถวทิ้งจากตาราง (upside สูงมากยังอาจเป็นสัญญาณจริงได้
# ถ้าหุ้นถูกมากจริงๆ เทียบ Fair Value ที่ ROE ยังถูก cap แล้ว — ดู ROE_VALUATION_CAP_PCT ด้านล่าง)

# ROE ที่ป้อนเข้าสูตร Justified P/B·P/E จริง (jpb_x/jpe_x) ต้อง "สมเหตุสมผลระยะยาว" กว่าแค่ผ่าน
# factor_snapshot._sane(roe, -300, 300) — ค่านั้นออกแบบมากันขยะไม่ให้ดันขึ้นหัวตาราง sort ธรรมดา
# เท่านั้น ไม่ใช่ valuation-safe สำหรับป้อนสูตร Gordon growth ที่สมมติ ROE คงที่ไปตลอด (perpetuity)
# ROE ที่พุ่งเกินนี้มักมาจากทุนบางผิดปกติ (buyback หนัก/ตัดขาดทุนก้อนใหญ่ปีก่อนกดฐาน equity ต่ำ)
# ไม่ใช่อัตรายั่งยืนจริง — cap เฉพาะตอนคำนวณ Fair Value เท่านั้น (roe_pct ที่โชว์ในตาราง/CSV
# ยังเป็นค่าจริงเสมอ ไม่ถูก cap ที่นี่)
ROE_VALUATION_CAP_PCT = 60.0

# (r − g) เป็นตัวส่วนของทั้ง jpb_x และ jpe_x — แค่กัน r <= g ("ต้องมากกว่า") ไม่พอ เพราะ r ที่ห่าง
# g แค่เศษเสี้ยว pp (กรอก coe_pct เองให้ห่าง g นิดเดียว) ก็ทำให้ตัวส่วนเข้าใกล้ศูนย์และตัวคูณระเบิด
# ได้เหมือนกัน แม้ ROE จะปกติดีไม่ชนแคป ROE_VALUATION_CAP_PCT เลยก็ตาม (คนละจุดกับบั๊ก ROE ด้านบน
# แต่ผลลัพธ์เดียวกัน) — บังคับ margin ขั้นต่ำเป็น "จุด" (percentage point) ไม่ใช่ %
MIN_R_G_SPREAD_PCT = 1.0

# ตัวเศษของ Justified P/E มีเทอม g/ROE — ROE (หลัง cap) ที่เข้าใกล้ 0 ทำให้เทอมนี้ระเบิด และถ้า
# ROE เป็น 0 เป๊ะจะ ZeroDivisionError · เกิดได้จริงเมื่อกรอก g ติดลบ (clamp ยอมถึง -2%) เพราะ
# guard "ROE > g" ด้านล่างจะอ่อนจนหุ้น ROE จิ๋ว/ศูนย์ผ่านเข้ามาถึงจุดคำนวณ — ต้องมี ROE เป็นบวก
# อย่างน้อยเท่านี้ถึงคำนวณ Justified P/E ได้ (Justified P/B ไม่มีเทอม g/ROE คำนวณต่อได้ตามปกติ)
MIN_ROE_FOR_JPE_PCT = 1.0


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
    ทำเองตรงนี้กันวน import app.py (เหมือน sources/dcf_screener.py)"""
    path = os.path.join(base_dir, "set_data.json")
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            stocks = json.load(f).get("stocks", [])
    except (OSError, json.JSONDecodeError):
        return out
    for s in stocks:
        sym = s.get("symbol")
        if sym:
            out[sym] = s
    return out


def resolve_assumptions(raw):
    """แปลง/clamp ค่าจากฟอร์ม (dict ดิบจาก JSON body — อาจเป็น str/None/ขาดหาย) ให้ปลอดภัยเสมอ

    2 กลุ่ม field:
    - g_pct/rf_pct/beta/erp_pct: มีค่าเริ่มต้นเสมอ (เว้นว่างไม่ได้ — เป็นสมมติฐานตลาด)
    - coe_pct/roe_pct: เว้นว่างได้ (คืน None) — None = ใช้ค่าจริงของหุ้นนั้น
        coe_pct = None → คำนวณ Cost of Equity จาก CAPM (Rf + β·ERP)
        roe_pct = None → ใช้ ROE จริงของหุ้นนั้นจาก factor_snapshot
    """
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

    return {
        "g_pct": _f("g_pct", -2, 8, LONG_TERM_GROWTH_PCT),
        "rf_pct": _f("rf_pct", -5, 20, RISK_FREE_PCT),
        "beta": _f("beta", -5, 10, BETA),
        "erp_pct": _f("erp_pct", 0, 20, ERP_PCT),
        # override ทั้งตลาด — None = ใช้ค่าจริงของหุ้นนั้น
        "coe_pct": _opt_f("coe_pct", 0.5, 40),
        "roe_pct": _opt_f("roe_pct", -100, 300),
    }


def compute_fair_value_for_symbol(sym, entry, snap, is_financial, assumptions=None):
    """คำนวณ Justified P/B + Justified P/E 1 ตัว — คืน (result_dict, error_reason)
    error_reason ไม่ None แปลว่าคำนวณไม่ได้ทั้งคู่ (result_dict มีแค่ symbol/name/sector ให้ UI
    แสดงแถวจาง) · คำนวณได้ข้างเดียว (P/B หรือ P/E) ถือว่า ok (error=None)"""
    a = assumptions or {}
    g_pct = a.get("g_pct", LONG_TERM_GROWTH_PCT)

    name = entry.get("name") if entry else None
    sector = entry.get("sector") if entry else None
    base = {"symbol": sym, "name": name, "sector": sector, "is_financial": is_financial}

    if not entry:
        return base, "ไม่พบใน set_data.json"
    price = entry.get("price")
    if not price:
        return base, "ไม่มีราคาสด"
    snap = snap or {}

    # Cost of Equity (r) — กรอก coe_pct ตรง = ข้าม CAPM
    if a.get("coe_pct") is not None:
        r_pct = a["coe_pct"]
    else:
        r_pct = a.get("rf_pct", RISK_FREE_PCT) + a.get("beta", BETA) * a.get("erp_pct", ERP_PCT)

    # ROE — กรอก roe_pct = บังคับทั้งตลาด, ไม่งั้นใช้ค่าจริงของหุ้น (winsorize แล้วใน snapshot)
    roe_pct = a["roe_pct"] if a.get("roe_pct") is not None else snap.get("roe")

    base.update({"price": price, "coe_pct": round(r_pct, 2), "g_pct": round(g_pct, 2),
                 "cur_pbv": snap.get("pbv_value"), "cur_pe": snap.get("pe_value"),
                 "cur_ps": snap.get("ps_value")})

    if roe_pct is None:
        return base, "ไม่มี ROE ในงบ"
    base["roe_pct"] = round(roe_pct, 2)

    if r_pct - g_pct < MIN_R_G_SPREAD_PCT:
        return base, f"Cost of Equity ต้องมากกว่า g อย่างน้อย {MIN_R_G_SPREAD_PCT:.1f} จุด (ห่างกันน้อยกว่านี้ตัวคูณจะระเบิด)"
    if roe_pct <= g_pct:
        return base, "ROE ต้องมากกว่า g (ไม่งั้นมูลค่าติดลบ)"

    # cap เฉพาะค่าที่ป้อนสูตร valuation จริง — roe_pct (raw) ยังคงถูกโชว์ใน base ด้านบนตามจริง
    # (ดู ROE_VALUATION_CAP_PCT ด้านบนไฟล์สำหรับเหตุผล)
    roe_calc_pct = min(roe_pct, ROE_VALUATION_CAP_PCT)

    bvps = snap.get("bvps")
    eps = snap.get("eps_latest")
    base["bvps"] = round(bvps, 2) if bvps is not None else None
    base["eps"] = round(eps, 2) if eps is not None else None

    # Justified P/B = (ROE − g) / (r − g)  — ratio ของ % หารกันได้ตรงๆ (unit ตัดกัน)
    jpb_x = (roe_calc_pct - g_pct) / (r_pct - g_pct)
    if bvps is not None and bvps > 0 and jpb_x > 0:
        jpb_fair = jpb_x * bvps
        base["jpb_x"] = round(jpb_x, 2)
        base["jpb_fair"] = round(jpb_fair, 2)
        base["jpb_upside_pct"] = round((jpb_fair / price - 1) * 100, 2)

    # Justified P/E = (1 − g/ROE) / (r − g)  [leading, ROE-based]
    # ตัวส่วน (r − g) ต้องเป็น "ทศนิยม" (ไม่ใช่ %) เพราะตัวเศษ (1 − g/ROE) ไร้หน่วย
    # g/ROE เป็น ratio → ใช้ % หารกันได้ (g_pct/roe_calc_pct)
    g_dec, r_dec = g_pct / 100.0, r_pct / 100.0
    if roe_calc_pct >= MIN_ROE_FOR_JPE_PCT:
        jpe_x = (1.0 - g_pct / roe_calc_pct) / (r_dec - g_dec)
        if eps is not None and eps > 0 and jpe_x > 0:
            jpe_fair = jpe_x * eps
            base["jpe_x"] = round(jpe_x, 2)
            base["jpe_fair"] = round(jpe_fair, 2)
            base["jpe_upside_pct"] = round((jpe_fair / price - 1) * 100, 2)

    if "jpb_fair" not in base and "jpe_fair" not in base:
        return base, "ต้องมี BVPS > 0 หรือ EPS > 0 อย่างน้อยหนึ่งอย่าง"
    return base, None


def build_snapshot(base_dir, callback=None, assumptions=None):
    """คำนวณ Fair Value ใหม่ทั้งตลาดหุ้นไทย เขียนทับตาราง pbv_pe_screener — รันซ้ำได้เสมอ
    ไม่ยิง network (อ่านจาก factor_snapshot + set_data.json) · คืน summary counts"""
    init_table(base_dir)
    assumptions = resolve_assumptions(assumptions)
    set_map = _load_set_data_map(base_dir)
    financial_syms = factor_snapshot._financial_sector_symbols(base_dir)
    snap_map = {r["symbol"]: r for r in factor_snapshot.get_snapshot(base_dir, is_dr=False)}

    syms = sorted(set_map.keys())
    total = len(syms)
    rows = []
    for i, sym in enumerate(syms):
        entry = set_map.get(sym)
        try:
            result, error = compute_fair_value_for_symbol(
                sym, entry, snap_map.get(sym), sym in financial_syms, assumptions)
        except Exception as exc:
            # กันหุ้นตัวเดียวคำนวณพัง (ค่าในงบเป็น type แปลก / ตัวหารเป็นศูนย์ที่ยังไม่ได้ guard)
            # แล้วทำให้ rebuild ทั้งตลาดล้ม 500 ทั้งก้อน — เหมือน sources/dcf_screener.py:build_snapshot
            result = {"symbol": sym, "name": entry.get("name") if entry else None,
                      "sector": entry.get("sector") if entry else None,
                      "is_financial": sym in financial_syms}
            error = f"คำนวณผิดพลาด: {exc}"
        rows.append((sym, 0 if error else 1,
                     json.dumps({**result, "error": error}, ensure_ascii=False)))
        if callback and (i + 1) % 100 == 0:
            callback(i + 1, total, f"PBV/PE screener {i + 1}/{total} ({sym})")

    if not rows:
        print(f"[pbv_pe_screener] rows ว่าง (0 ตัว) — ข้ามการเขียนทับ {TABLE} เก็บผลรอบก่อนไว้")
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
    fs._set_meta(base_dir, "pbv_pe_screener_at", now)
    fs._set_meta(base_dir, "pbv_pe_screener_assumptions", json.dumps(assumptions, ensure_ascii=False))

    ok_count = sum(1 for _, ok, _ in rows if ok)
    return {"total": len(rows), "ok": ok_count, "at": now, "assumptions": assumptions}


def get_snapshot(base_dir):
    """อ่านผลลัพธ์ทั้งหมดที่คำนวณไว้แล้ว — list ของ dict (แต่ละตัวมี symbol/name/sector เสมอ
    + field ผลคำนวณถ้า error เป็น None)"""
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


def snapshot_meta(base_dir, rows=None):
    """rows: ผลลัพธ์จาก get_snapshot() ที่มีอยู่แล้ว (กันอ่านตาราง pbv_pe_screener ซ้ำ 2 รอบ
    ในคำขอเดียว ซึ่งเสี่ยง race กับ rebuild ที่ DELETE+INSERT คั่นกลางระหว่าง 2 การอ่าน) —
    ไม่ใส่ = อ่านเองเหมือนเดิม (เผื่อ caller อื่นเรียกตรงๆ โดยไม่มี rows พร้อมอยู่แล้ว)"""
    if rows is None:
        rows = get_snapshot(base_dir)
    ok_count = sum(1 for r in rows if not r.get("error"))
    at = fs._get_meta(base_dir, "pbv_pe_screener_at")
    raw_assumptions = fs._get_meta(base_dir, "pbv_pe_screener_assumptions")
    try:
        assumptions = json.loads(raw_assumptions) if raw_assumptions else resolve_assumptions(None)
    except (TypeError, ValueError):
        assumptions = resolve_assumptions(None)
    return {"computed_at": at, "count": len(rows), "ok_count": ok_count, "assumptions": assumptions,
            "stale": fs.snapshot_stale_vs_sync(base_dir, "pbv_pe_screener_at")}
