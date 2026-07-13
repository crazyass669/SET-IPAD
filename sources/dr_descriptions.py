# -*- coding: utf-8 -*-
"""
dr_descriptions.py — คำอธิบายบริษัท (Description จากหน้า Profile ของ Yahoo Finance)
ของหุ้น DR/DRx (US/HK/CN/อื่นๆ) แปลเป็นไทยด้วย Google Translate (deep-translator)
แล้ว cache ไว้ในไฟล์ local (dr_descriptions.json)

ข้อมูลนี้มาจาก Yahoo Finance (longBusinessSummary) ไม่ใช่ Finnomena — publish ขึ้น
GitHub ได้ตามกฎที่ตกลงไว้ (ต่างจาก financials.db ที่ห้ามขึ้น GitHub)

sync ครั้งเดียวต่อบริษัทแล้วอยู่ได้นานมาก (คำอธิบายธุรกิจเปลี่ยนไม่บ่อย) ต่างจาก
ราคา/งบการเงินที่ต้องรีเฟรชถี่ — ดีฟอลต์ max_age_days=180
"""
import json
import os
import time
from datetime import datetime, timezone

_JSON_FILE = "dr_descriptions.json"


def _path(base_dir):
    return os.path.join(base_dir, _JSON_FILE)


def load_all(base_dir):
    p = _path(base_dir)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_all(base_dir, data):
    p = _path(base_dir)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, p)


def get(base_dir, sym):
    return load_all(base_dir).get(sym)


def _translate_th(text):
    """แปล EN -> TH ด้วย Google Translate (ฟรี ไม่ต้องใช้ API key) — ตัดที่ 4500
    ตัวอักษร กันเกิน limit ต่อ request ของ endpoint ฟรี (~5000 ตัวอักษร)"""
    from deep_translator import GoogleTranslator
    chunk = text[:4500]
    return GoogleTranslator(source="en", target="th").translate(chunk)


def resolve_yf_ticker(base_dir, sym, market=None):
    """หา yfinance ticker ของ symbol — เช็ค DR universe (มี field 'yf' ตรงๆ) ก่อน
    ถ้าไม่เจอ (หุ้น mirror US/HK ที่ไม่ได้อยู่ใน DR universe ที่คนคีวรอบคอบไว้)
    เดาจาก market ที่ frontend ส่งมา (อิง currency ของงบที่โหลดอยู่แล้ว):
      US -> symbol ตรงๆ (mirror US ใช้ ticker เดียวกับ Yahoo อยู่แล้ว)
      HK -> zero-pad เป็น 4 หลัก + '.HK' (ธรรมเนียม Yahoo สำหรับหุ้น HK)
    คืน None ถ้าหาไม่ได้เลย"""
    from sources.dr_universe import load_dr_universe
    sym = sym.upper().strip()
    entry = next((s for s in load_dr_universe(base_dir) if s["sym"] == sym), None)
    if entry:
        return entry["yf"], entry.get("etf", False)
    if market == "US":
        return sym, False
    if market == "HK":
        return sym.zfill(4) + ".HK", False
    return None, False


def fetch_one(base_dir, sym, market=None, force=False, max_age_days=180):
    """ดึง+แปล description ของหุ้นตัวเดียว แบบ on-demand (ไม่ผ่าน DR universe loop) —
    ใช้กับหน้าที่เปิดดูทีละตัว (chart modal / หน้างบการเงิน) ครอบคลุมทั้งหุ้น DR ที่
    curate ไว้ และหุ้น mirror US/HK ทั่วไปที่ไม่ได้อยู่ใน DR universe

    คืน (dict หรือ None, error_message หรือ None)"""
    import yfinance as yf

    sym = sym.upper().strip()
    store = load_all(base_dir)
    now = time.time()
    max_age = max_age_days * 86400

    # หา yf ticker เสมอไม่ว่า cache hit/miss — ใช้ต่อฝั่ง frontend สำหรับลิงก์ TradingView
    # (เช่น MICRON -> MU ที่เดาจาก currency อย่างเดียวไม่ได้ ต้องพึ่ง DR universe)
    yf_ticker, is_etf = resolve_yf_ticker(base_dir, sym, market=market)

    cached = store.get(sym)
    if (not force and cached and cached.get("th")
            and cached.get("fetched_ts") and (now - cached["fetched_ts"]) < max_age):
        return {**cached, "yf": yf_ticker}, None

    if not yf_ticker:
        return None, "ไม่ทราบตลาดของหุ้นนี้ (ระบุ market=US หรือ HK)"
    if is_etf:
        return None, "เป็น ETF/กองทุน — ไม่มี business description"

    try:
        info = yf.Ticker(yf_ticker).info
        en = (info.get("longBusinessSummary") or "").strip()
        if not en:
            return None, f"Yahoo ({yf_ticker}) ไม่มี business summary ให้"
        th = _translate_th(en)
        record = {
            "en": en,
            "th": th,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "website": info.get("website"),
            "fetched_ts": now,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        store[sym] = record
        _save_all(base_dir, store)
        return {**record, "yf": yf_ticker}, None
    except Exception as e:
        return None, str(e)


def sync_all(base_dir, symbols=None, force=False, max_age_days=180, callback=None):
    """ดึง longBusinessSummary จาก yfinance + แปลไทย สำหรับหุ้น DR ทั้งหมด (ข้าม ETF —
    ไม่มี business description) เซฟทีละตัวกันดับกลางทางแล้วเสียของเดิม (resume ได้)

    force=False: ข้ามตัวที่มีอยู่แล้วและอายุไม่เกิน max_age_days
    """
    import yfinance as yf
    from sources.dr_universe import load_dr_universe

    universe = load_dr_universe(base_dir)
    if symbols:
        wanted = set(symbols)
        universe = [s for s in universe if s["sym"] in wanted]
    universe = [s for s in universe if not s.get("etf")]

    store = load_all(base_dir)
    now = time.time()
    max_age = max_age_days * 86400

    todo = []
    for s in universe:
        cached = store.get(s["sym"])
        if (not force and cached and cached.get("th")
                and cached.get("fetched_ts") and (now - cached["fetched_ts"]) < max_age):
            continue
        todo.append(s)

    skipped = len(universe) - len(todo)
    total = len(todo)
    ok = fail = 0

    for i, s in enumerate(todo):
        if callback:
            callback(i, total, f"กำลังดึงคำอธิบายบริษัท {s['sym']} ({i + 1}/{total})")
        try:
            info = yf.Ticker(s["yf"]).info
            en = (info.get("longBusinessSummary") or "").strip()
            if not en:
                fail += 1
                continue
            th = _translate_th(en)
            store[s["sym"]] = {
                "en": en,
                "th": th,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "website": info.get("website"),
                "fetched_ts": now,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            ok += 1
            _save_all(base_dir, store)
        except Exception as e:
            print(f"[dr-desc] {s['sym']}: {e}")
            fail += 1

    if callback:
        callback(total, total, "เสร็จแล้ว")
    return {"ok": ok, "fail": fail, "skipped": skipped, "total": len(universe)}
