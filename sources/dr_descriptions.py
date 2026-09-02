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


def _find_dr_entry(base_dir, sym):
    from sources.dr_universe import load_dr_universe
    return next((s for s in load_dr_universe(base_dir) if s["sym"] == sym), None)


def resolve_yf_ticker(base_dir, sym, market=None):
    """หา yfinance ticker ของ symbol
      TH/SET -> symbol + '.BK' ตรงๆ เสมอ (หุ้นไทยไม่มีทางชนกับ DR universe — เช็คก่อนเลย
                กัน edge case ที่ symbol ไทยบังเอิญพ้องกับ sym ใน DR universe)
      อื่นๆ  -> เช็ค DR universe (มี field 'yf' ตรงๆ) ก่อน ถ้าไม่เจอ (หุ้น mirror US/HK
                ที่ไม่ได้อยู่ใน DR universe ที่คนคีวรอบคอบไว้) เดาจาก market ที่ frontend ส่งมา
                (อิง currency ของงบที่โหลดอยู่แล้ว):
      US -> symbol ตรงๆ (mirror US ใช้ ticker เดียวกับ Yahoo อยู่แล้ว)
      HK -> zero-pad เป็น 4 หลัก + '.HK' (ธรรมเนียม Yahoo สำหรับหุ้น HK)
      JP -> symbol + '.T' (ธรรมเนียม Yahoo สำหรับหุ้น JP/TSE)
    คืน None ถ้าหาไม่ได้เลย"""
    sym = sym.upper().strip()
    if market in ("TH", "SET"):
        return sym + ".BK", False
    entry = _find_dr_entry(base_dir, sym)
    if entry:
        return entry["yf"], entry.get("etf", False)
    if market == "US":
        return sym, False
    if market == "HK":
        # sym อาจมาพร้อม suffix ".HK" อยู่แล้ว (เช่น h.symbol ของ Tearsheet = "0700.HK" จาก
        # hk_index_metrics.json) — ตัดออกก่อน zfill แล้วต่อกลับ กัน ".HK.HK" ซ้อนสอง (ปฏิทิน
        # earnings ของหุ้น HK/JP เลยว่างถาวรเพราะ yfinance หา ticker พังนี้ไม่เจอ)
        base = sym[:-3] if sym.endswith(".HK") else sym
        return base.zfill(4) + ".HK", False
    if market == "JP":
        return (sym if sym.endswith(".T") else sym + ".T"), False
    return None, False


def _store_key(sym, market=None):
    """คีย์ใน store — ปกติใช้ sym ตัวเปล่า ยกเว้นรหัสหุ้น "ตัวเลขล้วน" ของตลาด HK/JP ที่ชน
    กันข้ามตลาดได้ (เช่น 1801 = Taisei ที่ JP และ Innovent Biologics ที่ HK — พิสูจน์แล้วว่า
    ชนจริง ทำให้คำอธิบายของอีกตลาดถูกเขียนทับ/ข้ามไปเงียบๆ แล้วแต่ลำดับที่กด sync/เปิดดู)
    ใส่ prefix ตลาดกันชนเฉพาะเคสนี้ — เช็ครูปแบบ sym (ตัวเลขล้วน) แทนการเช็ค DR universe
    ตรงๆ เพราะหุ้น DR ที่ curate ไว้ใช้ ticker ตัวอักษรเสมอ (ไม่มีทางเป็นตัวเลขล้วน) การเช็ค
    แบบนี้จึงให้ผลเหมือนกันทุกกรณี แต่ไม่ต้องอ่าน DR universe ซ้ำ และให้ frontend คำนวณ
    คีย์เดียวกันได้เองโดยไม่ต้องรู้จัก DR universe เลย (ดู _drDescKey ใน dashboard.js —
    ใช้ตอนหน้างบการเงินเต็มเปิดหุ้น DR ที่เทรดเป็น HKD/JPY ด้วย ซึ่ง sym เป็น ticker ตัวอักษร
    จึงไม่ถูก prefix เหมือนกับฝั่ง backend)"""
    sym = sym.upper().strip()
    if market in ("HK", "JP") and sym.isdigit():
        return f"{market}:{sym}"
    return sym


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
    key = _store_key(sym, market=market)

    cached = store.get(key)
    if (not force and cached and cached.get("th")
            and cached.get("fetched_ts") and (now - cached["fetched_ts"]) < max_age):
        return {**cached, "yf": yf_ticker}, None

    if not yf_ticker:
        return None, "ไม่ทราบตลาดของหุ้นนี้ (ระบุ market=US, HK หรือ JP)"
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
        store[key] = record
        _save_all(base_dir, store)
        return {**record, "yf": yf_ticker}, None
    except Exception as e:
        return None, str(e)


def _run_sync(base_dir, targets, force, max_age_days, callback):
    """ลูปดึง+แปล description ของ targets ([{"sym":..., "yf":...}]) ใช้ร่วมกันระหว่าง
    sync_all (DR universe) และ sync_index_universe (สมาชิกดัชนีหลัก US/HK/JP) — เซฟทีละตัว
    กันดับกลางทางแล้วเสียของเดิม (resume ได้)"""
    import yfinance as yf

    store = load_all(base_dir)
    now = time.time()
    max_age = max_age_days * 86400

    todo = []
    for t in targets:
        cached = store.get(t["sym"])
        if (not force and cached and cached.get("th")
                and cached.get("fetched_ts") and (now - cached["fetched_ts"]) < max_age):
            continue
        todo.append(t)

    skipped = len(targets) - len(todo)
    total = len(todo)
    ok = fail = 0

    for i, t in enumerate(todo):
        if callback:
            callback(i, total, f"กำลังดึงคำอธิบายบริษัท {t['sym']} ({i + 1}/{total})")
        try:
            info = yf.Ticker(t["yf"]).info
            en = (info.get("longBusinessSummary") or "").strip()
            if not en:
                fail += 1
                continue
            th = _translate_th(en)
            store[t["sym"]] = {
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
            print(f"[dr-desc] {t['sym']}: {e}")
            fail += 1

    if callback:
        callback(total, total, "เสร็จแล้ว")
    return {"ok": ok, "fail": fail, "skipped": skipped, "total": len(targets)}


def sync_all(base_dir, symbols=None, force=False, max_age_days=180, callback=None):
    """ดึง longBusinessSummary จาก yfinance + แปลไทย สำหรับหุ้น DR ทั้งหมด (ข้าม ETF —
    ไม่มี business description)

    force=False: ข้ามตัวที่มีอยู่แล้วและอายุไม่เกิน max_age_days
    """
    from sources.dr_universe import load_dr_universe

    universe = load_dr_universe(base_dir)
    if symbols:
        wanted = set(symbols)
        universe = [s for s in universe if s["sym"] in wanted]
    universe = [s for s in universe if not s.get("etf")]
    targets = [{"sym": s["sym"], "yf": s["yf"]} for s in universe]
    return _run_sync(base_dir, targets, force, max_age_days, callback)


def _index_universe_targets(base_dir):
    """รวม constituents ของดัชนีหลัก US (S&P500/Dow/Nasdaq100) + HK (HSI/HSCEI/HSTECH) +
    JP (Nikkei 225) เป็น [{"sym":..., "yf":...}] — ต่างจาก DR universe (318 ตัว curate มือ)
    ครอบคลุมหุ้น mirror ทั่วไปที่คนเปิดดูบ่อยแต่ไม่ได้อยู่ใน DR universe เลย
    "sym" ใช้เป็น store key ตรงๆ (ผ่าน _store_key: HK/JP ใส่ prefix ตลาดกันรหัสตัวเลข
    4 หลักชนกัน เช่น 1801 = Taisei(JP)/Innovent(HK) — ดู _drDescKey ฝั่ง dashboard.js
    ที่ต้องคำนวณคีย์แบบเดียวกันตอน lookup)"""
    from sources import us_index_membership as usm
    from sources import hk_index_membership as hkm
    from sources import jp_index_membership as jpm

    out = {}
    for t in usm.all_tickers(base_dir):
        out[t] = {"sym": t, "yf": t}
    for t in hkm.all_tickers(base_dir):
        sym = t[:-3] if t.endswith(".HK") else t
        key = _store_key(sym, market="HK")
        out[key] = {"sym": key, "yf": t}
    for t in jpm.all_tickers(base_dir):
        sym = t[:-2] if t.endswith(".T") else t
        key = _store_key(sym, market="JP")
        out[key] = {"sym": key, "yf": t}
    return list(out.values())


def sync_index_universe(base_dir, force=False, max_age_days=180, callback=None):
    """ดึง+แปล description ของหุ้นสมาชิกดัชนีหลัก US/HK/JP ทั้งชุด (S&P500/Dow/Nasdaq100/
    HSI/HSCEI/HSTECH/Nikkei225) — เสริม sync_all ที่ครอบคลุมแค่ DR universe ที่ curate ไว้"""
    targets = _index_universe_targets(base_dir)
    return _run_sync(base_dir, targets, force, max_age_days, callback)


def index_universe_coverage(base_dir):
    """คืน (covered, total) ของหุ้นสมาชิกดัชนีหลัก US/HK/JP ที่มีคำแปลไทยอยู่ในแคชแล้ว —
    ใช้แสดงในหน้า Data Health (ดู app.py data_health())"""
    targets = _index_universe_targets(base_dir)
    store = load_all(base_dir)
    covered = sum(1 for t in targets if (store.get(t["sym"]) or {}).get("th"))
    return covered, len(targets)
