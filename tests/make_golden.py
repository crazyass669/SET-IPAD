# -*- coding: utf-8 -*-
"""
สร้าง fixture + golden file สำหรับ regression test ของ calculation pipeline

รันครั้งเดียว (หรือเมื่อ *ตั้งใจ* เปลี่ยนสูตร):
    python tests/make_golden.py

จะสร้าง:
    tests/fixtures/history_sample.json  — ราคาย้อนหลังของหุ้นตัวอย่าง ~35 ตัว (คงที่ตลอดไป)
    tests/fixtures/golden.json          — ผลลัพธ์ pipeline ที่ถูกต้อง ณ วันสร้าง

จากนั้น tests/test_golden.py จะรัน pipeline เดิมบน fixture แล้ว diff กับ golden
ทุกครั้งหลัง refactor — ต้องได้ศูนย์ diff
"""
import json
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE_DIR = os.path.join(BASE, "tests", "fixtures")
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

# หุ้นตัวอย่าง — จงใจเลือกครบทุก edge case ของ validation layer
SAMPLE_SYMBOLS = [
    # ปกติ ขนาดใหญ่ หลาก sector
    "PTT", "AOT", "CPALL", "DELTA", "KBANK", "ADVANC", "SCB", "BDMS",
    "CPF", "TRUE", "GULF", "SCC", "MINT", "BEM", "CRC",
    # RS สูง / RS ต่ำ
    "SMT", "TRT", "ML", "SANKO", "MILL", "EMPIRE", "DPAINT",
    # stale (พักเทรด) / thin (เทรดเบาบาง)
    "ACAP", "F&D", "S&J",
    # penny (< 0.10 บาท)
    "BLISS", "B", "CHO",
    # short_hist (IPO ใหม่ ไม่มี ret_1y)
    "MASTEC", "NTF",
    # REIT / property fund
    "MJLF", "CPNREIT",
    # mai
    "QTCG", "SAUCE", "BTNC",
]


def build_fixture():
    from set_data_fetcher import load_history
    hist = load_history(BASE)
    if not hist:
        raise SystemExit("ไม่พบ set_history.json — ต้องมีข้อมูลจริงก่อนสร้าง fixture")

    data = json.load(open(os.path.join(BASE, "set_data.json"), encoding="utf-8"))
    info_map = {s["symbol"]: {k: s[k] for k in
                              ("symbol", "ticker", "name", "market", "industry", "sector")}
                for s in data["stocks"]}

    fixture = {"stocks": {}}
    missing = []
    for sym in SAMPLE_SYMBOLS:
        info = info_map.get(sym)
        h = hist["stocks"].get(info["ticker"]) if info else None
        if not info or not h:
            missing.append(sym)
            continue
        # ตัดเหลือ 600 แท่งท้าย พอสำหรับทุก metric (ต้องการสูงสุด 500)
        fixture["stocks"][sym] = {
            "info":    info,
            "dates":   h["dates"][-600:],
            "closes":  h["closes"][-600:],
            "volumes": h["volumes"][-600:],
        }
    if missing:
        print(f"คำเตือน: ไม่พบข้อมูล {missing} — ข้าม")
    return fixture


def run_pipeline(fixture):
    """รัน pipeline เดียวกับ production: process_stock -> validate -> rank -> summarize"""
    import pandas as pd
    from set_data_fetcher import (process_stock, validate_stocks, rank_rs,
                                  summarize_groups)

    stocks = []
    for sym, d in fixture["stocks"].items():
        dates = pd.to_datetime(d["dates"])
        close = pd.Series(d["closes"], index=dates, dtype=float)
        vol   = pd.Series(d["volumes"], index=dates, dtype=float)
        r = process_stock(d["info"], close, vol)
        if r:
            stocks.append(r)

    data_as_of = max(d["dates"][-1] for d in fixture["stocks"].values())
    dq_summary = validate_stocks(stocks, data_as_of)
    stocks     = rank_rs(stocks)
    sectors    = summarize_groups(stocks, "sector")
    industries = summarize_groups(stocks, "industry")

    # ตัด field ที่ไม่เอาเข้า golden:
    #  - price_history/vol_history: ใหญ่และเป็น pass-through ไม่ใช่สูตร
    #  - ret_ytd: ขึ้นกับ datetime.now().year — ไม่ deterministic ข้ามปี
    SKIP = {"price_history", "vol_history", "ret_ytd"}
    slim = [{k: v for k, v in s.items() if k not in SKIP} for s in stocks]
    slim.sort(key=lambda s: s["symbol"])

    return {
        "data_as_of": data_as_of,
        "dq_summary": dq_summary,
        "stocks":     slim,
        "sectors":    sectors,
        "industries": industries,
    }


if __name__ == "__main__":
    os.makedirs(FIXTURE_DIR, exist_ok=True)
    fx_path = os.path.join(FIXTURE_DIR, "history_sample.json")

    if os.path.exists(fx_path):
        print("ใช้ fixture เดิม (ห้าม regenerate เพื่อให้ golden เทียบได้ข้ามเวลา)")
        fixture = json.load(open(fx_path, encoding="utf-8"))
    else:
        fixture = build_fixture()
        with open(fx_path, "w", encoding="utf-8") as f:
            json.dump(fixture, f, ensure_ascii=False)
        print(f"เขียน {fx_path} ({len(fixture['stocks'])} หุ้น)")

    golden = run_pipeline(fixture)
    gd_path = os.path.join(FIXTURE_DIR, "golden.json")
    with open(gd_path, "w", encoding="utf-8") as f:
        json.dump(golden, f, ensure_ascii=False, indent=1, sort_keys=True)
    print(f"เขียน {gd_path} (stocks={len(golden['stocks'])}, "
          f"sectors={len(golden['sectors'])}, rs_universe={golden['dq_summary']['rs_universe']})")
