"""
run_static_update.py — รันโดย GitHub Actions เพื่ออัปเดต JSON files
สร้าง: set_data.json, set_dr_data.json, set_indices.json
"""
import os, sys, json, time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 1. Full Refresh SET stocks ────────────────────────────────
log("=== เริ่ม Full Refresh SET stocks ===")
from set_data_fetcher import run_with_progress

def cb(current, total, msg):
    if total > 0:
        pct = round(current / total * 100)
        if pct % 10 == 0 or "บันทึก" in msg or "เสร็จ" in msg:
            log(f"  [{pct:3d}%] {msg}")
    else:
        log(f"  {msg}")

try:
    run_with_progress(cb, base_dir=BASE_DIR, period="max")
    log("✅ SET stocks เสร็จ")
except Exception as e:
    log(f"❌ SET stocks ผิดพลาด: {e}")
    sys.exit(1)

# ── 2. DR data ────────────────────────────────────────────────
log("=== เริ่ม DR data fetch ===")
try:
    from app import _fetch_dr_full, _DR_STATIC, DR_CACHE_FILE
    dr_data = _fetch_dr_full(_DR_STATIC)
    with open(os.path.join(BASE_DIR, "set_dr_data.json"), "w", encoding="utf-8") as f:
        json.dump(dr_data, f, ensure_ascii=False, indent=2)
    log(f"✅ DR data เสร็จ ({len(dr_data.get('stocks', []))} stocks)")
except Exception as e:
    log(f"⚠️ DR data ผิดพลาด (ข้ามไป): {e}")

# ── 3. Indices data (yfinance) ────────────────────────────────
log("=== เริ่ม Indices data fetch (yfinance) ===")
try:
    import yfinance as yf
    import pandas as pd
    from app import INDEX_INFO

    all_syms = list(INDEX_INFO.keys())
    updated  = datetime.now().strftime("%Y-%m-%d %H:%M")

    raw = yf.download(all_syms, period="max", auto_adjust=True,
                      progress=False, group_by="ticker", threads=True)
    is_multi = len(all_syms) > 1

    def _get_close(sym):
        try:
            s = raw[sym]["Close"] if is_multi else raw["Close"]
            return s.dropna()
        except Exception:
            return pd.Series(dtype=float)

    def _ret(v, n):
        return round((v[-1] - v[-(n+1)]) / v[-(n+1)] * 100, 2) if len(v) > n else None

    idx_data = {}
    for sym in all_syms:
        info = INDEX_INFO[sym]
        try:
            close = _get_close(sym)
            if len(close) < 2:
                log(f"  ⚠ {sym}: ข้อมูลน้อยเกินไป")
                continue
            v = close.tolist()
            idx_data[sym] = {
                "sym": sym, "name": info["name"], "group": info["group"],
                "last": round(v[-1], 2),
                "ret_1d": _ret(v, 1), "ret_1w": _ret(v, 5),
                "ret_1m": _ret(v, 21), "ret_3m": _ret(v, 63),
                "ret_6m": _ret(v, 126), "ret_1y": _ret(v, 250),
                "closes": [round(x, 2) for x in v],
                "dates":  [str(d)[:10] for d in close.index.tolist()],
                "updated_at": updated,
            }
        except Exception as e:
            log(f"  ⚠ {sym}: {e}")

    out = {"updated_at": updated, "data": idx_data}
    with open(os.path.join(BASE_DIR, "set_indices.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"✅ Indices เสร็จ ({len(idx_data)} ดัชนี)")
except Exception as e:
    log(f"⚠️ Indices ผิดพลาด (ข้ามไป): {e}")

log("=== ทุกอย่างเสร็จ ===")
