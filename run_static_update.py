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

# ── 3. Indices data ───────────────────────────────────────────
log("=== เริ่ม Indices data fetch ===")
try:
    from app import _fetch_indices_tv, _load_indices_existing
    existing = _load_indices_existing()
    idx_data = _fetch_indices_tv(existing, full_refresh=True)
    out = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": idx_data}
    with open(os.path.join(BASE_DIR, "set_indices.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"✅ Indices เสร็จ ({len(idx_data)} ดัชนี)")
except Exception as e:
    log(f"⚠️ Indices ผิดพลาด (ข้ามไป): {e}")

log("=== ทุกอย่างเสร็จ ===")
