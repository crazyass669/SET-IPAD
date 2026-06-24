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

# ── 3. Indices data (TradingView WebSocket) ───────────────────
log("=== เริ่ม Indices data fetch (TradingView) ===")
try:
    import random, string, threading, json as _json
    import websocket as _ws
    from app import INDEX_INFO

    _YF_TV_OVERRIDES = {"^PFREIT.BK": "SET:PF_REIT"}

    def _yf_to_tv(yf_sym):
        if yf_sym in _YF_TV_OVERRIDES:
            return _YF_TV_OVERRIDES[yf_sym]
        s = yf_sym.lstrip("^").replace(".BK", "").replace("-", "_")
        return f"SET:{s}"

    def _fetch_tv_bars(tv_symbol, n_bars=5000, timeout=30):
        bars = []
        done = threading.Event()

        def _send(ws, func, args):
            msg = _json.dumps({"m": func, "p": args})
            ws.send(f"~m~{len(msg)}~m~{msg}")

        def on_msg(ws, message):
            for part in message.split("~m~"):
                if part.isdigit():
                    continue
                try:
                    d = _json.loads(part)
                    if d.get("m") == "timescale_update":
                        for k, v in d["p"][1].items():
                            if k.startswith("sds_"):
                                bars.extend(v.get("s", []))
                        done.set()
                except Exception:
                    pass

        def on_open(ws):
            cs = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
            _send(ws, "set_auth_token", ["unauthorized_user_token"])
            _send(ws, "chart_create_session", [cs, ""])
            _send(ws, "resolve_symbol", [cs, "sds_sym_1",
                  f'={{"symbol":"{tv_symbol}","adjustment":"splits"}}'])
            _send(ws, "create_series", [cs, "sds_1", "s1", "sds_sym_1", "D", n_bars])

        wsapp = _ws.WebSocketApp(
            "wss://data.tradingview.com/socket.io/websocket",
            header={"Origin": "https://www.tradingview.com"},
            on_message=on_msg, on_open=on_open,
        )
        t = threading.Thread(target=wsapp.run_forever)
        t.daemon = True
        t.start()
        done.wait(timeout=timeout)
        wsapp.close()

        from datetime import datetime as _dt, timezone as _tz
        result = []
        for bar in bars:
            v = bar.get("v", [])
            if len(v) >= 5:
                dt = _dt.fromtimestamp(v[0], tz=_tz.utc).strftime("%Y-%m-%d")
                result.append([dt, round(float(v[4]), 2)])
        return result

    updated = datetime.now().strftime("%Y-%m-%d %H:%M")
    idx_data = {}

    for sym, info in INDEX_INFO.items():
        tv_sym = _yf_to_tv(sym)
        try:
            pairs = _fetch_tv_bars(tv_sym, n_bars=5000, timeout=30)
            if not pairs:
                log(f"  ⚠ {tv_sym}: ไม่ได้ข้อมูล")
                continue
            dates = [p[0] for p in pairs]
            vals  = [p[1] for p in pairs]

            def _ret(n, _v=vals):
                return round((_v[-1] - _v[-(n+1)]) / _v[-(n+1)] * 100, 2) if len(_v) > n else None

            idx_data[sym] = {
                "sym": sym, "name": info["name"], "group": info["group"],
                "last": vals[-1],
                "ret_1d": _ret(1), "ret_1w": _ret(5),
                "ret_1m": _ret(21), "ret_3m": _ret(63),
                "ret_6m": _ret(126), "ret_1y": _ret(250),
                "closes": vals, "dates": dates, "updated_at": updated,
            }
            log(f"  ✓ {tv_sym} {len(vals)} bars → {dates[-1]}")
        except Exception as e:
            log(f"  ⚠ {tv_sym}: {e}")
        time.sleep(0.3)

    out = {"updated_at": updated, "data": idx_data}
    with open(os.path.join(BASE_DIR, "set_indices.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"✅ Indices เสร็จ ({len(idx_data)} ดัชนี)")
except Exception as e:
    log(f"⚠️ Indices ผิดพลาด (ข้ามไป): {e}")

log("=== ทุกอย่างเสร็จ ===")
