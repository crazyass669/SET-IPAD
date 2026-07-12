# -*- coding: utf-8 -*-
"""อัพเดทงบการเงินทั้งหมดในคำสั่งเดียว — sync 3 แหล่ง (Yahoo + SET + Finnomena)
ของหุ้นไทย + DR แล้ว build factor snapshot ต่อให้เลย (ไม่ต้องกดหลายที่ในแอป)

ใช้:
    python update_financials.py            อัพเดทหุ้นไทย + DR แล้ว build snapshot
    python update_financials.py th         เฉพาะหุ้นไทย (Yahoo+SET+Finnomena รายไตรมาส)
    python update_financials.py dr          เฉพาะ DR (Yahoo+Finnomena — ไม่มี SET)
    python update_financials.py all mirror  อัพเดททั้งหมด + rebuild mirror US/HK ด้วย
                                            (mirror ช้ากว่า — ปกติไม่ต้อง)

หมายเหตุ:
- ดึงสด + merge งวดใหม่เข้าของเดิม (ประวัติเก่าไม่หาย) เหมือนปุ่มในแอป
- เป็น local-only — financials.db ไม่ขึ้น GitHub
- ควรรันหลังบริษัทประกาศงบไตรมาส (ก.พ./พ.ค./ส.ค./พ.ย.)
- ไม่รีเฟรชหุ้น US/HK 'นอกพอร์ต' (ใช้ mirror_finnomena.py แยก)
"""
import io
import os
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import store as price_store            # noqa: E402
from sources import financials_store as fs       # noqa: E402
from sources import factor_snapshot as snap       # noqa: E402
from sources.dr_universe import load_dr_universe, sync_dr_universe  # noqa: E402

args = [a.lower() for a in sys.argv[1:]]
scope = "all"
for a in ("th", "dr", "all"):
    if a in args:
        scope = a
with_mirror = "mirror" in args


def _th_universe():
    tickers = sorted(price_store.get_last_dates(BASE).keys())
    return [t[:-3] if t.endswith(".BK") else t for t in tickers]


def _dr_universe():
    return sorted(s["sym"] for s in load_dr_universe(BASE) if not s.get("etf"))


def _progress(tag):
    last = [0]
    def cb(done, total, msg):
        # พิมพ์ทุก ~5% หรือทุก 25 ตัว กันล้นจอ
        if done - last[0] >= max(25, total // 20) or done == total:
            last[0] = done
            print(f"  [{tag}] {done}/{total}", flush=True)
    return cb


t0 = time.time()

# เช็ค DR ใหม่จาก SET ก่อน — DR/underlying ที่เพิ่งเข้าจะถูกเพิ่มเข้า universe อัตโนมัติ
# (derive yf ticker + ตรวจ Yahoo ให้เอง) แล้วจึงถูกดึงงบในขั้นถัดไป
if scope in ("all", "dr"):
    try:
        st = sync_dr_universe(BASE)
        if st.get("added") or st.get("appended"):
            print(f"[DR-sync] เจอของใหม่: underlying ใหม่ {st.get('added', 0)} · "
                  f"DR series ใหม่ {st.get('appended', 0)} · ยัง map ไม่ได้ (ต้อง curate มือ) {st.get('unmapped', 0)}", flush=True)
        elif st.get("unmapped"):
            print(f"[DR-sync] ไม่มี underlying ใหม่ที่ auto ได้ · ยัง map ไม่ได้ {st['unmapped']} ตัว (ดูหน้า DR)", flush=True)
    except Exception as e:
        print(f"[DR-sync] ข้าม (sync ล้มเหลว ใช้ universe เดิม): {e}", flush=True)

if scope in ("all", "th"):
    syms = _th_universe()
    print(f"[งบไทย] sync {len(syms)} หุ้น · แหล่ง Yahoo + SET + Yahoo-Q + Finnomena-Q ...", flush=True)
    r = fs.sync_all(BASE, syms, sources=("yahoo", "set", "yahoo_q", "finnomena_q"),
                    callback=_progress("ไทย"), is_dr=False)
    print(f"[งบไทย] เสร็จ: สำเร็จ {r['ok']}/{r['total']} (พลาด {r['fail']})", flush=True)

if scope in ("all", "dr"):
    syms = _dr_universe()
    print(f"[งบ DR] sync {len(syms)} หุ้น · แหล่ง Yahoo + Yahoo-Q + Finnomena-Q ...", flush=True)
    r = fs.sync_all(BASE, syms, sources=("yahoo", "yahoo_q", "finnomena_q"),
                    callback=_progress("DR"), is_dr=True)
    print(f"[งบ DR] เสร็จ: สำเร็จ {r['ok']}/{r['total']} (พลาด {r['fail']})", flush=True)

# หุ้น US/HK 'นอกพอร์ต' ที่ค้นบ่อย (จาก search_log) — refresh งวดใหม่รายตัว
refreshed_mirror = False
if scope in ("all", "dr"):
    port = set(_dr_universe())
    searched = [s for s in fs.get_recent_searches(BASE, days=90) if s not in port]
    if searched:
        print(f"[หุ้นค้นบ่อย] refresh งบ mirror US/HK {len(searched)} ตัวที่ค้นใน 90 วัน ...", flush=True)
        ok = 0
        for i, s in enumerate(searched):
            try:
                if fs.refresh_mirror_stock(BASE, s):
                    ok += 1
            except Exception:
                pass
            time.sleep(0.3)   # throttle กัน Finnomena บล็อก
        print(f"[หุ้นค้นบ่อย] อัพเดทได้ {ok}/{len(searched)} ตัว", flush=True)
        refreshed_mirror = ok > 0

print("[Snapshot] กำลัง build factor snapshot ...", flush=True)
res = snap.build_snapshot(BASE)
print(f"[Snapshot] หลัก: ไทย {res['set']} + DR {res['dr']} = {res['total']} แถว", flush=True)
# rebuild mirror snapshot ถ้าสั่ง mirror หรือมีการ refresh หุ้นค้นบ่อย (ให้ Screener สดตาม)
if with_mirror or refreshed_mirror:
    print("[Snapshot] กำลัง rebuild mirror snapshot ...", flush=True)
    m = snap.build_mirror_snapshot(BASE)
    print(f"[Snapshot] mirror: US {m.get('US', 0)} + HK {m.get('HK', 0)}", flush=True)

print(f"\n✅ เสร็จทั้งหมดใน {(time.time() - t0) / 60:.0f} นาที — รีเฟรชหน้าเว็บได้เลย (ไม่ต้อง restart)", flush=True)
