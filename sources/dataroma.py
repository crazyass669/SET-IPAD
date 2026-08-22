# -*- coding: utf-8 -*-
"""sources/dataroma.py — ขูดการถือครองของ "superinvestors"/hedge funds จาก Dataroma
(dataroma.com — สรุปแบบยื่น 13F ต่อ SEC รายไตรมาส ดีเลย์ ~45 วัน) เก็บลง cache local
data/hedge_holdings.json แล้วให้หน้า "Hedge Holdings" คำนวณหุ้นที่ถือซ้ำกัน (overlap/
consensus) เองฝั่ง client — pattern เดียวกับ us_index_membership: ขูด HTML ดิบ ไม่ผ่าน
mirror บุคคลที่สาม, ไม่พึ่ง lib ภายนอก (urllib + regex)

โครงตาราง Dataroma:
  managers.php          → รายชื่อกอง (code m=XXX, ชื่อ, มูลค่าพอร์ต, จำนวนหุ้น)
  holdings.php?m=XXX    → holdings ของกองนั้น + %พอร์ต + recent activity + มูลค่า
"""
import json
import os
import re
import time
import urllib.request

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
_BASE = "https://www.dataroma.com/m/"

CACHE_REL = os.path.join("data", "hedge_holdings.json")


def _ctx():
    """verify cert จริงผ่าน core.net (certifi) — เดิมปิด verify ทิ้ง (CERT_NONE) ซึ่งเปิดช่อง
    MITM โดยไม่จำเป็น cert ของ dataroma.com ใช้งานได้ปกติ (รีวิวความปลอดภัย 2026-08-02)"""
    from core.net import ssl_context
    return ssl_context()


def _get(url, tries=3):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": _UA})
            with urllib.request.urlopen(req, context=_ctx(), timeout=30) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def _clean(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def _num(s):
    """'$2,415,946,000' / '17.62' / '59,697,208' → float (None ถ้าว่าง)"""
    if not s:
        return None
    s = s.replace("$", "").replace(",", "").replace("%", "").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def _dollars(s):
    """'$2.31 B' / '$52 M' → ตัวเลข USD เต็ม"""
    if not s:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    mult = 1.0
    if s.endswith("B") or " B" in s:
        mult, s = 1e9, s.replace("B", "")
    elif s.endswith("M") or " M" in s:
        mult, s = 1e6, s.replace("M", "")
    elif s.endswith("K") or " K" in s:
        mult, s = 1e3, s.replace("K", "")
    v = _num(s)
    return v * mult if v is not None else None


# ---------------------------------------------------------------- managers list
def fetch_managers():
    """คืน list ของ {code, name, value, num_stocks} ทุกกองใน managers.php"""
    html = _get(_BASE + "managers.php")
    i = html.find('id="grid"')
    body = html[i:] if i >= 0 else html
    out = []
    for row in re.findall(r"<tr>(.*?)</tr>", body, re.DOTALL):
        m = re.search(r'holdings\.php\?m=([A-Za-z0-9]+)"\s*>([^<]+)</a>', row)
        if not m:
            continue
        val = re.search(r'class="val">([^<]+)</td>', row)
        cnt = re.search(r'class="cnt">([^<]+)</td>', row)
        out.append({
            "code": m.group(1),
            "name": _clean(m.group(2)),
            "value": _dollars(val.group(1)) if val else None,
            "num_stocks": int(_num(cnt.group(1))) if cnt else None,
        })
    return out


# ------------------------------------------------------------- one manager page
def fetch_holdings(code):
    """คืน dict ของกองเดียว: meta (period/date/value/num_stocks) + holdings[]"""
    html = _get(_BASE + f"holdings.php?m={code}")

    def meta(label):
        m = re.search(label + r":\s*<span>([^<]+)</span>", html)
        return _clean(m.group(1)) if m else None

    name_m = re.search(r'id="f_name">([^<]+)</div>', html)
    info = {
        "code": code,
        "name": _clean(name_m.group(1)) if name_m else code,
        "period": meta("Period"),
        "portfolio_date": meta("Portfolio date"),
        "num_stocks": int(_num(meta("No\\. of stocks")) or 0) or None,
        "value": _dollars(meta("Portfolio value")),
        "holdings": [],
    }

    i = html.find('id="grid"')
    body = html[i:] if i >= 0 else html
    for row in re.findall(r"<tr>(.*?)</tr>", body, re.DOTALL):
        sm = re.search(r'stock\.php\?sym=([^"]+)">([^<]+?)(?:<span>\s*-\s*([^<]*)</span>)?</a>', row)
        if not sm:
            continue
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        # คอลัมน์: 0 hist, 1 stock, 2 %port, 3 activity, 4 shares, 5 reported, 6 value,
        #          7 gap, 8 quote(current), 9 +/-, 10 52wLow, 11 52wHigh
        pct = _num(_clean(tds[2])) if len(tds) > 2 else None
        act = _clean(re.sub(r"<[^>]+>", "", tds[3])) if len(tds) > 3 else ""
        shares = _num(_clean(tds[4])) if len(tds) > 4 else None
        value = _num(re.sub(r"<[^>]+>", "", tds[6])) if len(tds) > 6 else None
        # current price — คอลัมน์ class="quote" (ราคาสดตอนขูด เท่ากันทุกกอง ใช้เก็บลง quotes map)
        qm = re.search(r'class="quote">([^<]+)</td>', row)
        current = _num(qm.group(1)) if qm else None
        info["holdings"].append({
            "sym": _clean(sm.group(1)),
            "name": _clean(sm.group(3)) if sm.lastindex >= 3 and sm.group(3) else "",
            "pct": pct,
            "activity": act if act and act != " " else "",
            "shares": shares,
            "value": value,
            "_current": current,   # ชั่วคราว — refresh_all จะยุบเป็น quotes map แล้วลบทิ้ง
        })
    return info


# ----------------------------------------------------------- sold-out (typ=s)
# holdings.php เป็น snapshot ปัจจุบัน — หุ้นที่กองขายทิ้งหมด (full exit) จะหายไปเลย ไม่โผล่
# ในนั้น ต้องดึงหน้า Activity แยก (m_activity.php?typ=s) ซึ่งมีประวัติ Buy/Add/Reduce/Sell
# ย้อนหลังหลายไตรมาส — ต้องการเฉพาะ "ไตรมาสล่าสุด" (บล็อกแรกหลัง quarter marker ตัวแรก) และ
# เฉพาะแถวที่ activity ขึ้นต้นด้วย "Sell" (ต่างจาก "Reduce" ที่ยังถือเหลืออยู่ — ยืนยันจาก
# ตัวอย่างจริง: Ackman ขาย HLT "Sell 100.00%" แล้ว HLT หายจาก holdings.php ไปเลย ในขณะที่
# "Reduce 95%" ของ GOOGL ยังอยู่ในโหลดิ้งปกติ) — HTML หน้านี้ไม่มี <tr> เปิดต่อแถว (malformed,
# เบราว์เซอร์ auto-fix ให้) จึง parse ด้วย sym anchor แทนการจับคู่ <tr>...</tr>
_Q_MARKER_RE = re.compile(r'class="q_chg"><td colspan="5"><b>(Q\d)</b>\s*&nbsp<b>(\d+)</b>')
_SELL_ROW_RE = re.compile(
    r'sym=([A-Z0-9.\-]+)">\1<span> - ([^<]*)</span></a></td>\s*'
    r'<td class="sell">([^<]*)</td>\s*<td class="sell">([^<]*)</td>\s*<td>([^<]*)</td>')


def fetch_sells(code):
    """คืน {period, sold_out:[{sym,name,shares_change,pct_change}]} ของกองเดียว —
    เฉพาะแถว activity='Sell...' (full exit) ในไตรมาสล่าสุดเท่านั้น (ไม่เอา Reduce เพราะ
    ซ้ำกับ activity ใน holdings.php อยู่แล้ว)"""
    html = _get(_BASE + f"m_activity.php?m={code}&typ=s")
    i = html.find('id="grid"')
    j = html.find('</table>', i) if i >= 0 else -1
    body = html[i:j] if i >= 0 and j >= 0 else ""

    markers = list(_Q_MARKER_RE.finditer(body))
    if not markers:
        return {"period": None, "sold_out": []}
    period = f"{markers[0].group(1)} {markers[0].group(2)}"
    end = markers[1].start() if len(markers) > 1 else len(body)
    slice_ = body[markers[0].end():end]

    sold_out = []
    for m in _SELL_ROW_RE.finditer(slice_):
        sym, name, act, sh, pct = m.groups()
        act = _clean(act)
        if not act.lower().startswith("sell"):
            continue  # Reduce = ยังถือเหลืออยู่ ไม่นับเป็น sold out
        sold_out.append({
            "sym": _clean(sym),
            "name": _clean(name),
            "shares_change": _num(sh),
            "pct_change": _num(pct),
        })
    return {"period": period, "sold_out": sold_out}


# ------------------------------------------------------------------ refresh all
def refresh_all(base_dir, callback=None, pause=0.4):
    """ขูดทุกกอง → เขียน data/hedge_holdings.json — callback(current,total,msg) ราย gอง"""
    managers = fetch_managers()
    total = len(managers)
    if total == 0:
        # Dataroma อาจเปลี่ยนโครงสร้าง HTML แล้ว regex ไม่ match อะไรเลย (fetch_managers()
        # คืน [] แบบเงียบๆ ไม่ raise) — ห้ามปล่อยให้เขียนทับ cache เดิมด้วยผลว่างเปล่า
        raise RuntimeError("fetch_managers() คืนค่าว่างเปล่า — Dataroma อาจเปลี่ยนโครงสร้าง HTML, "
                            "ไม่เขียนทับ cache เดิมเพื่อกันข้อมูลหาย")
    result = {}
    quotes = {}   # sym -> current price ล่าสุด (รวมจากทุกกอง ครอบทุกตัว ไม่จำกัดแค่ top 100)
    for idx, mgr in enumerate(managers, 1):
        code = mgr["code"]
        if callback:
            callback(idx, total, f"ดึง {mgr['name']} ({idx}/{total})")
        try:
            info = fetch_holdings(code)
            # เก็บมูลค่า/จำนวนจากหน้า list เผื่อหน้า holdings ไม่มี
            info["value"] = info.get("value") or mgr.get("value")
            info["num_stocks"] = info.get("num_stocks") or mgr.get("num_stocks")
            # ยุบ current ราย holding เข้า quotes map แล้วลบออกจาก holding (JSON ไม่บวม)
            for h in info["holdings"]:
                cur = h.pop("_current", None)
                if cur and h["sym"] not in quotes:
                    quotes[h["sym"]] = cur
        except Exception as e:  # noqa: BLE001 — กองเดียวพลาดไม่ควรล้มทั้งงาน
            result[code] = {"code": code, "name": mgr["name"], "error": str(e),
                            "holdings": [], "sold_out": [], "value": mgr.get("value"),
                            "num_stocks": mgr.get("num_stocks")}
            time.sleep(pause)
            continue

        time.sleep(pause)
        # หน้า sold-out แยก try ของตัวเอง — พังแล้วไม่ควรทิ้ง holdings ที่ดึงมาสำเร็จแล้วข้างบน
        # (เดิมอยู่ try ก้อนเดียวกัน ทำให้ manager นั้นทั้งกองโดนแทนที่ด้วย entry ว่างเปล่า
        # ทั้งที่ holdings จริงดึงมาสำเร็จแล้ว)
        try:
            sells = fetch_sells(code)
            info["sold_out"] = sells["sold_out"]
            info["sold_out_period"] = sells["period"]
        except Exception:  # noqa: BLE001
            info["sold_out"] = []
            info["sold_out_period"] = None
        result[code] = info
        time.sleep(pause)  # เกรงใจเซิร์ฟเวอร์ Dataroma

    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "dataroma.com",
        "manager_count": len(result),
        "quotes": quotes,
        "managers": result,
    }
    path = os.path.join(base_dir, CACHE_REL)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)
    return payload


def load_cache(base_dir):
    path = os.path.join(base_dir, CACHE_REL)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


if __name__ == "__main__":  # ทดสอบเร็ว: python -m sources.dataroma
    ms = fetch_managers()
    print("managers:", len(ms), "| first:", ms[0])
    h = fetch_holdings(ms[0]["code"])
    print("holdings sample:", h["name"], h["period"], "| n=", len(h["holdings"]))
    print(h["holdings"][:2])
