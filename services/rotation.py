# -*- coding: utf-8 -*-
"""
services/rotation.py — Quadrant-change alerts สำหรับ Rotation map

กติกา:
  - Quadrant จากแกนเดียวกับ Rotation map: x = ret_3m (trend), y = ret_1m (momentum)
      Leading (+,+) | Weakening (+,-) | Lagging (-,-) | Improving (-,+)
  - Dead zone: ถ้า |ret| ของแกนใด < DEAD_ZONE_PCT ถือว่าวันนั้น "ไม่มีสัญญาณ"
    (ไม่นับต่อ ไม่ reset) — กัน flip-flop ของกลุ่มที่แกว่งรอบเส้นแกน
  - เปลี่ยน quadrant ต้องอยู่ quadrant ใหม่ครบ CONFIRM_DAYS วันทำการติดกัน
    (นับเมื่อ data_as_of เปลี่ยนเท่านั้น — อัปเดตซ้ำวันเดียวกันไม่นับซ้ำ)
    แวะ quadrant อื่นระหว่างนับ = เริ่มนับใหม่
  - เจอกลุ่มครั้งแรก = seed เงียบ (บันทึกสถานะ ไม่ยิง alert)

state เก็บใน rotation_state.json (ไฟล์เล็ก ~30KB) เขียนแบบ atomic
"""
import json
import os
from datetime import date

from core.store import _atomic_write_json

STATE_FILE    = "rotation_state.json"
CONFIRM_DAYS  = 3      # ต้องอยู่ quadrant ใหม่กี่วันทำการติดกันถึงยืนยัน
DEAD_ZONE_PCT = 0.3    # |ret| ต่ำกว่านี้ = วันไม่มีสัญญาณ
PENDING_STALE_DAYS = 10  # ปฏิทินวัน — pending ที่ไม่ขยับ (ค้างใน dead zone) เกินนี้ไม่โชว์
                         # กัน UI ค้างป้าย "⏳ x/3 วัน" ทั้งที่ไม่มีความเคลื่อนไหวจริงมาสัปดาห์

# ชุดสัญญาณเร็ว: แกน x=1M, y=1W — เห็น rotation เร็วกว่าชุดหลัก (3M/1M) ได้เป็นสัปดาห์
# แต่ 1W แกว่ง ±2-3%/สัปดาห์เป็นปกติ noise สูงกว่ามาก จึงใช้ dead zone กว้างขึ้น
# และฝั่ง UI แสดงแยกป้าย "⚡ สัญญาณเร็ว" ชัดเจน ไม่ปนกับ alert หลัก
FAST_DEAD_ZONE_PCT = 0.8

QUADRANTS = ("Leading", "Weakening", "Lagging", "Improving")


def quadrant_of(ret_3m, ret_1m, dead_zone=DEAD_ZONE_PCT):
    """คืนชื่อ quadrant หรือ None ถ้าข้อมูลไม่พอ/อยู่ใน dead zone"""
    if ret_3m is None or ret_1m is None:
        return None
    if abs(ret_3m) < dead_zone or abs(ret_1m) < dead_zone:
        return None
    if ret_3m > 0 and ret_1m > 0:
        return "Leading"
    if ret_3m > 0:
        return "Weakening"
    if ret_1m > 0:
        return "Improving"
    return "Lagging"


def _advance(entry, quad, date):
    """เดิน state machine หนึ่งวัน — คืน (entry ใหม่, transition หรือ None)"""
    if quad is None:
        return entry, None                     # วันไม่มีสัญญาณ — สถานะคงเดิม
    if entry is None:
        return {"confirmed": quad, "since": date, "pending": None}, None  # seed เงียบ
    if quad == entry["confirmed"]:
        entry["pending"] = None                # กลับ quadrant เดิม — ล้มการนับ
        return entry, None

    p = entry.get("pending")
    if p and p["quadrant"] == quad:
        p["days"] += 1
        p["last_date"] = date
    else:                                      # เริ่มนับใหม่ (quadrant ใหม่/สลับตัว)
        p = {"quadrant": quad, "days": 1, "first_date": date, "last_date": date}
    entry["pending"] = p

    if p["days"] >= CONFIRM_DAYS:
        transition = {"from": entry["confirmed"], "to": quad,
                      "date": date, "started": p["first_date"]}
        return {"confirmed": quad, "since": p["first_date"], "pending": None}, transition
    return entry, None


def _state_path(base_dir):
    return os.path.join(base_dir, STATE_FILE)


def load_state(base_dir):
    path = _state_path(base_dir)
    if not os.path.exists(path):
        return {"last_processed": None, "groups": {}, "transitions": []}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_processed": None, "groups": {}, "transitions": []}


def pending_of(groups, today=None):
    """groups (state["groups"]/["groups_fast"]) -> list ของ quadrant ที่กำลังนับยืนยัน
    ตัด entry ที่เงียบมานาน (ค้างใน dead zone ไม่ขยับ) เกิน PENDING_STALE_DAYS ทิ้ง —
    ไม่งั้น UI จะโชว์ "⏳ 2/3 วัน" ค้างเป็นสัปดาห์ทั้งที่ไม่มีสัญญาณต่อแล้ว"""
    today_d = None
    if today:
        try:
            today_d = date.fromisoformat(today)
        except Exception:
            today_d = None
    pending = []
    for key, e in groups.items():
        p = e.get("pending")
        if not p:
            continue
        if today_d is not None:
            try:
                gap = (today_d - date.fromisoformat(p["last_date"])).days
                if gap > PENDING_STALE_DAYS:
                    continue
            except Exception:
                pass
        gtype, name = key.split(":", 1)
        pending.append({"type": gtype, "name": name,
                        "from": e.get("confirmed"), "to": p["quadrant"],
                        "days": p["days"], "need": CONFIRM_DAYS,
                        "since": p["first_date"], "last_date": p["last_date"]})
    pending.sort(key=lambda x: -x["days"])
    return pending


def update_rotation_state(base_dir, data_as_of, sectors, industries):
    """เรียกหลัง summarize_groups ทุกครั้งที่ข้อมูลอัปเดต (Quick/Full refresh)
    เดิน state machine เฉพาะเมื่อ data_as_of ใหม่กว่าที่ประมวลผลไปแล้ว
    เดินขนานกัน 2 ชุด: หลัก (3M/1M) และสัญญาณเร็ว (1M/1W) — กติกาเดียวกัน ต่างแค่แกน+dead zone"""
    if not data_as_of:
        return
    state = load_state(base_dir)
    last = state.get("last_processed")
    if last and data_as_of <= last:
        return  # วันเดิม/ย้อนหลัง — ไม่นับซ้ำ

    configs = (
        # (key ของ groups ใน state, key ของ transitions, ฟังก์ชันหา quadrant, ป้าย log)
        ("groups",      "transitions",
         lambda g: quadrant_of(g.get("ret_3m"), g.get("ret_1m")), ""),
        ("groups_fast", "transitions_fast",
         lambda g: quadrant_of(g.get("ret_1m"), g.get("ret_1w"), dead_zone=FAST_DEAD_ZONE_PCT), " ⚡fast"),
    )
    for groups_key, trans_key, quad_fn, tag in configs:
        state.setdefault(groups_key, {})   # state เก่าก่อนมีชุดเร็ว — seed เงียบรอบแรก
        state.setdefault(trans_key, [])
        current_keys = set()
        for gtype, groups in (("sector", sectors or []), ("industry", industries or [])):
            for g in groups:
                key  = f"{gtype}:{g['name']}"
                current_keys.add(key)
                quad = quad_fn(g)
                entry, trans = _advance(state[groups_key].get(key), quad, data_as_of)
                if entry is not None:
                    state[groups_key][key] = entry
                if trans:
                    trans.update({"type": gtype, "name": g["name"]})
                    state[trans_key].insert(0, trans)
                    print(f"[Rotation{tag}] {gtype} '{g['name']}': "
                          f"{trans['from']} -> {trans['to']} (ยืนยัน {CONFIRM_DAYS} วัน)")
        # ลบ key ที่ไม่อยู่ในรอบนี้แล้ว (sector/industry ถูกเปลี่ยนชื่อ/ยุบทิ้ง)
        # กันสถานะค้างใน state ไปตลอดกาลโดยไม่มีทางถูกอัปเดต/ล้าง
        for stale_key in list(state[groups_key].keys()):
            if stale_key not in current_keys:
                del state[groups_key][stale_key]
        state[trans_key] = state[trans_key][:50]

    state["last_processed"] = data_as_of
    _atomic_write_json(_state_path(base_dir), state)
