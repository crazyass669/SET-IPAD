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

from core.store import _atomic_write_json

STATE_FILE    = "rotation_state.json"
CONFIRM_DAYS  = 3      # ต้องอยู่ quadrant ใหม่กี่วันทำการติดกันถึงยืนยัน
DEAD_ZONE_PCT = 0.3    # |ret| ต่ำกว่านี้ = วันไม่มีสัญญาณ

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


def update_rotation_state(base_dir, data_as_of, sectors, industries):
    """เรียกหลัง summarize_groups ทุกครั้งที่ข้อมูลอัปเดต (Quick/Full refresh)
    เดิน state machine เฉพาะเมื่อ data_as_of ใหม่กว่าที่ประมวลผลไปแล้ว"""
    if not data_as_of:
        return
    state = load_state(base_dir)
    last = state.get("last_processed")
    if last and data_as_of <= last:
        return  # วันเดิม/ย้อนหลัง — ไม่นับซ้ำ

    for gtype, groups in (("sector", sectors or []), ("industry", industries or [])):
        for g in groups:
            key  = f"{gtype}:{g['name']}"
            quad = quadrant_of(g.get("ret_3m"), g.get("ret_1m"))
            entry, trans = _advance(state["groups"].get(key), quad, data_as_of)
            if entry is not None:
                state["groups"][key] = entry
            if trans:
                trans.update({"type": gtype, "name": g["name"]})
                state["transitions"].insert(0, trans)
                print(f"[Rotation] {gtype} '{g['name']}': "
                      f"{trans['from']} -> {trans['to']} (ยืนยัน {CONFIRM_DAYS} วัน)")

    state["transitions"]   = state["transitions"][:50]
    state["last_processed"] = data_as_of
    _atomic_write_json(_state_path(base_dir), state)
