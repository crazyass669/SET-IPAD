# -*- coding: utf-8 -*-
"""core/run_log.py — บันทึกผลการรันของแต่ละกลไกอัพเดท (Quick Update, Full
Refresh, GitHub Actions auto-update, update_financials.py, mirror_finnomena.py)

2 ไฟล์:
  - logs/update_status.json  — "ผลล่าสุด" ต่อ source เดียว (เขียนทับทุกรอบ) ใช้
    โชว์ banner เตือนตอนเปิดแอปถ้ารอบล่าสุดล้มเหลว แม้ผู้ใช้ไม่ได้เฝ้าหน้าจอตอนรันจริง
  - logs/update_history.json — ประวัติสะสมต่อ source (เก็บ MAX_HISTORY_PER_SOURCE
    รอบล่าสุด ตัดของเก่าทิ้ง) ใช้ดูแนวโน้ม "ล้มติดกันกี่รอบ / ล้มตอนไหนบ้าง" ต่างจาก
    update_status.json ที่บอกได้แค่สถานะปัจจุบัน

record_run() เขียนทั้งสองไฟล์พร้อมกันเสมอ — เรียกที่เดียวได้ทั้งคู่ ไม่ต้องแก้ที่
เรียกใช้เดิมที่มีอยู่แล้ว 10+ จุด"""
import json
import os
from datetime import datetime

MAX_HISTORY_PER_SOURCE = 50


def _log_path(base_dir):
    d = os.path.join(base_dir, "logs")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "update_status.json")


def _history_path(base_dir):
    d = os.path.join(base_dir, "logs")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "update_history.json")


def record_run(base_dir, source, ok, message=""):
    """เรียกตอนจบการรันของกลไกอัพเดทแต่ละตัว (สำเร็จหรือล้มเหลวก็เรียก) —
    เขียนทับ "ผลล่าสุด" ของ source เดิม + append เข้าประวัติสะสม (ตัดเหลือ
    MAX_HISTORY_PER_SOURCE รอบล่าสุด)

    ห้าม raise ออกไปหา caller เด็ดขาด — ฟังก์ชันนี้ถูกเรียกท้าย except block ของ
    job หลายสิบตัว (Quick Update/Full Refresh/financials sync ฯลฯ) เพื่อบันทึกผล
    ก่อน/หลัง _update(done=True,...) ถ้าเขียนไฟล์พัง (ดิสก์เต็ม/โปรแกรม backup
    ล็อกไฟล์ชั่วคราว) ต้องไม่ทำให้โค้ดหลังจากนี้ใน except block หลุดไม่ทำงาน —
    เจอบั๊กจริงแล้ว 1 ครั้ง (code review 2026-08-26): _run_refresh/
    _run_financials_update_all เรียก record_run ก่อน _update(done=True,...)
    พังกลางทางแล้วปล่อยให้ SSE ค้างสถานะ "กำลังทำงาน" ตลอดไป"""
    at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record = {"ok": bool(ok), "at": at, "message": str(message)[:500]}

    try:
        path = _log_path(base_dir)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
        data[source] = record
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        print(f"[run_log] เขียน update_status.json ไม่สำเร็จ ({source}): {e}")

    try:
        hpath = _history_path(base_dir)
        try:
            with open(hpath, encoding="utf-8") as f:
                hist = json.load(f)
        except Exception:
            hist = {}
        hist[source] = (hist.get(source) or [])[-(MAX_HISTORY_PER_SOURCE - 1):] + [record]
        htmp = hpath + ".tmp"
        with open(htmp, "w", encoding="utf-8") as f:
            json.dump(hist, f, ensure_ascii=False)
        os.replace(htmp, hpath)
    except Exception as e:
        print(f"[run_log] เขียน update_history.json ไม่สำเร็จ ({source}): {e}")


def read_status(base_dir):
    try:
        with open(_log_path(base_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def read_history(base_dir, source=None):
    """ประวัติสะสมทั้งหมด (dict: source -> list ของ {ok, at, message} เรียงเก่า->ใหม่)
    ส่ง source มาเพื่อกรองเฉพาะกลไกเดียว"""
    try:
        with open(_history_path(base_dir), encoding="utf-8") as f:
            hist = json.load(f)
    except Exception:
        hist = {}
    if source:
        return hist.get(source, [])
    return hist


def read_recent_failures(base_dir, limit=100):
    """รวมทุก source เฉพาะรอบที่ล้มเหลว (ok=False) เรียงเวลาล่าสุดก่อน — ใช้โชว์
    ตาราง "ประวัติล้มเหลวล่าสุด" ในหน้า Data Health (ต่างจาก /api/update-status ที่
    บอกได้แค่รอบล่าสุดต่อ source)"""
    hist = read_history(base_dir)
    # rec ต้องเป็น dict เท่านั้น — กันไฟล์ประวัติเสีย/ถูกแก้มือทำ record ผิดรูปจน
    # .get() throw AttributeError จน /api/update-history คืน 500
    fails = [{"source": src, **rec} for src, recs in hist.items()
             for rec in recs if isinstance(rec, dict) and not rec.get("ok")]
    fails.sort(key=lambda x: x.get("at", ""), reverse=True)
    return fails[:limit]
