# -*- coding: utf-8 -*-
"""ย้ายประวัติ (ราคา/งบการเงิน/ปันผล) จากชื่อย่อหุ้นเก่าไปชื่อย่อใหม่ — ใช้ตอนหุ้นไทย
เปลี่ยนชื่อย่อ (เช่น PSTC -> POWER) หรือควบรวมกิจการแบบที่หุ้นนึงยังอยู่แต่เปลี่ยนชื่อ
(ต่างจากกรณีถูก absorb หายไปเลย ให้ใช้ mark_delisted.py แทน)

ใช้:
    python rename_symbol.py PSTC POWER
    (หรือดับเบิลคลิก rename_symbol.bat แล้วพิมพ์ชื่อเก่า/ใหม่เมื่อถาม)

ทำอะไรบ้าง (เฉพาะหุ้นไทย SET/mai — ticker รูปแบบ SYMBOL.BK):
  1. สำรอง set_prices.db + financials.db ก่อนแก้ทุกครั้ง (กู้คืนได้ถ้าพลาด)
  2. ย้ายแท่งราคา/งบ/ปันผล/calendar events จาก symbol เก่า -> symbol ใหม่
     ถ้าชื่อใหม่มีข้อมูลอยู่แล้วบางส่วน (เช่น ระบบเริ่มดึงเองตั้งแต่วันที่เปลี่ยนชื่อ)
     จะเก็บของใหม่ไว้ก่อนเมื่อวันที่/แหล่งข้อมูลชนกัน (INSERT OR IGNORE)
  3. ลบข้อมูลใต้ symbol เก่าทิ้งหลังย้ายเสร็จ กันโผล่ซ้ำใน universe/drift check

หลังรันเสร็จ: restart แอป (หรือรอ cache หมดอายุ) แล้วกด "⚡ Quick Update" อีกรอบ
เพื่อให้ RS/EMA/Stage คำนวณจากประวัติเต็มใหม่"""
import io
import os
import sqlite3
import sys
import time
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from sources import financials_store  # noqa: E402

PRICES_DB = os.path.join(BASE, "set_prices.db")
FIN_DB    = os.path.join(BASE, "financials.db")


def _backup_prices_db():
    if not os.path.exists(PRICES_DB):
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BASE, f"set_prices_backup_{stamp}_rename.db")
    print(f"[Backup] กำลังสำรอง set_prices.db -> {os.path.basename(dest)} ...", flush=True)
    t0 = time.time()
    src = sqlite3.connect(PRICES_DB)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    print(f"[Backup] เสร็จใน {time.time() - t0:.0f} วิ", flush=True)
    return dest


def _connect(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def _rename_prices(old_sym, new_sym):
    if not os.path.exists(PRICES_DB):
        print("[set_prices.db] ไม่พบไฟล์ — ข้าม")
        return
    old_t, new_t = f"{old_sym}.BK", f"{new_sym}.BK"
    con = _connect(PRICES_DB)
    try:
        n_old = con.execute("SELECT COUNT(*) FROM prices WHERE ticker=?", (old_t,)).fetchone()[0]
        if not n_old:
            print(f"[set_prices.db] ไม่มีข้อมูล {old_t} — ข้าม")
            return
        n_new_before = con.execute("SELECT COUNT(*) FROM prices WHERE ticker=?", (new_t,)).fetchone()[0]
        con.execute("""INSERT OR IGNORE INTO prices (ticker,date,close,volume,open,high,low,adj_close)
                       SELECT ?, date, close, volume, open, high, low, adj_close
                       FROM prices WHERE ticker=?""", (new_t, old_t))
        con.execute("DELETE FROM prices WHERE ticker=?", (old_t,))
        con.commit()
        n_new_after, dmin, dmax = con.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM prices WHERE ticker=?", (new_t,)).fetchone()
        print(f"[set_prices.db] {old_t} ({n_old} แท่ง) -> {new_t} "
              f"(ก่อนมี {n_new_before} -> หลังมี {n_new_after} แท่ง, {dmin}..{dmax})")
    finally:
        con.close()


def _rename_financials(old_sym, new_sym):
    if not os.path.exists(FIN_DB):
        print("[financials.db] ไม่พบไฟล์ — ข้าม")
        return
    financials_store.backup_db(BASE)
    con = _connect(FIN_DB)
    try:
        moved = {}
        for table, cols, key_cols in [
            ("financials",      "symbol, source, payload, synced_at",            "symbol, source"),
            ("dividends",       "symbol, market, ex_date, dps, synced_at",       "symbol, market, ex_date"),
            ("calendar_events", "symbol, market, type, date, confidence, source, detail, synced_at",
                                                                                  "symbol, market, type, date"),
        ]:
            n_old = con.execute(f"SELECT COUNT(*) FROM {table} WHERE symbol=?", (old_sym,)).fetchone()[0]
            if not n_old:
                continue
            rest = ", ".join(c.strip() for c in cols.split(",")[1:])
            con.execute(f"""INSERT OR IGNORE INTO {table} (symbol, {rest})
                            SELECT ?, {rest} FROM {table} WHERE symbol=?""", (new_sym, old_sym))
            con.execute(f"DELETE FROM {table} WHERE symbol=?", (old_sym,))
            moved[table] = n_old
        con.commit()
        if moved:
            print(f"[financials.db] ย้ายแล้ว: " + ", ".join(f"{t} {n} แถว" for t, n in moved.items()))
        else:
            print(f"[financials.db] ไม่มีข้อมูล {old_sym} ในตารางไหนเลย — ข้าม")
    finally:
        con.close()


def main():
    if len(sys.argv) < 3:
        print("ใช้: python rename_symbol.py <ชื่อเก่า> <ชื่อใหม่>")
        print("เช่น: python rename_symbol.py PSTC POWER")
        sys.exit(1)
    old_sym, new_sym = sys.argv[1].strip().upper(), sys.argv[2].strip().upper()
    if old_sym == new_sym:
        print("ชื่อเก่า/ใหม่ห้ามซ้ำกัน")
        sys.exit(1)

    print(f"=== ย้ายข้อมูล {old_sym} -> {new_sym} (หุ้นไทย SET/mai) ===")
    _backup_prices_db()
    _rename_prices(old_sym, new_sym)
    _rename_financials(old_sym, new_sym)
    print("\nเสร็จแล้ว — restart แอป (หรือรอ cache หมดอายุ) แล้วกด \"⚡ Quick Update\" "
          "อีกรอบเพื่อคำนวณ RS/EMA/Stage จากประวัติเต็มใหม่")


if __name__ == "__main__":
    main()
