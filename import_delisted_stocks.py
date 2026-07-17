# -*- coding: utf-8 -*-
"""import_delisted_stocks.py — เติมหุ้นที่เพิกถอน/หายไปจาก set_prices.db เข้าไปจาก
set-archive_EOD_1970-LAST.zip (ราคาดิบทั้งตลาดปี 1975-2025 ไม่ตัดหุ้นเพิกถอนออก)

แก้ survivorship bias ในชุดข้อมูลที่ backtest_*.py ใช้ (bt_lib.load_ohlc อ่านจาก
set_prices.db ตรงๆ) — ยืนยันแล้วว่า set_prices.db (930 ตัวปัจจุบัน) ไม่มี
EARTH/STARK/ROBINS/GSTEL ฯลฯ เลยสักแถว ทั้งที่ยังเทรดอยู่หลายปีก่อนถูกเพิกถอน

วิธีคัดกรอง ticker ให้เหลือแต่หุ้นสามัญจริง (ไม่ใช่ DW/warrant/sector-index):
  1. ตัด ticker ที่มี "-" ต่อท้าย (กระดานเทรดอื่น เช่น -O/-F/-R) และตัวที่ยาวเกิน 8
     ตัวอักษร (หุ้นปัจจุบันยาวสุด 8 ตัว — FUTURERT/HYDROGEN/PROSPECT ฯลฯ)
  2. ตัด sector index ("$COMM", "$BANK", ...) และ pattern ของ DW/warrant
     (เช่น "SCB28C1401A" หรือ "PTTU23" — ตัวเลข+C/P+ตัวเลข หรือลงท้ายตัวอักษร+ปี 2 หลัก)
  3. ตัดตัวที่มีอยู่ใน set_prices.db แล้ว (.BK) และเทรดน้อยกว่า 100 วัน (กันเศษ/parse error)
เหลือ ~884 ตัว — ตรวจ manual แล้วว่าเป็นหุ้นเพิกถอน/ควบรวมจริง (TMB→ควบ TTB, INTUCH
เพิกถอน 2025 หลัง Gulf tender, BIGC/MAKRO/OISHI ถูก tender ออก, ROBINS/EARTH/STARK/
GSTEL ล้มละลาย ฯลฯ) ไม่ใช่ noise

adj_close: archive ไม่มีข้อมูลปรับปันผล/split ให้ปล่อย NULL — bt_lib.load_ohlc()
fallback เป็น close เองอยู่แล้วเมื่อ adj_close เป็น None (บรรทัด 39 ของ bt_lib.py)
"""
import os
import re
import sqlite3
import zipfile

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ZIP_PATH = os.path.join(BASE_DIR, "set-archive_EOD_1970-LAST.zip")
DB_PATH = os.path.join(BASE_DIR, "set_prices.db")

KNOWN_INDEX = {"SET", "SET50", "SET100", "sSET", "SETCLMV", "SETHD", "SETWB",
               "SETTHSI", "mai", "SETESG"}
WARRANT_RE1 = re.compile(r"\d[CP]\d")           # เช่น SCB28C1401A
WARRANT_RE2 = re.compile(r"[A-Z]\d{2}[A-Z]?$")  # เช่น PTTU23, EGCOZ18X
MIN_DAYS = 100


def _clean_ticker(tk):
    if not tk or "-" in tk or tk.startswith("$"):
        return None
    if tk in KNOWN_INDEX:
        return None
    if len(tk) > 8:
        return None
    if WARRANT_RE1.search(tk) or WARRANT_RE2.search(tk):
        return None
    return tk


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT DISTINCT ticker FROM prices")
    current = {r[0].replace(".BK", "") for r in cur.fetchall()}

    print("Pass 1/2: สแกน ticker ทั้งหมดในไฟล์ archive...")
    z = zipfile.ZipFile(ZIP_PATH)
    names = [n for n in z.namelist() if n.endswith(".csv")]
    counts = {}
    for i, n in enumerate(names):
        try:
            with z.open(n) as f:
                lines = f.read().decode("utf-8", "ignore").splitlines()[1:]
        except Exception:
            continue   # zip entry เสีย (เจอ 1 ไฟล์) — ข้ามไปเฉยๆ ไม่กระทบภาพรวม
        for line in lines:
            tk = line.split(",", 1)[0]
            tk = _clean_ticker(tk)
            if tk and tk not in current:
                counts[tk] = counts.get(tk, 0) + 1
        if i % 3000 == 0:
            print(f"  {i}/{len(names)} ไฟล์...")

    candidates = {tk for tk, c in counts.items() if c >= MIN_DAYS}
    print(f"เจอ ticker ที่หายไปจาก set_prices.db: {len(candidates)} ตัว (>= {MIN_DAYS} วัน)")

    print("Pass 2/2: ดึงราคาเต็มของ ticker เหล่านี้...")
    rows_by_ticker = {tk: [] for tk in candidates}
    for i, n in enumerate(names):
        try:
            with z.open(n) as f:
                lines = f.read().decode("utf-8", "ignore").splitlines()[1:]
        except Exception:
            continue
        date = n.rsplit("_EOD_", 1)[1][:10]
        for line in lines:
            parts = line.split(",")
            if len(parts) != 7:
                continue
            tk = parts[0]
            if tk not in candidates:
                continue
            try:
                o, h, l, c = (float(x) for x in parts[2:6])
                v = int(float(parts[6]))
            except ValueError:
                continue
            rows_by_ticker[tk].append((date, c, v, o, h, l))
        if i % 3000 == 0:
            print(f"  {i}/{len(names)} ไฟล์...")

    print("เขียนเข้า set_prices.db ...")
    n_rows = 0
    for tk, rows in rows_by_ticker.items():
        rows.sort(key=lambda r: r[0])
        yf_ticker = tk + ".BK"
        cur.executemany(
            "INSERT OR REPLACE INTO prices (ticker, date, close, volume, open, high, low, adj_close) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            [(yf_ticker, d, c, v, o, h, l) for d, c, v, o, h, l in rows])
        n_rows += len(rows)

    con.commit()
    print(f"เสร็จ: {len(candidates)} ticker, {n_rows} แถวราคารวม")

    cur.execute("SELECT COUNT(DISTINCT ticker) FROM prices")
    print("รวม ticker ทั้งหมดใน set_prices.db ตอนนี้:", cur.fetchone()[0])
    con.close()


if __name__ == "__main__":
    main()
