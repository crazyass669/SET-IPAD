# -*- coding: utf-8 -*-
"""บันทึกว่าหุ้นตัวหนึ่งเพิกถอน/ถูกควบรวมหายไปแล้ว (ไม่มีชื่อใหม่ให้ย้ายข้อมูลไปหา —
ถ้ามีชื่อใหม่/เปลี่ยนชื่อย่อ ให้ใช้ rename_symbol.py แทน) ผลคือบันทึกลง delisted_log.json
ให้ backtest รุ่นถัดไปรู้ว่าหุ้นนี้ "ยังอยู่จริง" ถึงวันไหน (point-in-time membership)

หมายเหตุ: ไม่ได้ลบราคา/งบเก่าออกจาก set_prices.db/financials.db (เก็บไว้ backtest ได้)
ระบบจะกรองออกจาก universe งบการเงินให้เองตอนราคานิ่งเกิน 180 วัน (_financials_universe
ใน app.py) อันนี้แค่บันทึกเหตุผลไว้ล่วงหน้าให้ถูกต้อง แทนที่จะรอจนระบบเดาเหตุผลเอง
("ราคาไม่ขยับเกิน 180 วัน") ทั้งที่จริงๆ รู้สาเหตุแน่ชัดแล้ว (ควบรวม/เพิกถอนตามข่าว)

ใช้:
    python mark_delisted.py BPP "ควบรวมเข้า BANPU มีผลสมบูรณ์ตามกฎหมาย 31 ก.ค. 2569"
    python mark_delisted.py BPP "ควบรวมเข้า BANPU" --last-seen 2026-07-30
    (--last-seen ไม่ใส่ก็ได้ — จะไปหาวันที่ราคาล่าสุดใน set_prices.db ให้เอง)
"""
import io
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import delisted_log, store as price_store  # noqa: E402


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) < 2:
        print('ใช้: python mark_delisted.py <SYMBOL> "<เหตุผล>" [--last-seen YYYY-MM-DD]')
        print('เช่น: python mark_delisted.py BPP "ควบรวมเข้า BANPU มีผลสมบูรณ์ตามกฎหมาย 31 ก.ค. 2569"')
        sys.exit(1)
    symbol, reason = args[0].strip().upper(), args[1].strip()

    last_seen = None
    for i, a in enumerate(sys.argv):
        if a == "--last-seen" and i + 1 < len(sys.argv):
            last_seen = sys.argv[i + 1]

    if last_seen is None:
        dates = price_store.get_last_dates(BASE)
        last_seen = dates.get(f"{symbol}.BK") or dates.get(symbol)

    delisted_log.record_delisted(BASE, symbol, "TH", reason, last_seen=last_seen)
    print(f"บันทึกแล้ว: TH:{symbol} — {reason} (ราคาล่าสุดที่เจอ: {last_seen or 'ไม่ทราบ'})")
    print("ดูผลได้ที่ delisted_log.json — ไม่ได้ลบราคา/งบเก่าออก แค่บันทึกเหตุผลไว้")


if __name__ == "__main__":
    main()
