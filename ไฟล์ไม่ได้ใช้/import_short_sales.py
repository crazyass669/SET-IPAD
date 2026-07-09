"""
import_short_sales.py
อ่าน Excel ข้อมูลการขายชอร์ต → short_sales_data.json

*** DEPRECATED — ไม่ต้องใช้แล้วในการใช้งานปกติ ***
ยอดสะสมรายงวดถูกดึงอัตโนมัติจาก SET API (fromDate/toDate) ทุกครั้งที่
Quick Update แล้ว (ดู short_sales_daily_update ใน app.py)
เก็บสคริปต์นี้ไว้เผื่อ backfill ฉุกเฉิน/สร้างไฟล์ใหม่จากศูนย์เท่านั้น
"""
import os, json, re, sys, io
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def parse_thai_date_range(s):
    """'05 ม.ค. 2569 - 24 มิ.ย. 2569' → ('2026-01-05', '2026-06-24')"""
    month_map = {
        'ม.ค.': 1, 'ก.พ.': 2, 'มี.ค.': 3, 'เม.ย.': 4,
        'พ.ค.': 5, 'มิ.ย.': 6, 'ก.ค.': 7, 'ส.ค.': 8,
        'ก.ย.': 9, 'ต.ค.': 10, 'พ.ย.': 11, 'ธ.ค.': 12,
    }
    parts = str(s).split(' - ')
    def parse_one(p):
        p = p.strip()
        tokens = p.split()
        day  = int(tokens[0])
        mon  = month_map.get(tokens[1], 1)
        year = int(tokens[2]) - 543
        return f"{year:04d}-{mon:02d}-{day:02d}"
    if len(parts) == 2:
        return parse_one(parts[0]), parse_one(parts[1])
    return None, None

def num(v):
    if v is None or (isinstance(v, float) and v != v): return None
    try: return float(v)
    except: return None

def run(excel_path):
    try:
        from python_calamine import CalamineWorkbook
    except ImportError:
        raise RuntimeError("ต้องติดตั้ง: pip install python-calamine")

    rows = CalamineWorkbook.from_path(excel_path).get_sheet_by_name("Short Sales").to_python()

    # parse period from row 6 (index 6)
    period_str = rows[6][0] if len(rows) > 6 else ""
    date_from, date_to = parse_thai_date_range(period_str)
    print(f"Period: {date_from} to {date_to}")

    # data starts at row index 12 (after 2 sub-header rows at 11, 12)
    stocks = {}
    for r in rows[12:]:
        sym = r[0]
        if not sym or not isinstance(sym, str): continue
        if sym in ('หมายเหตุ',) or sym.startswith('ไม่รวม') or sym.startswith('ข้อมูล'): break

        local_vol    = num(r[1])
        nvdr_vol     = num(r[2])
        total_vol    = num(r[3])
        value_baht   = num(r[4])
        pct_value    = num(r[5])   # decimal เช่น 0.0607
        local_pos    = num(r[6])
        nvdr_pos     = num(r[7])
        total_pos    = num(r[8])
        pct_pos      = num(r[9])   # decimal เช่น 0.0169

        stocks[sym] = {
            "period_vol":       int(total_vol)    if total_vol  else 0,
            "period_local_vol": int(local_vol)    if local_vol  else 0,
            "period_nvdr_vol":  int(nvdr_vol)     if nvdr_vol   else 0,
            "period_value":     round(value_baht / 1e6, 2) if value_baht else 0,  # ล้านบาท
            "period_pct_value": round(pct_value * 100, 4)  if pct_value  else 0,  # %
            "short_pos":        int(total_pos)    if total_pos  else 0,
            "short_pos_local":  int(local_pos)    if local_pos  else 0,
            "short_pos_nvdr":   int(nvdr_pos)     if nvdr_pos   else 0,
            "short_pos_pct":    round(pct_pos, 4) if pct_pos else 0,               # Excel ส่งเป็น % แล้ว
            "daily": [],  # จะเพิ่ม snapshot รายวันจาก API ต่อไป
        }

    out = {
        "period_from": date_from,
        "period_to":   date_to,
        "imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_api_update": None,
        "stocks": stocks,
    }

    out_path = os.path.join(BASE_DIR, "short_sales_data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    size = os.path.getsize(out_path) / 1024
    active = sum(1 for v in stocks.values() if v["period_vol"] > 0)
    print(f"บันทึก short_sales_data.json ({size:.0f} KB)")
    print(f"หุ้นทั้งหมด: {len(stocks)} / มี short volume: {active}")
    print()

    # top 10 by short position %
    top = sorted(stocks.items(), key=lambda x: x[1]["short_pos_pct"], reverse=True)[:10]
    print("Top 10 Short Position %:")
    for sym, v in top:
        print(f"  {sym:10s}  pos%={v['short_pos_pct']:.2f}%  pos={v['short_pos']/1e6:.1f}M  %val6m={v['period_pct_value']:.2f}%")

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if not path:
        # หาไฟล์ล่าสุดใน Downloads
        import glob
        dl = os.path.expanduser("~/Downloads")
        files = glob.glob(os.path.join(dl, "Short Sales*.xlsx"))
        if files:
            path = sorted(files)[-1]
            print(f"ใช้ไฟล์: {path}")
        else:
            print("ไม่พบไฟล์ Short Sales*.xlsx ใน Downloads")
            sys.exit(1)
    run(path)
