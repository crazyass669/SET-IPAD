"""
import_market_stats.py
อ่าน Table_PE.xls + Table_PBV.xls → สร้าง set_market_stats.json
"""
import os, json, pandas as pd, numpy as np
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def read_xls_table(fname):
    path = os.path.join(BASE_DIR, fname)
    tables = pd.read_html(path, header=None)
    # table[1] คือข้อมูลจริง
    t = tables[1].copy()
    t.columns = t.iloc[0]
    t = t.iloc[1:].reset_index(drop=True)
    t.columns = [str(c).strip() for c in t.columns]
    return t

def parse_month_year(s):
    """'May-2026' → '2026-05'"""
    try:
        dt = datetime.strptime(str(s).strip(), "%b-%Y")
        return dt.strftime("%Y-%m")
    except:
        return None

def calc_stats(values):
    v = [x for x in values if x is not None and not (isinstance(x, float) and np.isnan(x))]
    if not v:
        return {}
    arr = sorted(v)
    current = v[-1]
    avg = sum(v) / len(v)
    std = float(np.std(v))          # population std
    pct = round(sum(1 for x in arr if x <= current) / len(arr) * 100, 1)
    zscore = round((current - avg) / std, 2) if std else 0
    return {
        "current":    round(current, 2),
        "min":        round(min(arr), 2),
        "max":        round(max(arr), 2),
        "avg":        round(avg, 2),
        "median":     round(arr[len(arr)//2], 2),
        "std":        round(std, 2),
        "zscore":     zscore,
        "percentile": pct,
        "bands": {
            "+3σ": round(avg + 3*std, 2),
            "+2σ": round(avg + 2*std, 2),
            "+1σ": round(avg + 1*std, 2),
            "-1σ": round(avg - 1*std, 2),
            "-2σ": round(avg - 2*std, 2),
            "-3σ": round(avg - 3*std, 2),
        },
    }

def process_table(df):
    col_date = "Month-Year"
    data_cols = [c for c in df.columns if c != col_date]
    result = {"dates": [], "series": {c: [] for c in data_cols}}
    rows = []
    for _, row in df.iterrows():
        d = parse_month_year(row[col_date])
        if not d:
            continue
        vals = {}
        for c in data_cols:
            try:
                val = float(row[c])
                vals[c] = round(val, 2) if not np.isnan(val) else None
            except:
                vals[c] = None
        rows.append((d, vals))
    # เรียงจากเก่าไปใหม่ (oldest → newest)
    rows.sort(key=lambda x: x[0])
    for d, vals in rows:
        result["dates"].append(d)
        for c in data_cols:
            result["series"][c].append(vals.get(c))
    return result

print("อ่าน Table_PE.xls...")
pe_df  = read_xls_table("Table_PE.xls")
print("อ่าน Table_PBV.xls...")
pbv_df = read_xls_table("Table_PBV.xls")

pe_data  = process_table(pe_df)
pbv_data = process_table(pbv_df)

output = {
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "pe": {
        "dates":  pe_data["dates"],
        "series": pe_data["series"],
        "stats":  {k: calc_stats(v) for k, v in pe_data["series"].items()},
    },
    "pbv": {
        "dates":  pbv_data["dates"],
        "series": pbv_data["series"],
        "stats":  {k: calc_stats(v) for k, v in pbv_data["series"].items()},
    },
}

out_path = os.path.join(BASE_DIR, "set_market_stats.json")

# ซีรีส์ที่มาจาก Market_Statistics_Month_th_TH.xls เท่านั้น (Table_PE/PBV.xls ไม่มี) —
# ต้องคงไว้ เดิมสคริปต์นี้เขียนทับทั้งไฟล์ ทำให้ประวัติ ปันผล/มูลค่าหลักทรัพย์/breadth
# ที่สะสมมาหลายปีหายทุกครั้งที่ rebuild (ดู import_market_stats_monthly.py)
if os.path.exists(out_path):
    try:
        with open(out_path, encoding="utf-8") as f:
            _old = json.load(f)
        for _k in ("div_yield", "mkt_cap", "breadth"):
            if _k in _old:
                output[_k] = _old[_k]
                print(f"คงซีรีส์เดิมไว้: {_k} ({len(_old[_k].get('dates', []))} เดือน)")
    except Exception as e:
        print(f"⚠️ อ่าน set_market_stats.json เดิมไม่ได้ ({e}) — เขียนใหม่ทั้งไฟล์")

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

size = os.path.getsize(out_path) / 1024
print(f"บันทึก set_market_stats.json ({size:.0f} KB)")
print(f"P/E  SET: {len(pe_data['dates'])} เดือน ({pe_data['dates'][0]} – {pe_data['dates'][-1]})")
print(f"P/BV SET: {len(pbv_data['dates'])} เดือน ({pbv_data['dates'][0]} – {pbv_data['dates'][-1]})")
print()
pe_stats = output["pe"]["stats"].get("SET", {})
pbv_stats = output["pbv"]["stats"].get("SET", {})
print(f"P/E  SET ปัจจุบัน : {pe_stats.get('current')}x  (z={pe_stats.get('zscore'):+}σ)")
print(f"P/E  SET ค่าเฉลี่ย : {pe_stats.get('avg')}x  ±1σ={pe_stats.get('std')}x")
print(f"P/E  SET bands:")
for k,v in pe_stats.get('bands',{}).items():
    print(f"       {k:4s} = {v}x")
print()
print(f"P/BV SET ปัจจุบัน : {pbv_stats.get('current')}x  (z={pbv_stats.get('zscore'):+}σ)")
print(f"P/BV SET ค่าเฉลี่ย : {pbv_stats.get('avg')}x  ±1σ={pbv_stats.get('std')}x")
print(f"P/BV SET bands:")
for k,v in pbv_stats.get('bands',{}).items():
    print(f"       {k:4s} = {v}x")
