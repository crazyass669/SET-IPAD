# -*- coding: utf-8 -*-
"""
Golden-file regression test — รันหลังทุก refactor step:
    python tests/test_golden.py

รัน calculation pipeline บน fixture คงที่ แล้วเทียบกับ golden.json
ถ้า diff != 0 แปลว่า refactor เปลี่ยนพฤติกรรมการคำนวณ (ห้ามผ่าน)
exit code: 0 = ผ่าน, 1 = ไม่ผ่าน
"""
import json
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE_DIR = os.path.join(BASE, "tests", "fixtures")
sys.path.insert(0, BASE)
sys.stdout.reconfigure(encoding="utf-8")

from make_golden import run_pipeline  # noqa: E402  (pipeline runner เดียวกับตอนสร้าง golden)


def _diff(path, a, b, out):
    """เทียบ nested dict/list — เก็บทุกจุดต่างพร้อม path"""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: หายจากผลใหม่")
            elif k not in b:
                out.append(f"{path}.{k}: เกินมาในผลใหม่")
            else:
                _diff(f"{path}.{k}", a[k], b[k], out)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: ความยาว {len(a)} != {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            _diff(f"{path}[{i}]", x, y, out)
    elif a != b:
        out.append(f"{path}: {a!r} != {b!r}")


def main():
    fx_path = os.path.join(FIXTURE_DIR, "history_sample.json")
    gd_path = os.path.join(FIXTURE_DIR, "golden.json")
    if not (os.path.exists(fx_path) and os.path.exists(gd_path)):
        print("ไม่พบ fixture/golden — รัน python tests/make_golden.py ก่อน")
        return 1

    fixture = json.load(open(fx_path, encoding="utf-8"))
    golden  = json.load(open(gd_path, encoding="utf-8"))

    actual = json.loads(json.dumps(run_pipeline(fixture)))  # normalize types ผ่าน JSON

    diffs = []
    _diff("$", golden, actual, diffs)

    if diffs:
        print(f"❌ FAIL — {len(diffs)} จุดต่างจาก golden:")
        for d in diffs[:40]:
            print("  ", d)
        if len(diffs) > 40:
            print(f"   ... และอีก {len(diffs)-40} จุด")
        return 1

    print(f"✅ PASS — pipeline ตรงกับ golden ทุกค่า "
          f"({len(golden['stocks'])} หุ้น, {len(golden['sectors'])} sectors)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
