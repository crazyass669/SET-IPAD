# -*- coding: utf-8 -*-
"""sources/tradingview.py — TradingView WebSocket client + SET index metadata"""
import json
import random
import string
import threading

INDEX_INFO = {
    "^SET.BK":      {"name": "SET Index",                   "group": "SET_INDICES"},
    "^SET50.BK":    {"name": "SET50",                        "group": "SET_INDICES"},
    "^SET100.BK":   {"name": "SET100",                       "group": "SET_INDICES"},
    "^SSET.BK":     {"name": "sSET",                         "group": "SET_INDICES"},
    "^SETHD.BK":    {"name": "SETHD",                        "group": "SET_INDICES"},
    "^MAI.BK":      {"name": "mai",                          "group": "SET_INDICES"},
    "^AGRO.BK":     {"name": "เกษตรและอาหาร",               "group": "SET_INDUSTRY"},
    "^CONSUMP.BK":  {"name": "สินค้าอุปโภคบริโภค",          "group": "SET_INDUSTRY"},
    "^FINCIAL.BK":  {"name": "การเงิน",                     "group": "SET_INDUSTRY"},
    "^INDUS.BK":    {"name": "อุตสาหกรรม",                  "group": "SET_INDUSTRY"},
    "^PROPCON.BK":  {"name": "อสังหาฯและก่อสร้าง",          "group": "SET_INDUSTRY"},
    "^RESOURC.BK":  {"name": "ทรัพยากร",                    "group": "SET_INDUSTRY"},
    "^SERVICE.BK":  {"name": "บริการ",                      "group": "SET_INDUSTRY"},
    "^TECH.BK":     {"name": "เทคโนโลยี",                   "group": "SET_INDUSTRY"},
    "^AGRI.BK":     {"name": "เกษตร",                       "group": "SET_SECTORS"},
    "^FOOD.BK":     {"name": "อาหารและเครื่องดื่ม",         "group": "SET_SECTORS"},
    "^FASHION.BK":  {"name": "แฟชั่น",                      "group": "SET_SECTORS"},
    "^HOME.BK":     {"name": "ของใช้ในครัวเรือน",           "group": "SET_SECTORS"},
    "^PERSON.BK":   {"name": "ของใช้ส่วนตัว",               "group": "SET_SECTORS"},
    "^BANK.BK":     {"name": "ธนาคาร",                      "group": "SET_SECTORS"},
    "^FIN.BK":      {"name": "เงินทุนและหลักทรัพย์",        "group": "SET_SECTORS"},
    "^INSUR.BK":    {"name": "ประกัน",                      "group": "SET_SECTORS"},
    "^AUTO.BK":     {"name": "ยานยนต์",                     "group": "SET_SECTORS"},
    "^IMM.BK":      {"name": "วัสดุอุตสาหกรรม",            "group": "SET_SECTORS"},
    "^PAPER.BK":    {"name": "กระดาษและวัสดุพิมพ์",         "group": "SET_SECTORS"},
    "^PETRO.BK":    {"name": "ปิโตรเคมี",                   "group": "SET_SECTORS"},
    "^PKG.BK":      {"name": "บรรจุภัณฑ์",                  "group": "SET_SECTORS"},
    "^STEEL.BK":    {"name": "เหล็ก",                       "group": "SET_SECTORS"},
    "^ETRON.BK":    {"name": "อิเล็กทรอนิกส์",             "group": "SET_SECTORS"},
    "^ICT.BK":      {"name": "สื่อสาร/IT",                  "group": "SET_SECTORS"},
    "^CONMAT.BK":   {"name": "วัสดุก่อสร้าง",               "group": "SET_SECTORS"},
    "^PROP.BK":     {"name": "พัฒนาอสังหาฯ",               "group": "SET_SECTORS"},
    "^PFREIT.BK":   {"name": "กองทุนอสังหาฯ/REIT",         "group": "SET_SECTORS"},
    "^CONS.BK":     {"name": "รับเหมาก่อสร้าง",            "group": "SET_SECTORS"},
    "^ENERG.BK":    {"name": "พลังงาน",                     "group": "SET_SECTORS"},
    # ^MINE.BK (เหมืองแร่) ตัดออก — หุ้นตัวสุดท้ายใน sector นี้ถูกเพิกถอนไปแล้ว
    # ตั้งแต่ พ.ค. 2023 (ไม่มีหุ้นเหลือใน universe ปัจจุบันเลย) ราคาดัชนีค้างไม่ขยับ
    # ทำให้ 1D/1M/1Y% ที่โชว์ดูเหมือนข้อมูลสดทั้งที่จริงหยุดคำนวณไปแล้ว
    "^COMM.BK":     {"name": "สื่อและโฆษณา",                "group": "SET_SECTORS"},
    "^HELTH.BK":    {"name": "สุขภาพ",                      "group": "SET_SECTORS"},
    "^MEDIA.BK":    {"name": "สื่อ",                        "group": "SET_SECTORS"},
    "^TOURISM.BK":  {"name": "ท่องเที่ยว",                  "group": "SET_SECTORS"},
    "^PROF.BK":     {"name": "บริการเฉพาะกิจ",              "group": "SET_SECTORS"},
    "^TRANS.BK":    {"name": "ขนส่งและโลจิสติกส์",          "group": "SET_SECTORS"},
    "^AGRO-M.BK":   {"name": "เกษตรและอาหาร (mai)",        "group": "MAI_INDUSTRY"},
    "^CONSUMP-M.BK":{"name": "สินค้าอุปโภคบริโภค (mai)",  "group": "MAI_INDUSTRY"},
    "^FINCIAL-M.BK":{"name": "การเงิน (mai)",              "group": "MAI_INDUSTRY"},
    "^INDUS-M.BK":  {"name": "อุตสาหกรรม (mai)",           "group": "MAI_INDUSTRY"},
    "^PROPCON-M.BK":{"name": "อสังหาฯและก่อสร้าง (mai)",  "group": "MAI_INDUSTRY"},
    "^RESOURC-M.BK":{"name": "ทรัพยากร (mai)",             "group": "MAI_INDUSTRY"},
    "^SERVICE-M.BK":{"name": "บริการ (mai)",                "group": "MAI_INDUSTRY"},
    "^TECH-M.BK":   {"name": "เทคโนโลยี (mai)",            "group": "MAI_INDUSTRY"},
}



# แปลง Yahoo symbol → TradingView symbol
# pattern: ^AGRO.BK → SET:AGRO  / ^AGRO-M.BK → SET:AGRO_M
_YF_TV_OVERRIDES = {
    "^PFREIT.BK": "SET:PF_REIT",
}

def _yf_to_tv(yf_sym: str) -> str:
    if yf_sym in _YF_TV_OVERRIDES:
        return _YF_TV_OVERRIDES[yf_sym]
    s = yf_sym.lstrip("^").replace(".BK", "").replace("-", "_")
    return f"SET:{s}"


def _fetch_tv_bars(tv_symbol: str, n_bars: int = 5000, timeout: int = 20):
    """ดึง daily OHLCV จาก TradingView WebSocket คืน list of [date_str, close]"""
    import websocket as _ws
    import traceback as tb

    bars = []
    done = threading.Event()

    def _send(ws, func, args):
        msg = json.dumps({"m": func, "p": args})
        ws.send(f"~m~{len(msg)}~m~{msg}")

    def on_msg(ws, message):
        for part in message.split("~m~"):
            if part.isdigit():
                continue
            try:
                d = json.loads(part)
                if d.get("m") == "timescale_update":
                    for k, v in d["p"][1].items():
                        if k.startswith("sds_"):
                            bars.extend(v.get("s", []))
                    done.set()
            except Exception:
                pass

    def on_open(ws):
        cs = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
        _send(ws, "set_auth_token", ["unauthorized_user_token"])
        _send(ws, "chart_create_session", [cs, ""])
        _send(ws, "resolve_symbol", [cs, "sds_sym_1",
              f'={{"symbol":"{tv_symbol}","adjustment":"splits"}}'])
        _send(ws, "create_series", [cs, "sds_1", "s1", "sds_sym_1", "D", n_bars])

    try:
        wsapp = _ws.WebSocketApp(
            "wss://data.tradingview.com/socket.io/websocket",
            header={"Origin": "https://www.tradingview.com"},
            on_message=on_msg, on_open=on_open,
        )
        t = threading.Thread(target=wsapp.run_forever)
        t.daemon = True
        t.start()
        done.wait(timeout=timeout)
        wsapp.close()
    except Exception:
        print(f"[TV] {tv_symbol}: {tb.format_exc()}")

    result = []
    for bar in bars:
        v = bar.get("v", [])
        if len(v) >= 5:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(v[0], tz=timezone.utc).strftime("%Y-%m-%d")
            result.append([dt, round(float(v[4]), 2)])  # [date, close]
    return result


