# -*- coding: utf-8 -*-
"""sources/dr_universe.py — DR/DRx static mapping: underlying foreign stock → SET DR tickers"""

# เวลาซื้อขายปกติของแต่ละตลาด (timezone, เปิด, ปิด เป็นนาทีนับจากเที่ยงคืน)
# + buffer ก่อนเปิด/หลังปิด ที่ราคายังไหลปนกับแท่งล่าสุดได้ (pre-market/after-hours)
# ตลาดสหรัฐฯ มี pre-market/after-hours ที่ retail เทรดจริงและ Yahoo เอาราคามาทับ
# แท่งล่าสุดไปเรื่อยๆ (พิสูจน์แล้วว่า Close ของแท่งที่ "ยังไม่นิ่ง" ขยับได้แม้เช็คห่างกัน
# แค่ 1 ชม.) ตลาดเอเชีย/ยุโรปอื่นๆ ไม่มีวัฒนธรรม extended-hours เด่นชัดแบบนี้ ใช้ buffer=0
_REGION_TZ = {
    # pre_buf=5.5 ชม. -> ครอบตั้งแต่ 4:00 ET ที่ Yahoo เริ่มมีราคา pre-market จริง
    # (5 ชม. เดิม = เริ่มเช็คแค่ 4:30 ET เผื่อไม่พอ ราคาที่ไหลอยู่ 4:00-4:30 ET
    # หลุดรอดไปถูกบันทึกเป็น "ราคาปิด" นิ่งทั้งที่ยังไม่นิ่งจริง)
    "US": ("America/New_York", 9 * 60 + 30, 16 * 60, int(5.5 * 60), 4 * 60),
    "HK": ("Asia/Hong_Kong",   9 * 60 + 30, 16 * 60,       0,      0),
    "CN": ("Asia/Shanghai",    9 * 60 + 30, 15 * 60,       0,      0),
    "EU": ("Europe/Paris",     9 * 60,      17 * 60 + 30,  0,      0),
    "JP": ("Asia/Tokyo",       9 * 60,      15 * 60,       0,      0),
    "SG": ("Asia/Singapore",   9 * 60,      17 * 60,       0,      0),
    "VN": ("Asia/Ho_Chi_Minh", 9 * 60,      15 * 60,       0,      0),
    "TW": ("Asia/Taipei",      9 * 60,      13 * 60 + 30,  0,      0),
}


def is_latest_bar_stable(region):
    """เช็คว่าตอนนี้อยู่นอกช่วงที่ราคาแท่งล่าสุดของตลาดนั้นยังไหลอยู่หรือไม่

    Yahoo ไม่สร้างแท่งใหม่ของวันถัดไปจนกว่าตลาดจะเปิดเทรดจริง ระหว่างนั้น
    (pre-market / กำลังเทรด / after-hours) ราคาล่าสุดจะไปทับ Close ของแท่งเก่าสุด
    ที่มีอยู่แทน ทำให้ตัวเลข "ราคาปิด" ที่เห็นขยับได้เรื่อยๆ ไม่นิ่งจริง
    """
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        return True  # เผื่อ python ไม่มี zoneinfo -> ไม่บล็อกอะไร

    cfg = _REGION_TZ.get(region)
    if not cfg:
        return True
    tz, open_min, close_min, pre_buf, post_buf = cfg
    now = datetime.now(ZoneInfo(tz))
    now_min = now.hour * 60 + now.minute
    return not (open_min - pre_buf <= now_min <= close_min + post_buf)


def region_today_date(region):
    """คืนวันที่ปัจจุบัน (date) ตาม timezone ของตลาดนั้น — คืน None ถ้าไม่รู้จัก region
    หรือไม่มี zoneinfo ใช้คู่กับ is_latest_bar_stable เพื่อเช็คว่าแท่งราคาล่าสุดที่ดึงมา
    เป็นของ "วันนี้จริง" ก่อนตัดทิ้งเพราะไม่นิ่ง (ดูคอมเมนต์ตรงจุดเรียกใช้ใน app.py)"""
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        return None
    cfg = _REGION_TZ.get(region)
    if not cfg:
        return None
    return datetime.now(ZoneInfo(cfg[0])).date()


def region_expected_trading_date(region):
    """คืนวันที่ของแท่งปิดล่าสุดที่ "ควรจะมี" อยู่แล้วในข้อมูล (ตลาดปิดสนิท ราคานิ่งแล้ว)

    ต่างจาก region_today_date ตรงที่ถ้าเรียกตอนก่อนตลาดเปิดของวันนี้ (ยังไม่เข้าช่วง
    pre-market buffer ด้วยซ้ำ) จะย้อนกลับไปวันทำการก่อนหน้าแทนวันนี้ — แก้บั๊กที่ Quick
    Update รันตอนดึก/เช้ามืดเวลาไทย (~03:00 ICT) ตรงกับ "ก่อนตลาด HK/JP เปิดของวันถัดไป"
    ตามเวลาท้องถิ่นพอดี ถ้าใช้ region_today_date ตรงๆ จะได้ expected=วันนี้ทั้งที่ตลาดยัง
    ไม่เปิดเทรดของวันนั้นเลย ทำให้แท่งปิดจริงของเมื่อวาน (ถูกต้องสมบูรณ์แล้ว) โดนตีความผิด
    ว่า stale แล้วไม่ถูกบันทึกลง DB (ดูจุดเรียกใช้ใน app.py _run_index_gap_update)"""
    from datetime import datetime, timedelta
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        return None
    cfg = _REGION_TZ.get(region)
    if not cfg:
        return None
    tz, open_min, close_min, pre_buf, post_buf = cfg
    now = datetime.now(ZoneInfo(tz))
    now_min = now.hour * 60 + now.minute
    d = now.date()
    if now_min < open_min - pre_buf:
        d -= timedelta(days=1)
    while d.weekday() >= 5:   # เสาร์=5, อาทิตย์=6
        d -= timedelta(days=1)
    return d


# DR / DRx static mapping — underlying foreign stock → Thai SET DR tickers
_DR_STATIC = [
    # ── United States ─────────────────────────────────────────────────────
    {"sym":'AAPL', "name":'Apple Inc.', "region":'US', "yf":"AAPL", "ind":'Consumer Electronics', "drs":["AAPL01", "AAPL03", "AAPL19", "AAPL80"]},
    {"sym":'APPL', "name":'AppLovin Corporation', "region":'US', "yf":"APP", "ind":'Mobile Advertising & AI', "drs":["APPL03"]},
    {"sym":'ABBV', "name":'AbbVie Inc.', "region":'US', "yf":"ABBV", "ind":'Biopharmaceuticals', "drs":["ABBV19", "ABBV80"]},
    {"sym":'ABNB', "name":'Airbnb, Inc.', "region":'US', "yf":"ABNB", "ind":'Travel & Hospitality', "drs":["ABNB06"]},
    {"sym":'ADBE', "name":'Adobe Inc.', "region":'US', "yf":"ADBE", "ind":'Creative & Document Cloud', "drs":["ADBE03", "ADBE06"]},
    {"sym":'AFRM', "name":'Affirm Holdings, Inc.', "region":'US', "yf":"AFRM", "ind":'Buy Now Pay Later', "drs":["AFRM03"]},
    {"sym":'AMAT', "name":'Applied Materials, Inc.', "region":'US', "yf":"AMAT", "ind":'Semiconductor Equipment', "drs":["AMAT23", "AMAT01", "AMAT19"]},
    {"sym":'AMD', "name":'Advanced Micro Devices, Inc.', "region":'US', "yf":"AMD", "ind":'AI Chips & Processors', "drs":["AMD03", "AMD23", "AMD80"]},
    {"sym":'AMGN', "name":'Amgen Inc.', "region":'US', "yf":"AMGN", "ind":'Biotechnology', "drs":["AMGN06"]},
    {"sym":'AMZN', "name":'Amazon.com, Inc.', "region":'US', "yf":"AMZN", "ind":'E-commerce & Cloud', "drs":["AMZN01", "AMZN03", "AMZN06", "AMZN23", "AMZN80", "AMZN19"]},
    {"sym":'ANET', "name":'Arista Networks, Inc.', "region":'US', "yf":"ANET", "ind":'Cloud Networking', "drs":["ANET23", "ANET80"]},
    {"sym":'APLD', "name":'Applied Digital Corp.', "region":'US', "yf":"APLD", "ind":'AI Data Centers', "drs":["APLD03"]},
    {"sym":'ASML', "name":'ASML Holding N.V.', "region":'US', "yf":"ASML", "ind":'Semiconductor Lithography', "drs":["ASML01"]},
    {"sym":'ASTS', "name":'AST SpaceMobile, Inc.', "region":'US', "yf":"ASTS", "ind":'Space-Based Broadband', "drs":["ASTS03", "ASTS01"]},
    {"sym":'AVGO', "name":'Broadcom Inc.', "region":'US', "yf":"AVGO", "ind":'Semiconductors & Software', "drs":["AVGO23", "AVGO80"]},
    {"sym":'AXP', "name":'American Express Company', "region":'US', "yf":"AXP", "ind":'Financial Services', "drs":["AXP06"]},
    {"sym":'BAC', "name":'Bank of America Corp.', "region":'US', "yf":"BAC", "ind":'Commercial Banking', "drs":["BAC03"]},
    {"sym":'BDX', "name":'Becton, Dickinson and Company', "region":'US', "yf":"BDX", "ind":'Medical Devices', "drs":["BDX06"]},
    {"sym":'BKNG', "name":'Booking Holdings Inc.', "region":'US', "yf":"BKNG", "ind":'Online Travel Services', "drs":["BKNG03", "BKNG80"]},
    {"sym":'BLK', "name":'BlackRock, Inc.', "region":'US', "yf":"BLK", "ind":'Asset Management', "drs":["BLK06"]},
    {"sym":'BOEING', "name":'The Boeing Company', "region":'US', "yf":"BA", "ind":'Aerospace & Defense', "drs":["BOEING80"]},
    {"sym":'BRKB', "name":'Berkshire Hathaway Inc. B', "region":'US', "yf":"BRK-B", "ind":'Diversified Conglomerate', "drs":["BRKB23", "BRKB80"]},
    {"sym":'CCJ', "name":'Cameco Corporation', "region":'US', "yf":"CCJ", "ind":'Uranium Mining', "drs":["CCJ23"]},
    {"sym":'CEG', "name":'Constellation Energy Corp.', "region":'US', "yf":"CEG", "ind":'Clean Energy & Nuclear', "drs":["CEG23"]},
    {"sym":'CME', "name":'CME Group Inc.', "region":'US', "yf":"CME", "ind":'Financial Exchanges', "drs":["CME03"]},
    {"sym":'COHR', "name":'Coherent Corp.', "region":'US', "yf":"COHR", "ind":'Photonics & Lasers', "drs":["COHR23"]},
    {"sym":'COIN', "name":'Coinbase Global, Inc.', "region":'US', "yf":"COIN", "ind":'Crypto Exchange', "drs":["COIN23", "COIN80", "COIN01"]},
    {"sym":'COSTCO', "name":'Costco Wholesale Corp.', "region":'US', "yf":"COST", "ind":'Membership Retail', "drs":["COSTCO19"]},
    {"sym":'CRM', "name":'Salesforce, Inc.', "region":'US', "yf":"CRM", "ind":'CRM & Enterprise SaaS', "drs":["CRM01", "CRM06", "CRM80"]},
    {"sym":'CRSP', "name":'CRISPR Therapeutics AG', "region":'US', "yf":"CRSP", "ind":'Gene Editing', "drs":["CRSP03"]},
    {"sym":'CRWD', "name":'CrowdStrike Holdings, Inc.', "region":'US', "yf":"CRWD", "ind":'Cybersecurity AI', "drs":["CRWD06", "CRWD80"]},
    {"sym":'CRWV', "name":'CoreWeave, Inc.', "region":'US', "yf":"CRWV", "ind":'AI Cloud Infrastructure', "drs":["CRWV03"]},
    {"sym":'CSCO', "name":'Cisco Systems, Inc.', "region":'US', "yf":"CSCO", "ind":'Networking & Security', "drs":["CSCO06"]},
    {"sym":'DASH', "name":'DoorDash, Inc.', "region":'US', "yf":"DASH", "ind":'Food Delivery Platform', "drs":["DASH03"]},
    {"sym":'DDOG', "name":'Datadog, Inc.', "region":'US', "yf":"DDOG", "ind":'Cloud Monitoring & APM', "drs":["DDOG19"]},
    {"sym":'DELL', "name":'Dell Technologies Inc.', "region":'US', "yf":"DELL", "ind":'PCs & Enterprise IT', "drs":["DELL19"]},
    {"sym":'DISNEY', "name":'The Walt Disney Company', "region":'US', "yf":"DIS", "ind":'Media & Entertainment', "drs":["DISNEY19"]},
    {"sym":'DUOL', "name":'Duolingo, Inc.', "region":'US', "yf":"DUOL", "ind":'EdTech & Language Learning', "drs":["DUOL06"]},
    {"sym":'EOSE', "name":'Eos Energy Enterprises, Inc.', "region":'US', "yf":"EOSE", "ind":'Energy Storage', "drs":["EOSE03"]},
    {"sym":'ESTEE', "name":'Estee Lauder Companies Inc.', "region":'US', "yf":"EL", "ind":'Prestige Beauty', "drs":["ESTEE80"]},
    {"sym":'EXPE', "name":'Expedia Group, Inc.', "region":'US', "yf":"EXPE", "ind":'Online Travel Platform', "drs":["EXPE06"]},
    {"sym":'FCX', "name":'Freeport-McMoRan Inc.', "region":'US', "yf":"FCX", "ind":'Copper & Gold Mining', "drs":["FCX23"]},
    {"sym":'FERRARI', "name":'Ferrari N.V.', "region":'US', "yf":"RACE", "ind":'Luxury Sports Cars', "drs":["FERRARI80"]},
    {"sym":'GDS', "name":'GDS Holdings Limited', "region":'US', "yf":"GDS", "ind":'China Data Centers', "drs":["GDS23"]},
    {"sym":'GEV', "name":'GE Vernova Inc.', "region":'US', "yf":"GEV", "ind":'Clean Power Equipment', "drs":["GEV23", "GEV80"]},
    {"sym":'GIGA', "name":'GigaDevice Semiconductor Inc.', "region":'HK', "yf":"3986.HK", "ind":'Memory & MCU Chips China', "drs":["GIGA23"]},
    {"sym":'GOLDUS', "name":'SPDR Gold Shares ETF', "region":'US', "yf":"GLD", "ind":'Gold ETF US', "drs":["GOLDUS03", "GOLDUS19", "GOLDUS80"]},
    {"sym":'GOOG', "name":'Alphabet Inc. Class C', "region":'US', "yf":"GOOG", "ind":'Search, AI & Advertising', "drs":["GOOG23", "GOOG80"]},
    {"sym":'GOOGL', "name":'Alphabet Inc. Class A', "region":'US', "yf":"GOOGL", "ind":'Search, AI & Advertising', "drs":["GOOGL01", "GOOGL03", "GOOGL19"]},
    {"sym":'GRAB', "name":'Grab Holdings Limited', "region":'US', "yf":"GRAB", "ind":'Super App Southeast Asia', "drs":["GRAB80"]},
    {"sym":'GSUS', "name":'The Goldman Sachs Group, Inc.', "region":'US', "yf":"GS", "ind":'Investment Banking', "drs":["GSUS06"]},
    {"sym":'HIMS', "name":'Hims & Hers Health, Inc.', "region":'US', "yf":"HIMS", "ind":'Telehealth & Wellness', "drs":["HIMS03"]},
    {"sym":'HOOD', "name":'Robinhood Markets, Inc.', "region":'US', "yf":"HOOD", "ind":'Commission-Free Trading', "drs":["HOOD03", "HOOD06", "HOOD80"]},
    {"sym":'IBM', "name":'IBM Corporation', "region":'US', "yf":"IBM", "ind":'Enterprise AI & Cloud', "drs":["IBM06"]},
    {"sym":'INTEL', "name":'Intel Corporation', "region":'US', "yf":"INTC", "ind":'Microprocessors & Foundry', "drs":["INTEL03", "INTEL23", "INTEL01", "INTEL19"]},
    {"sym":'IONQ', "name":'IonQ, Inc.', "region":'US', "yf":"IONQ", "ind":'Quantum Computing', "drs":["IONQ03", "IONQ23"]},
    {"sym":'ISRG', "name":'Intuitive Surgical, Inc.', "region":'US', "yf":"ISRG", "ind":'Robotic Surgery Systems', "drs":["ISRG01", "ISRG06", "ISRG19"]},
    {"sym":'JEPI', "name":'JPMorgan Premium Income ETF', "region":'US', "yf":"JEPI", "ind":'Covered Call Income ETF', "drs":["JEPI19"]},
    {"sym":'JGRO', "name":'JPMorgan Active Growth ETF', "region":'US', "yf":"JGRO", "ind":'Active Growth ETF', "drs":["JGRO19"]},
    {"sym":'JNJ', "name":'Johnson & Johnson', "region":'US', "yf":"JNJ", "ind":'Pharmaceuticals & MedTech', "drs":["JNJ03"]},
    {"sym":'KLAC', "name":'KLA Corporation', "region":'US', "yf":"KLAC", "ind":'Semiconductor Equipment', "drs":["KLAC23", "KLAC01", "KLAC19"]},
    {"sym":'KO', "name":'The Coca-Cola Company', "region":'US', "yf":"KO", "ind":'Beverages', "drs":["KO80"]},
    {"sym":'LITE', "name":'Lumentum Holdings Inc.', "region":'US', "yf":"LITE", "ind":'Optical & Photonic Products', "drs":["LITE23", "LITE01"]},
    {"sym":'LLY', "name":'Eli Lilly and Company', "region":'US', "yf":"LLY", "ind":'Pharmaceuticals', "drs":["LLY23", "LLY80"]},
    {"sym":'LRCX', "name":'Lam Research Corporation', "region":'US', "yf":"LRCX", "ind":'Etch & Deposition Systems', "drs":["LRCX23", "LRCX01", "LRCX19"]},
    {"sym":'LULU', "name":'Lululemon Athletica Inc.', "region":'US', "yf":"LULU", "ind":'Athletic Apparel', "drs":["LULU06"]},
    {"sym":'MA', "name":'Mastercard Incorporated', "region":'US', "yf":"MA", "ind":'Payment Processing', "drs":["MA80"]},
    {"sym":'MELI', "name":'MercadoLibre, Inc.', "region":'US', "yf":"MELI", "ind":'Latin America E-commerce', "drs":["MELI06", "MELI23"]},
    {"sym":'META', "name":'Meta Platforms, Inc.', "region":'US', "yf":"META", "ind":'Social Media & AI', "drs":["META01", "META06", "META23", "META80"]},
    {"sym":'MICRON', "name":'Micron Technology, Inc.', "region":'US', "yf":"MU", "ind":'Memory Semiconductors', "drs":["MICRON01", "MICRON03", "MICRON19", "MICRON23", "MICRON80"]},
    {"sym":'MNSO', "name":'MINISO Group Holding Ltd.', "region":'US', "yf":"MNSO", "ind":'Value Lifestyle Retail', "drs":["MNSO80"]},
    {"sym":'MNST', "name":'Monster Beverage Corporation', "region":'US', "yf":"MNST", "ind":'Energy Drinks', "drs":["MNST06"]},
    {"sym":'MP', "name":'MP Materials Corp.', "region":'US', "yf":"MP", "ind":'Rare Earth Materials', "drs":["MP23", "MP80"]},
    {"sym":'MRVL', "name":'Marvell Technology, Inc.', "region":'US', "yf":"MRVL", "ind":'Custom AI Silicon', "drs":["MRVL06", "MRVL23", "MRVL80"]},
    {"sym":'MS', "name":'Morgan Stanley', "region":'US', "yf":"MS", "ind":'Investment Banking', "drs":["MS06"]},
    {"sym":'MSFT', "name":'Microsoft Corporation', "region":'US', "yf":"MSFT", "ind":'Software, Cloud & AI', "drs":["MSFT01", "MSFT03", "MSFT06", "MSFT19", "MSFT23", "MSFT80"]},
    {"sym":'NBIS', "name":'Nebius Group N.V.', "region":'US', "yf":"NBIS", "ind":'AI Cloud Infrastructure', "drs":["NBIS03", "NBIS23", "NBIS01"]},
    {"sym":'NDAQ', "name":'Nasdaq, Inc.', "region":'US', "yf":"NDAQ", "ind":'Financial Exchanges', "drs":["NDAQ06"]},
    {"sym":'NEE', "name":'NextEra Energy, Inc.', "region":'US', "yf":"NEE", "ind":'Clean Energy Utilities', "drs":["NEE80"]},
    {"sym":'NEM', "name":'Newmont Corporation', "region":'US', "yf":"NEM", "ind":'Gold & Copper Mining', "drs":["NEM06", "NEM23"]},
    {"sym":'NET', "name":'Cloudflare, Inc.', "region":'US', "yf":"NET", "ind":'Network Security & CDN', "drs":["NET03"]},
    {"sym":'NFLX', "name":'Netflix, Inc.', "region":'US', "yf":"NFLX", "ind":'Entertainment Streaming', "drs":["NFLX06", "NFLX80"]},
    {"sym":'NIKE', "name":'NIKE, Inc.', "region":'US', "yf":"NKE", "ind":'Athletic Footwear & Apparel', "drs":["NIKE80"]},
    {"sym":'NOW', "name":'ServiceNow, Inc.', "region":'US', "yf":"NOW", "ind":'Enterprise SaaS Platform', "drs":["NOW19"]},
    {"sym":'NVDA', "name":'NVIDIA Corporation', "region":'US', "yf":"NVDA", "ind":'AI Chips & GPUs', "drs":["NVDA01", "NVDA03", "NVDA06", "NVDA19", "NVDA23", "NVDA80"]},
    {"sym":'NVTS', "name":'Navitas Semiconductor Corp.', "region":'US', "yf":"NVTS", "ind":'GaN Power Semiconductors', "drs":["NVTS03", "NVTS23"]},
    {"sym":'ON', "name":'ON Semiconductor Corp.', "region":'US', "yf":"ON", "ind":'Power & Signal Management', "drs":["ON23"]},
    {"sym":'ONON', "name":'On Holding AG', "region":'US', "yf":"ONON", "ind":'Performance Running Shoes', "drs":["ONON03"]},
    {"sym":'ORCL', "name":'Oracle Corporation', "region":'US', "yf":"ORCL", "ind":'Enterprise Software & DB', "drs":["ORCL01", "ORCL06", "ORCL19"]},
    {"sym":'PANW', "name":'Palo Alto Networks, Inc.', "region":'US', "yf":"PANW", "ind":'Cybersecurity Platform', "drs":["PANW80", "PANW19"]},
    {"sym":'PEP', "name":'PepsiCo, Inc.', "region":'US', "yf":"PEP", "ind":'Beverages & Snack Foods', "drs":["PEP80"]},
    {"sym":'PFIZER', "name":'Pfizer Inc.', "region":'US', "yf":"PFE", "ind":'Biopharmaceuticals', "drs":["PFIZER19"]},
    {"sym":'PLTR', "name":'Palantir Technologies Inc.', "region":'US', "yf":"PLTR", "ind":'AI & Big Data Analytics', "drs":["PLTR01", "PLTR03", "PLTR06", "PLTR23"]},
    {"sym":'PYPL', "name":'PayPal Holdings, Inc.', "region":'US', "yf":"PYPL", "ind":'Digital Payments', "drs":["PYPL06"]},
    {"sym":'QCOM', "name":'Qualcomm Inc.', "region":'US', "yf":"QCOM", "ind":'Wireless Semiconductors', "drs":["QCOM06"]},
    {"sym":'QQQM', "name":'Invesco Nasdaq 100 ETF', "region":'US', "yf":"QQQM", "ind":'Nasdaq 100 ETF', "drs":["QQQM19"]},
    {"sym":'RBLX', "name":'Roblox Corporation', "region":'US', "yf":"RBLX", "ind":'Metaverse Gaming Platform', "drs":["RBLX06"]},
    {"sym":'REMX', "name":'VanEck Rare Earth ETF', "region":'US', "yf":"REMX", "ind":'Rare Earth Metals ETF', "drs":["REMX03"]},
    {"sym":'RGTI', "name":'Rigetti Computing, Inc.', "region":'US', "yf":"RGTI", "ind":'Quantum Computing', "drs":["RGTI03"]},
    {"sym":'RKLB', "name":'Rocket Lab USA, Inc.', "region":'US', "yf":"RKLB", "ind":'Space Systems & Aerospace', "drs":["RKLB03", "RKLB23", "RKLB80", "RKLB01"]},
    {"sym":'SBUX', "name":'Starbucks Corporation', "region":'US', "yf":"SBUX", "ind":'Global Coffeehouse Chain', "drs":["SBUX80"]},
    {"sym":'SEAGATE', "name":'Seagate Technology Holdings', "region":'US', "yf":"STX", "ind":'Hard Disk Drives & Storage', "drs":["SEAGATE23"]},
    {"sym":'SHOP', "name":'Shopify Inc.', "region":'US', "yf":"SHOP", "ind":'E-commerce Platform', "drs":["SHOP03", "SHOP06"]},
    {"sym":'SIL', "name":'Global X Silver Miners ETF', "region":'US', "yf":"SIL", "ind":'Silver Mining ETF', "drs":["SIL03"]},
    {"sym":'SMCI', "name":'Super Micro Computer, Inc.', "region":'US', "yf":"SMCI", "ind":'AI Server Systems', "drs":["SMCI03"]},
    {"sym":'SNDK', "name":'SanDisk Corporation', "region":'US', "yf":"SNDK", "ind":'Flash Storage Solutions', "drs":["SNDK03", "SNDK23", "SNDK80"]},
    {"sym":'SNOW', "name":'Snowflake Inc.', "region":'US', "yf":"SNOW", "ind":'Cloud Data Platform', "drs":["SNOW06", "SNOW23"]},
    {"sym":'SOFI', "name":'SoFi Technologies, Inc.', "region":'US', "yf":"SOFI", "ind":'Digital Financial Services', "drs":["SOFI23"]},
    {"sym":'SP500US', "name":'SPDR Portfolio S&P 500 ETF', "region":'US', "yf":"SPYM", "ind":'S&P 500 ETF', "drs":["SP500US19", "SP500US80"]},
    {"sym":'SPBOND', "name":'SPDR Portfolio Aggregate Bond ETF', "region":'US', "yf":"SPAB", "ind":'Bond ETF', "drs":["SPBOND80"]},
    {"sym":'SPCOM', "name":'SPDR Communication Svc ETF', "region":'US', "yf":"XLC", "ind":'Comm Services ETF', "drs":["SPCOM80"]},
    {"sym":'SPENGY', "name":'SPDR Energy Sector ETF', "region":'US', "yf":"XLE", "ind":'Energy Sector ETF', "drs":["SPENGY80"]},
    {"sym":'SPFIN', "name":'SPDR Financial Sector ETF', "region":'US', "yf":"XLF", "ind":'Financial Sector ETF', "drs":["SPFIN80"]},
    {"sym":'SPHLTH', "name":'SPDR Healthcare Sector ETF', "region":'US', "yf":"XLV", "ind":'Healthcare Sector ETF', "drs":["SPHLTH80"]},
    {"sym":'SPOT', "name":'Spotify Technology S.A.', "region":'US', "yf":"SPOT", "ind":'Music Streaming', "drs":["SPOT06"]},
    {"sym":'SPTECH', "name":'SPDR Technology Sector ETF', "region":'US', "yf":"XLK", "ind":'Technology Sector ETF', "drs":["SPTECH80"]},
    {"sym":'SYNP', "name":'Synopsys, Inc.', "region":'US', "yf":"SNPS", "ind":'EDA & IP Software', "drs":["SYNP03", "SYNP23"]},
    {"sym":'TEL', "name":'Tokyo Electron Ltd.', "region":'JP', "yf":"8035.T", "ind":'Semiconductor Production Equipment', "drs":["TEL23", "TEL80"]},
    {"sym":'TER', "name":'Teradyne, Inc.', "region":'US', "yf":"TER", "ind":'Automated Test Equipment', "drs":["TER23", "TER01"]},
    {"sym":'TME', "name":'Tencent Music Entertainment', "region":'US', "yf":"TME", "ind":'Music Streaming China', "drs":["TME23"]},
    {"sym":'TRIPCOM', "name":'Trip.com Group Limited', "region":'US', "yf":"TCOM", "ind":'Online Travel China', "drs":["TRIPCOM23", "TRIPCOM80"]},
    {"sym":'TRVUS', "name":'Travelers Companies, Inc.', "region":'US', "yf":"TRV", "ind":'Property & Casualty Insurance', "drs":["TRVUS06"]},
    {"sym":'TSLA', "name":'Tesla, Inc.', "region":'US', "yf":"TSLA", "ind":'Electric Vehicles & Energy', "drs":["TSLA01", "TSLA03", "TSLA23", "TSLA80"]},
    {"sym":'UBER', "name":'Uber Technologies, Inc.', "region":'US', "yf":"UBER", "ind":'Ride-Hailing & Delivery', "drs":["UBER06"]},
    {"sym":'UNH', "name":'UnitedHealth Group Inc.', "region":'US', "yf":"UNH", "ind":'Health Insurance & Services', "drs":["UNH19"]},
    {"sym":'USTR', "name":'US Treasury Bond ETF', "region":'HK', "yf":"3450.HK", "ind":'US Treasury ETF', "drs":["USTR24"]},
    {"sym":'VISA', "name":'Visa Inc.', "region":'US', "yf":"V", "ind":'Payment Technology', "drs":["VISA06", "VISA80"]},
    {"sym":'VRT', "name":'Vertiv Holdings Co', "region":'US', "yf":"VRT", "ind":'Data Center Infrastructure', "drs":["VRT23", "VRT01"]},
    {"sym":'VT', "name":'Vanguard Total World ETF', "region":'US', "yf":"VT", "ind":'Global Equity ETF', "drs":["VT03"]},
    {"sym":'WMT', "name":'Walmart Inc.', "region":'US', "yf":"WMT", "ind":'Omnichannel Retail', "drs":["WMT06"]},
    {"sym":'WORLD', "name":'CSOP World ETF (HK)', "region":'HK', "yf":"3422.HK", "ind":'Global Equity ETF', "drs":["WORLD03"]},
    {"sym":'WORLDA', "name":'iShares MSCI World ETF (Milan)', "region":'EU', "yf":"SMSWLD.MI", "ind":'Global Equity ETF', "drs":["WORLDA01"]},
    {"sym":'ALAB', "name":'Astera Labs, Inc.', "region":'US', "yf":"ALAB", "ind":'AI Connectivity Semiconductors', "drs":["ALAB01"]},
    {"sym":'CAT', "name":'Caterpillar Inc.', "region":'US', "yf":"CAT", "ind":'Construction & Mining Equipment', "drs":["CAT19"]},
    {"sym":'CDNS', "name":'Cadence Design Systems, Inc.', "region":'US', "yf":"CDNS", "ind":'EDA & Chip Design Software', "drs":["CDNS23"]},
    {"sym":'CRDO', "name":'Credo Technology Group Holding Ltd', "region":'US', "yf":"CRDO", "ind":'High-Speed Connectivity Chips', "drs":["CRDO23"]},
    {"sym":'ETN', "name":'Eaton Corporation plc', "region":'US', "yf":"ETN", "ind":'Power Management & Electrical', "drs":["ETN23"]},
    {"sym":'FABRINET', "name":'Fabrinet', "region":'US', "yf":"FN", "ind":'Optical & Electronic Manufacturing', "drs":["FABRINET23"]},
    {"sym":'MPWR', "name":'Monolithic Power Systems, Inc.', "region":'US', "yf":"MPWR", "ind":'Power Semiconductors', "drs":["MPWR23"]},
    {"sym":'OKLO', "name":'Oklo Inc.', "region":'US', "yf":"OKLO", "ind":'Nuclear SMR Energy', "drs":["OKLO23"]},
    {"sym":'SYM', "name":'Symbotic Inc.', "region":'US', "yf":"SYM", "ind":'Warehouse Automation & AI Robotics', "drs":["SYM23"]},
    {"sym":'TSEMI', "name":'Tower Semiconductor Ltd.', "region":'US', "yf":"TSEM", "ind":'Analog Chip Foundry', "drs":["TSEMI23"]},
    {"sym":'GOLDM', "name":'SPDR Gold MiniShares Trust', "region":'US', "yf":"GLDM", "ind":'Gold ETF (US)', "drs":["GOLDM01"]},
    # ── Hong Kong / China ──────────────────────────────────────────────────
    {"sym":'AIA', "name":'AIA Group Limited', "region":'HK', "yf":"1299.HK", "ind":'Life Insurance APAC', "drs":["AIA06", "AIA19", "AIA23"]},
    {"sym":'ANTA', "name":'Anta Sports Products Ltd.', "region":'HK', "yf":"2020.HK", "ind":'Sportswear & Footwear', "drs":["ANTA13", "ANTA23"]},
    {"sym":'ASEMI', "name":'Asia Semiconductor ETF', "region":'HK', "yf":"3119.HK", "ind":'Asia Semiconductor ETF', "drs":["ASEMI23", "ASEMI24"]},
    {"sym":'BABA', "name":'Alibaba Group Holding Ltd.', "region":'HK', "yf":"9988.HK", "ind":'E-commerce & Cloud', "drs":["BABA01", "BABA06", "BABA13", "BABA23", "BABA80", "BABA19"]},
    {"sym":'BIDU', "name":'Baidu, Inc.', "region":'HK', "yf":"9888.HK", "ind":'AI & Chinese Search', "drs":["BIDU01", "BIDU06", "BIDU23", "BIDU80"]},
    {"sym":'BILIBILI', "name":'Bilibili Inc.', "region":'HK', "yf":"9626.HK", "ind":'Online Video & Gaming', "drs":["BILIBILI01"]},
    {"sym":'BIREN', "name":'Shanghai Biren Tech Co., Ltd.', "region":'HK', "yf":"6082.HK", "ind":'AI GPU Chips China', "drs":["BIREN23"]},
    {"sym":'BYDCOM', "name":'BYD Company Limited', "region":'HK', "yf":"1211.HK", "ind":'EV & Battery Manufacturing', "drs":["BYDCOM01", "BYDCOM80"]},
    {"sym":'CAMBRI', "name":'Cambricon Technologies Corporation Limited', "region":'CN', "yf":"688256.SS", "ind":'AI Chips China', "drs":["CAMBRI80"]},
    {"sym":'CATL', "name":'Contemporary Amperex Technology', "region":'HK', "yf":"3750.HK", "ind":'EV Battery Systems', "drs":["CATL01", "CATL23", "CATL80"]},
    {"sym":'CHHONGQ', "name":'China Hongqiao Group Ltd.', "region":'HK', "yf":"1378.HK", "ind":'Aluminium Producer China', "drs":["CHHONGQ19"]},
    {"sym":'CHMOBILE', "name":'China Mobile Limited', "region":'HK', "yf":"0941.HK", "ind":'Telecom Services China', "drs":["CHMOBILE19", "CHMOBILE23"]},
    {"sym":'CHNXT', "name":'CSOP China NextGen ETF', "region":'CN', "yf":"159682.SZ", "ind":'China Next-Gen Leaders ETF', "drs":["CHNXT5023"]},
    {"sym":'CMBANK', "name":'China Merchants Bank', "region":'HK', "yf":"3968.HK", "ind":'Commercial Banking China', "drs":["CMBANK23"]},
    {"sym":'CN', "name":'CSI 300 Index ETF (HK)', "region":'HK', "yf":"3188.HK", "ind":'China Broad Market ETF', "drs":["CN01", "CN23"]},
    {"sym":'CNBIO', "name":'China Biotech ETF', "region":'HK', "yf":"2820.HK", "ind":'China Biotech ETF', "drs":["CNBIO24"]},
    {"sym":'CNEV', "name":'Global X China EV & Battery ETF', "region":'HK', "yf":"2845.HK", "ind":'China EV ETF', "drs":["CNEV24"]},
    {"sym":'CNRE', "name":'China Northern Rare Earth Group', "region":'CN', "yf":"600111.SS", "ind":'Rare Earth Materials China', "drs":["CNRE80"]},
    {"sym":'CNROBOAI', "name":'Global X China Robotics & AI ETF', "region":'HK', "yf":"2807.HK", "ind":'China Robotics ETF', "drs":["CNROBOAI23"]},
    {"sym":'CNSEMI', "name":'China Semiconductor ETF', "region":'HK', "yf":"3191.HK", "ind":'China Semiconductor ETF', "drs":["CNSEMI23"]},
    {"sym":'CNSTAR', "name":'China AMC SSE STAR 50 ETF', "region":'CN', "yf":"588000.SS", "ind":'China STAR Market ETF', "drs":["CNSTAR5023"]},
    {"sym":'CNTECH', "name":'HS China Technology ETF', "region":'HK', "yf":"3088.HK", "ind":'China Technology ETF', "drs":["CNTECH01"]},
    {"sym":'CYPC', "name":'China Yangtze Power', "region":'CN', "yf":"600900.SS", "ind":'Hydropower China', "drs":["CYPC80"]},
    {"sym":'GAC', "name":'Guangzhou Automobile Group', "region":'HK', "yf":"2238.HK", "ind":'Automotive Group China', "drs":["GAC03"]},
    {"sym":'GANFENG', "name":'Ganfeng Lithium Group', "region":'HK', "yf":"1772.HK", "ind":'Lithium Mining & Refining', "drs":["GANFENG23"]},
    {"sym":'GEELY', "name":'Geely Automobile Holdings', "region":'HK', "yf":"0175.HK", "ind":'Smart EVs China', "drs":["GEELY06", "GEELY80"]},
    {"sym":'GOLD', "name":'SPDR Gold ETF (HK)', "region":'HK', "yf":"2840.HK", "ind":'Gold ETF (HK)', "drs":["GOLD03", "GOLD19"]},
    {"sym":'GSEMI', "name":'Global X Semiconductor ETF', "region":'JP', "yf":"2243.T", "ind":'Semiconductor ETF Japan', "drs":["GSEMI24"]},
    {"sym":'HAIERS', "name":'Haier Smart Home Co., Ltd.', "region":'HK', "yf":"6690.HK", "ind":'Home Appliances AI', "drs":["HAIERS19"]},
    {"sym":'HANSOH', "name":'Hansoh Pharmaceutical Group', "region":'HK', "yf":"3692.HK", "ind":'Pharmaceuticals China', "drs":["HANSOH19"]},
    {"sym":'HK', "name":'Hang Seng ETF (Tracker Fund)', "region":'HK', "yf":"2800.HK", "ind":'HK Broad Market ETF', "drs":["HK01", "HK13"]},
    {"sym":'HKCE', "name":'HSCEI ETF (China Enterprises)', "region":'HK', "yf":"2828.HK", "ind":'China Enterprise ETF', "drs":["HKCE01"]},
    {"sym":'HKEX', "name":'HK Exchanges & Clearing Ltd.', "region":'HK', "yf":"0388.HK", "ind":'Financial Exchange HK', "drs":["HKEX23"]},
    {"sym":'HKTECH', "name":'Hang Seng TECH Index ETF', "region":'HK', "yf":"3032.HK", "ind":'HK Tech ETF', "drs":["HKTECH13"]},
    {"sym":'HORIZON', "name":'Horizon Robotics, Inc.', "region":'HK', "yf":"9660.HK", "ind":'Automotive AI Chips', "drs":["HORIZON23"]},
    {"sym":'HSHD', "name":'Hang Seng High Dividend ETF', "region":'HK', "yf":"3110.HK", "ind":'HK High Dividend ETF', "drs":["HSHD23"]},
    {"sym":'HUAHONG', "name":'Hua Hong Semiconductor', "region":'HK', "yf":"1347.HK", "ind":'Foundry Services China', "drs":["HUAHONG23"]},
    {"sym":'HYGON', "name":'Hygon Information Technology Co., Ltd.', "region":'CN', "yf":"688041.SS", "ind":'China CPU/GPU Semiconductors (STAR Market)', "drs":["HYGON80"]},
    {"sym":'ICBC', "name":'Industrial & Commercial Bank', "region":'HK', "yf":"1398.HK", "ind":'State Banking China', "drs":["ICBC06", "ICBC19"]},
    {"sym":'IFLYTEK', "name":'iFLYTEK Co., Ltd.', "region":'CN', "yf":"002230.SZ", "ind":'AI Voice Technology', "drs":["IFLYTEK80"]},
    {"sym":'INDIA', "name":'CSOP India Technology ETF', "region":'HK', "yf":"3404.HK", "ind":'India Technology ETF', "drs":["INDIA01"]},
    {"sym":'JAP', "name":'CSOP Japan ETF', "region":'HK', "yf":"3150.HK", "ind":'Japan Market ETF (HK)', "drs":["JAP03"]},
    {"sym":'JAPAN1000', "name":'HS Japan Topix 100 IDX ETF', "region":'HK', "yf":"3410.HK", "ind":'Japan Topix 100 ETF (HK)', "drs":["JAPAN10001"]},
    {"sym":'JAPAN', "name":'ChinaAMC MSCI Japan Hedged to USD ETF', "region":'HK', "yf":"3160.HK", "ind":'Japan MSCI Hedged ETF (HK)', "drs":["JAPAN13"]},
    {"sym":'JD', "name":'JD.com, Inc.', "region":'HK', "yf":"9618.HK", "ind":'E-commerce Logistics China', "drs":["JD80"]},
    {"sym":'JDHEAL', "name":'JD Health International Inc.', "region":'HK', "yf":"6618.HK", "ind":'Healthcare E-commerce', "drs":["JDHEAL19"]},
    {"sym":'JLMAG', "name":'JL Mag Rare-Earth Co., Ltd.', "region":'HK', "yf":"6680.HK", "ind":'Rare Earth Magnets', "drs":["JLMAG80"]},
    {"sym":'KINGSOFT', "name":'Kingsoft Corporation Limited', "region":'HK', "yf":"3888.HK", "ind":'Software & Cloud China', "drs":["KINGSOFT23"]},
    {"sym":'KUAISH', "name":'Kuaishou Technology', "region":'HK', "yf":"1024.HK", "ind":'Short-Video Platform China', "drs":["KUAISH01", "KUAISH06", "KUAISH23", "KUAISH80"]},
    {"sym":'LENOVO', "name":'Lenovo Group Limited', "region":'HK', "yf":"0992.HK", "ind":'PCs & Smart Devices', "drs":["LENOVO13"]},
    {"sym":'LPGOLD', "name":'Laopu Gold Co., Ltd.', "region":'HK', "yf":"6181.HK", "ind":'Gold Jewelry China', "drs":["LPGOLD13"]},
    {"sym":'MAOGEP', "name":'Mao Geping Cosmetics Co.', "region":'HK', "yf":"1318.HK", "ind":'Premium Cosmetics China', "drs":["MAOGEP80"]},
    {"sym":'MEITUAN', "name":'Meituan', "region":'HK', "yf":"3690.HK", "ind":'Food Delivery & Local Services', "drs":["MEITUAN19", "MEITUAN23", "MEITUAN80"]},
    {"sym":'MIDEA', "name":'Midea Group Co., Ltd.', "region":'CN', "yf":"000333.SZ", "ind":'Home Appliances China', "drs":["MIDEA80"]},
    {"sym":'MIXUE', "name":'Mixue Group', "region":'HK', "yf":"2097.HK", "ind":'Budget Ice Cream & Tea', "drs":["MIXUE80"]},
    {"sym":'MONTAGE', "name":'Montage Technology Co., Ltd.', "region":'HK', "yf":"6809.HK", "ind":'Memory Interface Chips', "drs":["MONTAGE80"]},
    {"sym":'MOUTAI', "name":'Kweichow Moutai Co., Ltd.', "region":'CN', "yf":"600519.SS", "ind":'Premium Baijiu Distiller', "drs":["MOUTAI80"]},
    {"sym":'NAURA', "name":'NAURA Technology Group', "region":'CN', "yf":"002371.SZ", "ind":'Semiconductor Equipment', "drs":["NAURA23", "NAURA80"]},
    {"sym":'NDX', "name":'CSOP Nasdaq-100 ETF (HK)', "region":'HK', "yf":"3086.HK", "ind":'Nasdaq 100 ETF (HK)', "drs":["NDX01"]},
    {"sym":'NETEASE', "name":'NetEase, Inc.', "region":'HK', "yf":"9999.HK", "ind":'Internet & Online Gaming', "drs":["NETEASE80"]},
    {"sym":'NONGFU', "name":'Nongfu Spring Co., Ltd.', "region":'HK', "yf":"9633.HK", "ind":'Bottled Water & Beverages', "drs":["NONGFU80"]},
    {"sym":'OIL', "name":'CSOP Crude Oil ETF', "region":'HK', "yf":"3097.HK", "ind":'Crude Oil ETF (HK)', "drs":["OIL03", "OIL24"]},
    {"sym":'PETROCN', "name":'PetroChina Company Limited', "region":'HK', "yf":"0857.HK", "ind":'Oil & Gas China', "drs":["PETROCN80"]},
    {"sym":'PINGAN', "name":'Ping An Insurance Group', "region":'HK', "yf":"2318.HK", "ind":'Financial & Insurance', "drs":["PINGAN01", "PINGAN80"]},
    {"sym":'POPMART', "name":'Pop Mart International Group', "region":'HK', "yf":"9992.HK", "ind":'Collectible Toys & IP', "drs":["POPMART23", "POPMART80"]},
    {"sym":'SENSE', "name":'SenseTime Group Inc.', "region":'HK', "yf":"0020.HK", "ind":'AI & Computer Vision', "drs":["SENSE23"]},
    {"sym":'SINOBIO', "name":'Sino Biopharmaceutical', "region":'HK', "yf":"1177.HK", "ind":'Biopharmaceuticals China', "drs":["SINOBIO19"]},
    {"sym":'SMIC', "name":'SMIC (Semiconductor Mfg. Intl)', "region":'HK', "yf":"0981.HK", "ind":'Foundry Services China', "drs":["SMIC01", "SMIC03", "SMIC13", "SMIC23"]},
    {"sym":'SP', "name":'CSOP S&P 500 ETF (HK)', "region":'HK', "yf":"3195.HK", "ind":'S&P 500 ETF (HK)', "drs":["SP50001"]},
    {"sym":'STAR', "name":'CSOP STAR 50 ETF (HK)', "region":'HK', "yf":"3151.HK", "ind":'STAR Market ETF (HK)', "drs":["STAR5001"]},
    {"sym":'STEG', "name":'Singapore Technologies Engineering', "region":'SG', "yf":"S63.SI", "ind":'Defense & Engineering Singapore', "drs":["STEG19"]},
    {"sym":'SUNNY', "name":'Sunny Optical Technology', "region":'HK', "yf":"2382.HK", "ind":'Optics & Camera Modules', "drs":["SUNNY19", "SUNNY80"]},
    {"sym":'TENCENT', "name":'Tencent Holdings Limited', "region":'HK', "yf":"0700.HK", "ind":'Internet & Gaming China', "drs":["TENCENT01", "TENCENT06", "TENCENT13", "TENCENT19", "TENCENT23", "TENCENT80"]},
    {"sym":'UBTECH', "name":'UBTECH Robotics Corp.', "region":'HK', "yf":"9880.HK", "ind":'Humanoid Robotics', "drs":["UBTECH23"]},
    {"sym":'WUXI', "name":'WuXi Biologics (Cayman) Inc.', "region":'HK', "yf":"2269.HK", "ind":'Biologics CDMO', "drs":["WUXI06", "WUXI13"]},
    {"sym":'WUXIAT', "name":'WuXi AppTec Co., Ltd.', "region":'HK', "yf":"2359.HK", "ind":'Pharma R&D Services', "drs":["WUXIAT80"]},
    {"sym":'XIAOMI', "name":'Xiaomi Corporation', "region":'HK', "yf":"1810.HK", "ind":'Consumer Electronics & EVs', "drs":["XIAOMI01", "XIAOMI13", "XIAOMI19", "XIAOMI23", "XIAOMI80"]},
    {"sym":'XPENG', "name":'XPeng Inc.', "region":'HK', "yf":"9868.HK", "ind":'Smart EVs China', "drs":["XPENG03"]},
    {"sym":'ZAI', "name":'Knowledge Atlas Technology JSC Ltd.', "region":'HK', "yf":"2513.HK", "ind":'AI & Knowledge Graph China', "drs":["ZAI23"]},
    {"sym":'ZIJIN', "name":'Zijin Mining Group', "region":'HK', "yf":"2899.HK", "ind":'Gold & Copper Mining', "drs":["ZIJIN13", "ZIJIN23", "ZIJIN80"]},
    {"sym":'ZJINNO', "name":'Zhongji Innolight Co.', "region":'CN', "yf":"300308.SZ", "ind":'Optical Transceivers', "drs":["ZJINNO80"]},
    {"sym":'BONDUS', "name":'Premia US Treasury Floating Rate ETF (Acc)', "region":'HK', "yf":"9078.HK", "ind":'US Treasury Floating Rate ETF (HK)', "drs":["BONDUS01"]},
    # ── Japan ──────────────────────────────────────────────────────────────
    {"sym":'ASICS', "name":'ASICS Corporation', "region":'JP', "yf":"7936.T", "ind":'Sportswear & Running Shoes', "drs":["ASICS23"]},
    {"sym":'DISCO', "name":'DISCO Corporation', "region":'JP', "yf":"6146.T", "ind":'Precision Cutting Systems', "drs":["DISCO24"]},
    {"sym":'FANUC', "name":'FANUC Corporation', "region":'JP', "yf":"6954.T", "ind":'Factory Automation & Robots', "drs":["FANUC23"]},
    {"sym":'HITACHI', "name":'Hitachi, Ltd.', "region":'JP', "yf":"6501.T", "ind":'Digital Infrastructure', "drs":["HITACHI24"]},
    {"sym":'HONDA', "name":'Honda Motor Co., Ltd.', "region":'JP', "yf":"7267.T", "ind":'Automobiles & Motorcycles', "drs":["HONDA19"]},
    {"sym":'ITOCHU', "name":'ITOCHU Corporation', "region":'JP', "yf":"8001.T", "ind":'Diversified Trading House', "drs":["ITOCHU19"]},
    {"sym":'JPANIME', "name":'Global X Japan Games & Animation ETF', "region":'JP', "yf":"2640.T", "ind":'Japan Anime Thematic ETF', "drs":["JPANIME24"]},
    {"sym":'JPMUS', "name":'JPMorgan Chase & Co.', "region":'US', "yf":"JPM", "ind":'Global Banking', "drs":["JPMUS06", "JPMUS19"]},
    {"sym":'JPROBOAI', "name":'Global X Japan Robotics & AI ETF', "region":'JP', "yf":"2638.T", "ind":'Japan Robotics ETF', "drs":["JPROBOAI24"]},
    {"sym":'JPSEMI', "name":'Global X Japan Semiconductor ETF', "region":'JP', "yf":"2644.T", "ind":'Japan Semiconductor ETF', "drs":["JPSEMI24"]},
    {"sym":'JTEK', "name":'JPMorgan US Tech Leaders ETF', "region":'US', "yf":"JTEK", "ind":'US Tech Leaders ETF', "drs":["JTEK19"]},
    {"sym":'KEYENCE', "name":'KEYENCE Corporation', "region":'JP', "yf":"6861.T", "ind":'Factory Automation Sensors', "drs":["KEYENCE23"]},
    {"sym":'KIOXIA', "name":'Kioxia Holdings Corporation', "region":'JP', "yf":"285A.T", "ind":'NAND Flash Memory', "drs":["KIOXIA23"]},
    {"sym":'KONAMI', "name":'Konami Group Corporation', "region":'JP', "yf":"9766.T", "ind":'Video Games & Arcades', "drs":["KONAMI24"]},
    {"sym":'MITSU', "name":'Mitsubishi Heavy Industries, Ltd.', "region":'JP', "yf":"7011.T", "ind":'Heavy Industry & Defense Japan', "drs":["MITSU19"]},
    {"sym":'MUFG', "name":'Mitsubishi UFJ Financial Group', "region":'JP', "yf":"8306.T", "ind":'Banking & Financial Services', "drs":["MUFG19", "MUFG23"]},
    {"sym":'NIKKEI', "name":'Nikkei 225 Index ETF', "region":'JP', "yf":"1321.T", "ind":'Japan Broad Market ETF', "drs":["NIKKEI80"]},
    {"sym":'NINTENDO', "name":'Nintendo Co., Ltd.', "region":'JP', "yf":"7974.T", "ind":'Video Games & Consoles', "drs":["NINTENDO19", "NINTENDO23"]},
    {"sym":'SANRIO', "name":'Sanrio Company, Ltd.', "region":'JP', "yf":"8136.T", "ind":'Character Entertainment', "drs":["SANRIO23", "SANRIO80"]},
    {"sym":'SMFG', "name":'Sumitomo Mitsui Financial Grp', "region":'JP', "yf":"8316.T", "ind":'Banking & Securities Japan', "drs":["SMFG19"]},
    {"sym":'SOFTBANK', "name":'SoftBank Group Corp.', "region":'JP', "yf":"9984.T", "ind":'Tech Investment & Telecom', "drs":["SOFTBANK23", "SOFTBANK80"]},
    {"sym":'SONY', "name":'Sony Group Corporation', "region":'JP', "yf":"6758.T", "ind":'Electronics & Entertainment', "drs":["SONY80"]},
    {"sym":'SUSHI', "name":'Food & Life Companies (Sushiro)', "region":'JP', "yf":"3563.T", "ind":'Conveyor-belt Sushi Chain', "drs":["SUSHI23"]},
    {"sym":'TOYOTA', "name":'Toyota Motor Corporation', "region":'JP', "yf":"7203.T", "ind":'Automotive Group', "drs":["TOYOTA80"]},
    {"sym":'UNIQLO', "name":'Fast Retailing Co. (UNIQLO)', "region":'JP', "yf":"9983.T", "ind":'Fast Fashion Retail', "drs":["UNIQLO80"]},
    {"sym":'SHINCHEM', "name":'Shin-Etsu Chemical Co., Ltd.', "region":'JP', "yf":"4063.T", "ind":'Specialty Chemicals & Silicon Wafers', "drs":["SHINCHEM19"]},
    # ── Europe ─────────────────────────────────────────────────────────────
    {"sym":'HERMES', "name":'Hermes International S.A.', "region":'EU', "yf":"RMS.PA", "ind":'Luxury Fashion & Leather Goods', "drs":["HERMES80"]},
    {"sym":'LOREAL', "name":"L'Oreal S.A.", "region":'EU', "yf":"OR.PA", "ind":'Beauty & Personal Care', "drs":["LOREAL80"]},
    {"sym":'LVMH', "name":'LVMH Moet Hennessy Louis Vuitton', "region":'EU', "yf":"MC.PA", "ind":'Luxury Goods Conglomerate', "drs":["LVMH01"]},
    {"sym":'NOVOB', "name":'Novo Nordisk A/S', "region":'US', "yf":"NVO", "ind":'Diabetes & Obesity Drugs', "drs":["NOVOB80"]},
    {"sym":'SANOFI', "name":'Sanofi S.A.', "region":'US', "yf":"SNY", "ind":'Pharmaceuticals EU', "drs":["SANOFI80"]},
    {"sym":'DEAM', "name":'Invesco MDAX UCITS ETF (Acc)', "region":'EU', "yf":"DEAM.DE", "ind":'Germany Mid-Cap (MDAX) ETF', "drs":["DEAM19"]},
    # ── Singapore / ASEAN ───────────────────────────────────────────────────
    {"sym":'BONDAS', "name":'iShares JPM USD Asia Credit Bond ETF', "region":'SG', "yf":"N6M.SI", "ind":'ASEAN Bond ETF', "drs":["BONDAS19"]},
    {"sym":'DBS', "name":'DBS Group Holdings Ltd.', "region":'SG', "yf":"D05.SI", "ind":'Banking Singapore', "drs":["DBS19"]},
    {"sym":'INDIAESG', "name":'iShares MSCI India Climate Transition ETF', "region":'SG', "yf":"QK9.SI", "ind":'India ESG ETF', "drs":["INDIAESG19"]},
    {"sym":'SEMB', "name":'Sembcorp Industries Ltd.', "region":'SG', "yf":"U96.SI", "ind":'Utilities & Industrials SG', "drs":["SEMB19"]},
    {"sym":'SGX', "name":'Singapore Exchange Limited', "region":'SG', "yf":"S68.SI", "ind":'Financial Exchange SG', "drs":["SGX19"]},
    {"sym":'SIA', "name":'Singapore Airlines Ltd.', "region":'SG', "yf":"C6L.SI", "ind":'Premium Aviation SG', "drs":["SIA19"]},
    {"sym":'SINGTEL', "name":'Singtel Group', "region":'SG', "yf":"Z74.SI", "ind":'Telecom Group Singapore', "drs":["SINGTEL80"]},
    {"sym":'THAIBEV', "name":'Thai Beverage Public Co.', "region":'SG', "yf":"Y92.SI", "ind":'Beverages & F&B Thailand', "drs":["THAIBEV19"]},
    {"sym":'UOB', "name":'United Overseas Bank Limited', "region":'SG', "yf":"U11.SI", "ind":'Banking Singapore', "drs":["UOB19"]},
    {"sym":'VENTURE', "name":'Venture Corporation Limited', "region":'SG', "yf":"V03.SI", "ind":'Electronics Manufacturing SG', "drs":["VENTURE19"]},
    # ── Vietnam ────────────────────────────────────────────────────────────
    {"sym":'E1VFVN', "name":'VFMVN30 ETF', "region":'VN', "yf":"E1VFVN30.VN", "ind":'Vietnam VN30 Index ETF', "drs":["E1VFVN3001"]},
    {"sym":'FPTVN', "name":'FPT Corporation', "region":'VN', "yf":"FPT.VN", "ind":'Technology Vietnam', "drs":["FPTVN11", "FPTVN19"]},
    {"sym":'FUEVFVND', "name":'VN Diamond ETF', "region":'VN', "yf":"FUEVFVND.VN", "ind":'Vietnam Diamond ETF', "drs":["FUEVFVND01"]},
    {"sym":'GASVN', "name":'PetroVietnam Gas Corp.', "region":'VN', "yf":"GAS.VN", "ind":'Natural Gas Vietnam', "drs":["GASVN11"]},
    {"sym":'HPG', "name":'Hoa Phat Group', "region":'VN', "yf":"HPG.VN", "ind":'Steel Manufacturing Vietnam', "drs":["HPG19"]},
    {"sym":'MSN', "name":'Masan Group Corporation', "region":'VN', "yf":"MSN.VN", "ind":'FMCG & Resources Vietnam', "drs":["MSN11", "MSN19"]},
    {"sym":'MWG', "name":'Mobile World Investment Group', "region":'VN', "yf":"MWG.VN", "ind":'Electronics Retail Vietnam', "drs":["MWG11", "MWG19"]},
    {"sym":'VCB', "name":'Vietcombank', "region":'VN', "yf":"VCB.VN", "ind":'Banking Vietnam', "drs":["VCB11", "VCB19"]},
    {"sym":'VHM', "name":'Vinhomes JSC', "region":'VN', "yf":"VHM.VN", "ind":'Real Estate Vietnam', "drs":["VHM19"]},
    {"sym":'VNFIN', "name":'VN Finance Leader ETF', "region":'VN', "yf":"FUESSVFL.VN", "ind":'Vietnam Finance ETF', "drs":["VNFIN24"]},
    {"sym":'VNM', "name":'Vinamilk (Vietnam Dairy)', "region":'VN', "yf":"VNM.VN", "ind":'Dairy Products Vietnam', "drs":["VNM19"]},
    {"sym":'FUEKIVND', "name":'KIM Growth VN Diamond ETF', "region":'VN', "yf":"FUEKIVND.VN", "ind":'Vietnam Diamond ETF (KIM)', "drs":["VDIAMOND11"]},
    {"sym":'FUEKIV30', "name":'KIM Growth VN30 ETF', "region":'VN', "yf":"FUEKIV30.VN", "ind":'Vietnam VN30 Index ETF (KIM)', "drs":["V3011"]},
    # ── Taiwan ─────────────────────────────────────────────────────────────
    {"sym":'ADVANT', "name":'Advantest Corporation', "region":'JP', "yf":"6857.T", "ind":'Semiconductor Test Equipment', "drs":["ADVANT19", "ADVANT23"]},
    {"sym":'TAIWAN', "name":'Taiwan 50 ETF', "region":'TW', "yf":"0050.TW", "ind":'Taiwan Broad Market ETF', "drs":["TAIWAN19"]},
    {"sym":'TAIWANAI', "name":'Fubon MSCI Taiwan AI ETF', "region":'TW', "yf":"00952.TW", "ind":'Taiwan AI Thematic ETF', "drs":["TAIWANAI13"]},
    {"sym":'TAIWANHD', "name":'SPDR Taiwan High-Div ETF', "region":'TW', "yf":"00915.TW", "ind":'Taiwan High Dividend ETF', "drs":["TAIWANHD13"]},
]


# ============================================================
# ตรวจสอบ DR ใหม่/ถูกถอด — เทียบ _DR_STATIC (curate ด้วยมือ) กับรายชื่อ DR
# จริงที่ซื้อขายอยู่บน SET.or.th (/api/set/dr/list) — แค่ "รายงาน" ไม่แก้
# _DR_STATIC ให้อัตโนมัติ เพราะยังต้องมีคนใส่ industry/region/yf ticker ให้ครบ
# ============================================================
import re as _re


def _normalize_underlying(sym):
    """ตัดคำต่อท้าย ' ETF' และช่องว่างออก เพื่อเทียบชื่อข้าม 2 แหล่งที่เขียนไม่ตรงกัน
    เช่น SET.or.th เขียน 'ASEMI ETF' แต่ _DR_STATIC เขียนแค่ 'ASEMI'"""
    s = _re.sub(r"\s*ETF$", "", sym.strip(), flags=_re.IGNORECASE)
    s = _re.sub(r"\s+", "", s)
    return s.upper()


def fetch_live_dr_list():
    """ดึงรายชื่อ DR ทั้งหมดที่ซื้อขายอยู่จริงบน SET จาก /api/set/dr/list
    คืน list ของ dict ต่อ DR ticker (432 ตัว ณ ตอนสำรวจ ไม่ใช่ต่อ underlying)"""
    from sources.set_api import _bootstrap_headers, _get_json
    ctx, hdr = _bootstrap_headers()
    d = _get_json(ctx, hdr, "/api/set/dr/list")
    return d.get("data", [])


def check_dr_diff(base_dir=None):
    """เทียบ underlying stock ที่มี DR ซื้อขายอยู่จริงบน SET กับลิสต์ของเรา
    คืน {live_underlying_count, our_count, new: [...], removed: [...], renamed_new: [...], renamed_removed: [...]}
    - new: underlying ที่มี DR ซื้อขายจริงแล้ว และ DR ticker เหล่านั้นไม่มีใครถืออยู่เลย -> ต้องเพิ่มจริง
      (เทียบกับ _DR_STATIC + dr_universe_auto.json ถ้าส่ง base_dir มา — ตัวที่ auto-sync
      เพิ่มไปแล้วจะไม่โผล่เป็น 'new' ซ้ำ ทั้งที่มีข้อมูลราคาใช้งานได้จริงแล้ว)
    - removed: underlying ในลิสต์เราที่ DR ticker ทุกตัวของมันไม่มีซื้อขายบน SET แล้ว -> ถูกถอดจริง
    - renamed_new / renamed_removed: กรณี SET เปลี่ยนสตริง underlying identifier ของกองทุน/ตราสาร
      เดิม (พบมากในกอง DR ต่างประเทศที่ underlying เป็นชื่อยาว เช่น 'CAMCSI300' vs ที่เรา curate
      ไว้สั้นๆ ว่า 'CN') ทำให้ _normalize_underlying(...) ไม่ตรงกันทั้งที่ DR ticker เดียวกันยังซื้อขาย
      อยู่จริงทั้งคู่ — ไม่ต้องแก้อะไร แค่โชว์แยกไว้ให้เห็นว่าไม่ใช่ของใหม่/ของหาย"""
    live = fetch_live_dr_list()

    live_map = {}
    for e in live:
        norm = _normalize_underlying(e.get("underlying", ""))
        if not norm:
            continue
        if norm not in live_map:
            live_map[norm] = {
                "underlying_raw": e.get("underlying"),
                "name": e.get("underlyingName"),
                "exchange": e.get("underlyingExchange"),
                "url": e.get("underlyingUrl"),
                "dr_tickers": [],
            }
        live_map[norm]["dr_tickers"].append(e.get("symbol"))

    full_universe = load_dr_universe(base_dir) if base_dir else _DR_STATIC
    ours = {_normalize_underlying(s["sym"]): s for s in full_universe}
    live_drs = {e.get("symbol") for e in live if e.get("symbol")}

    new_items, renamed_new = [], []
    for norm, info in live_map.items():
        if norm in ours:
            continue
        item = {
            "symbol_guess": norm,
            "underlying_raw": info["underlying_raw"],
            "name": info["name"],
            "exchange": info["exchange"],
            "url": info["url"],
            "dr_tickers": sorted(info["dr_tickers"]),
        }
        already_owned = {t: owner for t in item["dr_tickers"]
                         for owner in ([e["sym"] for e in full_universe if t in e["drs"]] or [None])
                         if owner}
        if already_owned:
            item["already_tracked_as"] = sorted(set(already_owned.values()))
            renamed_new.append(item)
        else:
            new_items.append(item)

    removed_items, renamed_removed = [], []
    for norm, entry in ours.items():
        if norm in live_map:
            continue
        still_live = sorted(t for t in (entry.get("drs") or []) if t in live_drs)
        row = {"symbol": entry["sym"], "name": entry.get("name"),
               "region": entry.get("region"), "drs": entry.get("drs")}
        if still_live:
            row["still_trading_as"] = still_live
            renamed_removed.append(row)
        else:
            removed_items.append(row)

    return {
        "live_underlying_count": len(live_map),
        "our_count": len(full_universe),
        "new": sorted(new_items, key=lambda x: x["symbol_guess"]),
        "removed": sorted(removed_items, key=lambda x: x["symbol"]),
        "renamed_new": sorted(renamed_new, key=lambda x: x["symbol_guess"]),
        "renamed_removed": sorted(renamed_removed, key=lambda x: x["symbol"]),
    }


# ============================================================
# AUTO-SYNC: เพิ่ม DR ใหม่จาก SET อัตโนมัติ — ทั้ง series ใหม่ของ underlying เดิม
# และ underlying ใหม่ (หุ้น + ETF) เก็บผลใน dr_universe_auto.json แยกจาก _DR_STATIC
# (ไม่แก้ source file ด้วยโปรแกรม) — yf ticker แกะจากวงเล็บท้าย underlyingName ของ
# SET API เช่น "บริษัท FABRINET (FN)" -> FN, "(4063)" + ตลาดโตเกียว -> 4063.T
# ตรวจกับ Yahoo ก่อนเพิ่มจริง ตัวที่ derive/ตรวจไม่ผ่านเก็บใน unmapped พร้อมเหตุผล
# ============================================================
import json as _json
import os as _os
import time as _time

AUTO_FILE = "dr_universe_auto.json"

# คำใน underlyingExchange (ตัวพิมพ์เล็ก) -> (region, yf suffix) — เรียงเช็คบนลงล่าง
_EXCHANGE_MAP = [
    ("nasdaq", "US", ""), ("new york", "US", ""), ("archipelago", "US", ""),
    ("cboe", "US", ""), ("bats", "US", ""),
    ("hong kong", "HK", ".HK"),
    ("shanghai", "CN", ".SS"), ("shenzhen", "CN", ".SZ"),
    ("tokyo", "JP", ".T"), ("japan", "JP", ".T"),
    ("hochiminh", "VN", ".VN"), ("ho chi minh", "VN", ".VN"), ("hanoi", "VN", ".VN"),
    ("singapore", "SG", ".SI"),
    ("taiwan", "TW", ".TW"), ("taipei", "TW", ".TW"),
    ("deutsche", "EU", ".DE"), ("xetra", "EU", ".DE"),
    ("paris", "EU", ".PA"), ("amsterdam", "EU", ".AS"),
    ("milan", "EU", ".MI"), ("london", "EU", ".L"), ("euronext", "EU", ".PA"),
]


def _auto_path(base_dir):
    return _os.path.join(base_dir, AUTO_FILE)


def _load_auto(base_dir):
    try:
        with open(_auto_path(base_dir), encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return {"new_entries": [], "extra_drs": {}, "unmapped": []}


_dr_universe_cache = {}   # base_dir -> (mtime ของ dr_universe_auto.json หรือ None, result)


def load_dr_universe(base_dir):
    """_DR_STATIC + overlay จาก dr_universe_auto.json — ทุก entry มี field 'etf'
    (static ตรวจจากคำว่า ETF ในชื่อ/อุตสาหกรรม, auto ตรวจจาก underlyingClassName)

    cache ในหน่วยความจำต่อ base_dir คีย์ด้วย mtime ของไฟล์ auto — เดิมฟังก์ชันนี้
    เปิด+parse dr_universe_auto.json ใหม่ทุกครั้งที่เรียก (ถูกเรียกซ้ำหลายจุดต่อ
    1 request จาก app.py/dr_descriptions.py/financials_store.py) แคชไว้ตัด I/O
    ซ้ำ แต่ยัง invalidate อัตโนมัติถ้าไฟล์ auto เปลี่ยน (เช่นหลังปุ่ม sync)"""
    path = _auto_path(base_dir)
    try:
        mtime = _os.path.getmtime(path)
    except OSError:
        mtime = None
    cached = _dr_universe_cache.get(base_dir)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    auto = _load_auto(base_dir)
    extra = auto.get("extra_drs", {})
    out = []
    for s in _DR_STATIC:
        e = dict(s)
        add = [t for t in extra.get(e["sym"], []) if t not in e["drs"]]
        if add:
            e["drs"] = list(e["drs"]) + sorted(add)
        e["etf"] = bool(_re.search(r"\bETF\b", e.get("name", "") + " " + e.get("ind", ""), _re.IGNORECASE))
        out.append(e)
    static_syms = {e["sym"] for e in out}
    for e in auto.get("new_entries", []):
        if e.get("sym") in static_syms:
            continue  # ถูกย้ายเข้า _DR_STATIC แล้ว — ใช้ตัว curate แทน
        e = dict(e)
        e.setdefault("etf", False)
        out.append(e)
    _dr_universe_cache[base_dir] = (mtime, out)
    return out


def _derive_yf_ticker(underlying_name, exchange):
    """แกะ yf ticker จากวงเล็บท้ายชื่อ underlying + suffix ตามตลาด
    คืน (yf_ticker|None, region|None, reason|None)"""
    exch = (exchange or "").lower()
    region = suffix = None
    for kw, reg, suf in _EXCHANGE_MAP:
        if kw in exch:
            region, suffix = reg, suf
            break
    if region is None:
        return None, None, f"ไม่รู้จักตลาด: {exchange}"
    codes = _re.findall(r"\(([A-Za-z0-9\.\-]{1,12})\)", underlying_name or "")
    if not codes:
        return None, region, "หา ticker ในวงเล็บท้ายชื่อ underlying ไม่เจอ"
    code = codes[-1].upper()
    if suffix and code.endswith(suffix.upper()):
        code = code[:-len(suffix)]     # บางชื่อใส่ suffix มาแล้ว เช่น "(3692.HK)" — กันซ้ำเป็น .HK.HK
    if region == "HK":
        code = code.zfill(4)          # HKEX เลข 4 หลัก เช่น 700 -> 0700
    if region == "US":
        code = code.replace(".", "-")  # BRK.B -> BRK-B ตามรูปแบบ Yahoo
    return code + suffix, region, None


def _clean_underlying_name(name):
    """ตัดคำนำหน้าไทย + วงเล็บ ticker ท้ายชื่อออก ให้เหลือชื่อบริษัท/กองทุนอ่านง่าย"""
    s = (name or "").strip()
    for prefix in ("หุ้นสามัญของบริษัท", "บริษัท", "โครงการจัดการลงทุนต่างประเทศ"):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    s = _re.sub(r"\s*\([A-Za-z0-9\.\-]{1,12}\)\s*$", "", s)
    return s[:70] or name


def sync_dr_universe(base_dir, validate=True):
    """เทียบกับ /api/set/dr/list แล้วเพิ่มของใหม่อัตโนมัติ:
    - DR series ใหม่ของ underlying เดิม -> extra_drs (จับคู่ผ่าน underlying ของ sibling)
    - underlying ใหม่ (หุ้น/ETF) -> new_entries (ตรวจ yf ticker กับ Yahoo ก่อนถ้า validate=True)
    - derive ไม่ได้/ตรวจไม่ผ่าน -> unmapped พร้อมเหตุผล (โชว์ให้คน curate ต่อ)
    คืน {"appended": n, "added": n, "unmapped": n}"""
    live = fetch_live_dr_list()
    if not live:
        return {"appended": 0, "added": 0, "unmapped": 0, "error": "SET API ไม่ตอบ"}
    universe = load_dr_universe(base_dir)
    known_drs = {t for e in universe for t in e["drs"]}
    live_map = {x["symbol"]: x for x in live if x.get("symbol")}

    missing = sorted(s for s in live_map if s not in known_drs)
    auto = _load_auto(base_dir)
    if not missing:
        # อัปเดตเวลาเช็คไว้ แม้ไม่มีของใหม่ — ให้รู้ว่าระบบยังทำงานอยู่ · ยังต้องเคลียร์
        # unmapped ที่ถูก curate มือเข้า _DR_STATIC ไปแล้วออก (ไม่งั้นค้างแจ้งเตือนตลอดไป
        # เพราะ underlying ตัวนั้นมี DR อยู่ใน known_drs แล้ว จึงไม่เข้าเงื่อนไข missing อีก)
        mapped_now = {e["sym"] for e in universe}
        auto["unmapped"] = [m for m in auto.get("unmapped", [])
                            if _normalize_underlying(m.get("underlying") or "") not in mapped_now]
        auto["last_synced"] = _time.strftime("%Y-%m-%d %H:%M")
        from core.store import _atomic_write_json
        _atomic_write_json(_auto_path(base_dir), auto)
        return {"appended": 0, "added": 0, "unmapped": len(auto["unmapped"])}

    # map: live underlying -> sym ของ entry เราที่มี DR ของ underlying นั้นอยู่แล้ว
    dr_owner = {t: e["sym"] for e in universe for t in e["drs"]}
    und_owner = {}
    for t, own in dr_owner.items():
        u = live_map.get(t, {}).get("underlying")
        if u and u not in und_owner:
            und_owner[u] = own

    appended = added = 0
    new_unders = {}
    for s in missing:
        u = live_map[s].get("underlying")
        if u in und_owner:
            lst = auto.setdefault("extra_drs", {}).setdefault(und_owner[u], [])
            if s not in lst:
                lst.append(s)
                appended += 1
                print(f"[DR-sync] เติม DR ใหม่ {s} -> {und_owner[u]}")
        else:
            new_unders.setdefault(u or s, []).append(s)

    unmapped = []
    for u, tickers in sorted(new_unders.items()):
        x = live_map[tickers[0]]
        und_name = x.get("underlyingName") or ""
        yf_t, region, reason = _derive_yf_ticker(und_name, x.get("underlyingExchange"))
        is_etf = ("ETF" in (u or "").upper()
                  or "โครงการจัดการลงทุน" in und_name
                  or "ETF" in und_name.upper())
        if yf_t and validate:
            try:
                import yfinance as yf
                if yf.Ticker(yf_t).history(period="5d").empty:
                    reason, yf_t = f"Yahoo ไม่มีข้อมูล ticker ที่ derive ได้ ({yf_t})", None
            except Exception as ex:
                reason, yf_t = f"ตรวจ ticker กับ Yahoo ไม่สำเร็จ: {ex}", None
        if not yf_t:
            unmapped.append({"underlying": u, "drs": sorted(tickers), "reason": reason,
                             "name": und_name, "exchange": x.get("underlyingExchange")})
            print(f"[DR-sync] ⚠ เพิ่มอัตโนมัติไม่ได้ {u}: {reason}")
            continue
        entry = {
            "sym": _normalize_underlying(u or tickers[0]),
            "name": _clean_underlying_name(und_name),
            "region": region, "yf": yf_t,
            "ind": "Foreign ETF (auto)" if is_etf else "Foreign Stock (auto)",
            "drs": sorted(tickers), "etf": is_etf, "auto": True,
        }
        auto.setdefault("new_entries", []).append(entry)
        added += 1
        print(f"[DR-sync] เพิ่ม underlying ใหม่ {entry['sym']} ({yf_t}) drs={tickers}")

    # unmapped เก็บสะสม (กันรายงานหาย) แต่ไม่ซ้ำ underlying เดิม
    old_un = {m.get("underlying"): m for m in auto.get("unmapped", [])}
    for m in unmapped:
        old_un[m["underlying"]] = m
    # ตัด unmapped ที่ derive สำเร็จไปแล้ว หรือถูก curate เพิ่มมือเข้า _DR_STATIC ทีหลังออก
    # (universe ที่ load ไว้ตอนต้นฟังก์ชันครอบทั้ง _DR_STATIC + auto entries รอบก่อนๆ อยู่แล้ว)
    mapped_now = ({e["sym"] for e in auto.get("new_entries", [])}
                 | {e["sym"] for e in universe})
    auto["unmapped"] = [m for m in old_un.values()
                        if _normalize_underlying(m.get("underlying") or "") not in mapped_now]
    auto["last_synced"] = _time.strftime("%Y-%m-%d %H:%M")
    from core.store import _atomic_write_json
    _atomic_write_json(_auto_path(base_dir), auto)
    return {"appended": appended, "added": added, "unmapped": len(auto["unmapped"])}

