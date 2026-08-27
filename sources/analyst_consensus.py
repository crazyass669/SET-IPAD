# -*- coding: utf-8 -*-
"""sources/analyst_consensus.py — ฉันทามตินักวิเคราะห์จาก Yahoo (yfinance): ราคา
เป้าหมาย (mean/high/low/median), คำแนะนำ Buy/Hold/Sell, ประมาณการ EPS/รายได้
ล่วงหน้า + long-term growth — เก็บใน financials.db ตาราง analyst_consensus

pattern เดียวกับ sources/set_company.py / sources/factor_snapshot.py:
_connect() reuse financials_store._db_path, init_table() แบบ idempotent
(CREATE TABLE IF NOT EXISTS + _ensure_columns) ไม่มี central bootstrap

ทำไมเก็บ DB (ไม่ overlay สดทุกครั้ง): yfinance analyst endpoint ยิงช้า ~1-1.5
วิ/ตัว (ต่อ Ticker มี 3-4 network call) — ทั้งกระดานไทย ~930 ตัวต้อง sync แยก
รอบ ไม่ใช่ต่อ request · coverage ~40% ของหุ้นไทย (เอียงไปทาง cap ใหญ่ที่มี
นักวิเคราะห์ตาม) — ทุก filter/column ที่ใช้ข้อมูลชุดนี้ต้อง "ไม่มีข้อมูล = ไม่
กรองทิ้ง" เป็น default เหมือน ESG ไม่งั้นหุ้น 60% หายเงียบๆ

local-only เหมือน financials.db (ห้าม push ขึ้น GitHub)
"""
import json
import logging
import math
import random
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from sources import financials_store as fs

TABLE = "analyst_consensus"

# Yahoo ให้ recommendationMean แบบ 1=Strong Buy … 5=Strong Sell (ยิ่งต่ำยิ่ง bullish)
# คำนวณเองจาก count ต่อระดับ เพื่อให้ค่าตรงกับ Buy/Hold/Sell ที่เราเก็บจริง (บาง
# ticker Yahoo ไม่ส่ง recommendationMean มาแต่มี summary ครบ)
_REC_WEIGHT = {"strong_buy": 1, "buy": 2, "hold": 3, "sell": 4, "strong_sell": 5}

# คอลัมน์กลุ่ม "ผู้ถือหุ้น" (Yahoo major_holders / institutional_holders /
# insider_purchases) — เพิ่มทีหลัง Phase 1 ผ่าน ALTER TABLE (idempotent ใน init_table)
# ดึงเฉพาะตอน sync_dr (with_holders=True) เพราะ institutional_holders เต็มเฉพาะ
# underlying US/HK ของ DR — หุ้นไทยแทบว่าง และมันเพิ่ม network call ต่อ ticker อีก 3 ครั้ง
_HOLDER_COLS = {
    "insiders_pct_held": "REAL",
    "institutions_pct_held": "REAL",
    "institutions_float_pct_held": "REAL",
    "institutions_count": "INTEGER",
    "insider_buy_6m": "INTEGER",
    "insider_sell_6m": "INTEGER",
    "insider_net_6m": "INTEGER",
    "insider_total_held": "INTEGER",
    "insider_pct_net_6m": "REAL",
    "insider_pct_buy": "REAL",
    "insider_pct_sell": "REAL",
    "top_institutions": "TEXT",
    "holders_synced_at": "TEXT",
}


def _connect(base_dir):
    con = sqlite3.connect(fs._db_path(base_dir))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_table(base_dir):
    con = _connect(base_dir)
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}(
              symbol TEXT, market TEXT,
              price_at_fetch REAL, currency TEXT,
              target_mean REAL, target_high REAL, target_low REAL, target_median REAL,
              rec_strong_buy INTEGER, rec_buy INTEGER, rec_hold INTEGER,
              rec_sell INTEGER, rec_strong_sell INTEGER,
              rec_mean REAL, analyst_count INTEGER,
              eps_growth_this_y REAL, eps_growth_next_y REAL, rev_growth_next_y REAL,
              ltg_pct REAL,
              synced_at TEXT,
              PRIMARY KEY(symbol, market)
            )
        """)
        have = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE})")}
        for name, typ in _HOLDER_COLS.items():
            if name not in have:
                con.execute(f"ALTER TABLE {TABLE} ADD COLUMN {name} {typ}")
        con.commit()
    finally:
        con.close()


# ============================================================
# fetch — 1 ticker ต่อครั้ง (reuse session เดียวข้าม thread เหมือน yahoo.py)
# ============================================================

def _bare(symbol, market):
    """symbol แบบไม่มี suffix ตลาด — ใช้เป็น key ใน DB ให้ทุก path (sync_th จาก
    load_set_symbols, sync_dr จาก dr_universe['yf'] ที่มี '.HK' ต่อท้าย, get_one จาก
    h.symbol ในหน้า Tearsheet) ลงคีย์เดียวกันเป๊ะ ไม่งั้น '0700' vs '0700.HK' หา
    ไม่เจอกัน"""
    s = (symbol or "").upper().strip()
    m = (market or "TH").upper()
    if m == "HK" and s.endswith(".HK"):
        return s[:-3]
    if m == "JP" and s.endswith(".T"):
        return s[:-2]
    if m == "TH" and s.endswith(".BK"):
        return s[:-3]
    return s


def _yf_ticker(symbol, market):
    """เดา yf ticker จาก (symbol, ตลาด) — logic เดียวกับ
    financials_store.fetch_yahoo_full / dr_descriptions.resolve_yf_ticker
    รับ symbol ที่มี suffix มาแล้วได้ด้วย (เช่น '0700.HK') ผ่าน _bare กันซ้อน suffix"""
    m = (market or "TH").upper()
    sym = _bare(symbol, m)
    if m == "US":
        return sym
    if m == "HK":
        return sym.zfill(4) + ".HK"
    if m == "JP":
        return sym + ".T"
    return sym + ".BK"


def _f(v):
    """float ที่กัน NaN/None/Inf → None"""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _i(v):
    f = _f(v)
    return int(round(f)) if f is not None else None


def _rec_mean(counts):
    """recommendationMean 1..5 จาก dict count ต่อระดับ — None ถ้าไม่มีคำแนะนำเลย"""
    total = sum(c for c in counts.values() if c)
    if not total:
        return None
    s = sum(_REC_WEIGHT[k] * (counts.get(k) or 0) for k in _REC_WEIGHT)
    return round(s / total, 2)


def _row_from_df(df, want_row):
    """DataFrame ของ yfinance (index = '0q'/'+1q'/'0y'/'+1y'/'LTG') → dict ของ row
    ที่ต้องการ คืน {} ถ้าไม่มี"""
    try:
        if df is None or df.empty or want_row not in df.index:
            return {}
        return df.loc[want_row].to_dict()
    except Exception:
        return {}


def _fetch_holders(t):
    """โครงสร้างผู้ถือหุ้นจาก Yahoo — major_holders (สัดส่วนสถาบัน/insider),
    institutional_holders (10 อันดับสถาบัน), insider_purchases (สรุปซื้อ/ขาย insider
    6 เดือน) · คนละแหล่งกับ sec_filings.db (แบบ 59/246-2 ก.ล.ต. ไทย) · เต็มเฉพาะ
    underlying US/HK ของ DR — ทุก field จับ exception รายก้อน คืน dict (ไม่ raise)"""
    out = {k: None for k in _HOLDER_COLS if k not in ("top_institutions", "holders_synced_at")}
    out["top_institutions"] = None

    try:
        mh = t.major_holders
        if mh is not None and not mh.empty:
            col = mh.columns[0]
            g = lambda k: (_f(mh.loc[k, col]) if k in mh.index else None)
            out["insiders_pct_held"] = _pct(g("insidersPercentHeld"))
            out["institutions_pct_held"] = _pct(g("institutionsPercentHeld"))
            out["institutions_float_pct_held"] = _pct(g("institutionsFloatPercentHeld"))
            out["institutions_count"] = _i(g("institutionsCount"))
    except Exception:
        pass

    try:
        ih = t.institutional_holders
        if ih is not None and not ih.empty:
            rows = []
            for _, r in ih.head(10).iterrows():
                dt = r.get("Date Reported")
                rows.append({
                    "holder": str(r.get("Holder") or "")[:80],
                    "pct": _pct(r.get("pctHeld")),
                    "shares": _i(r.get("Shares")),
                    "value": _f(r.get("Value")),
                    "pct_change": _pct(r.get("pctChange")),
                    "date": (dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else None),
                })
            rows = [x for x in rows if x["holder"]]
            if rows:
                out["top_institutions"] = json.dumps(rows, ensure_ascii=False)
    except Exception:
        pass

    try:
        ip = t.insider_purchases
        if ip is not None and not ip.empty:
            lbl_col = ip.columns[0]
            m = {str(r[lbl_col]).strip(): r for _, r in ip.iterrows()}

            def _g(label, col="Shares"):
                r = m.get(label)
                return None if r is None else _f(r.get(col))
            out["insider_buy_6m"] = _i(_g("Purchases"))
            out["insider_sell_6m"] = _i(_g("Sales"))
            out["insider_net_6m"] = _i(_g("Net Shares Purchased (Sold)"))
            out["insider_total_held"] = _i(_g("Total Insider Shares Held"))
            out["insider_pct_net_6m"] = _pct(_g("% Net Shares Purchased (Sold)"))
            out["insider_pct_buy"] = _pct(_g("% Buy Shares"))
            out["insider_pct_sell"] = _pct(_g("% Sell Shares"))
    except Exception:
        pass

    return out


def fetch_one(symbol, market, session=None, with_holders=False):
    """ดึงฉันทามตินักวิเคราะห์ของหุ้น 1 ตัวจาก yfinance — คืน dict (field ที่ไม่มีเป็น
    None) หรือ raise ถ้า ticker ใช้ไม่ได้/ไม่มีข้อมูลนักวิเคราะห์เลยสักช่อง

    session: curl_cffi session จาก sources.yahoo._TimeoutSession ใช้ร่วมกันข้าม
    thread เพื่อไม่ให้ crumb หมดอายุ (เหมือน fetch_market_caps_parallel)
    with_holders: ดึงกลุ่มผู้ถือหุ้นเพิ่ม (คีย์ 'holders' ใน output) — เปิดเฉพาะ
    sync_dr (underlying US/HK) เพราะเพิ่ม network call อีก 3 ครั้ง/ticker"""
    import yfinance as yf

    yft = _yf_ticker(symbol, market)
    t = yf.Ticker(yft, session=session)

    # ── ราคาเป้าหมาย ──────────────────────────────────────
    pt = {}
    try:
        pt = t.analyst_price_targets or {}
    except Exception:
        pt = {}

    # ── คำแนะนำ Buy/Hold/Sell (แถวเดือนล่าสุด = period '0m') ──
    counts = {}
    try:
        rs = t.recommendations_summary
        if rs is not None and not rs.empty:
            row = rs.iloc[0]  # '0m'
            counts = {
                "strong_buy":  _i(row.get("strongBuy")),
                "buy":         _i(row.get("buy")),
                "hold":        _i(row.get("hold")),
                "sell":        _i(row.get("sell")),
                "strong_sell": _i(row.get("strongSell")),
            }
    except Exception:
        counts = {}
    counts = {k: (counts.get(k) or 0) for k in _REC_WEIGHT}

    # ── ประมาณการ EPS/รายได้ ล่วงหน้า + LTG ──────────────
    eps_df = _safe_df(t, "earnings_estimate")
    eps_est = _row_from_df(eps_df, "0y")
    eps_est_ny = _row_from_df(eps_df, "+1y")
    rev_est_ny = _row_from_df(_safe_df(t, "revenue_estimate"), "+1y")
    growth = _safe_df(t, "growth_estimates")
    ltg = None
    try:
        if growth is not None and not growth.empty and "LTG" in growth.index:
            col = "stockTrend" if "stockTrend" in growth.columns else growth.columns[0]
            ltg = _f(growth.loc["LTG", col])
    except Exception:
        ltg = None

    analyst_count = _i(eps_est.get("numberOfAnalysts")) or _i(eps_est_ny.get("numberOfAnalysts"))
    rec_total = sum(counts.values())
    if analyst_count is None and rec_total:
        analyst_count = rec_total

    out = {
        "price_at_fetch": _f(pt.get("current")),
        "currency": (eps_est.get("currency") or eps_est_ny.get("currency") or None),
        "target_mean":   _f(pt.get("mean")),
        "target_high":   _f(pt.get("high")),
        "target_low":    _f(pt.get("low")),
        "target_median": _f(pt.get("median")),
        "rec_strong_buy":  counts["strong_buy"],
        "rec_buy":         counts["buy"],
        "rec_hold":        counts["hold"],
        "rec_sell":        counts["sell"],
        "rec_strong_sell": counts["strong_sell"],
        "rec_mean": _rec_mean(counts),
        "analyst_count": analyst_count,
        "eps_growth_this_y": _pct(eps_est.get("growth")),
        "eps_growth_next_y": _pct(eps_est_ny.get("growth")),
        "rev_growth_next_y": _pct(rev_est_ny.get("growth")),
        "ltg_pct": _pct(ltg),
    }

    holders = _fetch_holders(t) if with_holders else None
    out["holders"] = holders
    holders_have = bool(holders) and any(
        v is not None for k, v in holders.items() if k != "holders_synced_at")

    # ไม่มีอะไรเลย (ticker เพี้ยน/หุ้นไม่มีนักวิเคราะห์ตาม) — raise ให้ caller นับเป็น miss
    if (out["target_mean"] is None and not rec_total
            and out["analyst_count"] is None and not holders_have):
        raise ValueError(f"ไม่มีข้อมูลนักวิเคราะห์สำหรับ {symbol} ({yft})")
    return out


def _safe_df(t, attr):
    try:
        return getattr(t, attr)
    except Exception:
        return None


def _pct(v):
    """Yahoo ส่ง growth เป็นสัดส่วน (0.6135) → เก็บเป็น % (61.35) ให้ตรงหน่วยกับ
    rev_cagr/g13_pct ใน factor_snapshot/dcf_screener"""
    f = _f(v)
    return round(f * 100, 2) if f is not None else None


# ============================================================
# sync — batch (parallel, upsert)
# ============================================================

def sync_batch(base_dir, targets, callback=None, workers=3, with_holders=False):
    """targets: list ของ (symbol, market) — ดึง parallel (workers น้อยเพราะ yfinance
    analyst endpoint หนักกว่า quote) แล้ว upsert เข้า analyst_consensus
    (PK symbol+market เขียนทับของเดิม) คืน (n_ok, n_total)
    with_holders: ดึง major/institutional/insider holders เพิ่ม (sync_dr เท่านั้น)"""
    from sources.yahoo import _TimeoutSession

    init_table(base_dir)
    # yfinance log 404/rate-limit ของหุ้นที่ไม่มี ticker/ไม่มีนักวิเคราะห์ตามเสียงดัง
    # มาก (หลายร้อยบรรทัดต่อ sync ทั้งกระดาน) — เราจับ exception เองครบแล้ว
    _yf_log = logging.getLogger("yfinance")
    _prev_level = _yf_log.level
    _yf_log.setLevel(logging.CRITICAL)

    session = _TimeoutSession()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    total = len(targets)

    def _work(pair):
        sym, mkt = pair
        time.sleep(random.uniform(0.2, 0.7))
        try:
            return sym, mkt, fetch_one(sym, mkt, session=session,
                                       with_holders=with_holders)
        except Exception as e:
            logging.debug(f"[AnalystConsensus] {sym}/{mkt}: {e}")
            return sym, mkt, None

    results = []
    done = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for sym, mkt, data in ex.map(_work, targets):
                done += 1
                if data:
                    results.append((sym, mkt, data))
                if callback and done % 25 == 0:
                    callback(done, total, f"ฉันทามตินักวิเคราะห์ {done}/{total}...")
    finally:
        _yf_log.setLevel(_prev_level)

    con = _connect(base_dir)
    try:
        for sym, mkt, d in results:
            con.execute(f"""
                INSERT INTO {TABLE}(symbol, market, price_at_fetch, currency,
                    target_mean, target_high, target_low, target_median,
                    rec_strong_buy, rec_buy, rec_hold, rec_sell, rec_strong_sell,
                    rec_mean, analyst_count, eps_growth_this_y, eps_growth_next_y,
                    rev_growth_next_y, ltg_pct, synced_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, market) DO UPDATE SET
                    price_at_fetch=excluded.price_at_fetch, currency=excluded.currency,
                    target_mean=excluded.target_mean, target_high=excluded.target_high,
                    target_low=excluded.target_low, target_median=excluded.target_median,
                    rec_strong_buy=excluded.rec_strong_buy, rec_buy=excluded.rec_buy,
                    rec_hold=excluded.rec_hold, rec_sell=excluded.rec_sell,
                    rec_strong_sell=excluded.rec_strong_sell, rec_mean=excluded.rec_mean,
                    analyst_count=excluded.analyst_count,
                    eps_growth_this_y=excluded.eps_growth_this_y,
                    eps_growth_next_y=excluded.eps_growth_next_y,
                    rev_growth_next_y=excluded.rev_growth_next_y,
                    ltg_pct=excluded.ltg_pct, synced_at=excluded.synced_at
            """, (
                sym, mkt, d["price_at_fetch"], d["currency"],
                d["target_mean"], d["target_high"], d["target_low"], d["target_median"],
                d["rec_strong_buy"], d["rec_buy"], d["rec_hold"], d["rec_sell"],
                d["rec_strong_sell"], d["rec_mean"], d["analyst_count"],
                d["eps_growth_this_y"], d["eps_growth_next_y"], d["rev_growth_next_y"],
                d["ltg_pct"], now,
            ))
            h = d.get("holders")
            if h:
                con.execute(f"""
                    UPDATE {TABLE} SET
                        insiders_pct_held=?, institutions_pct_held=?,
                        institutions_float_pct_held=?, institutions_count=?,
                        insider_buy_6m=?, insider_sell_6m=?, insider_net_6m=?,
                        insider_total_held=?, insider_pct_net_6m=?,
                        insider_pct_buy=?, insider_pct_sell=?,
                        top_institutions=?, holders_synced_at=?
                    WHERE symbol=? AND market=?
                """, (
                    h["insiders_pct_held"], h["institutions_pct_held"],
                    h["institutions_float_pct_held"], h["institutions_count"],
                    h["insider_buy_6m"], h["insider_sell_6m"], h["insider_net_6m"],
                    h["insider_total_held"], h["insider_pct_net_6m"],
                    h["insider_pct_buy"], h["insider_pct_sell"],
                    h["top_institutions"], now, sym, mkt,
                ))
        con.commit()
    finally:
        con.close()
    return len(results), total


def _fresh_symbols(base_dir, market, max_age_days):
    """set ของ symbol (ตลาดนี้) ที่ synced_at ใหม่กว่า max_age_days — ข้ามตอน sync
    รอบถัดไป (เหมือน skip_up_to_date ของ sync_all งบการเงิน) รอบแรกที่ตารางว่างจะ
    คืน set() ว่าง = sync ทุกตัว"""
    if max_age_days is None or not fs.db_exists(base_dir):
        return set()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_days * 86400
    con = _connect(base_dir)
    try:
        rows = con.execute(
            f"SELECT symbol, synced_at FROM {TABLE} WHERE market=?", (market,)).fetchall()
    except sqlite3.OperationalError:
        return set()
    finally:
        con.close()
    fresh = set()
    for sym, ts in rows:
        if not ts:
            continue
        try:
            if datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc).timestamp() >= cutoff:
                fresh.add(sym)
        except ValueError:
            pass
    return fresh


def _missing_holders(base_dir, market):
    """set ของ symbol (ตลาดนี้) ที่ยังไม่เคยดึง holders (holders_synced_at IS NULL) —
    ใช้บังคับ sync_dr ให้เก็บตกตอนเพิ่งเพิ่มคอลัมน์กลุ่มผู้ถือหุ้น"""
    if not fs.db_exists(base_dir):
        return set()
    con = _connect(base_dir)
    try:
        rows = con.execute(
            f"SELECT symbol FROM {TABLE} WHERE market=? AND holders_synced_at IS NULL",
            (market,)).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()
    finally:
        con.close()


def sync_th(base_dir, callback=None, max_age_days=7):
    """sync ฉันทามตินักวิเคราะห์หุ้นสามัญไทยทั้งกระดาน (SET+mai) — ~40% จะมีข้อมูล
    จริง (เอียงไป cap ใหญ่) · ข้ามตัวที่เพิ่ง sync ภายใน max_age_days (None = ยิงใหม่หมด)
    คืน (n_ok, n_total_attempted)"""
    from set_data_fetcher import load_set_symbols
    fresh = _fresh_symbols(base_dir, "TH", max_age_days)
    syms = [(s["symbol"], "TH") for s in load_set_symbols(base_dir)
            if s["symbol"] not in fresh]
    if not syms:
        return 0, 0
    return sync_batch(base_dir, syms, callback=callback)


def sync_dr(base_dir, callback=None, max_age_days=7):
    """sync ฉันทามตินักวิเคราะห์ของ 'underlying ต่างประเทศ' ของ DR ไทย — เฉพาะ region
    US/HK ที่ Yahoo coverage ดีมาก (เกือบ 100% ต่างจากหุ้นไทย ~40%) · เก็บใต้
    market='US'/'HK' (bare symbol ไม่มี suffix) ใช้กับการ์ดในหน้า Tearsheet ของหุ้น DR
    ที่ resolve เป็น underlying แล้ว · ข้ามตัวที่เพิ่ง sync ภายใน max_age_days
    คืน (n_ok, n_total_attempted)"""
    from sources.dr_universe import load_dr_universe
    targets, seen = [], set()
    for e in load_dr_universe(base_dir):
        region = (e.get("region") or "").upper()
        yf = (e.get("yf") or "").strip()
        if region not in ("US", "HK") or not yf or e.get("etf"):
            continue
        key = (_bare(yf, region), region)
        if key in seen:
            continue
        seen.add(key)
        targets.append(key)
    if not targets:
        return 0, 0
    fresh_us = _fresh_symbols(base_dir, "US", max_age_days)
    fresh_hk = _fresh_symbols(base_dir, "HK", max_age_days)
    # ตัวที่ยังไม่มี holders เลย (คอลัมน์เพิ่งเพิ่ม) — ต้อง sync ใหม่แม้ analyst จะสด
    miss_us = _missing_holders(base_dir, "US")
    miss_hk = _missing_holders(base_dir, "HK")
    pend = [(s, m) for (s, m) in targets
            if s not in (fresh_us if m == "US" else fresh_hk)
            or s in (miss_us if m == "US" else miss_hk)]
    if not pend:
        return 0, 0
    return sync_batch(base_dir, pend, callback=callback, with_holders=True)


# ============================================================
# read-back — overlay เข้า Screener+ / Tearsheet / DCF Screener
# ============================================================

_COLS = ("symbol", "market", "price_at_fetch", "currency", "target_mean",
         "target_high", "target_low", "target_median", "rec_strong_buy", "rec_buy",
         "rec_hold", "rec_sell", "rec_strong_sell", "rec_mean", "analyst_count",
         "eps_growth_this_y", "eps_growth_next_y", "rev_growth_next_y", "ltg_pct",
         "synced_at") + tuple(_HOLDER_COLS)


def _enrich(row):
    d = dict(zip(_COLS, row))
    tm, pf = d.get("target_mean"), d.get("price_at_fetch")
    d["target_upside_pct"] = (round((tm / pf - 1) * 100, 2)
                              if tm and pf and pf > 0 else None)
    rec_total = sum(d.get(k) or 0 for k in
                    ("rec_strong_buy", "rec_buy", "rec_hold", "rec_sell", "rec_strong_sell"))
    d["rec_total"] = rec_total
    d["buy_pct"] = (round(((d.get("rec_strong_buy") or 0) + (d.get("rec_buy") or 0))
                          / rec_total * 100, 1) if rec_total else None)
    # top_institutions เก็บเป็น JSON string ใน DB → list สำหรับ client
    ti = d.get("top_institutions")
    if ti:
        try:
            d["top_institutions"] = json.loads(ti)
        except (ValueError, TypeError):
            d["top_institutions"] = None
    d["has_holders"] = bool(d.get("holders_synced_at"))
    return d


def get_map(base_dir, market="TH"):
    """{symbol: {...ทุก field + target_upside_pct/buy_pct/rec_total ที่ derive ให้แล้ว}}
    ของตลาดที่ระบุ — คืน {} ถ้ายังไม่เคย sync ไม่ raise (ข้อมูลเสริม)"""
    if not fs.db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        cur = con.execute(
            f"SELECT {', '.join(_COLS)} FROM {TABLE} WHERE market=?", (market,))
        return {r[0]: _enrich(r) for r in cur.fetchall()}
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()


def get_one(base_dir, symbol, market="TH"):
    """ฉันทามตินักวิเคราะห์ของหุ้น 1 ตัวจาก DB (อ่านเร็ว ไม่ยิง Yahoo สด) — คืน None
    ถ้ายังไม่เคย sync หรือไม่พบ"""
    if not fs.db_exists(base_dir):
        return None
    con = _connect(base_dir)
    try:
        row = con.execute(
            f"SELECT {', '.join(_COLS)} FROM {TABLE} WHERE symbol=? AND market=?",
            (_bare(symbol, market), (market or "TH").upper())).fetchone()
        return _enrich(row) if row else None
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def get_meta(base_dir):
    """สรุป coverage + ความสดต่อตลาด — {market: {count, updated_at}} + รวม — ใช้โชว์
    ในหน้า Screener+/Data Health คืน {} ถ้ายังไม่เคย sync"""
    if not fs.db_exists(base_dir):
        return {}
    con = _connect(base_dir)
    try:
        cur = con.execute(
            f"SELECT market, COUNT(*), MAX(synced_at), "
            f"SUM(CASE WHEN target_mean IS NOT NULL THEN 1 ELSE 0 END), "
            f"SUM(CASE WHEN holders_synced_at IS NOT NULL THEN 1 ELSE 0 END) "
            f"FROM {TABLE} GROUP BY market")
        out = {}
        for mkt, n, ts, n_target, n_holders in cur.fetchall():
            out[mkt] = {"count": n or 0, "with_target": n_target or 0,
                        "with_holders": n_holders or 0, "updated_at": ts}
        return out
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()
