/* ============================================================
   SET Dashboard — เวอร์ชันมือถือ/iPad (viewer)
   อ่าน data/*.json ที่ GitHub Actions bake ไว้ (run_static_update.py)
   ไม่มี backend — ทุกอย่างคำนวณฝั่ง client จาก snapshot

   หน้าที่มี (เฟส 0): ตลาด · รายชื่อหุ้น · รายละเอียดหุ้น · วอทช์ลิสต์ ·
   หมุนเวียนกลุ่ม · เพิ่มเติม (stub)
   scope แช่ตายตัว: หุ้นไทย / DR / ETF — ฟีเจอร์ใหม่ไปเวอร์ชัน local
   ============================================================ */

/* ---------------- helpers ---------------- */
const $ = s => document.querySelector(s);
const el = (t, c, h) => { const e = document.createElement(t); if (c) e.className = c; if (h != null) e.innerHTML = h; return e; };
const nf = (n, d = 2) => (n == null || isNaN(n)) ? '–' : Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const sgn = n => n > 0 ? '+' : '';
const cls = n => n > 0 ? 'up' : n < 0 ? 'down' : 'flat';
const pct = (n, d = 2) => n == null ? '–' : sgn(n) + nf(n, d) + '%';
// ค่าเต็มเป็น "บาท" สำหรับ title (แตะค้าง/ชี้เมาส์เห็นตัวเลขจริงที่ไม่ถูกย่อ/ปัด)
function exactBaht(v) { return (v == null || isNaN(v)) ? '' : Math.round(v).toLocaleString('en-US') + ' บาท'; }
function fmtCap(b) { if (b == null) return '–'; const m = b / 1e6; if (m >= 1e6) return nf(m / 1e6, 3) + ' ล้านล้าน'; if (m >= 1e3) return nf(m / 1e3, 2) + ' พันล้าน'; return nf(m, 0) + ' ล้าน'; }
function fmtCapShort(b) { if (b == null) return '–'; const m = b / 1e6; if (m >= 1e6) return nf(m / 1e6, 3) + ' ลลบ.'; return nf(m, 0) + ' ลบ.'; }
function fmtBaht(v) {
  if (v == null || isNaN(v)) return '–';
  const s = v < 0 ? '-' : '', a = Math.abs(v);
  if (a >= 1e12) return s + nf(a / 1e12, 3) + ' ล้านล้าน';
  if (a >= 1e9) return s + nf(a / 1e9, 2) + ' พันล้าน';
  if (a >= 1e6) return s + nf(a / 1e6, 0) + ' ล้าน';
  return s + nf(a, 0);
}
function beQ(label) { const m = /^(\d{4})Q(\d)$/.exec(label); return m ? `Q${m[2]}/${(+m[1] + 543) % 100}` : label; }
function volX(t, a) { if (!t || !a) return ''; return nf(t / a, 1) + '× เฉลี่ย'; }
function shortName(n) {
  return String(n || '').replace(/^บริษัท\s*/, '').replace(/\s*จำกัด\s*\(มหาชน\)\s*$/, '')
    .replace(/\s*\(มหาชน\)\s*$/, '').replace(/\s*จำกัด\s*$/, '').trim();
}
const STAGE = { 1: ['สะสมฐาน', 'n'], 2: ['ขาขึ้น', 'g'], 3: ['แจกของ', 'w'], 4: ['ขาลง', 'r'] };
const THMONTH = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.', 'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.'];
function thaiDate(iso) {
  if (!iso) return '';
  const [y, m, d] = String(iso).slice(0, 10).split('-');
  return +d + ' ' + THMONTH[+m - 1] + ' ' + String(+y + 543).slice(2);
}

let _toastT;
function toast(msg) {
  const t = $('#toast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(_toastT); _toastT = setTimeout(() => t.classList.remove('show'), 3200);
}

async function loadJSON(url, timeoutMs = 30000) {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: ac.signal, cache: 'no-cache' });
    if (!r.ok) throw new Error(url + ' → HTTP ' + r.status);
    return await r.json();
  } finally { clearTimeout(to); }
}

/* ---------------- global state ---------------- */
const D = {
  stocks: [],      // SET + mai (normalized rows)
  dr: [],          // DR (normalized rows)
  etf: [],         // ETF (normalized rows)
  sectors: [],     // set_data.json .sectors (หมวดธุรกิจ ~35 กลุ่มละเอียด — สรุปรวมพร้อม ret_*)
  industries: [],  // set_data.json .industries (กลุ่มอุตสาหกรรม ~16 กลุ่มกว้าง + -mai)
  rows: [],        // all three concatenated
  byId: {},        // 'set:PTT' -> row
  fin: {},         // financials_analytics_yahoo .set  (symbol -> obj)
  finDr: {},       // .dr
  finQ: {},        // financials_quarterly .set (symbol -> {q, revenue, gross_profit, op_profit, net_profit})
  watchlist: [],   // ['AAV', ...]
  internals: null, // market_internals.json (new_highs / new_lows series ~3 เดือน)
  breadth: null,   // breadth_1y.json (pct_above_ema*, adv/dec, McClellan ย้อนหลัง 1 ปี)
  mstats: null,    // market_stats.json (PE/PBV/ปันผล/มูลค่าตลาด band ตลาด ย้อนหลังยาว)
  dailyVal: null,  // set_daily_valuation.json (PE/PBV/ปันผล/EPS SET & mai วันนี้)
  stockVal: null,  // stock_valuation_stats.json (PE/PBV + z-score รายหุ้น)
  indices: null,   // indices_data.json (ดัชนีกลุ่ม — โหลดเมื่อเปิดหน้าครั้งแรก, ~1.2MB gz)
  flow: null,      // market_flow.json (SET net buy/sell รายวัน — โหลดเมื่อเปิดหน้า "เงินทุน")
  flowS50: null,   // market_flow_s50.json (S50 futures)
  flowBond: null,  // market_flow_bond.json (ตราสารหนี้ ต่างชาติ)
  flowShort: null, // short_sales.json (short position รายหุ้น + daily_tail)
  flowSig: null,   // flow_signals.json (สัญญาณเงินทุนรวม — insider/short/nvdr dir + score)
  insider: {},     // { <days>: {r59:[...], r246:[...], fetched} } — โหลดตามปุ่ม 7/30/90/180
  rotAlerts: null, // rotation_alerts.json (transitions / pending — โหลดเมื่อเปิดหน้า "หมุนเวียน")
  asOf: '',
  mkt: null,       // computed market summary
};

/* ---------------- normalization ---------------- */
// SET / mai stock -> row
function rowSet(s) {
  return {
    id: 'set:' + s.symbol, kind: 'set',
    symbol: s.symbol, name: s.name_th || s.name || s.symbol,
    sub: s.sector || s.industry || '',
    tag: (s.market === 'mai' ? 'mai' : 'SET'),
    price: s.price, chg1d: s.ret_1d,
    ret_1w: s.ret_1w, ret_1m: s.ret_1m, ret_3m: s.ret_3m, ret_6m: s.ret_6m, ret_1y: s.ret_1y, ret_ytd: s.ret_ytd,
    rs: s.rs_score, mkt_cap: s.mkt_cap,
    high_52w: s.high_52w, low_52w: s.low_52w, ath_pct: s.ath_pct,
    above_ema20: s.above_ema20, above_ema50: s.above_ema50, above_ema200: s.above_ema200,
    ph: s.price_history || null, raw: s,
  };
}
// DR -> row
function rowDr(s) {
  const reg = s.region ? ('DR·' + s.region) : 'DR';
  return {
    id: 'dr:' + s.sym, kind: 'dr',
    symbol: s.sym, name: s.name || s.sym,
    sub: s.ind || '',
    tag: reg,
    price: s.price, chg1d: s.chg,
    ret_1w: s.ret_1w, ret_1m: s.ret_1m, ret_3m: s.ret_3m, ret_6m: s.ret_6m, ret_1y: s.ret_1y, ret_ytd: s.ret_ytd,
    rs: s.rs_score, mkt_cap: s.mkt_cap,
    high_52w: s.high_52w, low_52w: s.low_52w, ath_pct: s.ath_pct,
    above_ema20: null, above_ema50: s.above_ema50, above_ema200: s.above_ema200,
    ph: s.price_history || null, raw: s,
  };
}
const ETF_CAT = {
  TH_EQ: 'หุ้นไทย', FOREIGN: 'หุ้นต่างประเทศ', FOREIGN_EQ: 'หุ้นต่างประเทศ',
  BOND: 'ตราสารหนี้', FIXED_INCOME: 'ตราสารหนี้',
  COMMODITY: 'สินค้าโภคภัณฑ์', GOLD: 'ทองคำ', OIL: 'น้ำมัน',
  MIXED: 'ผสม', SECTOR: 'รายกลุ่ม', LEVERAGE: 'Leverage/Inverse',
};
// ETF -> row
function rowEtf(s) {
  return {
    id: 'etf:' + s.symbol, kind: 'etf',
    symbol: s.symbol, name: s.name_th || s.name_en || s.symbol,
    sub: s.category ? (ETF_CAT[s.category] || s.category) : 'กองทุน ETF',
    tag: 'ETF',
    price: s.price, chg1d: s.chg,
    ret_1w: s.ret_1w, ret_1m: s.ret_1m, ret_3m: s.ret_3m, ret_6m: s.ret_6m, ret_1y: s.ret_1y, ret_ytd: s.ret_ytd,
    rs: s.rs_score, mkt_cap: s.mkt_cap,
    high_52w: s.high_52w, low_52w: s.low_52w, ath_pct: s.ath_pct,
    above_ema20: null, above_ema50: s.above_ema50, above_ema200: s.above_ema200,
    ph: s.price_history || null, raw: s,
  };
}

/* ---------------- market computations (ported from dashboard.js) ---------------- */
// ทุก component ตัดหุ้นที่ค่านั้นเป็น null ออกจากทั้งเศษและส่วน (เหมือน c5 + rsDist) —
// หุ้น IPO ใหม่ที่ above_emaXXX/ret ยังเป็น null เดิมถูกนับเป็น bearish ถ่วง score ให้ต่ำเกินจริง
function _fracNonNull(stocks, key, test) {
  const w = stocks.filter(s => s[key] != null);
  return w.length ? w.filter(test).length / w.length * 100 : 50;
}
function calcFGI(stocks) {
  const n = stocks.length;
  if (!n) return { score: 50, c1: 50, c2: 50, c3: 50, c4: 50, c5: 50 };
  const c1 = _fracNonNull(stocks, 'above_ema50', s => s.above_ema50);
  const c2 = _fracNonNull(stocks, 'ret_1w', s => s.ret_1w > 0);
  const c3 = _fracNonNull(stocks, 'above_ema200', s => s.above_ema200);
  const c4 = _fracNonNull(stocks, 'ret_3m', s => s.ret_3m > 0);
  const w1m = stocks.filter(s => s.ret_1m != null);
  const avg1m = w1m.length ? w1m.reduce((a, s) => a + s.ret_1m, 0) / w1m.length : 0;
  const c5 = Math.max(0, Math.min(100, (avg1m + 15) / 30 * 100));
  const score = Math.round((c1 + c2 + c3 + c4 + c5) / 5);
  return { score, c1, c2, c3, c4, c5 };
}
function calcRegime(stocks) {
  const frac = (key, test) => {
    const w = stocks.filter(s => s[key] != null);
    return w.length ? w.filter(test).length / w.length * 100 : 0;
  };
  const pct200 = frac('above_ema200', s => s.above_ema200);
  const pct50 = frac('above_ema50', s => s.above_ema50);
  const pos3m = frac('ret_3m', s => s.ret_3m > 0);
  const pos1m = frac('ret_1m', s => s.ret_1m > 0);
  return Math.min(100, Math.round(pct200 * 0.35 + pct50 * 0.25 + pos3m * 0.25 + pos1m * 0.15));
}
function computeMarket() {
  const st = D.stocks;
  // % เหนือ EMA — นับเฉพาะหุ้นที่คำนวณเส้นได้ (above_emaXXX != null) เหมือน ver เต็ม
  const pEma = key => {
    const w = st.filter(s => s[key] != null);
    return w.length ? Math.round(w.filter(s => s[key]).length / w.length * 100) : 0;
  };
  const st1d = st.filter(s => s.chg1d != null);
  const avg1d = st1d.length ? st1d.reduce((a, s) => a + s.chg1d, 0) / st1d.length : 0;
  // นิยามเดียวกับ dashboard.js (นับ new-high ราย sector): price >= high_52w
  const nHigh = st.filter(s => s.high_52w > 0 && s.price >= s.high_52w).length;
  const nLow = st.filter(s => s.low_52w > 0 && s.price <= s.low_52w).length;
  // sector ranking: mean ret_1m by sector (exclude -mai bucket noise handled by keeping all)
  const agg = {};
  st.forEach(s => {
    if (!s.sub || s.ret_1m == null) return;
    (agg[s.sub] || (agg[s.sub] = [])).push(s.ret_1m);
  });
  const sectors = Object.entries(agg)
    .map(([k, v]) => [k, +(v.reduce((a, b) => a + b, 0) / v.length).toFixed(2), v.length])
    .sort((a, b) => b[1] - a[1]);
  return {
    total: st.length,
    ema20: pEma('above_ema20'), ema50: pEma('above_ema50'), ema200: pEma('above_ema200'),
    avg1d,
    rs80: st.filter(s => (s.rs || 0) >= 80).length,
    nHigh, nLow,
    fgi: calcFGI(st), regime: calcRegime(st),
    sectors,
  };
}
// penny flag = dq.flags ของ set_data.json (เหมือน _dqIsPenny ใน dashboard.js) —
// tick เดียวของหุ้นราคาไม่กี่สตางค์ทำ ret_1d/RS กระโดดจนครองอันดับปลอม
const isPenny = r => ((r.raw && r.raw.dq && r.raw.dq.flags) || []).includes('penny');
// RS distribution binning (ตรงกับ bins ใน renderOverview ของ dashboard.js —
// ตัด rs == null ออกก่อน ไม่งั้นหุ้น ineligible ไปกองใน bin แรก)
function rsDist(stocks) {
  const defs = [
    ['0–19', 0, 20, 'var(--down)'], ['20–39', 20, 40, '#e0682f'],
    ['40–59', 40, 60, 'var(--warn)'], ['60–69', 60, 70, '#5b8def'],
    ['70–79', 70, 80, 'var(--accent)'], ['80–89', 80, 90, '#4bb36b'],
    ['90–99', 90, 100, 'var(--up)'],
  ];
  return defs.map(([label, mn, mx, color]) => {
    const n = stocks.filter(s => s.rs != null && s.rs >= mn && s.rs < mx).length;
    // ความสูงแท่งใช้ density (หุ้นต่อช่วง 10 แต้ม) — bin 0–19/20–39/40–59 กว้าง 20 แต้ม
    // ถ้าพล็อตจำนวนดิบแท่งกว้างเท่ากันจะสูงเกินจริง ~2 เท่าเสมอ (rs เป็น percentile uniform)
    return { label, color, n, dens: n / ((mx - mn) / 10) };
  });
}

/* ---------------- boot ---------------- */
async function boot() {
  const bmsg = $('#bootMsg');
  let sd;
  try {
    sd = await loadJSON('../data/set_data.json', 45000);
  } catch (e) {
    return bootError('โหลดข้อมูลตลาดไม่สำเร็จ — ตรวจการเชื่อมต่อแล้วลองใหม่');
  }
  D.asOf = sd.data_as_of || sd.updated_at || '';
  D.stocks = (sd.stocks || []).map(rowSet);
  D.sectors = sd.sectors || [];
  D.industries = sd.industries || [];

  bmsg.textContent = 'กำลังโหลด DR / ETF / งบ…';
  // market_stats / set_daily_valuation / stock_valuation_stats ใช้แค่จอ Valuation (subpage
  // ใต้ "เพิ่มเติม") — lazy load ตอนเปิดหน้าครั้งแรก เหมือน indices / flow ไม่ยิงตอน boot
  const [dr, etf, fin, finQ, wl, intl, brd] = await Promise.allSettled([
    loadJSON('../data/dr_data.json', 30000),
    loadJSON('../data/etf_data.json', 30000),
    loadJSON('../data/financials_analytics_yahoo.json', 30000),
    loadJSON('../data/financials_quarterly.json', 30000),
    loadJSON('../data/watchlist.json', 15000),
    loadJSON('../data/market_internals.json', 20000),
    loadJSON('../data/breadth_1y.json', 20000),
  ]);
  const missing = [];
  if (dr.status === 'fulfilled') D.dr = (dr.value.stocks || []).map(rowDr); else missing.push('DR');
  if (etf.status === 'fulfilled') D.etf = (etf.value.stocks || []).map(rowEtf); else missing.push('ETF');
  if (fin.status === 'fulfilled') { D.fin = fin.value.set || {}; D.finDr = fin.value.dr || {}; } else missing.push('งบการเงิน');
  if (finQ.status === 'fulfilled') D.finQ = finQ.value.set || {};
  if (wl.status === 'fulfilled' && Array.isArray(wl.value)) D.watchlist = wl.value;
  if (intl.status === 'fulfilled') D.internals = intl.value;
  if (brd.status === 'fulfilled') D.breadth = brd.value;
  if (intl.status !== 'fulfilled' || brd.status !== 'fulfilled') missing.push('ข้อมูลย้อนหลัง');

  D.rows = [...D.stocks, ...D.dr, ...D.etf];
  D.rows.forEach(r => { D.byId[r.id] = r; });
  D.mkt = computeMarket();

  $('#boot').classList.add('hidden');
  $('#app').hidden = false;
  wireNav();
  renderMarket(); renderStocks(); renderWatch(); renderMore();
  // renderRotation() ไม่เรียกตอน boot — มี canvas + fetch rotation_alerts, ให้ go('rotation') จัดการ
  setScrHeader('market');
  if (missing.length) toast('โหลดไม่ครบ: ' + missing.join(', ') + ' — บางส่วนอาจไม่แสดง');
  registerSW();
}
function bootError(msg) {
  const b = $('#boot');
  b.innerHTML = `<div class="msg" style="font-size:15px;color:var(--text)">${msg}</div>
    <button class="btn" onclick="location.reload()">ลองใหม่</button>`;
}
function registerSW() {
  if ('serviceWorker' in navigator && location.protocol.startsWith('http')) {
    navigator.serviceWorker.register('sw.js').catch(() => {});
  }
}

/* ---------------- navigation ---------------- */
let curScreen = 'market', curId = null, detailFrom = 'market';
const TITLES = {
  market: () => ['ตลาด', 'SET & mai · ปิดตลาด ' + thaiDate(D.asOf)],
  stocks: () => ['รายชื่อหุ้น', D.rows.length + ' หลักทรัพย์ · ' + thaiDate(D.asOf)],
  watch: () => ['วอทช์ลิสต์', D.watchlist.length + ' รายการ · ปิดตลาด ' + thaiDate(D.asOf)],
  rotation: () => ['การหมุนเวียนกลุ่ม', 'แผนที่หมุนเวียน (RRG) + สัญญาณเปลี่ยนโซน'],
  more: () => ['เพิ่มเติม', 'ขอบเขตของเวอร์ชันมือถือ'],
  indices: () => ['ดัชนีกลุ่มอุตสาหกรรม', 'ผลตอบแทน · RS · โมเมนตัม รายดัชนี'],
  valuation: () => ['Valuation', 'กรอบ PE / PBV / ปันผล เทียบค่าเฉลี่ยย้อนหลัง'],
  heatmap: () => ['Heatmap', 'ภาพรวมตลาดไทยแยกกลุ่มอุตสาหกรรม'],
  flow: () => ['เงินทุน', 'กระแสเงิน · ชอร์ต · ผู้บริหาร · สัญญาณรวม'],
  // subtitle ที่แท้จริง (จำนวนหุ้นที่ตรงเงื่อนไข + วันที่) เขียนโดย applyScr() ทันทีหลัง render
  screener: () => ['สแกนหุ้น', 'กรอง RS · สเตจ · ราคา · งบการเงิน'],
};
const SUBPAGES = new Set(['indices', 'valuation', 'heatmap', 'flow', 'screener']);
function setScrHeader(scr) {
  if (scr === 'detail') {
    const r = D.byId[curId];
    $('#scrTitle').textContent = r.symbol;
    const sn = shortName(r.name);
    $('#scrSub').textContent = sn.length > 40 ? sn.slice(0, 40) + '…' : sn;
  } else {
    const [t, s] = TITLES[scr]();
    $('#scrTitle').textContent = t;
    $('#scrSub').textContent = s;
  }
}
// จำตำแหน่งเลื่อน (window.scrollY) ของแต่ละ screen ตอนออกจากจอนั้น —
// กดเข้าดูหุ้นรายตัวแล้วกดปุ่ม ‹ กลับ ให้ค้างตำแหน่งเดิม ไม่เด้งขึ้นบนสุด
const _scrollMem = {};
function go(scr, restore) {
  if (curScreen && curScreen !== scr) _scrollMem[curScreen] = window.scrollY;
  curScreen = scr;
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  $('#s-' + scr).classList.add('active');
  const navScr = SUBPAGES.has(scr) ? 'more' : scr;
  document.querySelectorAll('.tabbar button').forEach(b => b.classList.toggle('on', b.dataset.scr === navScr));
  $('#backBtn').hidden = scr !== 'detail' && !SUBPAGES.has(scr);
  setScrHeader(scr);
  // หน้ารายชื่อหุ้นเปิดค้าง view ไว้ — วาดซ้ำให้บรรทัดสรุป (#scrSub) ตรงกับ view/ฟิลเตอร์ปัจจุบัน
  if (scr === 'stocks') paintStkView();
  // หน้าที่มี canvas: วาดใหม่ตอนเปิด (canvas ที่ซ่อนอยู่ตอน render แรกกว้าง 0
  // หรือ viewport เปลี่ยนความกว้างตอนอยู่จออื่น — resize handler วาดแค่ curScreen)
  else if (scr === 'market' && D.mkt) renderMarket();
  else if (scr === 'rotation') renderRotation();
  else if (scr === 'indices') renderIndices();
  else if (scr === 'valuation') { renderValuation(); loadValuationData(); }
  else if (scr === 'heatmap') renderHeatmap();
  else if (scr === 'flow') renderFlow();
  else if (scr === 'screener') renderScreener();
  // restore = กดปุ่ม ‹ กลับจากหน้ารายละเอียดหุ้น (หรือ subpage) — คืนตำแหน่งเลื่อนเดิม
  const memY = restore ? _scrollMem[scr] : null;
  if (memY != null && memY > 0) {
    window.scrollTo(0, memY);
    requestAnimationFrame(() => window.scrollTo(0, memY));
  } else {
    window.scrollTo(0, 0);
  }
}
function openDetail(id) {
  if (!D.byId[id]) return;
  curId = id; detailFrom = curScreen;
  renderDetail();
  go('detail');
}
function wireNav() {
  $('#tabbar').addEventListener('click', e => { const b = e.target.closest('button'); if (b) go(b.dataset.scr); });
  $('#backBtn').addEventListener('click', () => go(curScreen === 'detail' ? detailFrom : 'more', true));
  $('#infoBtn').addEventListener('click', () => { $('#scrim').classList.add('open'); $('#sheet').classList.add('open'); });
  $('#scrim').addEventListener('click', () => { $('#scrim').classList.remove('open'); $('#sheet').classList.remove('open'); });
}

/* ---------------- row component ---------------- */
function listRow(r, metric = 'pct') {
  const b = el('button', 'row');
  let sub;
  if (metric === 'rs') sub = `<span class="dl flat">RS ${r.rs ?? '–'}</span>`;
  else if (metric === 'cap') sub = `<span class="dl flat">${fmtCapShort(r.mkt_cap)}</span>`;
  else if (metric === 'dy') { const dy = r.raw && r.raw.div_yield; sub = `<span class="dl flat">${dy == null ? 'ไม่จ่ายปันผล' : 'ปันผล ' + nf(dy, 2) + '%'}</span>`; }
  else if (metric === '1m') sub = `<span class="dl ${cls(r.ret_1m)}">${pct(r.ret_1m)} · 1ด</span>`;
  else sub = `<span class="dl ${cls(r.chg1d)}">${pct(r.chg1d)}</span>`;
  b.innerHTML = `
    <span class="lead">
      <span class="nm"><span class="tkr-lg">${r.symbol}</span><span class="mkt-tag">${r.tag}</span></span>
      <span class="sub2">${shortName(r.name)}${r.sub ? ' · ' + r.sub : ''}</span>
    </span>
    <span class="trail"><div class="px">${nf(r.price, r.price < 1 ? 3 : 2)}</div>${sub}</span>
    <span class="chev">›</span>`;
  b.addEventListener('click', () => openDetail(r.id));
  return b;
}
function rsLeaderRow(r) {
  const b = el('button', 'rlr');
  b.innerHTML = `
    <span class="rk ${r.rs >= 90 ? 'hot' : ''}">${r.rs ?? '–'}</span>
    <span class="mid2">
      <span class="tkr-lg">${r.symbol}</span>
      <span class="sub2">${shortName(r.name)}${r.sub ? ' · ' + r.sub : ''}</span>
    </span>
    <span class="mo"><span class="mv ${cls(r.ret_1m)}">${pct(r.ret_1m)}</span><span class="ml">1 เดือน</span></span>
    <span class="chev">›</span>`;
  b.addEventListener('click', () => openDetail(r.id));
  return b;
}
function secHead(t, linkTxt, linkFn) {
  const d = el('div', 'sec');
  d.innerHTML = `<div class="sec-head"><span class="sec-label">${t}</span>${linkTxt ? `<button class="sec-link">${linkTxt}</button>` : ''}</div>`;
  if (linkFn) d.querySelector('button').addEventListener('click', linkFn);
  return d;
}
function barRow(label, val, color, wide) {
  const r = el('div', 'bar-row');
  const w = Math.max(2, Math.min(100, val));
  r.innerHTML = `<span class="bl${wide ? ' wide' : ''}">${label}</span>
    <span class="track"><span class="fill ${color || ''}" style="width:${w}%"></span></span>
    <span class="bv">${Math.round(val)}%</span>`;
  return r;
}
function dvRow(name, v) {
  const r = el('div', 'bar-row');
  const w = Math.min(48, Math.abs(v) * 1.6 + 1);
  r.innerHTML = `<span class="bl wide">${name}</span>
    <span class="dv"><span class="mid"></span><span class="seg ${v >= 0 ? 'g' : 'r'}" style="width:${w}%"></span></span>
    <span class="bv ${cls(v)}">${pct(v, 1)}</span>`;
  return r;
}

/* ---------------- MARKET screen ---------------- */
let _mvSide = 'up';   // top movers: ขึ้นแรง/ลงแรง — เก็บระดับโมดูลเหมือนทุกจอ ไม่งั้น re-render (resize) รีเซ็ต
function renderMarket() {
  const m = $('#s-market'); m.innerHTML = '';
  const k = D.mkt;

  const hero = el('div', 'mkt-hero');
  hero.innerHTML = `
    <div class="as-of">อัปเดตอัตโนมัติหลังตลาดปิด · ล่าสุด ${thaiDate(D.asOf)}</div>
    <div class="big">
      <span class="idx ${cls(k.avg1d)}">${pct(k.avg1d)}</span>
      <span class="idx-d">ผลตอบแทนเฉลี่ยทั้งตลาดวันนี้</span>
    </div>
    <div class="hero-ctx">${k.total} หลักทรัพย์ใน SET &amp; mai</div>`;
  m.appendChild(hero);

  const regimeLbl = k.regime >= 65 ? 'ตลาดกระทิง' : k.regime >= 40 ? 'เป็นกลาง' : 'ตลาดหมี';
  const tiles = el('div', 'tiles');
  tiles.innerHTML = `
    <div class="tile"><div class="t-l">ความกว้างตลาด</div><div class="t-v">${k.ema200}%</div><div class="t-s">เหนือ EMA200</div></div>
    <div class="tile"><div class="t-l">โมเมนตัมระยะสั้น</div><div class="t-v">${k.ema20}%</div><div class="t-s">เหนือ EMA20</div></div>
    <div class="tile"><div class="t-l">สภาพตลาด</div><div class="t-v">${k.regime}</div><div class="t-s">${regimeLbl}</div></div>
    <div class="tile"><div class="t-l">RS 80 ขึ้นไป</div><div class="t-v">${k.rs80}</div><div class="t-s">หุ้นแข็งกว่าตลาด</div></div>`;
  m.appendChild(tiles);

  // fear & greed
  const g = k.fgi;
  const fgLbl = g.score >= 75 ? 'โลภสุดขีด' : g.score >= 55 ? 'โลภ' : g.score >= 45 ? 'เป็นกลาง' : g.score >= 25 ? 'กลัว' : 'กลัวสุดขีด';
  const fgWrap = el('div');
  fgWrap.appendChild(secHead('ดัชนีความกลัว–ความโลภ'));
  const fg = el('div', 'fg');
  fg.innerHTML = `
    <div class="fg-val"><b>${g.score}</b><span>${fgLbl} · คำนวณจากข้อมูลตลาดจริง</span></div>
    <div class="fg-scale"><div class="fg-mark" style="left:${g.score}%"></div></div>
    <div class="fg-ends"><span>กลัวสุดขีด</span><span>โลภสุดขีด</span></div>`;
  fgWrap.appendChild(fg);
  [['เหนือ EMA50', g.c1], ['หุ้นบวกใน 1 สัปดาห์', g.c2], ['เหนือ EMA200', g.c3],
   ['โมเมนตัม 3 เดือนเป็นบวก', g.c4], ['ผลตอบแทนเฉลี่ย 1 เดือน', g.c5]]
    .forEach(([lab, v]) => fgWrap.appendChild(barRow(lab, v, v >= 55 ? 'g' : v >= 45 ? '' : 'r', true)));
  m.appendChild(fgWrap);

  // RS distribution histogram
  const rd = rsDist(D.stocks);
  const rdMax = Math.max(...rd.map(b => b.dens), 1);
  const rs90 = rd[6].n, rs80 = rd[5].n + rd[6].n, rs70 = rd[4].n + rs80;
  const rdWrap = el('div');
  rdWrap.appendChild(secHead('การกระจายค่า RS'));
  const rdBars = el('div', 'rsd');
  rdBars.innerHTML = rd.map(b =>
    `<span class="col"><span class="cnt">${b.n}</span><span class="bar" style="height:${Math.round(b.dens / rdMax * 100)}%;background:${b.color}"></span></span>`
  ).join('');
  rdWrap.appendChild(rdBars);
  rdWrap.appendChild(el('div', 'rsd-x', rd.map(b => `<span>${b.label}</span>`).join('')));
  rdWrap.appendChild(el('div', 'rsd-sum', `RS 90+ : ${rs90} ตัว  ·  RS 80+ : ${rs80} ตัว  ·  RS 70+ : ${rs70} ตัว`));
  m.appendChild(rdWrap);

  // RS leaders — 10 หุ้นแข็งแกร่งสุด (กัน penny stock ออก เหมือน dashboard.js)
  const leaders = D.stocks.filter(s => s.rs != null && !isPenny(s))
    .sort((a, b) => (b.rs || 0) - (a.rs || 0)).slice(0, 10);
  if (leaders.length) {
    const ldWrap = el('div');
    ldWrap.appendChild(secHead('หุ้นนำตลาด · RS สูงสุด'));
    leaders.forEach(r => ldWrap.appendChild(rsLeaderRow(r)));
    m.appendChild(ldWrap);
  }

  // 52w high / low  (+ กราฟ new-high / new-low ย้อนหลัง ~3 เดือน)
  const hl = el('div');
  hl.appendChild(secHead('จุดสูง–ต่ำ 52 สัปดาห์ วันนี้'));
  hl.appendChild(statRow('ทำจุดสูงใหม่', `<span class="up">${k.nHigh} ตัว</span>`));
  hl.appendChild(statRow('ทำจุดต่ำใหม่', `<span class="down">${k.nLow} ตัว</span>`));
  const intl = D.internals;
  if (intl && Array.isArray(intl.new_highs) && intl.new_highs.length > 1) {
    hl.appendChild(el('div', 'chart-wrap', '<canvas class="spark" id="nhnlChart" style="height:140px"></canvas>'));
    hl.appendChild(el('div', 'chart-src',
      '<span class="up">▬</span> ทำจุดสูงใหม่  ·  <span class="down">▬</span> ทำจุดต่ำใหม่'));
    _kickCanvas('nhnlChart', cv => drawNHNL(cv, intl.dates, intl.new_highs, intl.new_lows));
  }
  m.appendChild(hl);

  // market breadth — ย้อนหลัง 1 ปี (adv/dec, % เหนือ EMA, McClellan)
  const bd = D.breadth;
  if (bd && Array.isArray(bd.dates) && bd.dates.length > 5) {
    const n1 = bd.dates.length - 1;
    const bw = el('div');
    bw.appendChild(secHead('ความกว้างของตลาด · ย้อนหลัง 1 ปี'));
    bw.appendChild(statRow('หุ้นบวก / ลบ วันนี้',
      `<span class="up">${bd.adv[n1]}</span> / <span class="down">${bd.dec[n1]}</span>`));
    bw.appendChild(el('div', 'chart-wrap', '<canvas class="spark" id="bdEmaChart" style="height:130px"></canvas>'));
    bw.appendChild(el('div', 'chart-src',
      `<span style="color:var(--warn)">▬</span> เหนือ EMA50 ${nf(bd.pct_above_ema50[n1], 0)}%   ·   <span class="up">▬</span> เหนือ EMA200 ${nf(bd.pct_above_ema200[n1], 0)}%`));
    _kickCanvas('bdEmaChart', cv => drawBreadthEMA(cv, bd.dates, bd.pct_above_ema50, bd.pct_above_ema200));
    const mcc = bd.mcclellan_osc[n1], mccSum = bd.mcclellan_sum[n1];
    bw.appendChild(el('div', 'chart-wrap', '<canvas class="spark" id="bdMccChart" style="height:120px"></canvas>'));
    bw.appendChild(el('div', 'chart-src',
      `McClellan Oscillator <span class="${cls(mcc)}">${nf(mcc, 1)}</span> · summation ${sgn(mccSum)}${nf(mccSum, 0)}`));
    _kickCanvas('bdMccChart', cv => drawMccBars(cv, bd.dates, bd.mcclellan_osc));
    m.appendChild(bw);
  }

  // top movers
  const mv = el('div');
  const head = el('div', 'sec');
  head.innerHTML = `<div class="sec-head"><span class="sec-label">หุ้นเคลื่อนไหวมากสุดวันนี้</span></div>
    <div class="seg" id="mvSeg"><button class="${_mvSide === 'up' ? 'on' : ''}" data-k="up">ขึ้นแรง</button><button class="${_mvSide === 'down' ? 'on' : ''}" data-k="down">ลงแรง</button></div>`;
  mv.appendChild(head);
  const mvList = el('div'); mvList.id = 'mvList';
  mv.appendChild(mvList); m.appendChild(mv);
  const paint = kk => {
    _mvSide = kk;
    mvList.innerHTML = '';
    // กัน penny ออกด้วย dq flag เหมือน gainers/losers ของ dashboard.js (เดิมใช้ price>0.5
    // ซึ่งตัดหุ้น 0.10–0.49 บาทที่ไม่ใช่ penny ทิ้งไปด้วย ทำให้รายชื่อไม่ตรงกับ ver เต็ม)
    const arr = D.stocks.filter(s => s.chg1d != null && !isPenny(s))
      .sort((a, b) => kk === 'up' ? b.chg1d - a.chg1d : a.chg1d - b.chg1d).slice(0, 20);
    arr.forEach(s => mvList.appendChild(listRow(s)));
  };
  paint(_mvSide);
  head.querySelector('#mvSeg').addEventListener('click', e => {
    const btn = e.target.closest('button'); if (!btn) return;
    head.querySelectorAll('#mvSeg button').forEach(x => x.classList.toggle('on', x === btn));
    paint(btn.dataset.k);
  });

  // sector strip
  const sc = el('div');
  sc.appendChild(secHead('กลุ่มอุตสาหกรรม · 1 เดือน', 'ดูทั้งหมด', () => go('rotation')));
  k.sectors.slice(0, 5).forEach(([n, v]) => sc.appendChild(dvRow(n, v)));
  k.sectors.slice(-3).forEach(([n, v]) => sc.appendChild(dvRow(n, v)));
  m.appendChild(sc);
}

/* ---------------- STOCKS list ---------------- */
// view tabs พอร์ตจากหน้า "หุ้นทั้งหมด" ของเว็บ (setStocksView / renderBreakout /
// renderMomentum / renderEMABreadth / renderSectorRankTable) — predicate เดียวกัน
// หุ้นไทย (SET+mai) เท่านั้นสำหรับทุก view ยกเว้น 'all' ที่รวม DR/ETF เหมือนเดิม
let stkView = 'all';
let listFilter = 'all', listSort = 'cap', listQuery = '';
let _boSide = 'high', _boRS = 70, _boEMA = '50';   // breakout: ใกล้จุดสูง/ต่ำ 52 สัปดาห์
let _momTf = 'all4';                                // momentum: จำนวน timeframe ที่ต้องบวกพร้อมกัน
let _secBy = 'sector';                              // emaBreadth / sectorRank: หมวดธุรกิจ vs กลุ่มอุตสาหกรรม

const STK_VIEWS = [
  ['all', 'ทั้งหมด'], ['rs80', 'RS 80+'], ['emerging', '🌱 กำลังมา'],
  ['stage2', 'Stage 2'], ['breakout', '52W สูง/ต่ำ'], ['momentum', '⚡ โมเมนตัม'],
  ['emaBreadth', 'ความกว้าง EMA'], ['sectorRank', 'จัดอันดับกลุ่ม'],
];
const KIND_FILTERS = [['all', 'ทั้งหมด'], ['SET', 'SET'], ['mai', 'mai'], ['DR', 'DR'], ['ETF', 'ETF']];
// DR แยกตามตลาดต้นทาง (raw.region ของ dr_data.json: US/HK/JP/VN/CN/SG/EU/TW) —
// 5 ประเทศหลักโชว์เดี่ยว ที่เหลือ (CN/SG/TW/…) รวมใน "อื่นๆ"
const DR_REGIONS = [
  ['all', 'ทั้งหมด'], ['US', '🇺🇸 สหรัฐ'], ['HK', '🇭🇰 ฮ่องกง'], ['JP', '🇯🇵 ญี่ปุ่น'],
  ['VN', '🇻🇳 เวียดนาม'], ['EU', '🇪🇺 ยุโรป'], ['other', '🌏 อื่นๆ'],
];
const DR_REGION_KNOWN = new Set(['US', 'HK', 'JP', 'VN', 'EU']);
let drRegion = 'all';
const SORT_OPTS = [['cap', 'มูลค่าตลาด'], ['pct', '% วันนี้'], ['ret_1m', '% 1 เดือน'], ['rs', 'RS'], ['az', 'ก–ฮ']];
const SORTF = {
  cap: (a, b) => (b.mkt_cap || 0) - (a.mkt_cap || 0),
  pct: (a, b) => (b.chg1d ?? -999) - (a.chg1d ?? -999),
  ret_1m: (a, b) => (b.ret_1m ?? -999) - (a.ret_1m ?? -999),
  rs: (a, b) => (b.rs ?? -1) - (a.rs ?? -1),
  mom: (a, b) => (((b.raw && b.raw.rs_momentum) ?? b.ret_1m ?? -999) - ((a.raw && a.raw.rs_momentum) ?? a.ret_1m ?? -999)),
  az: (a, b) => a.symbol < b.symbol ? -1 : 1,
};
const VIEW_DEFAULT_SORT = { all: 'cap', rs80: 'rs', emerging: 'mom', stage2: 'rs', momentum: 'ret_1m' };
const VIEW_NOTE = {
  rs80: 'หุ้นไทยที่ค่า RS ตั้งแต่ 80 ขึ้นไป — แข็งแรงกว่าตลาดในรอบ ~1 ปี',
  emerging: 'RS 35–79 กำลังไต่ขึ้นเร็ว: ผลตอบแทน 1 เดือน ≥ 3% และราคายืนเหนือ EMA50',
  stage2: 'สเตจ 2 ตามหลักไวน์สไตน์ — ราคายืนเหนือ EMA200 (ตัวกรองสเตจของแอปเต็ม)',
  momentum: 'ผลตอบแทนเป็นบวกพร้อมกันในทุกช่วงเวลาที่เลือก',
};

function matchesKind(r) {
  if (listFilter === 'all') return true;
  if (listFilter === 'DR') {
    if (r.kind !== 'dr') return false;
    if (drRegion === 'all') return true;
    const rg = (r.raw && r.raw.region) || '';
    return drRegion === 'other' ? !DR_REGION_KNOWN.has(rg) : rg === drRegion;
  }
  if (listFilter === 'ETF') return r.kind === 'etf';
  return r.tag === listFilter; // SET | mai
}
// พอร์ตจาก getStage() ของ dashboard.js — ถ้า raw.stage เป็น null ประเมินจาก
// above_ema200 + ema200_slope_pct (ไม่ guess ถ้าข้อมูลไม่พอ) ให้ผล Stage 2 ตรงกับตัวเต็ม
function stageOf(raw) {
  if (!raw) return null;
  if (raw.stage != null) return raw.stage;
  const above = raw.above_ema200;
  if (above == null) return null;
  const slope = raw.ema200_slope_pct ?? null;
  if (slope == null) return null;
  if (above && slope >= 0) return 2;
  if (above && slope < 0) return 3;
  if (!above && slope >= -1.5) return 1;
  return 4;
}
function viewPredicate(view) {
  if (view === 'rs80') return s => s.rs != null && s.rs >= 80;
  if (view === 'stage2') return s => stageOf(s.raw) === 2;
  if (view === 'emerging') return s => {
    const rs = s.rs || 0;
    return rs >= 35 && rs < 80 && (s.ret_1m || 0) >= 3 && s.above_ema50 === true && !isPenny(s);
  };
  if (view === 'momentum') return s => {
    const g = k => (s[k] ?? -1) > 0;
    if (_momTf === 'all4') return g('chg1d') && g('ret_1w') && g('ret_1m') && g('ret_3m');
    if (_momTf === '3tf') return g('ret_1w') && g('ret_1m') && g('ret_3m');
    return g('ret_1m') && g('ret_3m');
  };
  return () => true;
}

function renderStocks() {
  const c = $('#s-stocks'); c.innerHTML = '';
  const tabs = el('div', 'vtabs');
  tabs.innerHTML = STK_VIEWS.map(([k, t]) => `<button class="vtab ${stkView === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(tabs);
  const main = el('div'); main.id = 'stkMain'; c.appendChild(main);
  tabs.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    stkView = b.dataset.k;
    tabs.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    if (VIEW_DEFAULT_SORT[stkView]) listSort = VIEW_DEFAULT_SORT[stkView];
    b.scrollIntoView({ block: 'nearest', inline: 'center' });
    paintStkView();
  });
  paintStkView();
}

function paintStkView() {
  const main = $('#stkMain'); if (!main) return;
  main.innerHTML = '';
  if (stkView === 'breakout') return breakoutView(main);
  if (stkView === 'emaBreadth') return groupBreadthView(main);
  if (stkView === 'sectorRank') return sectorRankView(main);
  stockListView(main, stkView);
}

/* ---- stock-list views (all / rs80 / emerging / stage2 / momentum) ---- */
function stockListView(main, view) {
  const isAll = view === 'all';
  const pool = isAll ? D.rows : D.stocks;
  const pred = viewPredicate(view);
  const sortOpts = view === 'emerging'
    ? [['mom', 'เร่ง RS'], ...SORT_OPTS] : SORT_OPTS;
  if (!SORTF[listSort]) listSort = 'cap';

  main.innerHTML = `
    <div class="search"><svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="stkSearch" placeholder="ค้นหาชื่อย่อ / ชื่อบริษัท"></div>
    ${isAll ? `<div class="seg" id="stkKind">${KIND_FILTERS.map(([k, t]) => `<button data-k="${k}" class="${listFilter === k ? 'on' : ''}">${t}</button>`).join('')}</div>` : ''}
    ${isAll && listFilter === 'DR' ? `<div class="sortbar" id="drRegion">${DR_REGIONS.map(([k, t]) => `<button class="sortchip ${drRegion === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>` : ''}
    ${view === 'momentum' ? `<div class="seg" id="momTf">${[['all4', '1D·1W·1M·3M'], ['3tf', '1W·1M·3M'], ['2tf', '1M·3M']].map(([k, t]) => `<button data-k="${k}" class="${_momTf === k ? 'on' : ''}">${t}</button>`).join('')}</div>` : ''}
    <div class="sortbar" id="stkSort">${sortOpts.map(([k, t]) => `<button class="sortchip ${listSort === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>
    ${!isAll ? `<div class="note" id="viewNote"></div>` : ''}
    <div id="stkList"></div>`;
  $('#stkSearch').value = listQuery;

  const paint = () => {
    const q = listQuery.trim().toLowerCase();
    let arr = pool.filter(r => {
      if (isAll ? !matchesKind(r) : !pred(r)) return false;
      // ค้นได้ทั้ง ticker / ชื่อที่แสดง (name_th) / ชื่ออังกฤษเดิม (raw.name) — เหมือน
      // renderStocksTable ของตัวเต็มที่เช็ค symbol + name + name_th ครบ
      if (q) return r.symbol.toLowerCase().includes(q) || String(r.name).toLowerCase().includes(q)
        || !!(r.raw && r.raw.name && String(r.raw.name).toLowerCase().includes(q));
      return true;
    }).sort(SORTF[listSort] || SORTF.cap);
    const vn = $('#viewNote');
    if (vn) vn.textContent = (VIEW_NOTE[view] || '') + ' · ' + arr.length + ' ตัว';
    const box = $('#stkList'); box.innerHTML = '';
    if (!arr.length) {
      box.appendChild(el('div', 'list-cap', q ? `ไม่พบหลักทรัพย์ที่ตรงกับ “${listQuery}”` : 'ไม่มีหลักทรัพย์ที่ตรงเงื่อนไข'));
      $('#scrSub').textContent = '0 หลักทรัพย์ · ปิดตลาด ' + thaiDate(D.asOf);
      return;
    }
    const metric = listSort === 'rs' ? 'rs' : listSort === 'cap' ? 'cap'
      : (listSort === 'ret_1m' || listSort === 'mom') ? '1m' : 'pct';
    const MAX = 400;
    arr.slice(0, MAX).forEach(r => box.appendChild(listRow(r, metric)));
    if (arr.length > MAX) box.appendChild(el('div', 'list-cap', `แสดง ${MAX} จาก ${arr.length} — พิมพ์ค้นหาเพื่อกรอง`));
    $('#scrSub').textContent = arr.length + ' หลักทรัพย์ · ปิดตลาด ' + thaiDate(D.asOf);
  };

  $('#stkSearch').addEventListener('input', e => { listQuery = e.target.value; paint(); });
  const kindEl = $('#stkKind');
  if (kindEl) kindEl.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    const wasDr = listFilter === 'DR';
    listFilter = b.dataset.k;
    // เข้า/ออก DR: ต้อง re-render เพื่อโชว์/ซ่อนแถบเลือกประเทศ
    if (wasDr !== (listFilter === 'DR')) return paintStkView();
    kindEl.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  const drEl = $('#drRegion');
  if (drEl) drEl.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    drRegion = b.dataset.k;
    drEl.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  const tfEl = $('#momTf');
  if (tfEl) tfEl.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _momTf = b.dataset.k;
    tfEl.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  $('#stkSort').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    listSort = b.dataset.k;
    $('#stkSort').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  paint();
}

/* ---- breakout view: หุ้นใกล้ทำจุดสูง/ต่ำ 52 สัปดาห์ (พอร์ตจาก renderBreakout) ---- */
function breakoutView(main) {
  const RS_OPTS = [[0, 'RS ไม่กรอง'], [50, 'RS 50+'], [70, 'RS 70+'], [80, 'RS 80+']];
  const EMA_OPTS = [['any', 'EMA ไม่กรอง'], ['50', '> EMA50'], ['200', '> EMA50+200']];
  const DIST = 10;
  main.innerHTML = `
    <div class="seg" id="boSide">
      <button data-k="high" class="${_boSide === 'high' ? 'on' : ''}">ใกล้จุดสูง 52 สัปดาห์</button>
      <button data-k="low" class="${_boSide === 'low' ? 'on' : ''}">ใกล้จุดต่ำ 52 สัปดาห์</button>
    </div>
    <div class="sortbar" id="boRS">${RS_OPTS.map(([k, t]) => `<button class="sortchip ${_boRS === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>
    <div class="sortbar" id="boEMA">${EMA_OPTS.map(([k, t]) => `<button class="sortchip ${_boEMA === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>
    <div class="note" id="boNote"></div>
    <div id="boList"></div>`;

  const paint = () => {
    const isHigh = _boSide === 'high';
    const arr = D.stocks.map(s => {
      const anchor = isHigh ? s.high_52w : s.low_52w;
      const from = (anchor > 0 && s.price != null) ? (s.price - anchor) / anchor * 100 : null;
      return { s, from };
    }).filter(o => {
      if (o.from == null) return false;
      if ((o.s.rs || 0) < _boRS) return false;
      if (isHigh ? (o.from < -DIST) : (o.from > DIST)) return false;
      if (_boEMA === '50' && !o.s.above_ema50) return false;
      if (_boEMA === '200' && (!o.s.above_ema50 || !o.s.above_ema200)) return false;
      return true;
    }).sort((a, b) => isHigh ? b.from - a.from : a.from - b.from);
    $('#boNote').textContent = `${arr.length} หุ้น ${isHigh ? 'ใกล้ทำจุดสูงใหม่' : 'ใกล้ทำจุดต่ำใหม่'} 52 สัปดาห์ (ห่างไม่เกิน ${DIST}%)`;
    $('#scrSub').textContent = arr.length + ' หลักทรัพย์ · ปิดตลาด ' + thaiDate(D.asOf);
    const box = $('#boList'); box.innerHTML = '';
    if (!arr.length) { box.appendChild(el('div', 'list-cap', 'ไม่พบหุ้นที่ตรงเงื่อนไข — ลองผ่อนตัวกรอง RS / EMA')); return; }
    arr.slice(0, 200).forEach(({ s, from }) => box.appendChild(boRow(s, from, isHigh)));
  };

  $('#boSide').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _boSide = b.dataset.k;
    $('#boSide').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  $('#boRS').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _boRS = +b.dataset.k;
    $('#boRS').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  $('#boEMA').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _boEMA = b.dataset.k;
    $('#boEMA').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  paint();
}
function boRow(r, from, isHigh) {
  const b = el('button', 'rlr');
  const lbl = isHigh ? (from >= 0 ? 'จุดสูงใหม่' : nf(from, 1) + '%')
    : (from <= 0.5 ? 'จุดต่ำใหม่' : '+' + nf(from, 1) + '%');
  const lblCls = isHigh ? (from >= -2 ? 'up' : 'flat') : (from <= 2 ? 'down' : 'flat');
  b.innerHTML = `
    <span class="rk ${r.rs >= 90 ? 'hot' : ''}">${r.rs ?? '–'}</span>
    <span class="mid2">
      <span class="tkr-lg">${r.symbol}</span>
      <span class="sub2">${shortName(r.name)}${r.sub ? ' · ' + r.sub : ''}</span>
    </span>
    <span class="mo"><span class="mv ${lblCls}">${lbl}</span><span class="ml">${isHigh ? 'จากจุดสูง' : 'จากจุดต่ำ'}</span></span>
    <span class="chev">›</span>`;
  b.addEventListener('click', () => openDetail(r.id));
  return b;
}

/* ---- ความกว้าง EMA รายกลุ่ม (พอร์ตจาก renderEMABreadth) ---- */
function groupBreadthView(main) {
  main.innerHTML = `
    <div class="seg" id="gbBy">
      <button data-k="sector" class="${_secBy === 'sector' ? 'on' : ''}">หมวดธุรกิจ</button>
      <button data-k="industry" class="${_secBy === 'industry' ? 'on' : ''}">กลุ่มอุตสาหกรรม</button>
    </div>
    <div class="note">สัดส่วนหุ้นในกลุ่มที่ราคายืนเหนือ EMA20 / 50 / 200 — เรียงตามคะแนนรวม</div>
    <div id="gbList"></div>`;
  const paint = () => {
    const kk = _secBy;
    const groups = {};
    D.stocks.forEach(r => {
      const g = (r.raw && r.raw[kk]) || 'ไม่ระบุ';
      (groups[g] || (groups[g] = [])).push(r);
    });
    const rows = Object.entries(groups).map(([name, arr]) => {
      const n = arr.length;
      const p = f => Math.round(arr.filter(f).length / n * 100);
      const p20 = p(s => s.above_ema20 === true);
      const p50 = p(s => s.above_ema50 === true);
      const p200 = p(s => s.above_ema200 === true);
      return { name, n, p20, p50, p200, score: Math.round((p20 + p50 + p200) / 3) };
    }).sort((a, b) => b.score - a.score);
    const box = $('#gbList'); box.innerHTML = '';
    rows.forEach(r => box.appendChild(gbRow(r)));
    $('#scrSub').textContent = rows.length + (_secBy === 'industry' ? ' กลุ่มอุตสาหกรรม' : ' หมวดธุรกิจ') + ' · ' + thaiDate(D.asOf);
  };
  $('#gbBy').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _secBy = b.dataset.k;
    $('#gbBy').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  paint();
}
function gbRow(r) {
  const d = el('div', 'gbr');
  const mini = (lab, v) => {
    const cc = v >= 60 ? 'g' : v >= 40 ? '' : 'r';
    return `<span class="gb-mini"><span class="gb-ml">${lab}</span>
      <span class="track"><span class="fill ${cc}" style="width:${Math.max(2, v)}%"></span></span>
      <span class="gb-mv">${v}%</span></span>`;
  };
  d.innerHTML = `
    <div class="gb-top"><span class="gb-nm">${r.name}</span>
      <span class="gb-sc ${r.score >= 70 ? 'up' : r.score < 40 ? 'down' : 'flat'}">${r.score}</span></div>
    <div class="gb-sub">${r.n} หลักทรัพย์</div>
    <div class="gb-bars">${mini('EMA20', r.p20)}${mini('EMA50', r.p50)}${mini('EMA200', r.p200)}</div>`;
  return d;
}

/* ---- จัดอันดับกลุ่มตามผลตอบแทน (พอร์ตจาก renderSectorRankTable) ---- */
// name (อังกฤษจาก set_data.json) -> ดัชนีกลุ่มใน indices_data.json — พอร์ตจาก
// SECTOR_NAME_TO_IDX_SYM ของ dashboard.js (mai sector map ไปดัชนี industry group ของ mai)
const SEC_IDX_SYM = {
  "Agribusiness": "^AGRI.BK", "Food & Beverage": "^FOOD.BK", "Fashion": "^FASHION.BK",
  "Home & Office Products": "^HOME.BK", "Personal Products & Pharmaceuticals": "^PERSON.BK",
  "Banking": "^BANK.BK", "Finance & Securities": "^FIN.BK", "Insurance": "^INSUR.BK",
  "Automotive": "^AUTO.BK", "Industrial Materials & Machinery": "^IMM.BK",
  "Paper & Printing Materials": "^PAPER.BK", "Petrochemicals & Chemicals": "^PETRO.BK",
  "Packaging": "^PKG.BK", "Steel and Metal Products": "^STEEL.BK",
  "Construction Materials": "^CONMAT.BK", "Construction Services": "^CONS.BK",
  "Property Development": "^PROP.BK", "Property Fund & REITs": "^PFREIT.BK",
  "Energy & Utilities": "^ENERG.BK", "Commerce": "^COMM.BK", "Health Care Services": "^HELTH.BK",
  "Media & Publishing": "^MEDIA.BK", "Professional Services": "^PROF.BK",
  "Tourism & Leisure": "^TOURISM.BK", "Transportation & Logistics": "^TRANS.BK",
  "Electronic Components": "^ETRON.BK", "Information & Communication Technology": "^ICT.BK",
  "Agro & Food Industry": "^AGRO.BK", "Consumer Products": "^CONSUMP.BK",
  "Financials": "^FINCIAL.BK", "Industrials": "^INDUS.BK",
  "Property & Construction": "^PROPCON.BK", "Resources": "^RESOURC.BK",
  "Services": "^SERVICE.BK", "Technology": "^TECH.BK",
  "Agro & Food Industry -mai": "^AGRO-M.BK", "Consumer Products -mai": "^CONSUMP-M.BK",
  "Financials -mai": "^FINCIAL-M.BK", "Industrial -mai": "^INDUS-M.BK",
  "Property & Construction -mai": "^PROPCON-M.BK", "Resources -mai": "^RESOURC-M.BK",
  "Services -mai": "^SERVICE-M.BK", "Technology -mai": "^TECH-M.BK",
};
// override ret_* ของ sector/industry ด้วยดัชนีถ่วง market cap จริงของ SET (indices_data.json)
// แทนค่าเฉลี่ยหุ้นไม่ถ่วงน้ำหนักจาก set_data.json — ให้ตรงกับ _sectorWithIdxOverride ของตัวเต็ม
function secIdxOverride(g) {
  const sym = SEC_IDX_SYM[g.name];
  const ix = (sym && D.indices) ? D.indices[sym] : null;
  if (!ix) return g;
  return { ...g,
    ret_1d: ix.ret_1d ?? g.ret_1d, ret_1w: ix.ret_1w ?? g.ret_1w,
    ret_1m: ix.ret_1m ?? g.ret_1m, ret_3m: ix.ret_3m ?? g.ret_3m,
    ret_6m: ix.ret_6m ?? g.ret_6m, ret_1y: ix.ret_1y ?? g.ret_1y };
}
function sectorRankView(main) {
  main.innerHTML = `
    <div class="seg" id="srBy">
      <button data-k="sector" class="${_secBy === 'sector' ? 'on' : ''}">หมวดธุรกิจ</button>
      <button data-k="industry" class="${_secBy === 'industry' ? 'on' : ''}">กลุ่มอุตสาหกรรม</button>
    </div>
    <div class="note">อันดับผลตอบแทน (ดัชนีถ่วง market cap) ของแต่ละกลุ่มในแต่ละช่วงเวลา (1 = ดีสุด) — เรียงตามอันดับเฉลี่ย · โมเมนตัม = อันดับ 1 ปี ลบ 1 เดือน</div>
    <div class="grk grk-hd"><span class="sr-nm">กลุ่ม</span><span>1ด</span><span>3ด</span><span>6ด</span><span>1ปี</span><span class="sr-avg">เฉลี่ย</span></div>
    <div id="srList"></div>`;
  const paint = () => {
    // ถ้ายังไม่โหลด indices_data.json ใช้ค่าเฉลี่ยหุ้นดิบไปก่อน แล้ว re-render เมื่อโหลดเสร็จ
    // (loadIndicesData เรียก paintStkView กลับมาถ้ายังอยู่จอ "รายชื่อหุ้น")
    if (!D.indices && !_idxLoading) loadIndicesData();
    const groups = (_secBy === 'sector' ? D.sectors : D.industries).map(secIdxOverride);
    const H = [['ret_1m', 'm'], ['ret_3m', 'q'], ['ret_6m', 'h'], ['ret_1y', 'y']];
    const rank = {}, totals = {};
    groups.forEach(g => rank[g.name] = {});
    H.forEach(([f, k]) => {
      const valid = groups.filter(g => g[f] != null).sort((a, b) => b[f] - a[f]);
      valid.forEach((g, i) => rank[g.name][k] = i + 1);
      totals[k] = valid.length;
    });
    const rows = groups.map(g => {
      const r = rank[g.name];
      const have = ['y', 'h', 'q', 'm'].map(k => r[k]).filter(x => x != null);
      const avg = have.length === 4 ? +(have.reduce((a, b) => a + b, 0) / 4).toFixed(1) : null;
      const mom = (r.m != null && r.y != null) ? r.y - r.m : null;
      return { name: g.name, r1m: r.m, r3m: r.q, r6m: r.h, r1y: r.y, avg, mom };
    }).sort((a, b) => (a.avg == null ? 999 : a.avg) - (b.avg == null ? 999 : b.avg));
    const box = $('#srList'); box.innerHTML = '';
    rows.forEach(r => box.appendChild(srRow(r, totals)));
    $('#scrSub').textContent = groups.length + (_secBy === 'industry' ? ' กลุ่มอุตสาหกรรม' : ' หมวดธุรกิจ') + ' · ' + thaiDate(D.asOf);
  };
  $('#srBy').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _secBy = b.dataset.k;
    $('#srBy').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  paint();
}
function srRow(r, totals) {
  const d = el('div', 'grk');
  // ตัวหารเป็นจำนวนกลุ่มที่ "มีข้อมูล" ต่อ horizon (ตรงกับ _rankBadge ของตัวเต็ม)
  // ไม่ใช่จำนวนกลุ่มทั้งหมด — สำคัญกับ industries ที่ bucket -mai ข้อมูลบางเบา
  const badge = (rk, tot) => {
    if (rk == null) return '<span class="rkb flat">–</span>';
    const q = rk / (tot || 1);
    const c = q <= 0.25 ? 'a' : q <= 0.5 ? 'b' : q <= 0.75 ? 'c' : 'd';
    return `<span class="rkb ${c}">#${rk}</span>`;
  };
  let mom = '';
  if (r.mom != null && r.mom !== 0) {
    const fire = r.mom >= 10 ? ' 🔥' : r.mom <= -10 ? ' ❄️' : '';
    mom = `<span class="sr-mom ${r.mom > 0 ? 'up' : 'down'}">${r.mom > 0 ? '+' : ''}${r.mom}${fire}</span>`;
  }
  d.innerHTML = `
    <span class="sr-nm">${r.name}${mom}</span>
    <span>${badge(r.r1m, totals.m)}</span><span>${badge(r.r3m, totals.q)}</span><span>${badge(r.r6m, totals.h)}</span><span>${badge(r.r1y, totals.y)}</span>
    <span class="sr-avg">${r.avg ?? '–'}</span>`;
  return d;
}

/* ---------------- WATCHLIST ---------------- */
function resolveSym(sym) {
  return D.byId['set:' + sym] || D.byId['dr:' + sym] || D.byId['etf:' + sym] || null;
}
function renderWatch() {
  const c = $('#s-watch'); c.innerHTML = '';
  const found = D.watchlist.map(resolveSym).filter(Boolean);
  if (!found.length) {
    c.appendChild(el('div', 'list-cap', D.watchlist.length ? 'หลักทรัพย์ในวอทช์ลิสต์ไม่อยู่ในชุดข้อมูลนี้' : 'วอทช์ลิสต์ว่าง'));
  } else {
    c.appendChild(secHead('หุ้นที่ติดตาม'));
    found.forEach(r => c.appendChild(listRow(r)));
  }
  c.appendChild(el('div', 'note', 'วอทช์ลิสต์ซิงก์มาจากเวอร์ชันรันบนเครื่อง (อ่านอย่างเดียว) — เพิ่ม/ลบทำที่เครื่อง'));
}

/* ================================================================
   ROTATION + RRG (เฟส 2 ข้อ 8) — พอร์ตย่อจาก dashboard.js
   drawRotationScatter / renderRotAlerts / renderRotation
   ข้อมูล: set_data.json .sectors (35) / .industries (16)
           — ret_1w/1m/3m/6m/1y + avg_rs + count
           rotation_alerts.json (โหลดตอนเปิดหน้าครั้งแรก)
   ================================================================ */
let _rotView = 'sector';   // sector = หมวดธุรกิจ (35) · industry = กลุ่มอุตสาหกรรม (16)
let _rotTf = 'long';       // long = X:3ด Y:1ด · short = X:1ด Y:1สัปดาห์
let _rotSel = null;        // ชื่อกลุ่มที่แตะเลือก (โชว์เส้นทาง + ไฮไลต์)
let _rotAlertsLoading = false;
let _rotAlertsErr = false; // fetch rotation_alerts.json ล้มเหลวรอบล่าสุด (ไม่ค้างถาวร — retry ตอนเปิดหน้าใหม่)
let _rrgHit = [];          // [{name,x,y,r}] พิกัด CSS px สำหรับ hit-test การแตะ

const ROT_QUAD = {
  lead:    { th: 'นำ',     en: 'Leading',   c: '#3fb950' },
  improve: { th: 'ฟื้นตัว', en: 'Improving',  c: '#4f9cf0' },
  weaken:  { th: 'อ่อนแรง', en: 'Weakening',  c: '#e3b341' },
  lag:     { th: 'ตามหลัง', en: 'Lagging',   c: '#f85149' },
};
const _QEN2TH = { Leading: ROT_QUAD.lead, Improving: ROT_QUAD.improve, Weakening: ROT_QUAD.weaken, Lagging: ROT_QUAD.lag };
function rotQuadKey(x, y) {
  if (x > 0 && y > 0) return 'lead';
  if (x <= 0 && y > 0) return 'improve';
  if (x > 0 && y <= 0) return 'weaken';
  return 'lag';
}
const _rotAxis = tf => tf === 'short'
  ? { tf, xk: 'ret_1m', yk: 'ret_1w', xl: '1 เดือน', yl: '1 สัปดาห์', cap: { xLo: -15, xHi: 20, yLo: -10, yHi: 10 } }
  : { tf, xk: 'ret_3m', yk: 'ret_1m', xl: '3 เดือน', yl: '1 เดือน', cap: { xLo: -30, xHi: 60, yLo: -30, yHi: 60 } };

const ROT_PALETTE = ['#58a6ff', '#3fb950', '#ffa657', '#f85149', '#d2a8ff', '#79c0ff', '#56d364',
  '#e3b341', '#ff7b72', '#bc8cff', '#1f9e75', '#87ceeb', '#ffb347', '#ff6eb4', '#7ee787', '#40e0d0',
  '#ff8c42', '#9370db', '#20b2aa', '#ffd700', '#00bcd4', '#ff69b4', '#98fb98', '#dda0dd', '#f0e68c',
  '#87cefa', '#a8e6cf', '#90ee90', '#ffcba4', '#c3b1e1'];
function _rotColor(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return ROT_PALETTE[h % ROT_PALETTE.length];
}
const _RCFONT = '"IBM Plex Sans Thai","Noto Sans Thai",sans-serif';

async function loadRotAlerts() {
  if (D.rotAlerts || _rotAlertsLoading) return;
  _rotAlertsLoading = true;
  _rotAlertsErr = false;
  // fail = ปล่อย D.rotAlerts เป็น null (ไม่ตั้ง sentinel truthy) เพื่อให้ guard ข้างบน
  // ยอมให้ยิงซ้ำตอนเปิดหน้า "หมุนเวียน" ใหม่ — เหมือน loadValuationData/loadIndicesData
  try { D.rotAlerts = await loadJSON('../data/rotation_alerts.json', 20000); }
  catch (e) { _rotAlertsErr = true; }
  _rotAlertsLoading = false;
  if (curScreen === 'rotation') renderRotation();
}

// Relative Rotation Graph — scatter symlog + quadrant + ลูกศรทิศ + เส้นทาง (เลือก)
function drawRRG(cv, items, tf, sel) {
  const ax = _rotAxis(tf);
  const dpr = Math.min(3, window.devicePixelRatio || 1);
  const w = cv.clientWidth, h = cv.clientHeight || 340;
  cv.width = w * dpr; cv.height = h * dpr;
  const x = cv.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0); x.clearRect(0, 0, w, h);
  _rrgHit = [];

  const pts = items.filter(s => s[ax.xk] != null && s[ax.yk] != null);
  if (pts.length < 1) return;
  const PAD = { t: 18, r: 12, b: 22, l: 30 };
  const PW = w - PAD.l - PAD.r, PH = h - PAD.t - PAD.b;
  const SL = 3;
  const sl = v => Math.sign(v) * Math.log10(1 + Math.abs(v) / SL);
  const gx = s => s[ax.xk], gy = s => s[ax.yk];
  const sxs = pts.map(s => sl(gx(s))), sys = pts.map(s => sl(gy(s)));
  const xLow = Math.min(Math.min(...sxs), sl(ax.cap.xLo)) * 1.08;
  const xHigh = Math.max(Math.max(...sxs) * 1.15, sl(ax.cap.xHi));
  const yLow = Math.min(Math.min(...sys), sl(ax.cap.yLo)) * 1.08;
  const yHigh = Math.max(Math.max(...sys) * 1.15, sl(ax.cap.yHi));
  const toX = v => PAD.l + (sl(v) - xLow) / (xHigh - xLow) * PW;
  const toY = v => PAD.t + (yHigh - sl(v)) / (yHigh - yLow) * PH;
  const ox = toX(0), oy = toY(0);

  // quadrant fills
  [[PAD.l, PAD.t, ox - PAD.l, oy - PAD.t, 'rgba(79,156,240,.10)'],
   [ox, PAD.t, PAD.l + PW - ox, oy - PAD.t, 'rgba(63,185,80,.10)'],
   [PAD.l, oy, ox - PAD.l, PAD.t + PH - oy, 'rgba(248,81,73,.10)'],
   [ox, oy, PAD.l + PW - ox, PAD.t + PH - oy, 'rgba(227,179,65,.09)']]
    .forEach(([qx, qy, qw, qh, f]) => { x.fillStyle = f; x.fillRect(qx, qy, qw, qh); });
  // quadrant labels
  x.font = '700 10px ' + _RCFONT; x.textBaseline = 'top';
  x.fillStyle = 'rgba(79,156,240,.7)'; x.textAlign = 'left'; x.fillText('ฟื้นตัว', PAD.l + 4, PAD.t + 3);
  x.fillStyle = 'rgba(63,185,80,.75)'; x.textAlign = 'right'; x.fillText('นำ', PAD.l + PW - 4, PAD.t + 3);
  x.textBaseline = 'bottom';
  x.fillStyle = 'rgba(248,81,73,.7)'; x.textAlign = 'left'; x.fillText('ตามหลัง', PAD.l + 4, PAD.t + PH - 3);
  x.fillStyle = 'rgba(227,179,65,.8)'; x.textAlign = 'right'; x.fillText('อ่อนแรง', PAD.l + PW - 4, PAD.t + PH - 3);

  // center axes
  x.strokeStyle = 'rgba(139,149,161,.4)'; x.lineWidth = 1; x.setLineDash([4, 4]);
  x.beginPath(); x.moveTo(ox, PAD.t); x.lineTo(ox, PAD.t + PH); x.stroke();
  x.beginPath(); x.moveTo(PAD.l, oy); x.lineTo(PAD.l + PW, oy); x.stroke();
  x.setLineDash([]);
  // frame
  x.strokeStyle = 'rgba(139,149,161,.18)'; x.strokeRect(PAD.l, PAD.t, PW, PH);
  // tick labels (นอก dead zone)
  x.font = '9px "IBM Plex Mono",ui-monospace,monospace'; x.fillStyle = '#5b636e';
  const xt = [-30, -10, 10, 30, 60].filter(v => sl(v) > xLow && sl(v) < xHigh);
  const yt = [-30, -10, 10, 30, 60].filter(v => sl(v) > yLow && sl(v) < yHigh);
  x.textBaseline = 'top'; x.textAlign = 'center';
  xt.forEach(v => { if (v) x.fillText((v > 0 ? '+' : '') + v, toX(v), PAD.t + PH + 5); });
  x.textBaseline = 'middle'; x.textAlign = 'right';
  yt.forEach(v => { if (v) x.fillText((v > 0 ? '+' : '') + v, PAD.l - 4, toY(v)); });

  // bubbles + collision spring
  const R = items.length > 20 ? 8 : 11;
  const P = pts.map(s => {
    const px = toX(gx(s)), py = toY(gy(s));
    return { s, name: s.name, x: px, y: py, ox: px, oy: py, R, color: _rotColor(s.name) };
  });
  const MIN = R * 2 + 2;
  for (let it = 0; it < 40; it++) {
    for (let i = 0; i < P.length; i++) {
      for (let j = i + 1; j < P.length; j++) {
        const dx = P[j].x - P[i].x, dy = P[j].y - P[i].y;
        const d = Math.hypot(dx, dy) || 0.01;
        if (d < MIN) {
          const push = (MIN - d) / 2, nx = dx / d, ny = dy / d;
          P[i].x -= nx * push; P[i].y -= ny * push;
          P[j].x += nx * push; P[j].y += ny * push;
        }
      }
      P[i].x += (P[i].ox - P[i].x) * 0.06;
      P[i].y += (P[i].oy - P[i].y) * 0.06;
      P[i].x = Math.max(PAD.l + R, Math.min(PAD.l + PW - R, P[i].x));
      P[i].y = Math.max(PAD.t + R, Math.min(PAD.t + PH - R, P[i].y));
    }
  }

  // trail ของกลุ่มที่เลือก
  const selP = sel ? P.find(p => p.name === sel) : null;
  if (selP) {
    const s = selP.s;
    let anch = tf === 'short'
      ? [[s.ret_3m, s.ret_1m], [s.ret_1m, s.ret_1w]]
      : [[s.ret_1y, s.ret_6m], [s.ret_6m, s.ret_3m], [s.ret_3m, s.ret_1m]];
    // แกน x/y คำนวณจาก ret ระยะสั้น (1ด/3ด) — ret 6ด/1ปี ของบางกลุ่มหลุดกรอบไปไกล
    // ทำให้เส้น trail ทั้งเส้นถูกวาดนอก bitmap (มองไม่เห็น) — clamp เข้าขอบ plot
    const cX = v => Math.max(PAD.l, Math.min(PAD.l + PW, v));
    const cY = v => Math.max(PAD.t, Math.min(PAD.t + PH, v));
    const T = anch.filter(a => a[0] != null && a[1] != null).map(a => ({ x: cX(toX(a[0])), y: cY(toY(a[1])) }));
    T.push({ x: selP.x, y: selP.y });
    for (let i = 1; i < T.length; i++) {
      const a = T[i - 1], b = T[i], t1 = i / (T.length - 1);
      x.strokeStyle = selP.color; x.globalAlpha = 0.25 + t1 * 0.55; x.lineWidth = 2.5;
      x.beginPath(); x.moveTo(a.x, a.y); x.lineTo(b.x, b.y); x.stroke();
      x.globalAlpha = 0.3; x.fillStyle = selP.color;
      x.beginPath(); x.arc(a.x, a.y, selP.R * 0.45, 0, 7); x.fill();
    }
    x.globalAlpha = 1;
  }

  // วาดวง
  P.forEach(p => {
    const dim = sel && p.name !== sel;
    x.save();
    if (dim) {
      x.globalAlpha = 0.16; x.fillStyle = '#3d444d';
      x.beginPath(); x.arc(p.x, p.y, p.R, 0, 7); x.fill();
    } else {
      // ทิศเคลื่อนที่ (ตอนไม่มีตัวเลือก)
      if (!sel) {
        const px0 = tf === 'short' ? toX(p.s.ret_3m ?? p.s.ret_1m) : toX(p.s.ret_6m ?? p.s.ret_3m);
        const py0 = tf === 'short' ? toY(p.s.ret_1m ?? 0) : toY(p.s.ret_3m ?? 0);
        const dx = p.ox - px0, dy = p.oy - py0, len = Math.hypot(dx, dy);
        if (len > 1) {
          const a = Math.atan2(dy, dx), tip = p.R + 5, sz = 4;
          const tx = p.x + Math.cos(a) * tip, ty = p.y + Math.sin(a) * tip;
          x.globalAlpha = 0.7; x.fillStyle = p.color;
          x.beginPath();
          x.moveTo(tx, ty);
          x.lineTo(tx - Math.cos(a - 0.5) * sz, ty - Math.sin(a - 0.5) * sz);
          x.lineTo(tx - Math.cos(a + 0.5) * sz, ty - Math.sin(a + 0.5) * sz);
          x.closePath(); x.fill(); x.globalAlpha = 1;
        }
      }
      x.shadowColor = p.color; x.shadowBlur = 9;
      x.fillStyle = _hexA(p.color, 0.22);
      x.beginPath(); x.arc(p.x, p.y, p.R, 0, 7); x.fill();
      x.shadowBlur = 0;
      x.strokeStyle = p.color; x.lineWidth = p.name === sel ? 3 : 2;
      if ((p.s.n_valid ?? p.s.count ?? 9) < 3) x.setLineDash([3, 2]);
      x.beginPath(); x.arc(p.x, p.y, p.R, 0, 7); x.stroke(); x.setLineDash([]);
      if (p.s.avg_rs != null) {
        x.fillStyle = '#fff'; x.font = `700 ${p.R >= 11 ? 10 : 9}px ` + _RCFONT;
        x.textAlign = 'center'; x.textBaseline = 'middle';
        x.fillText(Math.round(p.s.avg_rs), p.x, p.y);
      }
    }
    x.restore();
    _rrgHit.push({ name: p.name, x: p.x, y: p.y, r: p.R });
  });

  // ป้ายชื่อกลุ่มที่เลือก
  if (selP) {
    x.font = '700 11px ' + _RCFONT;
    const lbl = selP.name.length > 22 ? selP.name.slice(0, 20) + '…' : selP.name;
    const tw = x.measureText(lbl).width + 12;
    let lx = selP.x + selP.R + 6, ly = selP.y - 9;
    if (lx + tw > PAD.l + PW) lx = selP.x - selP.R - 6 - tw;
    ly = Math.max(PAD.t + 2, Math.min(PAD.t + PH - 20, ly));
    x.fillStyle = 'rgba(13,17,23,.92)'; x.strokeStyle = selP.color; x.lineWidth = 1.4;
    if (x.roundRect) { x.beginPath(); x.roundRect(lx, ly, tw, 18, 4); x.fill(); x.stroke(); }
    else { x.fillRect(lx, ly, tw, 18); x.strokeRect(lx, ly, tw, 18); }
    x.fillStyle = '#fff'; x.textAlign = 'left'; x.textBaseline = 'middle';
    x.fillText(lbl, lx + 6, ly + 9);
  }
}
function _hexA(hex, a) {
  const r = parseInt(hex.slice(1, 3), 16), g = parseInt(hex.slice(3, 5), 16), b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${a})`;
}

function rrgLegend(elm, items, ax) {
  const pts = items.filter(s => s[ax.xk] != null && s[ax.yk] != null)
    .sort((a, b) => (b[ax.xk] + b[ax.yk]) - (a[ax.xk] + a[ax.yk]));
  elm.innerHTML = pts.map(s => {
    const on = _rotSel === s.name;
    const label = s.name.length > 22 ? s.name.slice(0, 21) + '…' : s.name;
    return `<button class="rrg-chip${on ? ' on' : ''}" data-n="${_esc(s.name)}" style="--cc:${_rotColor(s.name)}"><span class="rc-sq"></span>${label}</button>`;
  }).join('');
  elm.onclick = e => {
    const b = e.target.closest('button'); if (!b) return;
    _rotSel = _rotSel === b.dataset.n ? null : b.dataset.n;
    renderRotation();
  };
}

function rotSelCard(s, ax) {
  const q = ROT_QUAD[rotQuadKey(s[ax.xk], s[ax.yk])];
  const d = el('div', 'rot-sel');
  const g = [['1 สัปดาห์', s.ret_1w], ['1 เดือน', s.ret_1m], ['3 เดือน', s.ret_3m],
            ['6 เดือน', s.ret_6m], ['1 ปี', s.ret_1y]]
    .map(([k, v]) => `<span>${k}</span><b class="${cls(v)}">${pct(v, 1)}</b>`).join('');
  d.innerHTML = `
    <div class="rs-h"><span class="rs-n">${s.name}</span>
      <span class="rs-q" style="background:${_hexA(q.c, 0.16)};color:${q.c}">${q.th} · ${q.en}</span></div>
    <div class="rs-g">${g}
      <span>RS เฉลี่ย</span><b>${s.avg_rs == null ? '–' : Math.round(s.avg_rs)}</b>
      <span>จำนวนบริษัท</span><b>${s.count ?? '–'}</b></div>
    <button class="rs-clear">ล้างการเลือก</button>`;
  d.querySelector('.rs-clear').addEventListener('click', () => { _rotSel = null; renderRotation(); });
  return d;
}

function rotListRow(s, ax) {
  const yv = s[ax.yk], q = ROT_QUAD[rotQuadKey(s[ax.xk], yv)];
  const r = el('button', 'rot-row2');
  r.innerHTML = `
    <span class="rl"><span class="rn">${s.name}</span><span class="rc">${s.count ?? '–'} บริษัท · RS ${s.avg_rs == null ? '–' : Math.round(s.avg_rs)}</span></span>
    <span class="rq" style="color:${q.c}">${q.th}</span>
    <span class="rv ${cls(yv)}">${pct(yv, 1)}</span>`;
  r.addEventListener('click', () => {
    _rotSel = _rotSel === s.name ? null : s.name;
    renderRotation();
    if (_rotSel) window.scrollTo({ top: 0, behavior: 'smooth' });
  });
  return r;
}

const _qSpan = q => { const t = _QEN2TH[q]; return t ? `<b style="color:${t.c}">${t.th}</b>` : `<b>${q}</b>`; };

function rotAlertsSection() {
  const wrap = el('div');
  wrap.appendChild(secHead('การเปลี่ยนโซนหมุนเวียน'));
  const d = D.rotAlerts;
  if (!d) {
    wrap.appendChild(el('div', 'list-cap',
      _rotAlertsErr ? 'โหลดสัญญาณไม่สำเร็จ — เปิดหน้านี้ใหม่เพื่อลองอีกครั้ง' : 'กำลังโหลดสัญญาณ…'));
    loadRotAlerts();  // idempotent (guard D.rotAlerts||_loading) — ยิงซ้ำได้เมื่อ re-render/เปิดหน้าใหม่หลัง fail
    return wrap;
  }
  const trans = (d.transitions || []).slice(0, 8);
  const pend = (d.pending || []);
  const transF = (d.transitions_fast || []).slice(0, 6);
  const pendF = (d.pending_fast || []).filter(p => p.days >= 2);
  // sector -> D.sectors = "หมวดธุรกิจ" (35) · industry -> D.industries = "กลุ่มอุตสาหกรรม" (16)
  const tyLbl = t => t === 'industry' ? 'กลุ่ม' : 'หมวด';
  const tRow = t => `<div class="rot-al"><span class="ra-d">${thaiDate(t.date)}</span>
    <span class="ra-n">${_esc(t.name)}</span><span class="ra-t">${tyLbl(t.type)}</span>
    <span class="ra-q">${_qSpan(t.from)} → ${_qSpan(t.to)}</span></div>`;
  const pRow = p => `<div class="rot-al pend"><span class="ra-n">⏳ ${_esc(p.name)}</span>
    <span class="ra-t">${tyLbl(p.type)}</span>
    <span class="ra-q">เข้า ${_qSpan(p.to)} · ${p.days}/${p.need} วัน</span></div>`;
  if (!trans.length && !pend.length && !transF.length && !pendF.length) {
    wrap.appendChild(el('div', 'list-cap', 'ยังไม่มีสัญญาณเปลี่ยนโซนที่ยืนยันแล้ว'));
    return wrap;
  }
  wrap.appendChild(el('div', 'chart-src', 'ยืนยัน 3 วันทำการ · แกน 3 เดือน / 1 เดือน · ⏳ = กำลังนับยืนยัน'));
  wrap.appendChild(el('div', null, (trans.map(tRow).join('')) + pend.map(pRow).join('')));
  if (transF.length || pendF.length) {
    wrap.appendChild(el('div', 'chart-src', '⚡ สัญญาณเร็ว (แกน 1 เดือน / 1 สัปดาห์) — ไวกว่าแต่แม่นยำน้อยกว่า รอชุดหลักยืนยัน'));
    wrap.appendChild(el('div', null, transF.map(tRow).join('') + pendF.map(pRow).join('')));
  }
  return wrap;
}

function renderRotation() {
  const c = $('#s-rotation'); if (!c) return;
  c.innerHTML = '';
  const items = _rotView === 'industry' ? D.industries : D.sectors;
  const ax = _rotAxis(_rotTf);
  if (!items || !items.length) {
    c.appendChild(el('div', 'list-cap', 'ไม่มีข้อมูลกลุ่มอุตสาหกรรมในชุดข้อมูลนี้'));
    return;
  }

  const vseg = el('div', 'seg');
  vseg.innerHTML = `<button data-v="sector" class="${_rotView === 'sector' ? 'on' : ''}">หมวดธุรกิจ</button><button data-v="industry" class="${_rotView === 'industry' ? 'on' : ''}">กลุ่มอุตสาหกรรม</button>`;
  c.appendChild(vseg);
  const tseg = el('div', 'seg');
  tseg.innerHTML = `<button data-t="long" class="${_rotTf === 'long' ? 'on' : ''}">แกน 3ด / 1ด</button><button data-t="short" class="${_rotTf === 'short' ? 'on' : ''}">แกน 1ด / 1สัปดาห์</button>`;
  c.appendChild(tseg);
  vseg.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _rotView = b.dataset.v; _rotSel = null; renderRotation(); } });
  tseg.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _rotTf = b.dataset.t; renderRotation(); } });

  c.appendChild(el('div', 'chart-wrap', '<canvas class="rrg" id="rrgCanvas"></canvas>'));
  c.appendChild(el('div', 'chart-src',
    `แกนนอน = ผลตอบแทน ${ax.xl} · แกนตั้ง = ${ax.yl} · เลขในวง = RS · ลูกศร = ทิศเคลื่อนที่ · แตะวงเพื่อดูเส้นทาง`));
  const legend = el('div', 'rrg-legend'); legend.id = 'rrgLegend'; c.appendChild(legend);

  if (_rotSel) {
    const s = items.find(i => i.name === _rotSel);
    if (s) c.appendChild(rotSelCard(s, ax));
  }

  _kickCanvas('rrgCanvas', cv => {
    drawRRG(cv, items, _rotTf, _rotSel);
    rrgLegend(legend, items, ax);
    cv.onclick = e => {
      const rc = cv.getBoundingClientRect();
      const mx = e.clientX - rc.left, my = e.clientY - rc.top;
      let best = null, bd = 1e9;
      _rrgHit.forEach(p => {
        const d = Math.hypot(p.x - mx, p.y - my);
        if (d < p.r + 12 && d < bd) { bd = d; best = p.name; }
      });
      if (best) { _rotSel = _rotSel === best ? null : best; renderRotation(); }
    };
  });

  c.appendChild(rotAlertsSection());

  const list = el('div');
  list.appendChild(secHead(`เรียงตามผลตอบแทน ${ax.yl}`));
  [...items].filter(s => s[ax.yk] != null).sort((a, b) => b[ax.yk] - a[ax.yk])
    .forEach(s => list.appendChild(rotListRow(s, ax)));
  c.appendChild(list);

  // drawRRG/rrgLegend/list ตัดกลุ่มที่ ret แกนเป็น null ทิ้ง — นับเฉพาะที่แสดงจริง
  const nShown = items.filter(s => s[ax.xk] != null && s[ax.yk] != null).length;
  const nHid = items.length - nShown;
  $('#scrSub').textContent = `${nShown} ${_rotView === 'industry' ? 'กลุ่มอุตสาหกรรม' : 'หมวดธุรกิจ'}`
    + (nHid ? ` (ซ่อน ${nHid} — ข้อมูลไม่พอ)` : '') + ` · แกน ${ax.xl} / ${ax.yl}`;
}

/* ---------------- MORE (เมนูจริง) ---------------- */
// scr = ชื่อ screen (ถ้ามี #s-<scr> + entry ใน go()) → กดเข้าได้ · ไม่มี = ปุ่มสีจาง "เร็ว ๆ นี้"
// เฟส 2 ข้อ 4-8 ค่อยเติม scr ให้ทีละอัน (ดัชนีกลุ่ม/Valuation/Screener/Heatmap/เงินทุน)
const MORE_MENU = [
  { ic: '📊', t: 'ดัชนีกลุ่มอุตสาหกรรม', s: 'ผลตอบแทน SET แยกกลุ่ม + RS + โมเมนตัม', scr: 'indices' },
  { ic: '⚖️', t: 'Valuation ตลาด & รายหุ้น', s: 'กรอบ PE / PBV / ปันผล เทียบค่าเฉลี่ยย้อนหลัง', scr: 'valuation' },
  { ic: '🌡', t: 'Heatmap ตลาดไทย', s: 'ภาพรวมตลาดไทยแยกกลุ่มอุตสาหกรรม', scr: 'heatmap' },
  { ic: '💧', t: 'เงินทุน · Short · Insider', s: 'กระแสเงินต่างชาติ/สถาบัน + ชอร์ต + ธุรกรรมผู้บริหาร + สัญญาณรวม', scr: 'flow' },
  { ic: '🔍', t: 'สแกนหุ้น (Screener)', s: 'กรอง RS / สเตจ / ราคา / มูลค่าตลาด / ปันผล', scr: 'screener', mode: 'basic' },
  { ic: '🔬', t: 'สแกนด้วยงบการเงิน', s: 'กรองด้วย F-Score / ROE / มาร์จิ้น / การเติบโต', scr: 'screener', mode: 'plus' },
];
function renderMore() {
  const c = $('#s-more'); c.innerHTML = '';

  c.appendChild(secHead('เครื่องมือเพิ่มเติม'));
  const box = el('div');
  MORE_MENU.forEach(m => {
    const nav = !!m.scr;
    const row = el(nav ? 'button' : 'div', 'mitem' + (nav ? '' : ' soon'));
    row.innerHTML = `
      <span class="mi-ic">${m.ic}</span>
      <span class="mi-body"><span class="mi-t">${m.t}</span><span class="mi-s">${m.s}</span></span>
      ${nav ? '<span class="chev">›</span>' : '<span class="mi-badge">เร็ว ๆ นี้</span>'}`;
    if (nav) row.addEventListener('click', () => { if (m.mode) { _scrMode = m.mode; _scrSort = 'rs'; } go(m.scr); });
    box.appendChild(row);
  });
  c.appendChild(box);
  c.appendChild(el('div', 'note',
    'เวอร์ชันมือถือเน้นดูสรุปหุ้นไทย / DR / ETF รายวัน + งบย้อนหลัง — แตะที่หลักทรัพย์เพื่อดูกราฟ ผลตอบแทน และงบรายไตรมาสในแท็บ “งบการเงิน”'));
}

/* ---------------- DETAIL ---------------- */
let dTab = 'overview', dTf = '6M';
const TFDAYS = { '1D': 4, '1W': 9, '1M': 34, '3M': 95, '6M': 185, '1Y': 380 };
const TFRET = { '1D': 'chg1d', '1W': 'ret_1w', '1M': 'ret_1m', '3M': 'ret_3m', '6M': 'ret_6m', '1Y': 'ret_1y' };

function sliceHistory(ph, tfKey, curPrice) {
  if (!ph || !ph.length) return null;
  const lastDate = new Date(ph[ph.length - 1][0]);
  const cutoff = new Date(lastDate); cutoff.setDate(cutoff.getDate() - TFDAYS[tfKey]);
  let arr = ph.filter(p => new Date(p[0]) >= cutoff).map(p => p[1]).filter(v => v != null && v > 0);
  if (arr.length < 2) arr = ph.slice(-2).map(p => p[1]).filter(v => v != null && v > 0);
  // despike: ลบจุดเดี่ยวที่ต่างจากทั้งสองข้างมาก แต่สองข้างเองใกล้กัน (data glitch —
  // เจอใน ETF ตราสารหนี้บางตัว) — V-bottom จริง (ลงแล้วอยู่ต่ำต่อ) จะไม่โดนตัด
  for (let i = 1; i < arr.length - 1; i++) {
    const a = arr[i - 1], b = arr[i], n = arr[i + 1];
    if (Math.abs(b - a) / a > 0.12 && Math.abs(b - n) / n > 0.12 && Math.abs(a - n) / a < 0.04) {
      arr[i] = (a + n) / 2;
    }
  }
  // ให้ปลายเส้นตรงกับราคาปิดล่าสุดที่แสดง
  if (curPrice != null && curPrice > 0) arr[arr.length - 1] = curPrice;
  return arr;
}

function renderDetail() {
  const r = D.byId[curId], raw = r.raw, isSet = r.kind === 'set';
  const c = $('#s-detail'); c.innerHTML = '';
  const d = r.chg1d;
  const dec = r.price < 1 ? 3 : 2;

  const hero = el('div', 'd-hero');
  hero.innerHTML = `
    <div class="d-px">
      <span class="d-big">${nf(r.price, dec)}</span>
      <span class="d-chg ${cls(d)}">${sgn(d)}${nf(r.price * (d || 0) / 100, dec)}  (${pct(d)})</span>
    </div>
    <div class="d-time">${r.tag} · ${r.sub || '—'} · บาท · ปิดตลาด ${thaiDate(D.asOf)}</div>`;
  c.appendChild(hero);

  // timeframe + chart
  const tfKeys = r.ph ? Object.keys(TFRET) : [];
  if (r.ph) {
    const tf = el('div', 'tf');
    tf.innerHTML = tfKeys.map(k => `<button class="${k === dTf ? 'on' : ''}" data-k="${k}">${k}</button>`).join('');
    c.appendChild(tf);
    c.appendChild(el('div', 'chart-wrap', '<canvas class="spark" id="dChart"></canvas>'));
    const rn = el('div', 'range-note'); rn.id = 'dRange'; c.appendChild(rn);
    const drawTf = k => {
      dTf = k;
      tf.querySelectorAll('button').forEach(b => b.classList.toggle('on', b.dataset.k === k));
      drawSpark($('#dChart'), sliceHistory(r.ph, k, r.price));
      const ch = r[TFRET[k]];
      $('#dRange').innerHTML = `<span>ช่วง ${k}</span><span class="${cls(ch)}">${pct(ch)}</span>`;
    };
    tf.addEventListener('click', e => { const b = e.target.closest('button'); if (b) drawTf(b.dataset.k); });
    const kick = () => { const cv = $('#dChart'); if (cv && cv.clientWidth > 0) drawTf(tfKeys.includes(dTf) ? dTf : '6M'); else requestAnimationFrame(kick); };
    requestAnimationFrame(kick);
  }

  // tabs
  const tabs = [['overview', 'ภาพรวม'], ['tech', 'เทคนิค']];
  if (isSet) tabs.push(['fin', 'งบการเงิน']);
  if (!tabs.find(t => t[0] === dTab)) dTab = 'overview';
  const seg = el('div', 'seg');
  seg.innerHTML = tabs.map(([k, t]) => `<button class="${k === dTab ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(seg);
  const body = el('div'); body.id = 'dBody'; c.appendChild(body);
  const paintTab = k => {
    dTab = k;
    seg.querySelectorAll('button').forEach(b => b.classList.toggle('on', b.dataset.k === k));
    body.innerHTML = '';
    if (k === 'fin') body.append(...tabFin(r));
    else if (k === 'tech') body.append(...tabTech(r));
    else body.append(...tabOverview(r));
  };
  seg.addEventListener('click', e => { const b = e.target.closest('button'); if (b) paintTab(b.dataset.k); });
  paintTab(dTab);
}

function statRow(k, v, vClass, title) {
  const r = el('div', 'stat');
  const t = title ? ` title="${String(title).replace(/"/g, '&quot;')}"` : '';
  r.innerHTML = `<span class="k">${k}</span><span class="v ${vClass || ''}"${t}>${v}</span>`;
  return r;
}
function retPair(a, b) {
  return `<span class="${cls(a)}">${pct(a)}</span>  ·  <span class="${cls(b)}">${pct(b)}</span>`;
}
function tabOverview(r) {
  const raw = r.raw, isSet = r.kind === 'set';
  const rng = (r.high_52w && r.low_52w) ? (r.price - r.low_52w) / (r.high_52w - r.low_52w) * 100 : null;
  const out = [secHead('สรุปรายละเอียด'),
    statRow('1 สัปดาห์ / 1 เดือน', retPair(r.ret_1w, r.ret_1m)),
    statRow('3 เดือน / 6 เดือน', retPair(r.ret_3m, r.ret_6m)),
    statRow('ตั้งแต่ต้นปี / 1 ปี', retPair(r.ret_ytd, r.ret_1y)),
    statRow('ช่วง 52 สัปดาห์', `${nf(r.low_52w, 2)} – ${nf(r.high_52w, 2)}${rng != null ? `  (${nf(rng, 0)}%)` : ''}`),
  ];
  if (isSet) {
    out.push(
      statRow('P/E', raw.pe == null ? 'ขาดทุน / n/a' : nf(raw.pe, 2)),
      statRow('P/BV', nf(raw.pbv, 2)),
      statRow('ปันผล', raw.div_yield == null ? '–' : nf(raw.div_yield, 2) + '%'),
    );
  } else if (r.kind === 'etf') {
    out.push(
      statRow('NAV', nf(raw.nav, 2)),
      statRow('พรีเมียม / ดิสเคานต์', pct(raw.premium_pct, 2), cls(raw.premium_pct)),
      statRow('ปันผล', raw.div_yield == null ? '–' : nf(raw.div_yield, 2) + '%'),
    );
  } else {
    out.push(statRow('ตลาดต้นทาง', raw.region || '—'));
  }
  out.push(statRow('มูลค่าตลาด', fmtCap(r.mkt_cap), '', exactBaht(r.mkt_cap)));
  out.push(statRow('RS Score', `${r.rs ?? '–'} / 99`, r.rs >= 70 ? 'up' : r.rs < 40 ? 'down' : ''));
  if (isSet && STAGE[raw.stage]) out.push(statRow('สเตจ (Weinstein)', `<span class="pill ${STAGE[raw.stage][1]}">${raw.stage} · ${STAGE[raw.stage][0]}</span>`));
  return out;
}
function tabTech(r) {
  const raw = r.raw, isSet = r.kind === 'set';
  const emaRow = (n, above, ema) => above == null ? null :
    statRow('ราคาเทียบ EMA' + n, `<span class="pill ${above ? 'g' : 'r'}">${above ? 'เหนือ' : 'ต่ำกว่า'}</span>${ema ? '  ' + nf(ema, 2) : ''}`);
  const out = [secHead('แนวโน้มราคา')];
  if (isSet && STAGE[raw.stage]) out.push(statRow('สเตจ', `<span class="pill ${STAGE[raw.stage][1]}">${raw.stage} · ${STAGE[raw.stage][0]}</span>`));
  [emaRow(20, r.above_ema20, raw.ema20), emaRow(50, r.above_ema50, raw.ema50), emaRow(200, r.above_ema200, raw.ema200)]
    .forEach(x => x && out.push(x));
  if (raw.ema200_slope_pct != null) out.push(statRow('ความชัน EMA200 (1 ปี)', pct(raw.ema200_slope_pct, 1), cls(raw.ema200_slope_pct)));
  out.push(secHead('โมเมนตัม & ความผันผวน'));
  out.push(statRow('RS Score', `${r.rs ?? '–'} / 99`, r.rs >= 70 ? 'up' : r.rs < 40 ? 'down' : ''));
  if (raw.rs_momentum != null) out.push(statRow('การเปลี่ยนอันดับ RS (1 เดือน)', (raw.rs_momentum > 0 ? '+' : '') + raw.rs_momentum, cls(raw.rs_momentum)));
  if (raw.atr14_pct != null) out.push(statRow('ATR 14 วัน', nf(raw.atr14_pct, 2) + '% ต่อวัน'));
  if (r.ath_pct != null) out.push(statRow('ห่างจากจุดสูงสุดตลอดกาล', pct(r.ath_pct, 1), 'down'));
  if (raw.vol_today && raw.vol_avg20) out.push(statRow('ปริมาณซื้อขายวันนี้', volX(raw.vol_today, raw.vol_avg20)));
  if (raw.seas_hit != null) {
    out.push(secHead('ฤดูกาล · เดือนนี้ย้อนหลัง ' + raw.seas_n + ' ปี'));
    out.push(statRow('ความน่าจะเป็นบวก', nf(raw.seas_hit, 0) + '%', raw.seas_hit >= 60 ? 'up' : raw.seas_hit < 45 ? 'down' : ''));
    out.push(statRow('ผลตอบแทนเฉลี่ยเดือนนี้', pct(raw.seas_ret, 2), cls(raw.seas_ret)));
  }
  return out;
}
function tabFin(r) {
  const f = D.fin[r.symbol];
  const q = D.finQ[r.symbol];
  const out = [];
  if (q) out.push(...quarterlySection(q));
  if (!f) {
    if (!q) out.push(el('div', 'note', 'ยังไม่มีข้อมูลงบการเงินย้อนหลังของหลักทรัพย์นี้ในชุดข้อมูลมือถือ'));
    return out;
  }
  out.push(secHead('คุณภาพ & ความแข็งแรง'),
    el('div', 'note', `งบรายปีย้อนหลัง ~${f.years_span} ปี · ${f.quarters_available} ไตรมาสล่าสุด (งวด ${f.latest_quarter})`));
  const zLbl = f.z_zone === 'safe' ? 'ปลอดภัย' : f.z_zone === 'grey' ? 'เฝ้าระวัง' : 'เสี่ยง';
  out.push(
    statRow('Piotroski F-Score', `<span class="pill ${f.f_score >= 7 ? 'g' : f.f_score >= 4 ? 'n' : 'r'}">${f.f_score} / ${f.f_score_max}</span>`),
    statRow('Altman Z-Score', `<span class="pill ${f.z_zone === 'safe' ? 'g' : f.z_zone === 'grey' ? 'w' : 'r'}">${nf(f.z_score, 2)} · ${zLbl}</span>`),
    statRow('ROE', f.roe == null ? '–' : nf(f.roe, 1) + '%', f.roe >= 10 ? 'up' : ''),
    statRow('อัตรากำไรขั้นต้น', f.gross_margin == null ? '–' : nf(f.gross_margin, 1) + '%'),
    statRow('อัตรากำไรสุทธิ', f.net_margin == null ? '–' : nf(f.net_margin, 2) + '%'),
    statRow('D/E', nf(f.de_ratio, 2)),
    statRow('ความสามารถจ่ายดอกเบี้ย', f.interest_coverage == null ? '–' : nf(f.interest_coverage, 1) + '×'),
    statRow('FCF Yield', f.fcf_yield == null ? '–' : nf(f.fcf_yield, 1) + '%', f.fcf_yield > 0 ? 'up' : 'down'),
  );
  out.push(secHead('การเติบโต (ไตรมาสล่าสุด)'));
  out.push(
    statRow('รายได้ YoY', pct(f.rev_yoy_q, 2), cls(f.rev_yoy_q)),
    statRow('รายได้ QoQ', pct(f.rev_qoq, 2), cls(f.rev_qoq)),
    statRow('กำไรสุทธิ YoY', pct(f.profit_yoy_q, 2), cls(f.profit_yoy_q)),
    statRow('กำไรสุทธิ QoQ', pct(f.profit_qoq, 2), cls(f.profit_qoq)),
    statRow('กำไรเป็นบวกติดกัน', (f.profit_pos_streak_q ?? 0) + ' ปี', f.profit_pos_streak_q >= 3 ? 'up' : ''),
    statRow(`CAGR รายได้ / กำไร (${f.years_span} ปี)`,
      `<span class="${cls(f.rev_cagr)}">${pct(f.rev_cagr, 1)}</span>  ·  <span class="${cls(f.profit_cagr)}">${pct(f.profit_cagr, 1)}</span>`),
  );
  if (Array.isArray(f.f_score_detail)) {
    out.push(secHead('รายละเอียด F-Score'));
    const chk = el('div', 'fchk');
    f.f_score_detail.forEach(fi => {
      const row = el('div', 'fi ' + (fi.pass ? 'ok' : 'no'));
      row.innerHTML = `<span class="mk">${fi.pass ? '✓' : '✕'}</span><span>${fi.label}</span>`;
      chk.appendChild(row);
    });
    out.push(chk);
  }
  return out;
}

/* ---------------- quarterly P&L (SET.or.th) ---------------- */
function quarterlySection(q) {
  const M = q.unit === 'M฿' ? 1e6 : 1;   // ไฟล์เก็บเป็นล้านบาท — คูณกลับก่อนแสดง
  const sc = a => a.map(v => v == null ? null : v * M);
  const n = q.q.length, rev = sc(q.revenue), np = sc(q.net_profit);
  const show = Math.min(n, 12);
  const out = [secHead('กำไรขาดทุนรายไตรมาส')];

  // ---- revenue bar chart ----
  const cw = el('div', 'chart-wrap');
  cw.innerHTML = '<canvas class="spark" id="qChart" style="height:150px"></canvas>';
  out.push(cw);
  out.push(el('div', 'chart-src', `รายได้รายไตรมาส ${show} งวดล่าสุด · หน่วยล้านบาท · ที่มา ตลาดหลักทรัพย์ฯ`));
  requestAnimationFrame(function kick() {
    const cv = document.getElementById('qChart');
    if (cv && cv.clientWidth > 0) drawQBars(cv, q.q.slice(-show), rev.slice(-show));
    else requestAnimationFrame(kick);
  });

  // ---- latest quarter YoY ----
  const li = n - 1;
  if (n >= 5 && rev[li] != null && rev[li - 4]) {
    const revYoY = (rev[li] - rev[li - 4]) / Math.abs(rev[li - 4]) * 100;
    out.push(statRow(`รายได้ YoY (${beQ(q.q[li])})`, pct(revYoY, 1), cls(revYoY)));
  }
  if (n >= 5 && np[li] != null && np[li - 4]) {
    const npYoY = (np[li] - np[li - 4]) / Math.abs(np[li - 4]) * 100;
    out.push(statRow(`กำไรสุทธิ YoY (${beQ(q.q[li])})`, pct(npYoY, 1), cls(npYoY)));
  }

  // ---- table: last 8 quarters ----
  // แสดงตัวเลขเต็มหน่วยล้านบาท (ไม่ย่อเป็น "พันล้าน" ที่ปัดเศษจนเพี้ยน)
  const rows = Math.min(n, 8);
  out.push(el('div', 'chart-src', 'รายได้ / กำไรสุทธิ — หน่วยล้านบาท เต็มจำนวน (แตะค้างที่ตัวเลขเพื่อดูค่าเป็นบาท)'));
  const tbl = el('div', 'qtbl');
  tbl.innerHTML = `<div class="qtr qhd"><span>ไตรมาส</span><span>รายได้</span><span>กำไรสุทธิ</span><span>มาร์จิ้น</span></div>` +
    q.q.slice(-rows).map((lab, i) => {
      const idx = n - rows + i;
      const rv = rev[idx], nv = np[idx];                 // หน่วยบาท (สำหรับ margin + title)
      const rvM = rv == null ? null : rv / 1e6, nvM = nv == null ? null : nv / 1e6;
      const mg = (rv && nv != null) ? nv / rv * 100 : null;
      return `<div class="qtr">
        <span class="qp">${beQ(lab)}</span>
        <span title="${exactBaht(rv)}">${nf(rvM, 0)}</span>
        <span class="${cls(nv)}" title="${exactBaht(nv)}">${nf(nvM, 0)}</span>
        <span class="${mg == null ? '' : cls(mg)}">${mg == null ? '–' : nf(mg, 1) + '%'}</span>
      </div>`;
    }).join('');
  out.push(tbl);
  return out;
}

function drawQBars(cv, labels, vals) {
  const dpr = Math.min(3, window.devicePixelRatio || 1);
  const w = cv.clientWidth, h = cv.clientHeight || 150;
  cv.width = w * dpr; cv.height = h * dpr;
  const x = cv.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0); x.clearRect(0, 0, w, h);
  const nums = vals.map(v => (v == null || isNaN(v)) ? 0 : v);
  const mx = Math.max(...nums, 1), n = nums.length;
  const padB = 20, padT = 12, rpad = 66;
  const plotW = w - 10 - rpad, plotH = h - padB - padT;
  const bw = Math.min(26, plotW / n * 0.64), gap = (plotW - bw * n) / (n + 1);
  const every = n > 8 ? 3 : 2;
  x.font = '600 10px "IBM Plex Sans Thai",sans-serif'; x.textBaseline = 'middle';
  nums.forEach((v, i) => {
    const bh = Math.max(1, v / mx * plotH);
    const bx = 8 + gap + i * (bw + gap), by = padT + plotH - bh;
    x.fillStyle = i === n - 1 ? '#4493f8' : 'rgba(68,147,248,.34)';
    x.beginPath();
    if (x.roundRect) x.roundRect(bx, by, bw, bh, 3); else x.rect(bx, by, bw, bh);
    x.fill();
    if (i % every === 0 || i === n - 1) {
      x.fillStyle = '#8b95a1'; x.textAlign = 'center';
      x.fillText(beQ(labels[i]), bx + bw / 2, h - 9);
    }
  });
  // right-edge: สูงสุด (บน) + งวดล่าสุด (ที่ปลายแท่ง — เว้นถ้าเกือบเท่า max อยู่แล้ว)
  // ป้ายเป็น "ล้านบาท" เต็มจำนวน (ให้ตรงกับตารางด้านล่าง ไม่ย่อ/ปัด)
  x.textAlign = 'left';
  x.fillStyle = '#8b95a1'; x.fillText(nf(mx / 1e6, 0), w - rpad + 5, padT + 2);
  if (nums[n - 1] < mx * 0.9) {
    const ly = padT + plotH - Math.max(1, nums[n - 1] / mx * plotH);
    x.fillStyle = '#4493f8';
    x.fillText(nf(nums[n - 1] / 1e6, 0), w - rpad + 5, Math.min(h - padB - 4, ly + 4));
  }
}

/* ---------------- market-breadth mini charts (ตลาด) ---------------- */
function _cvBase(cv, hDef) {
  const dpr = Math.min(3, window.devicePixelRatio || 1);
  const w = cv.clientWidth, h = cv.clientHeight || hDef;
  cv.width = w * dpr; cv.height = h * dpr;
  const x = cv.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0); x.clearRect(0, 0, w, h);
  return { x, w, h };
}
// รอ canvas มีความกว้างจริงก่อนวาด (แท็บ/หน้าอาจยังซ่อน) — เลิกถ้า element หาย
function _kickCanvas(id, fn) {
  let tries = 0;
  requestAnimationFrame(function k() {
    const cv = document.getElementById(id);
    if (!cv) return;
    if (cv.clientWidth > 0) { fn(cv); return; }
    if (++tries > 120) return;
    requestAnimationFrame(k);
  });
}
const _MONO9 = '9px "IBM Plex Mono",ui-monospace,monospace';
// x-axis date labels — จัด align กันหลุดขอบ (ซ้าย=left, ขวา=right, กลาง=center)
// และเว้น label ที่ใกล้ตัวสุดท้ายเกินไป ไม่ให้ทับกับ label ขวาสุด
function _xLabels(x, dates, n, px, h, slice) {
  x.fillStyle = '#5b636e'; x.textBaseline = 'alphabetic';
  const step = Math.max(1, Math.round(n / 5));
  for (let i = 0; i < n; i++) {
    const last = i === n - 1;
    if (!last && (i % step !== 0 || i > n - 1 - step * 0.6)) continue;
    x.textAlign = last ? 'right' : i === 0 ? 'left' : 'center';
    x.fillText(dates[i].slice(slice[0], slice[1]), px(i), h - 4);
  }
}

function drawNHNL(cv, dates, nh, nl) {
  const { x, w, h } = _cvBase(cv, 140);
  const n = Math.min(nh.length, nl.length, dates.length);
  if (n < 2) return;
  const mx = Math.max(...nh.slice(0, n), ...nl.slice(0, n), 1);
  const pl = 28, pr = 10, pt = 8, pb = 16, plotH = h - pt - pb;
  const px = i => pl + i / (n - 1) * (w - pl - pr);
  const py = v => pt + plotH * (1 - v / mx);
  x.font = _MONO9; x.textBaseline = 'middle';
  [0, 0.5, 1].forEach(f => {
    const gy = pt + plotH * (1 - f);
    x.strokeStyle = 'rgba(139,149,161,.14)'; x.beginPath(); x.moveTo(pl, gy); x.lineTo(w - pr, gy); x.stroke();
    x.fillStyle = '#5b636e'; x.textAlign = 'right'; x.fillText(Math.round(mx * f), pl - 5, gy);
  });
  x.beginPath();
  for (let i = 0; i < n; i++) i ? x.lineTo(px(i), py(nh[i])) : x.moveTo(px(i), py(nh[i]));
  x.lineTo(px(n - 1), py(0)); x.lineTo(px(0), py(0)); x.closePath();
  x.fillStyle = 'rgba(63,185,80,.16)'; x.fill();
  x.beginPath();
  for (let i = 0; i < n; i++) i ? x.lineTo(px(i), py(nh[i])) : x.moveTo(px(i), py(nh[i]));
  x.strokeStyle = '#3fb950'; x.lineWidth = 2; x.lineJoin = 'round'; x.stroke();
  x.beginPath();
  for (let i = 0; i < n; i++) i ? x.lineTo(px(i), py(nl[i])) : x.moveTo(px(i), py(nl[i]));
  x.strokeStyle = '#f85149'; x.lineWidth = 1.5; x.stroke();
  _xLabels(x, dates, n, px, h, [5, 10]);
}

function drawBreadthEMA(cv, dates, a, b) {
  const { x, w, h } = _cvBase(cv, 130);
  const n = dates.length; if (n < 2) return;
  const pl = 26, pr = 12, pt = 8, pb = 16, plotH = h - pt - pb;
  const px = i => pl + i / (n - 1) * (w - pl - pr);
  const py = v => pt + plotH * (1 - v / 100);
  x.font = _MONO9; x.textBaseline = 'middle';
  [20, 50, 80].forEach(rv => {
    const gy = py(rv);
    x.strokeStyle = 'rgba(139,149,161,.14)'; x.setLineDash([3, 3]);
    x.beginPath(); x.moveTo(pl, gy); x.lineTo(w - pr, gy); x.stroke(); x.setLineDash([]);
    x.fillStyle = '#5b636e'; x.textAlign = 'right'; x.fillText(rv, pl - 5, gy);
  });
  const line = (arr, col) => {
    x.strokeStyle = col; x.lineWidth = 1.6; x.lineJoin = 'round'; x.beginPath();
    let started = false;
    arr.forEach((v, i) => {
      if (v == null) return;
      const xx = px(i), yy = py(v);
      started ? x.lineTo(xx, yy) : x.moveTo(xx, yy); started = true;
    });
    x.stroke();
  };
  line(a, '#d29922'); line(b, '#3fb950');
  _xLabels(x, dates, n, px, h, [2, 7]);
}

function drawMccBars(cv, dates, osc) {
  const { x, w, h } = _cvBase(cv, 120);
  const n = dates.length; if (n < 2) return;
  const vals = osc.slice(0, n);
  const m = Math.max(...vals.filter(v => v != null).map(Math.abs), 50);
  const pl = 26, pr = 12, pt = 6, pb = 16, plotH = h - pt - pb;
  const px = i => pl + i / (n - 1) * (w - pl - pr);
  const py = v => pt + plotH * (1 - (v + m) / (2 * m));
  const y0 = py(0), bw = Math.max(1, (w - pl - pr) / n - 0.5);
  x.font = _MONO9; x.textBaseline = 'middle';
  [-70, 0, 70].forEach(rv => {
    if (Math.abs(rv) > m) return;
    const gy = py(rv);
    x.strokeStyle = 'rgba(139,149,161,.14)'; x.beginPath(); x.moveTo(pl, gy); x.lineTo(w - pr, gy); x.stroke();
    x.fillStyle = '#5b636e'; x.textAlign = 'right'; x.fillText(rv, pl - 5, gy);
  });
  vals.forEach((v, i) => {
    if (v == null) return;
    const xx = px(i), yy = py(v);
    x.fillStyle = v >= 0 ? 'rgba(63,185,80,.7)' : 'rgba(248,81,73,.7)';
    x.fillRect(xx, Math.min(yy, y0), bw, Math.max(1, Math.abs(yy - y0)));
  });
  _xLabels(x, dates, n, px, h, [2, 7]);
}

/* ---------------- canvas sparkline ---------------- */
function drawSpark(cv, ser) {
  if (!cv) return;
  const dpr = Math.min(3, window.devicePixelRatio || 1);
  const w = cv.clientWidth, h = cv.clientHeight || 190;
  cv.width = w * dpr; cv.height = h * dpr;
  const x = cv.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0); x.clearRect(0, 0, w, h);
  if (!ser || ser.length < 2) return;
  const mn = Math.min(...ser), mx = Math.max(...ser), pad = 8, rpad = 48;
  const up = ser[ser.length - 1] >= ser[0];
  const col = up ? '#3fb950' : '#f85149';
  const px = i => pad + i / (ser.length - 1) * (w - pad - rpad);
  const py = v => { const t = (v - mn) / ((mx - mn) || 1); return h - pad - t * (h - pad * 2); };
  const fmt2 = n => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const grad = x.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, up ? 'rgba(63,185,80,.22)' : 'rgba(248,81,73,.20)');
  grad.addColorStop(1, 'rgba(63,185,80,0)');
  x.beginPath(); x.moveTo(px(0), py(ser[0]));
  ser.forEach((v, i) => x.lineTo(px(i), py(v)));
  x.lineTo(px(ser.length - 1), h - pad); x.lineTo(px(0), h - pad); x.closePath();
  x.fillStyle = grad; x.fill();
  x.strokeStyle = 'rgba(139,149,161,.28)'; x.setLineDash([3, 3]); x.lineWidth = 1;
  x.beginPath(); x.moveTo(pad, py(ser[0])); x.lineTo(w - rpad, py(ser[0])); x.stroke(); x.setLineDash([]);
  x.strokeStyle = col; x.lineWidth = 2; x.lineJoin = 'round';
  x.beginPath(); ser.forEach((v, i) => i ? x.lineTo(px(i), py(v)) : x.moveTo(px(i), py(v))); x.stroke();
  const ex = px(ser.length - 1), ey = py(ser[ser.length - 1]);
  x.fillStyle = up ? 'rgba(63,185,80,.25)' : 'rgba(248,81,73,.22)';
  x.beginPath(); x.arc(ex, ey, 6, 0, 7); x.fill();
  x.fillStyle = col; x.beginPath(); x.arc(ex, ey, 3.2, 0, 7); x.fill();
  x.font = '600 11px "IBM Plex Sans Thai",sans-serif'; x.textAlign = 'left'; x.textBaseline = 'middle';
  x.fillStyle = '#8b95a1';
  if (Math.abs(py(mx) - ey) > 15) x.fillText(fmt2(mx), w - rpad + 6, py(mx) + 6);
  if (Math.abs(py(mn) - ey) > 15) x.fillText(fmt2(mn), w - rpad + 6, py(mn) - 6);
  x.fillStyle = col; x.fillText(fmt2(ser[ser.length - 1]), w - rpad + 6, ey);
}

let _rz, _rzW = window.innerWidth;
window.addEventListener('resize', () => {
  // ทุกจอ layout ขับด้วยความกว้างล้วน (คอนเทนเนอร์ max-width, canvas width:100% สูงคงที่)
  // resize ที่ความสูงเปลี่ยนอย่างเดียว (URL bar เด้งตอนสกรอลล์บนมือถือ) ไม่ต้องวาดใหม่
  if (window.innerWidth === _rzW) return;
  _rzW = window.innerWidth;
  clearTimeout(_rz);
  _rz = setTimeout(() => {
    if (curScreen === 'detail') {
      const r = D.byId[curId];
      if (r && r.ph) drawSpark($('#dChart'), sliceHistory(r.ph, Object.keys(TFRET).includes(dTf) ? dTf : '6M', r.price));
    } else if (curScreen === 'market' && D.mkt) {
      renderMarket();   // มีหลาย canvas — วาดใหม่ทั้งหน้าง่ายกว่าไล่ redraw ทีละอัน
    } else if (curScreen === 'rotation') {
      renderRotation();
    } else if (curScreen === 'valuation') {
      renderValuation();
    } else if (curScreen === 'indices') {
      renderIndices();
    } else if (curScreen === 'heatmap') {
      renderHeatmap();
    } else if (curScreen === 'flow') {
      renderFlow();
    }
  }, 120);
});

/* ================================================================
   เฟส 2 ข้อ 4 + 6 : ดัชนีกลุ่มอุตสาหกรรม / Valuation / Heatmap ตลาดไทย
   พอร์ต 1:1 จาก dashboard.js (loadIndicesPage/renderValuation/renderHeatmap)
   ข้อมูล bake พร้อม: indices_data.json · market_stats.json ·
   set_daily_valuation.json · stock_valuation_stats.json · set_data.json
   ================================================================ */

/* ---- heat colors (ported จาก dashboard.js _heatColor / _heatColorRS) ---- */
function heatColor(v, cap = 15) {
  if (v == null) return 'var(--surface-hi)';
  const t = Math.min(Math.abs(v) / cap, 1);
  return v >= 0 ? `rgba(63,185,80,${0.14 + t * 0.86})` : `rgba(248,81,73,${0.14 + t * 0.86})`;
}
function heatColorRS(v) {
  if (v == null) return 'var(--surface-hi)';
  const t = Math.abs(v - 50) / 50;
  return v >= 50 ? `rgba(63,185,80,${0.14 + t * 0.86})` : `rgba(248,81,73,${0.14 + t * 0.86})`;
}
const heatTextOn = (v, cap) => Math.abs(v ?? 0) > cap * 0.55 ? '#fff' : 'var(--text)';
const rsTextOn = v => ((v ?? 50) > 68 || (v ?? 50) < 32) ? '#fff' : 'var(--text)';

/* mini area sparkline (สั้น เตี้ย — สำหรับการ์ดดัชนี) */
function drawTiny(cv, ser) {
  const { x, w, h } = _cvBase(cv, 52);
  const s = (ser || []).filter(v => v != null && !isNaN(v));
  if (s.length < 2) return;
  const mn = Math.min(...s), mx = Math.max(...s), rng = (mx - mn) || 1;
  const up = s[s.length - 1] >= s[0];
  const col = up ? '#3fb950' : '#f85149';
  const px = i => i / (s.length - 1) * w;
  const py = v => h - 3 - (v - mn) / rng * (h - 6);
  const grad = x.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, up ? 'rgba(63,185,80,.20)' : 'rgba(248,81,73,.18)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  x.beginPath(); x.moveTo(px(0), py(s[0]));
  s.forEach((v, i) => x.lineTo(px(i), py(v)));
  x.lineTo(px(s.length - 1), h); x.lineTo(0, h); x.closePath(); x.fillStyle = grad; x.fill();
  x.beginPath(); s.forEach((v, i) => i ? x.lineTo(px(i), py(v)) : x.moveTo(px(i), py(v)));
  x.strokeStyle = col; x.lineWidth = 1.6; x.lineJoin = 'round'; x.stroke();
}

/* ================= HEATMAP ตลาดไทย (ข้อ 6) ================= */
// พอร์ตจาก renderHeatmap / HM_CFG — grid ราย sector, สี = metric, แตะ = เปิดรายละเอียดหุ้น
// key ตรงกับ field ใน row (rowSet): วันนี้ = chg1d ไม่ใช่ ret_1d
const HM_METRICS = [
  ['chg1d', '1 วัน', 15], ['ret_1w', '1 สัปดาห์', 15], ['ret_1m', '1 เดือน', 20],
  ['ret_3m', '3 เดือน', 30], ['ret_ytd', 'ต้นปี', 30], ['ret_1y', '1 ปี', 50],
  ['rs', 'RS', 0], ['from_52wh', 'จาก 52W สูง', 40],
];
let _hmMetric = 'chg1d', _hmDir = 1;   // 1 = มาก→น้อย (descending) · -1 = น้อย→มาก — ตรงกับ _hmCmp ของ dashboard.js

function hmValue(r, key) {
  if (key === 'rs') return r.rs;
  if (key === 'from_52wh') return (r.high_52w > 0 && r.price != null) ? (r.price - r.high_52w) / r.high_52w * 100 : null;
  return r[key];
}

function renderHeatmap() {
  const c = $('#s-heatmap'); if (!c) return;
  c.innerHTML = '';
  const def = HM_METRICS.find(m => m[0] === _hmMetric) || HM_METRICS[0];
  const cap = def[2] || 20;
  const isRS = _hmMetric === 'rs';

  const bar = el('div', 'sortbar'); bar.id = 'hmMetricBar';
  bar.innerHTML = HM_METRICS.map(([k, t]) => `<button class="sortchip ${_hmMetric === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(bar);
  const dir = el('div', 'seg');
  dir.innerHTML = `<button data-d="1" class="${_hmDir === 1 ? 'on' : ''}">มาก → น้อย</button><button data-d="-1" class="${_hmDir === -1 ? 'on' : ''}">น้อย → มาก</button>`;
  c.appendChild(dir);
  c.appendChild(el('div', 'chart-src',
    (isRS ? 'เขียว = RS สูง · แดง = RS ต่ำ' : 'เขียว = บวก · แดง = ลบ') + ' · แตะเพื่อดูรายละเอียด'));
  const grid = el('div'); grid.id = 'hmGrid'; c.appendChild(grid);

  bar.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _hmMetric = b.dataset.k; renderHeatmap(); } });
  dir.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _hmDir = +b.dataset.d; renderHeatmap(); } });

  // cache ค่า metric ต่อ row ครั้งเดียว — sort / สีพื้น / label / ค่าเฉลี่ย ใช้ค่าเดียวกัน
  // (เดิม hmValue ถูกเรียกซ้ำหลายพันครั้งใน comparator)
  const vOf = new Map();
  const groups = {};
  D.stocks.forEach(r => {
    vOf.set(r, hmValue(r, _hmMetric));
    const g = (r.raw && r.raw.sector) || 'ไม่ระบุ';
    (groups[g] || (groups[g] = [])).push(r);
  });
  // null อยู่ท้ายเสมอไม่ว่าเรียงทิศไหน (เทียบ _hmCmp ของ dashboard.js) — สลับทิศเฉพาะค่าที่มีจริง
  const cmp = (va, vb) => {
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return (vb - va) * _hmDir;
  };
  const secAvgMap = new Map();
  for (const [sec, arr] of Object.entries(groups)) {
    let sum = 0, k = 0;
    arr.forEach(r => { const v = vOf.get(r); if (v != null) { sum += v; k++; } });
    secAvgMap.set(sec, k ? sum / k : null);
  }
  const secList = Object.entries(groups).sort((a, b) => cmp(secAvgMap.get(a[0]), secAvgMap.get(b[0])));

  grid.innerHTML = secList.map(([sec, arr]) => {
    const sorted = [...arr].sort((a, b) => cmp(vOf.get(a), vOf.get(b)));
    const avg = secAvgMap.get(sec);
    const avgCls = avg == null ? 'flat' : isRS ? (avg >= 50 ? 'up' : 'down') : (avg >= 0 ? 'up' : 'down');
    const avgTxt = avg == null ? '–' : isRS ? 'avg RS ' + Math.round(avg) : pct(avg, 1);
    const cells = sorted.map(r => {
      const v = vOf.get(r);
      const bg = isRS ? heatColorRS(v) : heatColor(v, cap);
      const tc = isRS ? rsTextOn(v) : heatTextOn(v, cap);
      const lbl = v == null ? '–' : isRS ? 'RS ' + Math.round(v) : (v > 0 ? '+' : '') + nf(v, 1) + '%';
      return `<button class="hm-cell" style="background:${bg};color:${tc}" data-id="${r.id}">
        <span class="hc-s">${r.symbol}</span><span class="hc-p">${nf(r.price, r.price < 1 ? 3 : 2)}</span><span class="hc-v">${lbl}</span></button>`;
    }).join('');
    return `<div class="hm-sec">
      <div class="hm-sh"><span class="hm-sn">${sec}</span><span class="${avgCls}">${avgTxt}</span><span class="hm-sc">${arr.length} หุ้น</span></div>
      <div class="hm-cells">${cells}</div></div>`;
  }).join('');

  grid.addEventListener('click', e => {
    const b = e.target.closest('.hm-cell'); if (b) openDetail(b.dataset.id);
  });
}

/* ================= ดัชนีกลุ่มอุตสาหกรรม (ข้อ 4) ================= */
// พอร์ตจาก loadIndicesPage / renderIdxGrid / renderIdxHeatmap
// indices_data.json ~1.2MB gz — โหลดตอนเปิดหน้าครั้งแรก (ไม่โหลดตอน boot)
const IDX_GROUPS = [
  ['ALL', 'ทั้งหมด'], ['SET_INDICES', 'ดัชนี SET'], ['SET_INDUSTRY', 'กลุ่มอุตสาหกรรม'],
  ['SET_SECTORS', 'หมวดธุรกิจ'], ['MAI_INDUSTRY', 'mai'],
];
const IDX_SORTS = [
  ['ret_1d', '1 วัน'], ['ret_1w', '1 สัปดาห์'], ['ret_1m', '1 เดือน'], ['ret_3m', '3 เดือน'],
  ['ret_6m', '6 เดือน'], ['ret_1y', '1 ปี'], ['mom', 'โมเมนตัม'], ['rs_set', 'RS'],
];
const IDX_GLBL = { SET_INDICES: 'ดัชนี SET', SET_INDUSTRY: 'กลุ่มอุตสาหกรรม', SET_SECTORS: 'หมวดธุรกิจ', MAI_INDUSTRY: 'mai' };
let _idxGroup = 'ALL', _idxSort = 'ret_1m', _idxView = 'cards', _idxLoading = false;

const idxShort = sym => String(sym).replace(/^\^/, '').replace(/\.BK$/, '');
// Momentum Score = 1M% − 6M%÷6 (เหมือน renderIdxGrid) — ไม่มี 6M คืน null
const idxMom = i => (i.ret_1m != null && i.ret_6m != null) ? +(i.ret_1m - i.ret_6m / 6).toFixed(2) : null;

async function loadIndicesData() {
  if (D.indices || _idxLoading) return;
  _idxLoading = true;
  try {
    D.indices = await loadJSON('../data/indices_data.json', 45000);
  } catch (e) {
    _idxLoading = false;
    const c = $('#s-indices');
    if (curScreen === 'indices') c.innerHTML = `<div class="list-cap">โหลดข้อมูลดัชนีไม่สำเร็จ — ${e.message}</div>`;
    return;
  }
  _idxLoading = false;
  if (curScreen === 'indices') renderIndices();
  else if (curScreen === 'stocks') paintStkView();   // จอ "จัดอันดับกลุ่ม" รอ override ดัชนี
}

function renderIndices() {
  const c = $('#s-indices'); if (!c) return;
  if (!D.indices) {
    c.innerHTML = '<div class="list-cap">กำลังโหลดข้อมูลดัชนี…</div>';
    loadIndicesData();
    return;
  }
  c.innerHTML = '';

  const view = el('div', 'seg');
  view.innerHTML = `<button data-v="cards" class="${_idxView === 'cards' ? 'on' : ''}">การ์ด</button><button data-v="heat" class="${_idxView === 'heat' ? 'on' : ''}">Heatmap</button>`;
  c.appendChild(view);
  const gbar = el('div', 'sortbar'); gbar.id = 'idxGroupBar';
  gbar.innerHTML = IDX_GROUPS.map(([k, t]) => `<button class="sortchip ${_idxGroup === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(gbar);
  const sbar = el('div', 'sortbar'); sbar.id = 'idxSortBar';
  sbar.innerHTML = IDX_SORTS.map(([k, t]) => `<button class="sortchip ${_idxSort === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(sbar);
  const body = el('div'); body.id = 'idxBody'; c.appendChild(body);

  view.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _idxView = b.dataset.v; renderIndices(); } });
  gbar.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _idxGroup = b.dataset.k; renderIndices(); } });
  sbar.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _idxSort = b.dataset.k; renderIndices(); } });

  let items = Object.values(D.indices).map(i => ({ ...i, mom: idxMom(i) }));
  if (_idxGroup !== 'ALL') items = items.filter(i => i.group === _idxGroup);
  const sortF = _idxSort === 'mom' ? (i => i.mom) : (i => i[_idxSort]);
  items.sort((a, b) => (sortF(b) ?? -999) - (sortF(a) ?? -999));

  if (!items.length) { body.appendChild(el('div', 'list-cap', 'ไม่มีดัชนีในกลุ่มนี้')); return; }
  if (_idxView === 'heat') { idxHeatmap(body, items); return; }
  items.forEach(i => body.appendChild(idxCard(i)));
}

function idxCard(i) {
  const d = el('div', 'idxr');
  const sid = 'ik_' + idxShort(i.sym).replace(/[^A-Za-z0-9]/g, '');
  d.innerHTML = `
    <div class="ir-top">
      <span class="ir-l"><span class="ir-sym">${idxShort(i.sym)}</span><span class="ir-nm">${i.name}</span></span>
      <span class="ir-r"><span class="ir-last">${nf(i.last, 2)}</span><span class="dl ${cls(i.ret_1d)}">${pct(i.ret_1d)}</span></span>
    </div>
    <canvas class="ir-spark" id="${sid}"></canvas>
    <div class="ir-rets">
      ${[['1สัปดาห์', 'ret_1w'], ['1เดือน', 'ret_1m'], ['3เดือน', 'ret_3m'], ['6เดือน', 'ret_6m'], ['1ปี', 'ret_1y']]
      .map(([t, k]) => `<span><i>${t}</i><b class="${cls(i[k])}">${pct(i[k], 1)}</b></span>`).join('')}
    </div>
    <div class="ir-foot">
      <span>RS <b class="${i.rs_set >= 50 ? 'up' : 'down'}">${i.rs_set ?? '–'}</b></span>
      <span>โมเมนตัม <b class="${cls(i.mom)}">${i.mom == null ? '–' : (i.mom > 0 ? '+' : '') + nf(i.mom, 1)}</b></span>
    </div>`;
  _kickCanvas(sid, cv => drawTiny(cv, (i.closes || []).slice(-64)));
  return d;
}

function idxHeatmap(container, items) {
  const key = _idxSort;
  const isRS = key === 'rs_set';
  const cap = { ret_1d: 8, ret_1w: 12, ret_1m: 18, ret_3m: 28, ret_6m: 40, ret_1y: 55, mom: 12 }[key] || 20;
  const groups = {};
  items.forEach(i => { (groups[i.group] || (groups[i.group] = [])).push(i); });
  const order = ['SET_INDICES', 'SET_INDUSTRY', 'SET_SECTORS', 'MAI_INDUSTRY'];
  container.innerHTML = order.filter(g => groups[g]).map(g => {
    const arr = [...groups[g]].sort((a, b) => ((key === 'mom' ? b.mom : b[key]) ?? -999) - ((key === 'mom' ? a.mom : a[key]) ?? -999));
    const cells = arr.map(i => {
      const v = key === 'mom' ? i.mom : i[key];
      const bg = isRS ? heatColorRS(v) : heatColor(v, cap);
      const tc = isRS ? rsTextOn(v) : heatTextOn(v, cap);
      const lbl = v == null ? '–' : isRS ? 'RS ' + Math.round(v)
        : key === 'mom' ? (v > 0 ? '+' : '') + nf(v, 1) : (v > 0 ? '+' : '') + nf(v, 1) + '%';
      return `<div class="hm-cell" style="background:${bg};color:${tc}"><span class="hc-s">${idxShort(i.sym)}</span><span class="hc-v">${lbl}</span></div>`;
    }).join('');
    return `<div class="hm-sec"><div class="hm-sh"><span class="hm-sn">${IDX_GLBL[g] || g}</span><span class="hm-sc">${arr.length}</span></div><div class="hm-cells">${cells}</div></div>`;
  }).join('');
}

/* ================= VALUATION (ข้อ 4) ================= */
// พอร์ตจาก renderValuation / _calcStatsClient / filterByPeriod + stock-valuation
const VAL_PERIODS = [['ALL', 'ทั้งหมด'], ['20Y', '20 ปี'], ['10Y', '10 ปี'], ['5Y', '5 ปี'], ['3Y', '3 ปี'], ['1Y', '1 ปี']];
let _valPeriod = 'ALL';
let _svMetric = 'pe', _svScope = 'mkt', _svZone = 'all', _svQ = '';

// ตัด null หัว-ท้ายออกก่อน filter ช่วงเวลา (ซีรีส์ mai เริ่มมีข้อมูลช้ากว่า SET หลายร้อยเดือน —
// ถ้าไม่ตัด กราฟ ALL จะบีบเส้นไปกองครึ่งขวา) — พอร์ตจาก _trimNulls ของ dashboard.js
function valTrimNulls(dates, vals) {
  if (!dates || !vals) return { dates: dates || [], vals: vals || [] };
  let lo = 0, hi = vals.length - 1;
  while (lo <= hi && vals[lo] == null) lo++;
  while (hi >= lo && vals[hi] == null) hi--;
  return { dates: dates.slice(lo, hi + 1), vals: vals.slice(lo, hi + 1) };
}
function valFilterPeriod(dates, vals, period) {
  if (!dates || !vals) return { dates: [], vals: [] };
  if (period === 'ALL') return { dates, vals };
  const yrs = { '1Y': 1, '3Y': 3, '5Y': 5, '10Y': 10, '20Y': 20 }[period] || 999;
  const cut = new Date(); cut.setFullYear(cut.getFullYear() - yrs);
  const cs = cut.toISOString().slice(0, 7);
  const idx = dates.findIndex(d => d >= cs);
  return idx < 0 ? { dates, vals } : { dates: dates.slice(idx), vals: vals.slice(idx) };
}
function valStats(vals) {
  const v = (vals || []).filter(x => x != null && !isNaN(x));
  if (v.length < 2) return null;
  const arr = [...v].sort((a, b) => a - b);
  const avg = v.reduce((a, b) => a + b, 0) / v.length;
  const std = Math.sqrt(v.reduce((a, b) => a + (b - avg) ** 2, 0) / v.length);
  const cur = v[v.length - 1];
  const r2 = x => Math.round(x * 100) / 100;
  return {
    current: r2(cur), min: r2(arr[0]), max: r2(arr[arr.length - 1]), avg: r2(avg), std: r2(std),
    zscore: std ? r2((cur - avg) / std) : 0,
    percentile: Math.round(arr.filter(x => x <= cur).length / arr.length * 1000) / 10,
  };
}
// cheapIsHigh: ปันผลยิ่งสูงยิ่งถูก → พลิกสัญญาณ z ตอนตีความ ถูก/แพง
function valZone(z, cheapIsHigh) {
  const zz = cheapIsHigh ? -z : z;
  if (zz > 2) return ['แพงผิดปกติ', 'r'];
  if (zz > 1) return ['แพงกว่าปกติ', 'w'];
  if (zz > -1) return ['ใกล้ค่าเฉลี่ย', 'n'];
  if (zz > -2) return ['ถูกกว่าปกติ', 'g'];
  return ['ถูกผิดปกติ', 'g'];
}

function drawValLine(cv, vals, avg) {
  const { x, w, h } = _cvBase(cv, 92);
  const s = (vals || []).map(v => (v == null || isNaN(v)) ? null : v);
  const nn = s.filter(v => v != null);
  if (nn.length < 2) return;
  // แกน y ตัด outlier 2% บน-ล่างทิ้ง (mai P/E เคยพุ่ง 200x ทำให้เส้นอื่นแบนหมด) — เส้นยังพล็อตครบ
  const srt = [...nn].sort((a, b) => a - b);
  const q = f => srt[Math.min(srt.length - 1, Math.max(0, Math.round(f * (srt.length - 1))))];
  let mn = q(0.02), mx = q(0.98);
  if (mx - mn < 1e-6) { mn = srt[0]; mx = srt[srt.length - 1]; }
  if (avg != null) { mn = Math.min(mn, avg); mx = Math.max(mx, avg); }
  const rng = (mx - mn) || 1, pr = 46, pt = 8, pb = 6, n = s.length;
  const px = i => i / (n - 1) * (w - pr);
  const py = v => { const t = Math.max(-0.12, Math.min(1.12, (v - mn) / rng)); return pt + (1 - t) * (h - pt - pb); };
  x.font = '600 10px ' + '"IBM Plex Mono",ui-monospace,monospace';
  x.textAlign = 'left'; x.textBaseline = 'middle';
  const ay = avg != null ? py(avg) : null;
  if (avg != null) {
    x.strokeStyle = 'rgba(210,153,34,.75)'; x.setLineDash([4, 3]); x.lineWidth = 1;
    x.beginPath(); x.moveTo(0, ay); x.lineTo(w - pr, ay); x.stroke(); x.setLineDash([]);
    x.fillStyle = '#d29922'; x.fillText(nf(avg, avg >= 100 ? 0 : 1), w - pr + 5, ay);
  }
  x.strokeStyle = '#4493f8'; x.lineWidth = 1.5; x.lineJoin = 'round'; x.beginPath();
  let started = false, li = 0;
  s.forEach((v, i) => { if (v == null) return; li = i; const xx = px(i), yy = py(v); started ? x.lineTo(xx, yy) : x.moveTo(xx, yy); started = true; });
  x.stroke();
  const last = s[li];
  let ly = py(last);
  if (ay != null && Math.abs(ly - ay) < 12) ly += (ly <= ay ? -12 : 12);
  ly = Math.max(8, Math.min(h - 8, ly));
  x.fillStyle = '#4493f8'; x.beginPath(); x.arc(px(li), py(last), 3, 0, 7); x.fill();
  x.fillText(nf(last, last >= 100 ? 0 : 1), w - pr + 5, ly);
}

function valBandCard(title, stat, cheapIsHigh, unit, series, key) {
  if (!stat || stat.current == null) return el('div', 'note', title + ' — ข้อมูลไม่พอ');
  const [zl, zc] = valZone(stat.zscore, cheapIsHigh);
  const u = unit || 'x';
  const d = el('div', 'vcard');
  // key มาจาก caller (เช่น "SET_pe") — อย่า derive จาก title เพราะชื่อไทยล้วน ("เงินปันผล SET")
  // จะเหลือ "vc_" เปล่า ๆ ชนกันได้
  const sid = 'vc_' + (String(key || title).replace(/[^A-Za-z0-9]/g, '') || 'band');
  const hasChart = series && Array.isArray(series.vals) && series.vals.filter(v => v != null).length >= 8;
  d.innerHTML = `
    <div class="vc-head"><span class="vc-t">${title}</span>
      <span class="pill ${zc}">${(stat.zscore >= 0 ? '+' : '') + nf(stat.zscore, 2)}σ · ${zl}</span></div>
    <div class="vc-now">${nf(stat.current, 2)}<i>${u}</i></div>
    <div class="vc-pct">เปอร์เซ็นไทล์ <b>${nf(stat.percentile, 0)}%</b> ของช่วงที่เลือก</div>
    <div class="track"><span class="fill" style="width:${Math.min(100, Math.max(2, stat.percentile))}%"></span></div>
    ${hasChart ? `<canvas class="vc-spark" id="${sid}"></canvas>` : ''}
    <div class="vc-stat">เฉลี่ย ${nf(stat.avg, 2)}${u} · ±1σ ${nf(stat.std, 2)} · ต่ำสุด ${nf(stat.min, 2)} · สูงสุด ${nf(stat.max, 2)}</div>`;
  if (hasChart) _kickCanvas(sid, cv => drawValLine(cv, series.vals, stat.avg));
  return d;
}

// market_stats (77KB) + set_daily_valuation + stock_valuation_stats (184KB) — โหลดตอนเปิดจอ
// Valuation ครั้งแรก (ไม่โหลดตอน boot) เหมือน loadIndicesData
// 3 ไฟล์แยกอิสระ — โหลดเฉพาะที่ยังขาด, retry ทุกครั้งที่เปิดจอใหม่ (ดู go('valuation'))
// จนครบทั้ง 3 ไฟล์ (ไฟล์ที่พังไม่ทำให้ section อื่นค้างถาวร)
let _valLoading = false, _valComplete = false;
const _VAL_FILES = [
  ['mstats', '../data/market_stats.json', 20000],
  ['dailyVal', '../data/set_daily_valuation.json', 15000],
  ['stockVal', '../data/stock_valuation_stats.json', 20000],
];
const _valHasAny = () => !!(D.mstats || D.dailyVal || D.stockVal);
async function loadValuationData() {
  if (_valComplete || _valLoading) return;
  _valLoading = true;
  const jobs = _VAL_FILES.filter(([k]) => !D[k]);
  const res = await Promise.allSettled(jobs.map(([, url, to]) => loadJSON(url, to)));
  let gained = false;
  res.forEach((r, i) => { if (r.status === 'fulfilled') { D[jobs[i][0]] = r.value; gained = true; } });
  _valLoading = false;
  _valComplete = !!(D.mstats && D.dailyVal && D.stockVal);
  if (curScreen !== 'valuation') return;
  if (gained) renderValuation();
  else if (!_valHasAny())
    $('#s-valuation').innerHTML = '<div class="list-cap">โหลดข้อมูลมูลค่าไม่สำเร็จ — เปิดหน้านี้อีกครั้งเพื่อลองใหม่</div>';
}

function renderValuation() {
  const c = $('#s-valuation'); if (!c) return;
  if (!_valHasAny()) {
    c.innerHTML = '<div class="list-cap">กำลังโหลดข้อมูลมูลค่า…</div>';
    loadValuationData();
    return;
  }
  c.innerHTML = '';
  const ms = D.mstats, dv = D.dailyVal, sv = D.stockVal;

  // 1) สรุปวันนี้
  if (dv) {
    c.appendChild(secHead('สรุปมูลค่าตลาดวันนี้'));
    c.appendChild(el('div', 'chart-src', 'ณ ' + (dv.as_of || thaiDate(D.asOf)) + ' · ที่มา ตลาดหลักทรัพย์ฯ'));
    const g = el('div', 'vsnap');
    const cel = (k, v) => `<span class="vs-c"><i>${k}</i><b>${v}</b></span>`;
    ['SET', 'mai'].forEach(mk => {
      const d = dv[mk]; if (!d) return;
      const box = el('div', 'vs-row');
      box.innerHTML = `<div class="vs-mk">${mk}</div><div class="vs-grid">
        ${cel('P/E', nf(d.pe, 2) + 'x')}${cel('P/BV', nf(d.pbv, 2) + 'x')}${cel('เงินปันผล', nf(d.div_yield, 2) + '%')}
        ${cel('EPS', nf(d.eps, 2))}${cel('มูลค่าตลาด', fmtBaht((d.mkt_cap || 0) * 1e6))}${cel('Turnover YTD', nf(d.turnover_ytd, 1) + '%')}</div>`;
      g.appendChild(box);
    });
    c.appendChild(g);
  }

  // 2) กรอบมูลค่าย้อนหลัง SET / mai
  if (ms && ms.pe && ms.pbv) {
    c.appendChild(secHead('กรอบมูลค่าย้อนหลัง'));
    const pbar = el('div', 'sortbar'); pbar.id = 'valPeriodBar';
    pbar.innerHTML = VAL_PERIODS.map(([k, t]) => `<button class="sortchip ${_valPeriod === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
    c.appendChild(pbar);
    pbar.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _valPeriod = b.dataset.k; renderValuation(); } });

    const mkBand = (mkKey, label) => {
      const wrap = el('div', 'vband');
      wrap.appendChild(el('div', 'vband-h', label));
      const defs = [['pe', 'P/E', ms.pe, false, 'x'], ['pbv', 'P/BV', ms.pbv, false, 'x']];
      if (mkKey === 'SET' && ms.div_yield) defs.push(['divyield', 'เงินปันผล', ms.div_yield, true, '%']);
      defs.forEach(([mk, t, stat, cheapHigh, u]) => {
        const raw = stat.series && stat.series[mkKey];
        if (!raw) return;
        // ตัด null หัว-ท้ายก่อน (mai เริ่มช้า) แล้วค่อย filter ช่วง — กราฟ ALL ไม่บีบไปกองครึ่งขวา
        const trimmed = valTrimNulls(stat.dates, raw);
        const f = valFilterPeriod(trimmed.dates, trimmed.vals, _valPeriod);
        const st = (_valPeriod === 'ALL' && stat.stats && stat.stats[mkKey]) ? stat.stats[mkKey] : valStats(f.vals);
        wrap.appendChild(valBandCard(t + ' ' + mkKey, st, cheapHigh, u, f, mkKey + '_' + mk));
      });
      return wrap;
    };
    c.appendChild(mkBand('SET', 'SET Index'));
    c.appendChild(mkBand('mai', 'mai Index'));
  }

  // 3) มูลค่ารายหุ้น
  if (sv && Array.isArray(sv.stocks)) {
    c.appendChild(secHead('มูลค่ารายหุ้น'));
    c.appendChild(el('div', 'chart-src', 'ถูก/แพงเทียบค่าเฉลี่ยของตลาดหรือกลุ่มอุตสาหกรรม (z-score) · แตะเพื่อดูรายละเอียด'));
    const mrow = el('div', 'seg');
    mrow.innerHTML = `<button data-k="pe" class="${_svMetric === 'pe' ? 'on' : ''}">P/E</button><button data-k="pbv" class="${_svMetric === 'pbv' ? 'on' : ''}">P/BV</button>`;
    c.appendChild(mrow);
    const srow = el('div', 'seg');
    srow.innerHTML = `<button data-k="mkt" class="${_svScope === 'mkt' ? 'on' : ''}">เทียบทั้งตลาด</button><button data-k="sec" class="${_svScope === 'sec' ? 'on' : ''}">เทียบกลุ่ม</button>`;
    c.appendChild(srow);
    const ZONES = [['all', 'ทั้งหมด'], ['cheap', 'ถูกผิดปกติ'], ['under', 'ถูกกว่าปกติ'], ['mid', 'ปานกลาง'], ['over', 'แพงกว่าปกติ'], ['exp', 'แพงผิดปกติ']];
    const zrow = el('div', 'sortbar'); zrow.id = 'svZoneBar';
    zrow.innerHTML = ZONES.map(([k, t]) => `<button class="sortchip ${_svZone === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
    c.appendChild(zrow);
    const srch = el('div', 'search');
    srch.innerHTML = `<svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="svSearch" placeholder="ค้นหาชื่อย่อ / ชื่อบริษัท">`;
    c.appendChild(srch);
    const list = el('div'); list.id = 'svList'; c.appendChild(list);
    $('#svSearch').value = _svQ;

    const inZone = z => {
      if (z == null) return false;
      if (_svZone === 'all') return true;
      if (_svZone === 'cheap') return z <= -2;
      if (_svZone === 'under') return z > -2 && z <= -1;
      if (_svZone === 'mid') return z > -1 && z < 1;
      if (_svZone === 'over') return z >= 1 && z < 2;
      return z >= 2;   // exp
    };
    const paint = () => {
      const zf = `${_svMetric}_z_${_svScope}`;
      const q = _svQ.trim().toLowerCase();
      const arr = sv.stocks.filter(s => {
        if (s[_svMetric] == null) return false;
        if (!inZone(s[zf])) return false;
        if (q) return s.symbol.toLowerCase().includes(q) || String(s.name || '').toLowerCase().includes(q);
        return true;
      }).sort((a, b) => (a[zf] ?? 999) - (b[zf] ?? 999));
      const box = $('#svList'); box.innerHTML = '';
      box.appendChild(el('div', 'note', arr.length + ' หุ้น · เรียงจากถูกสุด (z ต่ำ) ไปแพงสุด'));
      arr.slice(0, 300).forEach(s => box.appendChild(svRow(s, zf)));
      if (arr.length > 300) box.appendChild(el('div', 'list-cap', 'แสดง 300 จาก ' + arr.length + ' — พิมพ์ค้นหาเพื่อกรอง'));
    };
    mrow.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _svMetric = b.dataset.k; renderValuation(); } });
    srow.addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _svScope = b.dataset.k; renderValuation(); } });
    zrow.addEventListener('click', e => {
      const b = e.target.closest('button'); if (!b) return;
      _svZone = b.dataset.k;
      zrow.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
      paint();
    });
    $('#svSearch').addEventListener('input', e => { _svQ = e.target.value; paint(); });
    paint();
  }

  if (!c.children.length) c.appendChild(el('div', 'list-cap', 'ข้อมูล Valuation ยังโหลดไม่ครบ — ลองรีเฟรช'));
  else if (!_valComplete) c.appendChild(el('div', 'list-cap', 'บางส่วนยังโหลดไม่สำเร็จ — เปิดจอนี้อีกครั้งเพื่อลองใหม่'));
}

function svRow(s, zf) {
  const z = s[zf];
  const b = el('button', 'row');
  const r = D.byId['set:' + s.symbol];
  const zCls = z == null ? 'flat' : z < 0 ? 'up' : z >= 1 ? 'down' : 'flat';
  b.innerHTML = `
    <span class="lead">
      <span class="nm"><span class="tkr-lg">${s.symbol}</span></span>
      <span class="sub2">${shortName(s.name || '')}${s.sector ? ' · ' + s.sector : ''}</span>
    </span>
    <span class="trail">
      <div class="px">${nf(s[_svMetric], 2)}<span style="font-size:11px;color:var(--text-mute)">x</span></div>
      <span class="dl ${zCls}">${z == null ? '–' : (z >= 0 ? '+' : '') + nf(z, 2) + 'σ'}</span>
    </span>
    ${r ? '<span class="chev">›</span>' : ''}`;
  if (r) b.addEventListener('click', () => openDetail('set:' + s.symbol));
  return b;
}

/* ================================================================
   เฟส 2 ข้อ 5 : เงินทุน — กระแสเงิน / ชอร์ต / ผู้บริหาร / สัญญาณรวม
   พอร์ต 1:1 จาก dashboard.js (renderFlowPage / renderShort* /
   renderInsider* / _renderConfluenceTable) — หน้าตาใหม่ ข้อมูลครบ
   ข้อมูล bake พร้อม: market_flow{,_s50,_bond}.json · short_sales.json ·
   insider_trades_{7,30,90,180}.json · major_changes_{7,30,90,180}.json ·
   flow_signals.json — โหลดเมื่อเปิดหน้า (ไม่โหลดตอน boot)
   ================================================================ */
const FLOW_CFG = {
  set:  { label: 'SET', unit: 'ล้านบาท', hasIndex: true, src: 'ที่มา ตลาดหลักทรัพย์ฯ',
          legend: [{ k: 'fund', t: 'สถาบัน+โบรก', c: '#d9a520' },
                   { k: 'foreign', t: 'ต่างชาติ', c: '#3fb950' },
                   { k: 'retail', t: 'รายย่อย', c: '#f85149' }] },
  s50:  { label: 'S50 ฟิวเจอร์ส', unit: 'สัญญา', hasIndex: false, src: 'ที่มา ตลาดสัญญาซื้อขายล่วงหน้า (TFEX)',
          legend: [{ k: 'fund', t: 'สถาบัน', c: '#d9a520' },
                   { k: 'foreign', t: 'ต่างชาติ', c: '#3fb950' },
                   { k: 'retail', t: 'ในประเทศ', c: '#f85149' }] },
  bond: { label: 'ตราสารหนี้', unit: 'ล้านบาท', hasIndex: false, src: 'ที่มา สมาคมตลาดตราสารหนี้ไทย',
          legend: [{ k: 'foreign', t: 'ต่างชาติ (NR)', c: '#3fb950' }] },
};
let _flowTab = 'cf';
let _cfMarket = 'set', _cfPeriod = 3, _cfView = 'cum';
let _shMinPct = 0, _shSort = 'pos', _shQ = '';
let _insDays = 30, _insQ = '';
let _cfxFilter = 'all', _cfxQ = '';
let _flowLoading = false;
const _insLoading = new Set();   // days ที่กำลังโหลดอยู่ — เป็น Set กันกดปุ่มวันหลายครั้งเร็ว ๆ แล้ว fetch ซ้ำ / เคลียร์ flag ข้ามกัน

const _sstk = sym => D.byId['set:' + sym] || null;
// ชื่อผู้บริหาร/ผู้ถือหุ้นใหญ่มาจากการขูดหน้าเว็บ ก.ล.ต. ดิบ ๆ — escape ก่อนใส่ innerHTML
const _esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

async function loadFlowBundle() {
  if (D.flow || _flowLoading) return;
  _flowLoading = true;
  const [mf, s50, bond, sh, sig] = await Promise.allSettled([
    loadJSON('../data/market_flow.json', 30000),
    loadJSON('../data/market_flow_s50.json', 30000),
    loadJSON('../data/market_flow_bond.json', 30000),
    loadJSON('../data/short_sales.json', 30000),
    loadJSON('../data/flow_signals.json', 20000),
  ]);
  if (mf.status === 'fulfilled') D.flow = mf.value;
  if (s50.status === 'fulfilled') D.flowS50 = s50.value; else if (!D.flowS50) D.flowS50 = { rows: [], _failed: true };
  if (bond.status === 'fulfilled') D.flowBond = bond.value; else if (!D.flowBond) D.flowBond = { rows: [], _failed: true };
  if (sh.status === 'fulfilled') D.flowShort = sh.value;
  if (sig.status === 'fulfilled') D.flowSig = sig.value;
  _flowLoading = false;
  // ต้องมี D.flow เสมอหลัง settle — renderFlow() ใช้ !D.flow เป็น gate เรียก loadFlowBundle
  // ถ้า market_flow.json ตัวเดียว fail (แต่ short/sig ผ่าน) แล้วไม่ตั้ง sentinel = วนโหลดไม่จบ
  if (!D.flow) D.flow = { rows: [], _failed: true };
  if (curScreen === 'flow') renderFlow();
}

async function loadInsiderBundle(days) {
  if (D.insider[days] || _insLoading.has(days)) return;
  _insLoading.add(days);
  const [it, mc] = await Promise.allSettled([
    loadJSON('../data/insider_trades_' + days + '.json', 30000),
    loadJSON('../data/major_changes_' + days + '.json', 20000),
  ]);
  D.insider[days] = {
    r59: (it.status === 'fulfilled' && Array.isArray(it.value.records)) ? it.value.records : [],
    r246: (mc.status === 'fulfilled' && Array.isArray(mc.value.records)) ? mc.value.records : [],
    fetched: (it.status === 'fulfilled' && it.value.fetched_at) || '',
  };
  _insLoading.delete(days);
  // re-render เฉพาะเมื่อผู้ใช้ยังดูช่วงวันนี้อยู่ — กัน response ที่ตอบช้าของช่วงที่ทิ้งไปแล้ว
  // มา rebuild ทับ (ล้างช่องค้นหา insider / scroll ของช่วงที่กำลังดู)
  if (curScreen === 'flow' && _flowTab === 'ins' && _insDays === days) renderFlow();
}

function renderFlow() {
  const c = $('#s-flow'); if (!c) return;
  if (!D.flow) {
    c.innerHTML = '<div class="list-cap">กำลังโหลดข้อมูลเงินทุน…</div>';
    loadFlowBundle();
    return;
  }
  c.innerHTML = '';
  const tabs = el('div', 'vtabs'); tabs.id = 'flowTabs';
  const T = [['cf', 'กระแสเงิน'], ['short', 'ชอร์ต'], ['ins', 'ผู้บริหาร'], ['cfx', 'สัญญาณรวม']];
  tabs.innerHTML = T.map(([k, t]) => `<button class="vtab ${_flowTab === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  c.appendChild(tabs);
  const body = el('div'); body.id = 'flowBody'; c.appendChild(body);
  tabs.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _flowTab = b.dataset.k;
    tabs.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    b.scrollIntoView({ block: 'nearest', inline: 'center' });
    paintFlowTab();
  });
  paintFlowTab();
}
function paintFlowTab() {
  const body = $('#flowBody'); if (!body) return;
  body.innerHTML = '';
  if (_flowTab === 'cf') return cfView(body);
  if (_flowTab === 'short') return shView(body);
  if (_flowTab === 'ins') return insView(body);
  cfxView(body);
}

/* ---------------- กระแสเงิน (Capital Flow) ---------------- */
function flowRowsForPeriod(rows, months) {
  const s = (rows || []).slice().sort((a, b) => a.date.localeCompare(b.date));
  if (!months) return s;
  const c = new Date(); c.setDate(1); c.setMonth(c.getMonth() - months);
  const cs = `${c.getFullYear()}-${String(c.getMonth() + 1).padStart(2, '0')}-01`;
  return s.filter(r => r.date >= cs);
}
const _cfAxis = v => {
  const a = Math.abs(v);
  if (a >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return (v / 1e3).toFixed(a >= 1e4 ? 0 : 1) + 'K';
  return Math.round(v).toString();
};
const _cfNum = v => v == null ? '–' : (v > 0 ? '+' : '') + Math.round(v).toLocaleString('en-US');

function drawFlowChart(cv, rows, cfg, view) {
  const { x, w, h } = _cvBase(cv, 170);
  if (!rows || rows.length < 2) return;
  const keys = cfg.legend.map(l => l.k);
  const pl = 42, pr = 10, pt = 14, pb = 18, plotH = h - pt - pb, plotW = w - pl - pr;
  const cum = {}; keys.forEach(k => cum[k] = 0);
  const ser = rows.map(r => {
    const o = { date: r.date, set: r.set };
    keys.forEach(k => { if (view === 'cum') { cum[k] += (r[k] || 0); o[k] = cum[k]; } else o[k] = r[k] || 0; });
    return o;
  });
  const vals = ser.flatMap(r => keys.map(k => r[k]));
  const mn = Math.min(...vals, 0), mx = Math.max(...vals, 0), rng = (mx - mn) || 1;
  const px = i => pl + i / (ser.length - 1) * plotW;
  const py = v => pt + plotH - ((v - mn) / rng) * plotH;
  // zero line
  x.strokeStyle = 'rgba(139,149,161,.22)'; x.setLineDash([4, 4]); x.lineWidth = 1;
  x.beginPath(); x.moveTo(pl, py(0)); x.lineTo(w - pr, py(0)); x.stroke(); x.setLineDash([]);
  // SET index (เส้นประจาง) บนแกนขวาเสมือน
  if (cfg.hasIndex) {
    const sv = ser.map(r => r.set).filter(v => v != null);
    if (sv.length > 1) {
      const smn = Math.min(...sv), smx = Math.max(...sv), sr = (smx - smn) || 1;
      x.strokeStyle = 'rgba(180,190,210,.5)'; x.lineWidth = 1; x.setLineDash([2, 3]); x.beginPath();
      let st = false;
      ser.forEach((r, i) => { if (r.set == null) return; const xx = px(i), yy = pt + plotH - ((r.set - smn) / sr) * plotH; st ? x.lineTo(xx, yy) : x.moveTo(xx, yy); st = true; });
      x.stroke(); x.setLineDash([]);
    }
  }
  cfg.legend.forEach(({ k, c }) => {
    x.strokeStyle = c; x.lineWidth = 1.8; x.lineJoin = 'round'; x.beginPath();
    ser.forEach((r, i) => i ? x.lineTo(px(i), py(r[k])) : x.moveTo(px(i), py(r[k])));
    x.stroke();
  });
  x.font = _MONO9; x.fillStyle = '#5b636e'; x.textAlign = 'right'; x.textBaseline = 'middle';
  x.fillText(_cfAxis(mx), pl - 4, pt + 4);
  x.fillText(_cfAxis(mn), pl - 4, pt + plotH);
  _xLabels(x, ser.map(s => s.date), ser.length, px, h, [2, 7]);
}

function cfView(body) {
  const src = { set: D.flow, s50: D.flowS50, bond: D.flowBond }[_cfMarket];
  const cfg = FLOW_CFG[_cfMarket];
  body.innerHTML = `
    <div class="seg" id="cfMkt">${['set', 's50', 'bond'].map(k => `<button data-k="${k}" class="${_cfMarket === k ? 'on' : ''}">${FLOW_CFG[k].label}</button>`).join('')}</div>
    <div class="sortbar" id="cfPer">${[[1, '1 เดือน'], [3, '3 เดือน'], [6, '6 เดือน'], [12, '1 ปี'], [0, 'ทั้งหมด']].map(([k, t]) => `<button class="sortchip ${_cfPeriod === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>
    <div class="seg" id="cfViewSeg">${[['cum', 'สะสม'], ['net', 'รายวัน']].map(([k, t]) => `<button data-k="${k}" class="${_cfView === k ? 'on' : ''}">${t}</button>`).join('')}</div>
    <div class="chart-wrap"><canvas class="spark" id="cfChart" style="height:170px"></canvas></div>
    <div class="chart-src" id="cfSrc"></div>
    <div class="flow-leg" id="cfLegend"></div>
    <div class="ftbl" id="cfTbl"></div>`;

  body.querySelector('#cfMkt').addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _cfMarket = b.dataset.k; cfView(body); } });
  body.querySelector('#cfPer').addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _cfPeriod = +b.dataset.k; cfView(body); } });
  body.querySelector('#cfViewSeg').addEventListener('click', e => { const b = e.target.closest('button'); if (b) { _cfView = b.dataset.k; cfView(body); } });

  if (!src || !Array.isArray(src.rows) || !src.rows.length) {
    body.querySelector('#cfSrc').textContent = src && src._failed ? 'โหลดข้อมูลไม่สำเร็จ — ลองรีเฟรช' : 'ยังไม่มีข้อมูลกระแสเงินของตลาดนี้';
    return;
  }
  const rows = flowRowsForPeriod(src.rows, _cfPeriod);
  if (rows.length < 2)
    body.querySelector('.chart-wrap').innerHTML = '<div class="list-cap" style="padding:24px 0;text-align:center">ช่วงที่เลือกมีข้อมูลไม่พอวาดกราฟ — เลือกช่วงยาวขึ้น</div>';
  else
    _kickCanvas('cfChart', cv => drawFlowChart(cv, rows, cfg, _cfView));
  const span = rows.length ? `${rows[0].date} – ${rows[rows.length - 1].date}` : '';
  body.querySelector('#cfSrc').textContent = `หน่วย ${cfg.unit} · ${span} · ${cfg.src}`;

  body.querySelector('#cfLegend').innerHTML = cfg.legend.map(({ k, t, c }) => {
    const tot = rows.reduce((s, r) => s + (r[k] || 0), 0);
    const cc = tot > 0 ? 'up' : tot < 0 ? 'down' : 'flat';
    return `<span><i style="background:${c}"></i>${t} <b class="${cc}">${_cfNum(tot)}</b></span>`;
  }).join('') + (cfg.hasIndex ? '<span><i style="background:rgba(180,190,210,.7)"></i>SET (เส้นประ)</span>' : '');

  // ตาราง (ใหม่สุดอยู่บน) — cap 90 แถว
  const nk = cfg.legend.length;
  const gt = `grid-template-columns:52px repeat(${cfg.hasIndex ? nk + 1 : nk},1fr)`;
  const head = `<div class="ftr fhd" style="${gt}"><span>วันที่</span>${cfg.legend.map(l => `<span>${l.t}</span>`).join('')}${cfg.hasIndex ? '<span>SET</span>' : ''}</div>`;
  const totCells = cfg.legend.map(l => {
    const tot = rows.reduce((s, r) => s + (r[l.k] || 0), 0);
    return `<span class="${cls(tot)}">${_cfNum(tot)}</span>`;
  }).join('');
  const totRow = `<div class="ftr ftot" style="${gt}"><span>รวมช่วงนี้</span>${totCells}${cfg.hasIndex ? '<span></span>' : ''}</div>`;
  const recent = rows.slice().reverse().slice(0, 90);
  const dataRows = recent.map(r => {
    const cells = cfg.legend.map(l => `<span class="${cls(r[l.k])}">${_cfNum(r[l.k])}</span>`).join('');
    const setCell = cfg.hasIndex ? `<span class="flat">${r.set != null ? nf(r.set, 2) : '–'}</span>` : '';
    return `<div class="ftr" style="${gt}"><span>${r.date.slice(2)}</span>${cells}${setCell}</div>`;
  }).join('');
  body.querySelector('#cfTbl').innerHTML = head + totRow + dataRows +
    (rows.length > 90 ? `<div class="list-cap">แสดง 90 วันล่าสุด จาก ${rows.length} วันในช่วงนี้</div>` : '');
}

/* ---------------- ชอร์ต (Short Sales) ---------------- */
function squeezeCandidates() {
  const S = D.flowShort; if (!S || !S.stocks) return [];
  const sig = {};
  (D.flowSig && D.flowSig.stocks || []).forEach(x => { sig[x.symbol] = x; });
  const out = [];
  Object.entries(S.stocks).forEach(([sym, v]) => {
    if ((v.short_pos_pct || 0) < 1) return;
    const g = sig[sym];
    if (!g || !g.insider || !(g.insider.dir > 0)) return;
    out.push({ sym, pos: v.short_pos_pct || 0, net: g.insider.net_value_mbaht, nBuy: g.insider.n_buy || 0 });
  });
  return out.sort((a, b) => b.pos - a.pos);
}

function shView(body) {
  body.innerHTML = '';   // เรียกซ้ำตอนกดชิป/กรอง — ล้างก่อนเสมอ กันซ้อน
  const S = D.flowShort;
  if (!S || !S.stocks) { body.appendChild(el('div', 'list-cap', 'ยังไม่มีข้อมูลชอร์ต')); return; }
  const all = Object.entries(S.stocks).map(([sym, v]) => ({ sym, ...v }));
  const hasPos = all.filter(v => (v.short_pos || 0) > 0).length;
  const o05 = all.filter(v => (v.short_pos_pct || 0) >= 0.5).length;
  const o1 = all.filter(v => (v.short_pos_pct || 0) >= 1).length;
  const o2 = all.filter(v => (v.short_pos_pct || 0) >= 2).length;

  body.appendChild(el('div', 'chart-src',
    `ข้อมูล ${S.period_from || '–'} – ${S.period_to || '–'} · อัปเดต ${S.last_api_update || '–'} · ที่มา ตลาดหลักทรัพย์ฯ`));

  const chips = el('div', 'shsum');
  chips.innerHTML = [[0, hasPos, 'มี Short'], [0.5, o05, '> 0.5%'], [1, o1, '> 1%'], [2, o2, '> 2%']]
    .map(([p, n, t]) => `<button class="shc ${_shMinPct === p && p > 0 ? 'on' : ''}" data-p="${p}"><b>${n}</b><i>${t}</i></button>`).join('');
  body.appendChild(chips);
  chips.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    const p = +b.dataset.p;
    _shMinPct = (p > 0 && _shMinPct === p) ? 0 : p;
    shView(body);
  });

  const sq = squeezeCandidates();
  if (sq.length) {
    body.appendChild(secHead('🎯 Squeeze Radar · ชอร์ตสูง + ผู้บริหารซื้อ'));
    const wrap = el('div', 'sqz');
    wrap.innerHTML = sq.slice(0, 12).map(r => {
      const st = _sstk(r.sym);
      return `<div class="sqzc" data-sym="${r.sym}">
        <span class="sq-s">${r.sym}</span>
        <div class="sq-r"><span>Short Pos</span><b class="down">${nf(r.pos, 2)}%</b></div>
        <div class="sq-r"><span>ผู้บริหารซื้อ</span><b>${r.nBuy} ครั้ง</b></div>
        ${r.net != null ? `<div class="sq-r"><span>ซื้อสุทธิ</span><b class="up">+${nf(Math.abs(r.net), 0)} ลบ.</b></div>` : ''}
        ${st ? `<div class="sq-r"><span>ราคา</span><b>${nf(st.price, st.price < 1 ? 3 : 2)}</b></div>` : ''}
      </div>`;
    }).join('');
    wrap.addEventListener('click', e => {
      const cd = e.target.closest('.sqzc'); if (cd && _sstk(cd.dataset.sym)) openDetail('set:' + cd.dataset.sym);
    });
    body.appendChild(wrap);
  }

  body.appendChild(secHead('ชอร์ตรายหุ้น'));
  const srch = el('div', 'search');
  srch.innerHTML = `<svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="shSearch" placeholder="ค้นหาชื่อย่อ">`;
  body.appendChild(srch);
  const SORTS = [['pos', 'Pos %'], ['posM', 'จำนวนหุ้น'], ['val', '% มูลค่าซื้อขาย'], ['d1', '% วันนี้']];
  const sbar = el('div', 'sortbar'); sbar.id = 'shSortBar';
  sbar.innerHTML = SORTS.map(([k, t]) => `<button class="sortchip ${_shSort === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  body.appendChild(sbar);
  const list = el('div'); list.id = 'shList'; body.appendChild(list);
  $('#shSearch').value = _shQ;

  const paint = () => {
    const q = _shQ.trim().toUpperCase();
    let arr = all.filter(v => {
      if (_shMinPct > 0 ? (v.short_pos_pct || 0) < _shMinPct
        : !((v.short_pos_pct || 0) > 0 || (v.short_pos || 0) > 0 || (v.period_pct_value || 0) > 0)) return false;
      if (q && !v.sym.toUpperCase().includes(q)) return false;
      return true;
    });
    const SF = {
      pos: (a, b) => (b.short_pos_pct || 0) - (a.short_pos_pct || 0),
      posM: (a, b) => (b.short_pos || 0) - (a.short_pos || 0),
      val: (a, b) => (b.period_pct_value || 0) - (a.period_pct_value || 0),
      d1: (a, b) => ((_sstk(b.sym) || {}).chg1d ?? -999) - ((_sstk(a.sym) || {}).chg1d ?? -999),
    };
    arr.sort(SF[_shSort] || SF.pos);
    const box = $('#shList'); box.innerHTML = '';
    box.appendChild(el('div', 'note', arr.length + ' หุ้น' + (_shMinPct > 0 ? ` · Short Pos ≥ ${_shMinPct}%` : '')));
    arr.slice(0, 200).forEach(v => box.appendChild(shRow(v)));
    if (arr.length > 200) box.appendChild(el('div', 'list-cap', 'แสดง 200 จาก ' + arr.length + ' — พิมพ์ค้นหาเพื่อกรอง'));
  };
  $('#shSearch').addEventListener('input', e => { _shQ = e.target.value; paint(); });
  sbar.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _shSort = b.dataset.k;
    sbar.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  paint();
}
function shRow(v) {
  const st = _sstk(v.sym);
  const pos = v.short_pos_pct || 0;
  const tap = !!st;
  const b = el(tap ? 'button' : 'div', 'shr');
  const barW = Math.min(100, pos / 4 * 100);
  const barC = pos >= 2 ? 'var(--down)' : pos >= 1 ? '#e0682f' : pos >= 0.5 ? 'var(--warn)' : 'var(--text-mute)';
  // เทรนด์ pp ของ short_pos_pct จาก daily_tail (ชอร์ตลด = เขียว)
  const tail = Array.isArray(v.daily_tail) ? v.daily_tail : [];
  let trHtml = '';
  if (tail.length >= 2) {
    const d = (v.short_pos_pct || 0) - (tail[0][2] || 0);
    if (Math.abs(d) >= 0.005) trHtml = `<span class="${d < 0 ? 'up' : 'down'}">${d < 0 ? '▼' : '▲'}${nf(Math.abs(d), 2)}pp</span>`;
  }
  // คอลัมน์ขวา — ไม่ซ้ำกับ pos% ตรงกลาง: sort=pos โชว์ %มูลค่าซื้อขาย, อื่น ๆ โชว์ค่าที่เรียง
  let trailMain, trailLbl, trailCls = 'flat';
  if (_shSort === 'posM') { trailMain = nf(v.short_pos / 1e6, 2) + 'M'; trailLbl = 'หุ้น'; }
  else if (_shSort === 'd1') { trailMain = st ? pct(st.chg1d) : '–'; trailLbl = 'วันนี้'; trailCls = st ? cls(st.chg1d) : 'flat'; }
  else { trailMain = nf(v.period_pct_value || 0, 2) + '%'; trailLbl = '% มูลค่าซื้อขาย'; }
  b.innerHTML = `
    <span class="s-l">
      <span class="tkr-lg">${v.sym}</span>
      <span class="sub2">${st ? shortName(st.name) + (st.sub ? ' · ' + st.sub : '') : 'ไม่อยู่ในชุดข้อมูลนี้'}</span>
    </span>
    <span class="s-pos">${nf(pos, 2)}%<span class="pb" style="width:${Math.max(4, barW)}%;background:${barC}"></span></span>
    <span class="s-tr"><span class="${trailCls}">${trailMain}</span><span class="s-tl">${trailLbl}</span>${trHtml ? '<br>' + trHtml : ''}</span>`;
  if (tap) b.addEventListener('click', () => openDetail('set:' + v.sym));
  return b;
}

/* ---------------- ผู้บริหาร (Insider / ผู้ถือหุ้นใหญ่) ---------------- */
function insAccum(recs) {
  const m = {};
  (recs || []).forEach(rec => {
    const s = rec.symbol; if (!s) return;
    const a = m[s] || (m[s] = { buys: 0, sells: 0, bv: 0, sv: 0, ppl: new Set(), spl: new Set() });
    const val = (rec.qty && rec.price && isFinite(rec.price)) ? rec.qty * rec.price : 0;
    if (rec.action === 'buy') { a.buys++; a.ppl.add(rec.name || '?'); a.bv += val; }
    else if (rec.action === 'sell') { a.sells++; a.spl.add(rec.name || '?'); a.sv += val; }
  });
  return Object.entries(m).map(([sym, a]) => {
    const netVal = a.bv - a.sv;
    const dom = (a.bv > 0 || a.sv > 0) ? netVal : (a.buys - a.sells);
    return { sym, ...a, netVal, dom };
  }).filter(r => r.dom !== 0).sort((x, y) => Math.abs(y.dom) - Math.abs(x.dom));
}

function insView(body) {
  body.innerHTML = `
    <div class="sortbar" id="insDaysBar">${[7, 30, 90, 180].map(d => `<button class="sortchip ${_insDays === d ? 'on' : ''}" data-k="${d}">${d} วัน</button>`).join('')}</div>
    <div id="insInner"></div>`;
  body.querySelector('#insDaysBar').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _insDays = +b.dataset.k; insView(body);
  });
  const dd = D.insider[_insDays];
  const inner = body.querySelector('#insInner');
  if (!dd) { inner.innerHTML = '<div class="list-cap">กำลังโหลดข้อมูลผู้บริหาร…</div>'; loadInsiderBundle(_insDays); return; }
  paintInsider(inner, dd);
}

function paintInsider(inner, dd) {
  inner.innerHTML = '';
  const r59 = dd.r59, r246 = dd.r246;
  const b59 = r59.filter(x => x.action === 'buy'), s59 = r59.filter(x => x.action === 'sell');
  const b246 = r246.filter(x => x.action === 'buy'), s246 = r246.filter(x => x.action === 'sell');
  if (dd.fetched) inner.appendChild(el('div', 'chart-src', 'อัปเดต ' + dd.fetched + ' · ที่มา สำนักงาน ก.ล.ต.'));

  const tiles = el('div', 'tiles');
  tiles.innerHTML = `
    <div class="tile"><div class="t-l">ผู้บริหารซื้อ</div><div class="t-v up">${b59.length}</div><div class="t-s">${new Set(b59.map(x => x.symbol)).size} หุ้น</div></div>
    <div class="tile"><div class="t-l">ผู้บริหารขาย</div><div class="t-v down">${s59.length}</div><div class="t-s">${new Set(s59.map(x => x.symbol)).size} หุ้น</div></div>
    <div class="tile"><div class="t-l">ผู้ถือหุ้นใหญ่เพิ่ม</div><div class="t-v up">${b246.length}</div><div class="t-s">รายการ</div></div>
    <div class="tile"><div class="t-l">ผู้ถือหุ้นใหญ่ลด</div><div class="t-v down">${s246.length}</div><div class="t-s">รายการ</div></div>`;
  inner.appendChild(tiles);

  // สะสม net buy / net sell ราย symbol (แบบ 59 — ผู้บริหาร)
  const acc = insAccum(r59);
  if (acc.length) {
    const nb = acc.filter(r => r.dom > 0).length, ns = acc.filter(r => r.dom < 0).length;
    inner.appendChild(secHead(`สะสมผู้บริหาร ${_insDays} วัน · ซื้อสุทธิ ${nb} · ขายสุทธิ ${ns}`));
    const box = el('div');
    acc.slice(0, 50).forEach(r => {
      const st = _sstk(r.sym);
      const tap = !!st;
      const hasVal = r.bv > 0 || r.sv > 0;
      const up = r.dom > 0;
      const row = el(tap ? 'button' : 'div', 'inr');
      const side = up ? r.ppl.size : r.spl.size;
      row.innerHTML = `
        <span class="i-l">
          <span class="i-t"><span class="tk">${r.sym}</span>${side >= 2 ? ' <span class="mkt-tag">คลัสเตอร์ ' + side + ' คน</span>' : ''}</span>
          <span class="i-s">${st ? (st.sub || shortName(st.name)) : '—'} · ซื้อ ${r.buys} / ขาย ${r.sells}</span>
        </span>
        <span class="i-r ${up ? 'up' : 'down'}">${hasVal ? (up ? '+' : '−') + nf(Math.abs(r.netVal) / 1e6, 1) + ' ลบ.' : (up ? '+' : '') + r.dom + ' ครั้ง'}
          <span class="i-sub">${hasVal ? 'มูลค่าสุทธิ' : 'จำนวนครั้งสุทธิ'}</span></span>`;
      if (tap) row.addEventListener('click', () => openDetail('set:' + r.sym));
      box.appendChild(row);
    });
    inner.appendChild(box);
  }

  // รายการล่าสุด (ผู้บริหาร + ผู้ถือหุ้นใหญ่ รวมกัน)
  inner.appendChild(secHead('รายการล่าสุด'));
  const srch = el('div', 'search');
  srch.innerHTML = `<svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="insSearch" placeholder="ค้นหาชื่อย่อ / ชื่อบุคคล">`;
  inner.appendChild(srch);
  const listBox = el('div'); listBox.id = 'insList'; inner.appendChild(listBox);
  $('#insSearch').value = _insQ;

  const rows = [
    ...r59.map(x => ({ ...x, src: 'exec' })),
    ...r246.map(x => ({ ...x, src: 'major' })),
  ].sort((a, b) => (b.trade_date || '').localeCompare(a.trade_date || ''));

  const paint = () => {
    const q = _insQ.trim().toLowerCase();
    const arr = rows.filter(x => {
      if (!q) return true;
      const who = (x.src === 'exec' ? x.name : x.holder) || '';
      return (x.symbol || '').toLowerCase().includes(q) || who.toLowerCase().includes(q);
    });
    const box = $('#insList'); box.innerHTML = '';
    box.appendChild(el('div', 'note', arr.length + ' รายการ'));
    arr.slice(0, 150).forEach(x => box.appendChild(insRecRow(x)));
    if (arr.length > 150) box.appendChild(el('div', 'list-cap', 'แสดง 150 จาก ' + arr.length + ' — พิมพ์ค้นหาเพื่อกรอง'));
  };
  $('#insSearch').addEventListener('input', e => { _insQ = e.target.value; paint(); });
  paint();
}
function insRecRow(x) {
  const isExec = x.src === 'exec';
  const st = _sstk(x.symbol);
  const tap = !!st;
  const b = el(tap ? 'button' : 'div', 'inr');
  const act = x.action === 'buy' ? '<span class="up">▲ ซื้อ</span>' : x.action === 'sell' ? '<span class="down">▼ ขาย</span>' : '<span class="flat">—</span>';
  const who = _esc((isExec ? x.name : x.holder) || '—');
  let detail;
  if (isExec) detail = `${(x.qty || 0).toLocaleString('en-US')} หุ้น${x.price ? ' · ฿' + nf(x.price, 2) : ''}`;
  else detail = x.pct_before != null ? `${nf(x.pct_after, 2)}% ← ${nf(x.pct_before, 2)}%` : '—';
  b.innerHTML = `
    <span class="i-l">
      <span class="i-t"><span class="tk">${x.symbol}</span> <span class="mkt-tag">${isExec ? 'ผู้บริหาร' : 'ผู้ถือหุ้นใหญ่'}</span></span>
      <span class="i-s">${thaiDate(x.trade_date)} · ${who}</span>
    </span>
    <span class="i-r">${act}<span class="i-sub">${detail}</span></span>`;
  if (tap) b.addEventListener('click', () => openDetail('set:' + x.symbol));
  return b;
}

/* ---------------- สัญญาณเงินทุนรวม (Confluence) ---------------- */
const _dirIco = d => d > 0 ? '<span class="up">▲</span>' : d < 0 ? '<span class="down">▼</span>' : '<span class="flat">–</span>';

function cfxView(body) {
  const sig = D.flowSig;
  if (!sig || !Array.isArray(sig.stocks)) { body.appendChild(el('div', 'list-cap', 'ยังไม่มีข้อมูลสัญญาณเงินทุนรวม')); return; }
  body.appendChild(el('div', 'chart-src',
    `${sig.count || sig.stocks.length} หุ้นมีสัญญาณ · อัปเดต ${sig.generated_at || '–'} · ผสานสัญญาณผู้บริหาร + ชอร์ต + ต่างชาติ (NVDR)`));

  const FILT = [['all', 'ทั้งหมด'], ['bull', 'ขาขึ้น'], ['bear', 'ขาลง'], ['triple', 'ครบ 3 ⭐']];
  const fbar = el('div', 'sortbar'); fbar.id = 'cfxFilt';
  fbar.innerHTML = FILT.map(([k, t]) => `<button class="sortchip ${_cfxFilter === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('');
  body.appendChild(fbar);
  const srch = el('div', 'search');
  srch.innerHTML = `<svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="cfxSearch" placeholder="ค้นหาชื่อย่อ">`;
  body.appendChild(srch);
  body.appendChild(el('div', 'cfx-hd', '<span>หุ้น</span><span>ราคา</span><span>คะแนน</span><span>สัญญาณ</span>'));
  const list = el('div'); list.id = 'cfxList'; body.appendChild(list);
  $('#cfxSearch').value = _cfxQ;

  const paint = () => {
    let rows = sig.stocks.slice();
    if (_cfxFilter === 'bull') rows = rows.filter(r => r.score >= 2);
    else if (_cfxFilter === 'bear') rows = rows.filter(r => r.score <= -2);
    else if (_cfxFilter === 'triple') rows = rows.filter(r => r.n_signals === 3);
    const q = _cfxQ.trim().toUpperCase();
    if (q) rows = rows.filter(r => r.symbol.toUpperCase().includes(q));
    rows.sort(_cfxFilter === 'bear'
      ? (a, b) => a.score - b.score || b.n_signals - a.n_signals
      : (a, b) => b.score - a.score || b.n_signals - a.n_signals);
    const box = $('#cfxList'); box.innerHTML = '';
    box.appendChild(el('div', 'note', rows.length + ' หุ้น'));
    rows.slice(0, 300).forEach(r => box.appendChild(cfxRow(r)));
    if (rows.length > 300) box.appendChild(el('div', 'list-cap', 'แสดง 300 จาก ' + rows.length));
  };
  fbar.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    _cfxFilter = b.dataset.k;
    fbar.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    paint();
  });
  $('#cfxSearch').addEventListener('input', e => { _cfxQ = e.target.value; paint(); });
  paint();
}
function cfxRow(r) {
  const st = _sstk(r.symbol);
  const tap = !!st;
  const b = el(tap ? 'button' : 'div', 'cfxr');
  const iv = r.insider || {}, sv = r.short || {}, nv = r.nvdr || {};
  const sc = r.score;
  const scCls = sc >= 2 ? 'up' : sc <= -2 ? 'down' : 'flat';
  const star = r.n_signals === 3 ? ' <span class="warn-star">⭐</span>' : '';
  const parts = [];
  if (iv.net_value_mbaht != null) parts.push(`ผบห ${iv.net_value_mbaht >= 0 ? '+' : ''}${nf(iv.net_value_mbaht, 0)} ลบ.`);
  if (sv.short_pos_pct != null) parts.push(`ชอร์ต ${nf(sv.short_pos_pct, 2)}%`);
  if (nv.nvdr_pct != null) parts.push(`ต่างชาติ ${nf(nv.nvdr_pct, 2)}%`);
  b.innerHTML = `
    <span class="c-l">
      <span class="tkr-lg">${r.symbol}${star}</span>
      <span class="sub2">${st ? (st.sub || shortName(st.name)) : 'ไม่อยู่ในชุดข้อมูลนี้'}</span>
      <span class="c-v2">${parts.join(' · ') || '—'}</span>
    </span>
    <span class="c-px">${st ? nf(st.price, st.price < 1 ? 3 : 2) : '–'}<br><span class="${st ? cls(st.chg1d) : 'flat'}" style="font-size:11px">${st ? pct(st.chg1d) : ''}</span></span>
    <span class="c-sc ${scCls}">${sc > 0 ? '+' : ''}${sc}</span>
    <span class="c-d">${_dirIco(iv.dir)}${_dirIco(sv.dir)}${_dirIco(nv.dir)}</span>`;
  if (tap) b.addEventListener('click', () => openDetail('set:' + r.symbol));
  return b;
}

/* ================================================================
   เฟส 2 ข้อ 7 : สแกนหุ้น (Screener) + สแกนด้วยงบการเงิน (Screener+)
   พอร์ตเงื่อนไข 1:1 จาก dashboard.js runScreener() (+ financial section
   ผ่าน _mergeFinAnalyticsInto) — หน้าตาใหม่แบบชิปกดเลือก ไม่ใช่ฟอร์มกรอกเลข
   ข้อมูล bake พร้อม: set_data.json (D.stocks) + financials_analytics_yahoo.json (D.fin)
   *** งบ Yahoo: QoQ/YoY-Q = รายไตรมาส (~6 งวด) · "เป็นบวกติดกัน" = รายปี
       (compute_positive_streaks บน payload รายปี — เหมือน _patchPosStreakForStatic) ***
   หุ้นไทย (SET+mai) เท่านั้น — เหมือน view tabs หน้ารายชื่อหุ้น
   ================================================================ */
let _scrMode = 'basic';           // 'basic' | 'plus'
let _scrSort = 'rs';
const SCR_DEFAULTS = {
  rsMin: null, stage: null, r1m: null, r3m: null, priceMin: null, priceMax: null,
  capMin: null, dyMin: null, peMax: null, pbvMax: null,
  ema20: false, ema50: false, ema200: false, mkt: 'ALL',
  // งบการเงิน (plus)
  fScore: null, zSafe: false, roeMin: null, gmMin: null, nmMin: null, deMax: null,
  fcfMin: null, growthMin: null, revYoyq: null, profitYoyq: null, profitPos: null, revPos: null,
};
const _scrF = { ...SCR_DEFAULTS };

const SCR_RS_OPTS    = [[null, 'ไม่กรอง'], [50, '50+'], [60, '60+'], [70, '70+'], [80, '80+']];
const SCR_STAGE_OPTS = [[null, 'ไม่กรอง'], [2, '2 · ขาขึ้น'], [1, '1 · สะสมฐาน'], [3, '3 · แจกของ'], [4, '4 · ขาลง']];
const SCR_R1M_OPTS   = [[null, 'ไม่กรอง'], [0, '≥ 0%'], [3, '≥ 3%'], [5, '≥ 5%'], [10, '≥ 10%']];
const SCR_R3M_OPTS   = [[null, 'ไม่กรอง'], [0, '≥ 0%'], [10, '≥ 10%'], [20, '≥ 20%']];
const SCR_CAP_OPTS   = [[null, 'ไม่กรอง'], [1e9, '≥ 1 พันลบ.'], [5e9, '≥ 5 พันลบ.'], [1e10, '≥ 1 หมื่นลบ.'], [5e10, '≥ 5 หมื่นลบ.']];
const SCR_DY_OPTS    = [[null, 'ไม่กรอง'], [2, '≥ 2%'], [3, '≥ 3%'], [5, '≥ 5%']];
const SCR_PE_OPTS    = [[null, 'ไม่กรอง'], [10, '≤ 10'], [15, '≤ 15'], [25, '≤ 25']];
const SCR_PBV_OPTS   = [[null, 'ไม่กรอง'], [1, '≤ 1'], [1.5, '≤ 1.5'], [2, '≤ 2'], [3, '≤ 3']];
const SCR_FSC_OPTS   = [[null, 'ไม่กรอง'], [5, '≥ 5'], [6, '≥ 6'], [7, '≥ 7'], [8, '≥ 8']];
const SCR_ROE_OPTS   = [[null, 'ไม่กรอง'], [5, '≥ 5%'], [10, '≥ 10%'], [15, '≥ 15%'], [20, '≥ 20%']];
const SCR_GM_OPTS    = [[null, 'ไม่กรอง'], [10, '≥ 10%'], [20, '≥ 20%'], [30, '≥ 30%']];
const SCR_NM_OPTS    = [[null, 'ไม่กรอง'], [5, '≥ 5%'], [10, '≥ 10%'], [15, '≥ 15%']];
const SCR_DE_OPTS    = [[null, 'ไม่กรอง'], [0.5, '≤ 0.5'], [1, '≤ 1'], [2, '≤ 2']];
const SCR_FCF_OPTS   = [[null, 'ไม่กรอง'], [0, '≥ 0%'], [3, '≥ 3%'], [5, '≥ 5%']];
const SCR_GRW_OPTS   = [[null, 'ไม่กรอง'], [3, '≥ 3'], [5, '≥ 5'], [7, '≥ 7']];
const SCR_YOYQ_OPTS  = [[null, 'ไม่กรอง'], [0, '≥ 0%'], [10, '≥ 10%'], [25, '≥ 25%']];
const SCR_PYOYQ_OPTS = [[null, 'ไม่กรอง'], [0, '≥ 0%'], [25, '≥ 25%'], [50, '≥ 50%']];
const SCR_POSY_OPTS  = [[null, 'ไม่กรอง'], [2, '≥ 2 ปี'], [3, '≥ 3 ปี'], [4, '≥ 4 ปี']];

const SCR_PRESETS_BASIC = [
  ['เริ่มโมเมนตัม', { rsMin: 50, r1m: 3, ema50: true }],
  ['โมเมนตัมแรง', { rsMin: 70, r1m: 5, r3m: 10, ema50: true, ema200: true }],
  ['ปันผลสูง + RS', { rsMin: 60, dyMin: 3 }],
  ['P/BV ต่ำ ฟื้นตัว', { pbvMax: 1.5, rsMin: 50, r1m: 0 }],
];
const SCR_PRESETS_PLUS = [
  ['CANSLIM', { rsMin: 80, ema200: true, profitYoyq: 25, revYoyq: 0 }],
  ['คุณภาพ ROE สูง', { roeMin: 15, nmMin: 10, deMax: 1 }],
  ['งบแข็งแรง', { fScore: 7, zSafe: true, deMax: 1 }],
  ['กำไรโตเด่น', { profitYoyq: 25, revYoyq: 10, growthMin: 5 }],
];

const SCR_SORTS_BASIC = [['rs', 'RS'], ['pct', '% วันนี้'], ['ret_1m', '% 1 เดือน'], ['cap', 'มูลค่าตลาด'], ['dy', 'ปันผล']];
const SCR_SORTS_PLUS  = [['rs', 'RS'], ['fscore', 'F-Score'], ['roe', 'ROE'], ['revyoy', 'รายได้ YoY'], ['cap', 'มูลค่าตลาด']];
const _sfin = sym => D.fin[sym] || {};
const SCR_SORTF = {
  rs:     (a, b) => (b.rs ?? -1) - (a.rs ?? -1),
  pct:    (a, b) => (b.chg1d ?? -999) - (a.chg1d ?? -999),
  ret_1m: (a, b) => (b.ret_1m ?? -999) - (a.ret_1m ?? -999),
  cap:    (a, b) => (b.mkt_cap ?? 0) - (a.mkt_cap ?? 0),
  dy:     (a, b) => ((b.raw && b.raw.div_yield) ?? -1) - ((a.raw && a.raw.div_yield) ?? -1),
  fscore: (a, b) => (_sfin(b.symbol).f_score ?? -1) - (_sfin(a.symbol).f_score ?? -1),
  roe:    (a, b) => (_sfin(b.symbol).roe ?? -999) - (_sfin(a.symbol).roe ?? -999),
  revyoy: (a, b) => (_sfin(b.symbol).rev_yoy_q ?? -999) - (_sfin(a.symbol).rev_yoy_q ?? -999),
};

// เงื่อนไขตรงกับ runScreener() ของ dashboard.js — ตัวกรองที่ไม่ได้ตั้ง (null/false) ข้ามไป
// ค่างบที่เป็น null ถูกตัดออกเมื่อเปิดตัวกรองนั้น ๆ (เหมือน `s.xxx == null || s.xxx < min`)
function scrPass(r) {
  const f = _scrF, raw = r.raw;
  if (f.rsMin != null && (r.rs ?? -1) < f.rsMin) return false;
  if (f.stage != null && raw.stage !== f.stage) return false;
  if (f.r1m != null && (r.ret_1m ?? -Infinity) < f.r1m) return false;
  if (f.r3m != null && (r.ret_3m ?? -Infinity) < f.r3m) return false;
  if (f.priceMin != null && (r.price ?? 0) < f.priceMin) return false;
  if (f.priceMax != null && f.priceMax > 0 && (r.price ?? 0) > f.priceMax) return false;
  if (f.capMin != null && (!r.mkt_cap || r.mkt_cap < f.capMin)) return false;
  if (f.dyMin != null && (raw.div_yield == null || raw.div_yield < f.dyMin)) return false;
  if (f.peMax != null && (raw.pe == null || raw.pe <= 0 || raw.pe > f.peMax)) return false;
  if (f.pbvMax != null && (raw.pbv == null || raw.pbv <= 0 || raw.pbv > f.pbvMax)) return false;
  if (f.ema20 && !r.above_ema20) return false;
  if (f.ema50 && !r.above_ema50) return false;
  if (f.ema200 && !r.above_ema200) return false;
  if (f.mkt !== 'ALL' && raw.market !== f.mkt) return false;
  if (_scrMode === 'plus') {
    const a = D.fin[r.symbol];
    if (f.fScore != null    && !(a && a.f_score    != null && a.f_score    >= f.fScore))    return false;
    if (f.zSafe             && !(a && a.z_zone === 'safe'))                                  return false;
    if (f.roeMin != null    && !(a && a.roe        != null && a.roe        >= f.roeMin))    return false;
    if (f.gmMin != null     && !(a && a.gross_margin != null && a.gross_margin >= f.gmMin)) return false;
    if (f.nmMin != null     && !(a && a.net_margin != null && a.net_margin >= f.nmMin))     return false;
    if (f.deMax != null     && !(a && a.de_ratio   != null && a.de_ratio   <= f.deMax))     return false;
    if (f.fcfMin != null    && !(a && a.fcf_yield  != null && a.fcf_yield  >= f.fcfMin))    return false;
    if (f.growthMin != null && !(a && a.growth_score != null && a.growth_score >= f.growthMin)) return false;
    if (f.revYoyq != null    && !(a && a.rev_yoy_q    != null && a.rev_yoy_q    >= f.revYoyq))    return false;
    if (f.profitYoyq != null && !(a && a.profit_yoy_q != null && a.profit_yoy_q >= f.profitYoyq)) return false;
    if (f.profitPos != null  && (( a && a.profit_pos_streak_q) ?? -1) < f.profitPos)         return false;
    if (f.revPos != null     && (( a && a.rev_pos_streak_q)    ?? -1) < f.revPos)            return false;
  }
  return true;
}

function scrClearF() {
  Object.assign(_scrF, SCR_DEFAULTS);
}
function scrApplyPreset(obj) { scrClearF(); Object.assign(_scrF, obj); renderScreener(); }

const _scrGrp = (label, key, opts) => `<div class="scr-f"><div class="scr-fl">${label}</div>
  <div class="sortbar" data-scrkey="${key}">${opts.map(([v, t]) =>
    `<button class="sortchip ${(_scrF[key] ?? null) === v ? 'on' : ''}" data-v="${v == null ? '' : v}">${t}</button>`).join('')}</div></div>`;

function renderScreener() {
  const c = $('#s-screener'); if (!c) return;
  c.innerHTML = '';
  const wrap = el('div');   // handler ผูกกับ wrap ที่สร้างใหม่ทุกครั้ง — ไม่สะสม listener บน #s-screener
  const presets = _scrMode === 'plus' ? SCR_PRESETS_PLUS : SCR_PRESETS_BASIC;
  const sorts   = _scrMode === 'plus' ? SCR_SORTS_PLUS   : SCR_SORTS_BASIC;
  if (!sorts.some(([k]) => k === _scrSort)) _scrSort = 'rs';   // กัน sort ค้างข้ามโหมด

  let h = `<div class="seg">
      <button data-mode="basic" class="${_scrMode === 'basic' ? 'on' : ''}">พื้นฐาน</button>
      <button data-mode="plus" class="${_scrMode === 'plus' ? 'on' : ''}">+ งบการเงิน</button>
    </div>
    <div class="sortbar">${presets.map(([n], i) => `<button class="sortchip" data-preset="${i}">${n}</button>`).join('')}<button class="sortchip" id="scrClear">ล้างตัวกรอง</button></div>`;

  h += _scrGrp('RS ขั้นต่ำ', 'rsMin', SCR_RS_OPTS)
    + _scrGrp('สเตจ (Weinstein)', 'stage', SCR_STAGE_OPTS)
    + _scrGrp('ผลตอบแทน 1 เดือน', 'r1m', SCR_R1M_OPTS)
    + _scrGrp('ผลตอบแทน 3 เดือน', 'r3m', SCR_R3M_OPTS)
    + `<div class="scr-f"><div class="scr-fl">ช่วงราคา (บาท)</div><div class="scr-price">
        <input id="scrPMin" inputmode="decimal" placeholder="ต่ำสุด"><input id="scrPMax" inputmode="decimal" placeholder="สูงสุด"></div></div>`
    + _scrGrp('มูลค่าตลาดขั้นต่ำ', 'capMin', SCR_CAP_OPTS)
    + _scrGrp('เงินปันผลขั้นต่ำ', 'dyMin', SCR_DY_OPTS)
    + _scrGrp('P/E ไม่เกิน', 'peMax', SCR_PE_OPTS)
    + _scrGrp('P/BV ไม่เกิน', 'pbvMax', SCR_PBV_OPTS)
    + `<div class="scr-f"><div class="scr-fl">ราคายืนเหนือเส้นค่าเฉลี่ย</div><div class="sortbar" id="scrEma">
        ${[['ema20', 'EMA20'], ['ema50', 'EMA50'], ['ema200', 'EMA200']].map(([k, t]) =>
          `<button class="sortchip ${_scrF[k] ? 'on' : ''}" data-ema="${k}">${t}</button>`).join('')}</div></div>`
    + `<div class="scr-f"><div class="scr-fl">ตลาด</div><div class="seg" id="scrMkt">
        ${[['ALL', 'ทั้งหมด'], ['SET', 'SET'], ['mai', 'mai']].map(([v, t]) =>
          `<button data-mkt="${v}" class="${_scrF.mkt === v ? 'on' : ''}">${t}</button>`).join('')}</div></div>`;

  if (_scrMode === 'plus') {
    h += `<div class="scr-fl scr-fgrp">ตัวกรองงบการเงิน</div>`
      + _scrGrp('Piotroski F-Score', 'fScore', SCR_FSC_OPTS)
      + `<div class="scr-f"><div class="scr-fl">Altman Z-Score</div><div class="sortbar">
          <button class="sortchip ${_scrF.zSafe ? 'on' : ''}" id="scrZSafe">เฉพาะโซนปลอดภัย</button></div></div>`
      + _scrGrp('ROE ขั้นต่ำ', 'roeMin', SCR_ROE_OPTS)
      + _scrGrp('อัตรากำไรขั้นต้นขั้นต่ำ', 'gmMin', SCR_GM_OPTS)
      + _scrGrp('อัตรากำไรสุทธิขั้นต่ำ', 'nmMin', SCR_NM_OPTS)
      + _scrGrp('D/E ไม่เกิน', 'deMax', SCR_DE_OPTS)
      + _scrGrp('FCF Yield ขั้นต่ำ', 'fcfMin', SCR_FCF_OPTS)
      + _scrGrp('Growth Score ขั้นต่ำ', 'growthMin', SCR_GRW_OPTS)
      + _scrGrp('รายได้ YoY (ไตรมาสล่าสุด)', 'revYoyq', SCR_YOYQ_OPTS)
      + _scrGrp('กำไรสุทธิ YoY (ไตรมาสล่าสุด)', 'profitYoyq', SCR_PYOYQ_OPTS)
      + _scrGrp('กำไรเป็นบวกติดกัน', 'profitPos', SCR_POSY_OPTS)
      + _scrGrp('รายได้เป็นบวกติดกัน', 'revPos', SCR_POSY_OPTS);
  }
  wrap.innerHTML = h;

  if (_scrMode === 'plus' && (!D.fin || !Object.keys(D.fin).length))
    wrap.appendChild(el('div', 'note', '⚠ ยังโหลดข้อมูลงบการเงินไม่ได้ — ตัวกรองงบจะไม่มีผลจนกว่าจะรีเฟรช'));

  wrap.appendChild(secHead('ผลการสแกน'));
  const sbar = el('div', 'sortbar'); sbar.id = 'scrSortBar';
  sbar.innerHTML = sorts.map(([k, t]) => `<button class="sortchip ${_scrSort === k ? 'on' : ''}" data-sort="${k}">${t}</button>`).join('');
  wrap.appendChild(sbar);
  const res = el('div'); res.id = 'scrResults'; wrap.appendChild(res);
  c.appendChild(wrap);

  const pmin = $('#scrPMin'), pmax = $('#scrPMax');
  pmin.value = _scrF.priceMin ?? ''; pmax.value = _scrF.priceMax ?? '';
  let _pxT;
  const pxDeb = () => { clearTimeout(_pxT); _pxT = setTimeout(applyScr, 160); };
  pmin.addEventListener('input', e => { _scrF.priceMin = parseFloat(e.target.value) || null; pxDeb(); });
  pmax.addEventListener('input', e => { _scrF.priceMax = parseFloat(e.target.value) || null; pxDeb(); });

  const sibsOn = b => [...b.parentElement.children].forEach(x => x.classList && x.classList.toggle('on', x === b));
  wrap.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    if (b.dataset.mode) { if (_scrMode !== b.dataset.mode) { _scrMode = b.dataset.mode; _scrSort = 'rs'; renderScreener(); } return; }
    if (b.dataset.preset != null) { scrApplyPreset(presets[+b.dataset.preset][1]); return; }
    if (b.id === 'scrClear') { scrClearF(); renderScreener(); return; }
    if (b.dataset.ema) { _scrF[b.dataset.ema] = !_scrF[b.dataset.ema]; b.classList.toggle('on'); applyScr(); return; }
    if (b.dataset.mkt != null) { _scrF.mkt = b.dataset.mkt; sibsOn(b); applyScr(); return; }
    if (b.id === 'scrZSafe') { _scrF.zSafe = !_scrF.zSafe; b.classList.toggle('on'); applyScr(); return; }
    if (b.dataset.sort) { _scrSort = b.dataset.sort; sibsOn(b); applyScr(); return; }
    const bar = b.closest('.sortbar[data-scrkey]');
    if (bar) {
      const raw = b.dataset.v;
      _scrF[bar.dataset.scrkey] = raw === '' ? null : +raw;
      sibsOn(b);
      applyScr();
    }
  });

  applyScr();
}

function _scrBasicMetric() {
  return ({ rs: 'rs', cap: 'cap', ret_1m: '1m', dy: 'dy', pct: 'pct' })[_scrSort] || 'pct';
}

function applyScr() {
  const box = $('#scrResults'); if (!box) return;
  const arr = D.stocks.filter(scrPass).sort(SCR_SORTF[_scrSort] || SCR_SORTF.rs);
  box.innerHTML = '';
  box.appendChild(el('div', 'note', arr.length + ' หุ้นตรงเงื่อนไข'
    + (_scrMode === 'plus' ? ' · เรียงจากมากไปน้อย' : '')));
  if (!arr.length) {
    box.appendChild(el('div', 'list-cap', 'ไม่พบหุ้นที่ตรงเงื่อนไข — ลองผ่อนตัวกรองลง'));
    $('#scrSub').textContent = '0 หุ้นตรงเงื่อนไข · ' + thaiDate(D.asOf);
    return;
  }
  const MAX = 200;
  const metric = _scrBasicMetric();
  arr.slice(0, MAX).forEach(r => box.appendChild(_scrMode === 'plus' ? scrPlusRow(r) : listRow(r, metric)));
  if (arr.length > MAX) box.appendChild(el('div', 'list-cap', `แสดง ${MAX} จาก ${arr.length} — ใส่ตัวกรองเพิ่มเพื่อแคบผล`));
  $('#scrSub').textContent = arr.length + ' หุ้นตรงเงื่อนไข · ' + thaiDate(D.asOf);
}

function scrPlusRow(r) {
  const a = D.fin[r.symbol] || {};
  const b = el('button', 'scrr');
  const fCls = a.f_score == null ? 'flat' : a.f_score >= 7 ? 'up' : a.f_score >= 4 ? 'flat' : 'down';
  const zTxt = a.z_zone === 'safe' ? 'ปลอดภัย' : a.z_zone === 'grey' ? 'เฝ้าระวัง' : a.z_zone === 'distress' ? 'เสี่ยง' : '–';
  const zCls = a.z_zone === 'safe' ? 'up' : a.z_zone === 'distress' ? 'down' : 'flat';
  const fin = [
    `<b class="${fCls}">F ${a.f_score ?? '–'}/${a.f_score_max ?? 9}</b>`,
    `<b class="${zCls}">Z ${zTxt}</b>`,
    `<b class="${a.roe >= 10 ? 'up' : 'flat'}">ROE ${a.roe == null ? '–' : nf(a.roe, 0) + '%'}</b>`,
    `<b class="${cls(a.rev_yoy_q)}">รายได้ ${a.rev_yoy_q == null ? '–' : pct(a.rev_yoy_q, 0)}</b>`,
    `<b class="${cls(a.profit_yoy_q)}">กำไร ${a.profit_yoy_q == null ? '–' : pct(a.profit_yoy_q, 0)}</b>`,
  ].join('');
  b.innerHTML = `
    <span class="sc-l">
      <span class="sc-t"><span class="tkr-lg">${r.symbol}</span><span class="mkt-tag">${r.tag}</span></span>
      <span class="sc-sub">${shortName(r.name)}${r.sub ? ' · ' + r.sub : ''}</span>
      <span class="sc-fin">${fin}</span>
    </span>
    <span class="sc-r">
      <span class="sc-px">${nf(r.price, r.price < 1 ? 3 : 2)}</span>
      <span class="sc-ch ${cls(r.chg1d)}">${pct(r.chg1d)}</span>
    </span>`;
  b.addEventListener('click', () => openDetail(r.id));
  return b;
}

/* ---------------- start ---------------- */
boot();
