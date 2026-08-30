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
function fmtCap(b) { if (b == null) return '–'; const m = b / 1e6; if (m >= 1e6) return nf(m / 1e6, 2) + ' ล้านล้าน'; if (m >= 1e3) return nf(m / 1e3, 1) + ' พันล้าน'; return nf(m, 0) + ' ล้าน'; }
function fmtCapShort(b) { if (b == null) return '–'; const m = b / 1e6; if (m >= 1e6) return nf(m / 1e6, 2) + ' ลลบ.'; return nf(m, 0) + ' ลบ.'; }
function fmtBaht(v) {
  if (v == null || isNaN(v)) return '–';
  const s = v < 0 ? '-' : '', a = Math.abs(v);
  if (a >= 1e12) return s + nf(a / 1e12, 2) + ' ล้านล้าน';
  if (a >= 1e9) return s + nf(a / 1e9, a >= 1e10 ? 0 : 1) + ' พันล้าน';
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
  rows: [],        // all three concatenated
  byId: {},        // 'set:PTT' -> row
  fin: {},         // financials_analytics_yahoo .set  (symbol -> obj)
  finDr: {},       // .dr
  finQ: {},        // financials_quarterly .set (symbol -> {q, revenue, gross_profit, op_profit, net_profit})
  watchlist: [],   // ['AAV', ...]
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
function calcFGI(stocks) {
  const n = stocks.length;
  if (!n) return { score: 50, c1: 50, c2: 50, c3: 50, c4: 50, c5: 50 };
  const c1 = stocks.filter(s => s.above_ema50).length / n * 100;
  const c2 = stocks.filter(s => (s.ret_1w || 0) > 0).length / n * 100;
  const c3 = stocks.filter(s => s.above_ema200).length / n * 100;
  const c4 = stocks.filter(s => (s.ret_3m || 0) > 0).length / n * 100;
  const w1m = stocks.filter(s => s.ret_1m != null);
  const avg1m = w1m.length ? w1m.reduce((a, s) => a + s.ret_1m, 0) / w1m.length : 0;
  const c5 = Math.max(0, Math.min(100, (avg1m + 15) / 30 * 100));
  const score = Math.round((c1 + c2 + c3 + c4 + c5) / 5);
  return { score, c1, c2, c3, c4, c5 };
}
function calcRegime(stocks) {
  const n = stocks.length || 1;
  const pct200 = stocks.filter(s => s.above_ema200).length / n * 100;
  const pct50 = stocks.filter(s => s.above_ema50).length / n * 100;
  const pos3m = stocks.filter(s => (s.ret_3m || 0) > 0).length / n * 100;
  const pos1m = stocks.filter(s => (s.ret_1m || 0) > 0).length / n * 100;
  return Math.min(100, Math.round(pct200 * 0.35 + pct50 * 0.25 + pos3m * 0.25 + pos1m * 0.15));
}
function computeMarket() {
  const st = D.stocks, n = st.length || 1;
  const p = f => Math.round(st.filter(f).length / n * 100);
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
    ema20: p(s => s.above_ema20), ema50: p(s => s.above_ema50), ema200: p(s => s.above_ema200),
    avg1d,
    rs80: st.filter(s => (s.rs || 0) >= 80).length,
    nHigh, nLow,
    fgi: calcFGI(st), regime: calcRegime(st),
    sectors,
  };
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

  bmsg.textContent = 'กำลังโหลด DR / ETF / งบ…';
  const [dr, etf, fin, finQ, wl] = await Promise.allSettled([
    loadJSON('../data/dr_data.json', 30000),
    loadJSON('../data/etf_data.json', 30000),
    loadJSON('../data/financials_analytics_yahoo.json', 30000),
    loadJSON('../data/financials_quarterly.json', 30000),
    loadJSON('../data/watchlist.json', 15000),
  ]);
  const missing = [];
  if (dr.status === 'fulfilled') D.dr = (dr.value.stocks || []).map(rowDr); else missing.push('DR');
  if (etf.status === 'fulfilled') D.etf = (etf.value.stocks || []).map(rowEtf); else missing.push('ETF');
  if (fin.status === 'fulfilled') { D.fin = fin.value.set || {}; D.finDr = fin.value.dr || {}; } else missing.push('งบการเงิน');
  if (finQ.status === 'fulfilled') D.finQ = finQ.value.set || {};
  if (wl.status === 'fulfilled' && Array.isArray(wl.value)) D.watchlist = wl.value;

  D.rows = [...D.stocks, ...D.dr, ...D.etf];
  D.rows.forEach(r => { D.byId[r.id] = r; });
  D.mkt = computeMarket();

  $('#boot').classList.add('hidden');
  $('#app').hidden = false;
  wireNav();
  renderMarket(); renderStocks(); renderWatch(); renderRotation(); renderMore();
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
  rotation: () => ['การหมุนเวียนกลุ่ม', 'ผลตอบแทน 1 เดือน ตามกลุ่มอุตสาหกรรม'],
  more: () => ['เพิ่มเติม', 'เครื่องมือทั้งหมด'],
};
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
function go(scr) {
  curScreen = scr;
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  $('#s-' + scr).classList.add('active');
  document.querySelectorAll('.tabbar button').forEach(b => b.classList.toggle('on', b.dataset.scr === scr));
  $('#backBtn').hidden = scr !== 'detail';
  setScrHeader(scr);
  window.scrollTo(0, 0);
}
function openDetail(id) {
  if (!D.byId[id]) return;
  curId = id; detailFrom = curScreen;
  renderDetail();
  go('detail');
}
function wireNav() {
  $('#tabbar').addEventListener('click', e => { const b = e.target.closest('button'); if (b) go(b.dataset.scr); });
  $('#backBtn').addEventListener('click', () => go(detailFrom));
  $('#infoBtn').addEventListener('click', () => { $('#scrim').classList.add('open'); $('#sheet').classList.add('open'); });
  $('#scrim').addEventListener('click', () => { $('#scrim').classList.remove('open'); $('#sheet').classList.remove('open'); });
}

/* ---------------- row component ---------------- */
function listRow(r, metric = 'pct') {
  const b = el('button', 'row');
  let sub;
  if (metric === 'rs') sub = `<span class="dl flat">RS ${r.rs ?? '–'}</span>`;
  else if (metric === 'cap') sub = `<span class="dl flat">${fmtCapShort(r.mkt_cap)}</span>`;
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

  // 52w high / low
  const hl = el('div');
  hl.appendChild(secHead('จุดสูง–ต่ำ 52 สัปดาห์ วันนี้'));
  hl.appendChild(statRow('ทำจุดสูงใหม่', `<span class="up">${k.nHigh} ตัว</span>`));
  hl.appendChild(statRow('ทำจุดต่ำใหม่', `<span class="down">${k.nLow} ตัว</span>`));
  m.appendChild(hl);

  // top movers
  const mv = el('div');
  const head = el('div', 'sec');
  head.innerHTML = `<div class="sec-head"><span class="sec-label">หุ้นเคลื่อนไหวมากสุดวันนี้</span></div>
    <div class="seg" id="mvSeg"><button class="on" data-k="up">ขึ้นแรง</button><button data-k="down">ลงแรง</button></div>`;
  mv.appendChild(head);
  const mvList = el('div'); mvList.id = 'mvList';
  mv.appendChild(mvList); m.appendChild(mv);
  const paint = kk => {
    mvList.innerHTML = '';
    const arr = D.stocks.filter(s => s.chg1d != null && s.price > 0.5)
      .sort((a, b) => kk === 'up' ? b.chg1d - a.chg1d : a.chg1d - b.chg1d).slice(0, 8);
    arr.forEach(s => mvList.appendChild(listRow(s)));
  };
  paint('up');
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
let listFilter = 'all', listSort = 'cap', listQuery = '';
const FILTERS = [['all', 'ทั้งหมด'], ['SET', 'SET'], ['mai', 'mai'], ['DR', 'DR'], ['ETF', 'ETF']];
function matchesFilter(r) {
  if (listFilter === 'all') return true;
  if (listFilter === 'DR') return r.kind === 'dr';
  if (listFilter === 'ETF') return r.kind === 'etf';
  return r.tag === listFilter; // SET | mai
}
function renderStocks() {
  const c = $('#s-stocks'); c.innerHTML = '';
  c.innerHTML = `
    <div class="search"><svg class="mag" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg><input id="stkSearch" placeholder="ค้นหาชื่อย่อ / ชื่อบริษัท"></div>
    <div class="seg" id="stkFilter">${FILTERS.map(([k, t]) => `<button data-k="${k}" class="${listFilter === k ? 'on' : ''}">${t}</button>`).join('')}</div>
    <div class="sortbar" id="stkSort">${[['cap', 'มูลค่าตลาด'], ['pct', '% วันนี้'], ['ret_1m', '% 1 เดือน'], ['rs', 'RS'], ['az', 'ก–ฮ']]
      .map(([k, t]) => `<button class="sortchip ${listSort === k ? 'on' : ''}" data-k="${k}">${t}</button>`).join('')}</div>
    <div id="stkList"></div>`;
  $('#stkSearch').value = listQuery;
  const S = {
    cap: (a, b) => (b.mkt_cap || 0) - (a.mkt_cap || 0),
    pct: (a, b) => (b.chg1d ?? -999) - (a.chg1d ?? -999),
    ret_1m: (a, b) => (b.ret_1m ?? -999) - (a.ret_1m ?? -999),
    rs: (a, b) => (b.rs ?? -1) - (a.rs ?? -1),
    az: (a, b) => a.symbol < b.symbol ? -1 : 1,
  };
  const paint = () => {
    const q = listQuery.trim().toLowerCase();
    let arr = D.rows.filter(r => {
      if (!matchesFilter(r)) return false;
      if (q) return r.symbol.toLowerCase().includes(q) || String(r.name).toLowerCase().includes(q);
      return true;
    }).sort(S[listSort]);
    const box = $('#stkList'); box.innerHTML = '';
    if (!arr.length) { box.appendChild(el('div', 'list-cap', q ? `ไม่พบหลักทรัพย์ที่ตรงกับ “${listQuery}”` : 'ไม่มีข้อมูล')); return; }
    const metric = listSort === 'rs' ? 'rs' : listSort === 'cap' ? 'cap' : listSort === 'ret_1m' ? '1m' : 'pct';
    const MAX = 400;
    arr.slice(0, MAX).forEach(r => box.appendChild(listRow(r, metric)));
    if (arr.length > MAX) box.appendChild(el('div', 'list-cap', `แสดง ${MAX} จาก ${arr.length} — พิมพ์ค้นหาเพื่อกรอง`));
    $('#scrSub').textContent = arr.length + ' หลักทรัพย์ · ปิดตลาด ' + thaiDate(D.asOf);
  };
  $('#stkSearch').addEventListener('input', e => { listQuery = e.target.value; paint(); });
  $('#stkFilter').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    listFilter = b.dataset.k;
    $('#stkFilter').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
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

/* ---------------- ROTATION ---------------- */
function renderRotation() {
  const c = $('#s-rotation'); c.innerHTML = '';
  const secs = D.mkt.sectors;
  const rowF = ([name, v, cnt]) => {
    const r = el('div', 'rot-row');
    const w = Math.min(48, Math.abs(v) * 1.7 + 1);
    r.innerHTML = `
      <span class="rl"><span class="rn">${name}</span><span class="rc">${cnt} บริษัท</span></span>
      <span class="dv"><span class="mid"></span><span class="seg ${v >= 0 ? 'g' : 'r'}" style="width:${w}%"></span></span>
      <span class="rv ${cls(v)}">${pct(v, 1)}</span>`;
    return r;
  };
  c.appendChild(secHead('นำตลาด'));
  secs.filter(s => s[1] > 0).forEach(s => c.appendChild(rowF(s)));
  c.appendChild(secHead('ตามหลังตลาด'));
  secs.filter(s => s[1] <= 0).forEach(s => c.appendChild(rowF(s)));
}

/* ---------------- MORE (stub) ---------------- */
function renderMore() {
  const c = $('#s-more'); c.innerHTML = '';
  const groups = [
    ['ภาพรวม & สแกน', [['📈', 'แนวโน้ม / Rotation'], ['🗂', 'ดัชนีกลุ่ม'], ['🔬', 'Screener+ (กรองด้วยงบ)'], ['🌡', 'Heatmap'], ['🎯', 'สัญญาณเงินทุนรวม']]],
    ['ราคา & การซื้อขาย', [['💧', 'Capital Flow'], ['📉', 'Short Sales'], ['⚖️', 'Valuation ทั้งตลาด']]],
    ['รายตัว', [['👤', 'Insider / ผู้ถือหุ้นใหญ่']]],
  ];
  groups.forEach(([g, items]) => {
    c.appendChild(secHead(g));
    items.forEach(([ic, t]) => {
      const b = el('button', 'mitem');
      b.innerHTML = `<span class="mi-ic">${ic}</span><span class="mi-t">${t}</span><span class="chev">›</span>`;
      b.addEventListener('click', () => toast('เมนูนี้มีเฉพาะเวอร์ชันรันบนเครื่อง'));
      c.appendChild(b);
    });
  });
  c.appendChild(el('div', 'note', 'เมนูที่ต้องดึงข้อมูลสด (Screener กรองด้วยงบ, DCF/Fair-value ทั้งตลาด, หุ้นต่างประเทศ, ข่าวรายตัว) มีเฉพาะเวอร์ชันรันบนเครื่อง'));
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

function statRow(k, v, vClass) {
  const r = el('div', 'stat');
  r.innerHTML = `<span class="k">${k}</span><span class="v ${vClass || ''}">${v}</span>`;
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
  out.push(statRow('มูลค่าตลาด', fmtCap(r.mkt_cap)));
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
    statRow('กำไรเป็นบวกติดกัน', (f.profit_pos_streak_q ?? 0) + ' ไตรมาส', f.profit_pos_streak_q >= 4 ? 'up' : ''),
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
  out.push(el('div', 'chart-src', `รายได้รายไตรมาส ${show} งวดล่าสุด · ที่มา ตลาดหลักทรัพย์ฯ`));
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
  const rows = Math.min(n, 8);
  const tbl = el('div', 'qtbl');
  tbl.innerHTML = `<div class="qtr qhd"><span>ไตรมาส</span><span>รายได้</span><span>กำไรสุทธิ</span><span>มาร์จิ้น</span></div>` +
    q.q.slice(-rows).map((lab, i) => {
      const idx = n - rows + i;
      const rv = rev[idx], nv = np[idx];
      const mg = (rv && nv != null) ? nv / rv * 100 : null;
      return `<div class="qtr">
        <span class="qp">${beQ(lab)}</span>
        <span>${fmtBaht(rv)}</span>
        <span class="${cls(nv)}">${fmtBaht(nv)}</span>
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
  x.textAlign = 'left';
  x.fillStyle = '#8b95a1'; x.fillText(fmtBaht(mx), w - rpad + 5, padT + 2);
  if (nums[n - 1] < mx * 0.9) {
    const ly = padT + plotH - Math.max(1, nums[n - 1] / mx * plotH);
    x.fillStyle = '#4493f8';
    x.fillText(fmtBaht(nums[n - 1]), w - rpad + 5, Math.min(h - padB - 4, ly + 4));
  }
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

let _rz;
window.addEventListener('resize', () => {
  clearTimeout(_rz);
  _rz = setTimeout(() => {
    if (curScreen === 'detail') {
      const r = D.byId[curId];
      if (r && r.ph) drawSpark($('#dChart'), sliceHistory(r.ph, Object.keys(TFRET).includes(dTf) ? dTf : '6M', r.price));
    }
  }, 120);
});

/* ---------------- start ---------------- */
boot();
