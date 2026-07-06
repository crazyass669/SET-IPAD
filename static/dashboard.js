
// ============================================================
// STATE
// ============================================================
let DATA = null;
let watchlist = (() => { try { return JSON.parse(localStorage.getItem("set_wl") || "[]"); } catch { return []; } })();
let stockSort = { key: "rs_score", dir: -1 };
let sectorSort = { key: "ret_1m", dir: -1 };
let rotView = "sector";
let rotTimeframe = 'long';  // 'long'=3M/1M, 'short'=1M/1W
let sectorView = "sector";
let rsFilter = "ALL";
let industryFilter = "ALL";
let sectorFilter = "ALL";
let stageFilter = "ALL";
let stockSearch = "";
let dqOnlyFilter = false;

// ============================================================
// STATIC MODE — รันบน GitHub Pages (ไม่มี Flask server)
// ข้อมูลทั้งหมดถูก precompute เป็นไฟล์ใน data/ โดย GitHub Actions
// (ดู run_static_update.py) — override fetch ให้ /api/* ชี้ไปไฟล์ static
// ============================================================
const IS_STATIC = location.hostname.endsWith('github.io')
  || location.protocol === 'file:'
  || new URLSearchParams(location.search).has('static');

if (IS_STATIC) {
  document.addEventListener('DOMContentLoaded', () => document.body.classList.add('static-mode'));

  const STATIC_MAP = {
    '/api/data':                  'data/set_data.json',
    '/api/indices':               'data/indices_data.json',
    '/api/dr':                    'data/dr_data.json',
    '/api/nvdr':                  'data/nvdr_data.json',
    '/api/short-sales':           'data/short_sales.json',
    '/api/market-stats':          'data/market_stats.json',
    '/api/market-stats-meta':     'data/market_stats_meta.json',
    '/api/market-flow':           'data/market_flow.json',
    '/api/market-internals':      'data/market_internals.json',
    '/api/rotation-alerts':       'data/rotation_alerts.json',
    '/api/stock-valuation-stats': 'data/stock_valuation_stats.json',
    '/api/insider-trades':        'data/insider_trades.json',
    '/api/major-changes':         'data/major_changes.json',
    '/api/prices':                'data/prices.json',
  };

  function _staticURL(url) {
    const [path, query] = url.split('?');
    if (path === '/api/breadth') {
      const m = /range=(\w+)/.exec(query || '');
      return `data/breadth_${m ? m[1] : '1y'}.json`;
    }
    return STATIC_MAP[path] || null;
  }

  const _origFetch = window.fetch.bind(window);
  window.fetch = function (url, opts) {
    if (typeof url !== 'string' || !url.startsWith('/api/')) return _origFetch(url, opts);
    const mapped = _staticURL(url);
    if (mapped) return _origFetch(mapped);
    if (url.startsWith('/api/status')) {
      return Promise.resolve(new Response('{"running":false}', { headers: { 'Content-Type': 'application/json' } }));
    }
    // endpoint ที่ต้องมี server จริง (quick-update, history ยาว, งบการเงินสด ฯลฯ)
    return Promise.resolve(new Response(
      '{"error":"ฟีเจอร์นี้ใช้ได้เฉพาะเวอร์ชันรันบนเครื่อง (Flask) — เวอร์ชันเว็บอัปเดตข้อมูลอัตโนมัติวันละ 2 รอบ"}',
      { status: 404, headers: { 'Content-Type': 'application/json' } }));
  };
}

// ============================================================
// LOAD DATA — ดึงจาก Flask /api/data
// ============================================================
async function loadData() {
  try {
    // ไม่ใส่ cache-buster — server มี ETag แล้ว ถ้าข้อมูลไม่เปลี่ยนจะได้ 304 (โหลดเร็วมาก)
    const r = await fetch("/api/data");
    if (!r.ok) {
      document.getElementById("updated-at").textContent = "ยังไม่มีข้อมูล — กด Refresh";
      return;
    }
    DATA = await r.json();
    DATA.stocks.forEach(s => {
      if (!s.sector || s.sector === '-') s.sector = s.market === 'mai' ? 'MAI' : 'Unknown';
      // คำนวณ ret_1y จาก price_history ถ้า null (ใช้แท่ง -260 = ~1 ปี ไม่ใช่ bar แรกสุด)
      if (s.ret_1y == null && s.price_history && s.price_history.length >= 260) {
        const idx1y = Math.max(0, s.price_history.length - 260);
        const first = s.price_history[idx1y][1];
        if (first > 0) s.ret_1y = +((s.price - first) / first * 100).toFixed(2);
      }
    });
    _enrichTechSignals(DATA.stocks);
    DATA.sectors.forEach(s => {
      if (!s.name || s.name === '-') s.name = 'MAI';
    });
    // คำนวณ ret_1y เฉลี่ยของแต่ละ group ใหม่
    ['sectors','industries'].forEach(key => {
      const groupKey = key === 'sectors' ? 'sector' : 'industry';
      DATA[key].forEach(g => {
        if (g.ret_1y == null) {
          const vals = DATA.stocks.filter(s => s[groupKey] === g.name && s.ret_1y != null).map(s => s.ret_1y);
          // เคารพกฎเดียวกับ backend (G3): กลุ่มต้องมีข้อมูล >= 3 ตัวถึงจะเฉลี่ย
          g.ret_1y = vals.length >= 3 ? +(vals.reduce((a,b)=>a+b,0)/vals.length).toFixed(2) : null;
        }
      });
    });
    const _upType  = DATA.update_type || '—';
    const _upTime  = DATA.updated_at  || '—';
    const _asOf    = DATA.data_as_of  || '—';
    const _typeIcon = _upType === 'Full Refresh' ? '🔄' : '⚡';
    document.getElementById("updated-at").innerHTML =
      `<span title="ประเภท: ${_upType}\nดึงข้อมูลเมื่อ: ${_upTime}\nข้อมูลราคาถึงวัน: ${_asOf}" style="cursor:default">` +
      `${_typeIcon} <b>${_asOf}</b> <span style="color:var(--text2);font-size:10px">${_upTime.slice(11,16)}</span>` +
      `</span>`;
    _populateScrIndustry();
    loadScreenerSettings();
    initScreenerAutosave();
    renderSavedPresets();
    renderDqBanner();
    _rotAlertsData = null;   // ข้อมูลใหม่ -> ดึง rotation alerts ใหม่
    _breadthData   = null;   // ข้อมูลใหม่ -> ดึง breadth ใหม่
    _breadthCacheByRange = {};
    loadRegimeLight();       // ไฟ regime บน nav (async ไม่ block การ render)
    renderAll();
    initAlertSystem();
    checkPeReminder();
  } catch(e) {
    console.error("loadData error:", e);
    document.getElementById("updated-at").textContent = "ไม่สามารถโหลดข้อมูลได้: " + e.message;
  }
}

// ============================================================
// REFRESH — ดึงข้อมูลใหม่พร้อม progress bar
// ============================================================
let _refreshStart = 0;

async function _startJob(apiEndpoint, btnId, btnLabel, body = null, onDone = null) {
  const btn = document.getElementById(btnId);
  btn.disabled = true;
  btn.textContent = btnLabel.replace(/^[⚡⟳] /, m => m) + "...";

  try {
    const opts = { method: "POST" };
    if (body) { opts.headers = { "Content-Type": "application/json" }; opts.body = JSON.stringify(body); }
    const r = await fetch(apiEndpoint, opts);
    if (!r.ok) {
      const err = await r.json();
      alert("⚠️ " + (err.error || "เกิดข้อผิดพลาด"));
      btn.disabled = false; btn.textContent = btnLabel; return;
    }
  } catch(e) {
    alert("❌ ไม่สามารถเชื่อมต่อ server ได้");
    btn.disabled = false; btn.textContent = btnLabel; return;
  }

  _refreshStart = Date.now();
  const overlay = document.getElementById("progress-overlay");
  overlay.style.display = "flex";
  document.getElementById("progress-bar-fill").style.width = "0%";
  document.getElementById("progress-pct").textContent = "0%";
  document.getElementById("progress-msg").textContent = "กำลังเริ่ม...";
  document.getElementById("progress-eta").textContent = "";

  const es = new EventSource("/api/progress");
  es.onmessage = function(e) {
    const s = JSON.parse(e.data);
    const pct = s.total > 0 ? Math.min(Math.round(s.current / s.total * 100), 99) : 0;
    document.getElementById("progress-bar-fill").style.width = pct + "%";
    document.getElementById("progress-pct").textContent = pct + "%";
    document.getElementById("progress-msg").textContent = s.message || "";
    if (pct > 5 && pct < 99) {
      const elapsed = (Date.now() - _refreshStart) / 1000;
      const eta = Math.round(elapsed / pct * (100 - pct));
      const mm = Math.floor(eta / 60), ss = eta % 60;
      document.getElementById("progress-eta").textContent =
        "เหลืออีกประมาณ " + (mm > 0 ? mm + " นาที " : "") + ss + " วินาที";
    }
    if (s.done) {
      es.close();
      document.getElementById("progress-bar-fill").style.width = "100%";
      document.getElementById("progress-pct").textContent = "100%";
      setTimeout(function() {
        overlay.style.display = "none";
        btn.disabled = false; btn.textContent = btnLabel;
        if (s.error) {
          const usedFallback = (s.message || "").includes("ข้อมูลล่าสุด");
          if (usedFallback) {
            alert("⚠️ ดึงข้อมูลใหม่ไม่สำเร็จ\nกำลังใช้ข้อมูลล่าสุดแทน");
            loadData();
          } else {
            alert("❌ เกิดข้อผิดพลาด:\n" + s.error);
          }
        } else {
          resetNhCache();
          // clear all page caches so next visit fetches fresh data
          _idxData = null; _valData = null; _nvdrData = null;
          _shortData = null; _insData = null;
          _drLoaded = false; _drData = null;
          loadData();
          // if already on a data page, reload it immediately
          const _activePage = document.querySelector('.page.active')?.id;
          if (_activePage === 'page-indices')   loadIndicesPage();
          if (_activePage === 'page-dr')        loadDRPage();
          if (_activePage === 'page-valuation') loadValuationPage();
          if (_activePage === 'page-short')     loadShortPage();
          if (_activePage === 'page-insider')   loadInsiderPage();
          if (onDone) onDone();
        }
      }, 800);
    }
  };
  es.onerror = function() {
    es.close();
    overlay.style.display = "none";
    btn.disabled = false; btn.textContent = btnLabel;
    loadData();
  };
}

function startRefresh() {
  const dlg = document.getElementById('refresh-dialog');
  dlg.style.display = 'flex';
}
function confirmRefresh(period) {
  document.getElementById('refresh-dialog').style.display = 'none';
  // ล้าง DR cache ก่อน เพื่อให้ DR ดึงใหม่ด้วยหลัง SET เสร็จ
  fetch('/api/dr-full-refresh', { method: 'POST' }).catch(() => {});
  _startJob("/api/refresh", "refresh-btn", "⟳ Full Refresh", { period }, () => {
    // callback หลัง SET เสร็จ: เริ่ม DR refresh ใน background
    _drLoaded = false;
    _drData   = null;
    const statusEl = document.getElementById('dr-status');
    if (statusEl) statusEl.textContent = 'กำลังอัปเดตข้อมูลหุ้นต่างประเทศ...';
    fetch('/api/dr')
      .then(r => r.json())
      .then(d => {
        if (d.stocks) {
          _drData   = d.stocks;
          _drLoaded = true;
          const ts = d.ts ? d.ts.replace('T',' ').slice(0,16) : '—';
          if (statusEl) statusEl.innerHTML =
            `อัปเดต: ${ts} &nbsp;|&nbsp; ${_drData.length} stocks`;
          if (document.getElementById('page-dr')?.classList.contains('active'))
            renderDRTable();
        }
      }).catch(() => {});
  });
}
function startQuickUpdate() {
  fetch('/api/dr-quick-update', { method: 'POST' }).catch(() => {});
  _startJob("/api/quick-update", "quick-update-btn", "⚡ Quick Update");
}

async function restartServer() {
  if (!confirm('ยืนยันการ Restart Server?\n\nหน้าเว็บจะ reload อัตโนมัติหลัง server พร้อม')) return;
  const btn = document.getElementById('restart-btn');
  btn.disabled = true; btn.textContent = '↺ กำลัง Restart...';
  try {
    await fetch('/api/restart', {method:'POST'});
  } catch(e) { /* server ปิดก่อน response — ปกติ */ }
  // รอ server ขึ้นมาใหม่
  const status = document.getElementById('updated-at');
  if (status) status.textContent = 'รอ server restart...';
  await new Promise(r => setTimeout(r, 2000));
  // poll จน server ตอบ
  for (let i = 0; i < 30; i++) {
    try {
      const r = await fetch('/api/status');
      if (r.ok) {
        btn.disabled = false; btn.textContent = '↺ Restart';
        if (status) status.textContent = 'Restart สำเร็จ — กำลัง reload...';
        await new Promise(r => setTimeout(r, 500));
        location.reload();
        return;
      }
    } catch(e) { /* ยังไม่พร้อม */ }
    await new Promise(r => setTimeout(r, 1000));
  }
  btn.disabled = false; btn.textContent = '↺ Restart';
  alert('Server ไม่ตอบสนอง — ลองรีโหลดหน้าเอง');
}

function renderAll() {
  renderOverview();
  renderRotation();
  renderRSLeaders();
  renderStage();
  renderSectors();
  renderStocks();
  renderWatchlist();
  renderBreakout();
  renderMomentum();
  renderHeatmap();
  renderEMABreadth();
  renderFundamentals();
  loadNewHighChart();
}

// ============================================================
// HELPERS
// ============================================================
// format market cap
function fmtCap(v, isReit) {
  if (!v) return isReit ? '<span class="badge badge-blue">REIT</span>' : '<span class="text2">—</span>';
  if (v >= 1e12) return (v/1e12).toFixed(1) + 'T';
  if (v >= 1e9)  return (v/1e9).toFixed(1)  + 'B';
  if (v >= 1e6)  return (v/1e6).toFixed(1)  + 'M';
  return v.toLocaleString();
}

// format volume
function fmtVol(v) {
  if (!v) return '<span class="text2">—</span>';
  if (v >= 1e9)  return (v/1e9).toFixed(1)  + 'B';
  if (v >= 1e6)  return (v/1e6).toFixed(1)  + 'M';
  if (v >= 1e3)  return (v/1e3).toFixed(1)  + 'K';
  return v.toLocaleString();
}

// วาด sparkline ลงใน canvas element
function drawSparkline(canvas, prices, retVal) {
  if (!canvas || !prices || prices.length < 2) return;
  const W = canvas.width  = 60;
  const H = canvas.height = 24;
  const ctx = canvas.getContext('2d');
  const vals = prices.map(p => p[1]);
  const mn = Math.min(...vals), mx = Math.max(...vals);
  const range = mx - mn || 1;
  const toY = v => H - 2 - (v - mn) / range * (H - 4);
  const toX = (i, n) => (i / (n-1)) * (W - 2) + 1;
  const color = (retVal || 0) >= 0 ? '#3fb950' : '#f85149';
  ctx.clearRect(0,0,W,H);
  ctx.beginPath();
  vals.forEach((v,i) => i===0 ? ctx.moveTo(toX(i,vals.length), toY(v)) : ctx.lineTo(toX(i,vals.length), toY(v)));
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

// render sparklines หลังจาก DOM update
function renderSparklinesInTable(tbodyId, stocks) {
  requestAnimationFrame(() => {
    const rows = document.querySelectorAll('#' + tbodyId + ' tr[data-sym]');
    const stockMap = Object.fromEntries(stocks.map(s => [s.symbol, s]));
    rows.forEach(row => {
      const sym = row.dataset.sym;
      const s = stockMap[sym];
      if (!s || !s.price_history) return;
      const canvas = row.querySelector('.spark-canvas');
      if (canvas) drawSparkline(canvas, s.price_history, s.ret_1m);
    });
  });
}

// แปลง Yahoo Finance ticker → TradingView symbol
function yfToTVSym(yf) {
  if (!yf) return '';
  if (yf.endsWith('.HK')) return `HKEX:${parseInt(yf)}`;       // 0700.HK → HKEX:700
  if (yf.endsWith('.T'))  return `TSE:${yf.slice(0,-2)}`;      // 6758.T  → TSE:6758
  if (yf.endsWith('.SS')) return `SSE:${yf.slice(0,-3)}`;      // 600519.SS → SSE:600519
  if (yf.endsWith('.SZ')) return `SZSE:${yf.slice(0,-3)}`;     // 000333.SZ → SZSE:000333
  if (yf.endsWith('.SI')) return `SGX:${yf.slice(0,-3)}`;      // D05.SI → SGX:D05
  if (yf.endsWith('.TW')) return `TWSE:${yf.slice(0,-3)}`;     // 0050.TW → TWSE:0050
  if (yf.endsWith('.PA')) return `EURONEXT:${yf.slice(0,-3)}`; // MC.PA → EURONEXT:MC
  if (yf.endsWith('.MI')) return `MIL:${yf.slice(0,-3)}`;      // SMSWLD.MI → MIL:SMSWLD
  if (yf.endsWith('.VN')) return yf.slice(0,-3);               // FPT.VN → FPT (TV resolves)
  return yf.replace(/-/g, '.');                                 // BRK-B → BRK.B (US stocks)
}

// แปลง Yahoo Finance index symbol → TradingView symbol (^BANK.BK → SET:BANK)
function idxToTVSym(sym) {
  if (sym === '^PFREIT.BK') return 'SET:PF_REIT';
  return 'SET:' + sym.replace(/^\^/, '').replace(/\.BK$/, '').replace(/-/g, '_');
}

function tvLink(sym) {
  return `<a class="tv-link" href="https://www.tradingview.com/chart/?symbol=SET:${sym}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="ดูใน TradingView">↗</a>`;
}
// ชื่อย่อสำหรับกลุ่มที่ชื่อยาวเกิน
const ROT_SHORT_NAME = {
  "Information & Communication Technology": "Info & Comm Tech (ICT)",
  "Personal Products & Pharmaceuticals":    "Personal & Pharma",
  "Industrial Materials & Machinery":       "Ind. Materials & Mach.",
  "Paper & Printing Materials":             "Paper & Printing",
  "Petrochemicals & Chemicals":             "Petrochemicals",
};
function rotName(name) { return ROT_SHORT_NAME[name] || name; }

// reverse lookup: sector/industry name → index ticker (built from IDX_TO_SECTOR at runtime)
let _SECTOR_TO_IDX = null;
function _getSectorToIdx() {
  if (_SECTOR_TO_IDX) return _SECTOR_TO_IDX;
  _SECTOR_TO_IDX = {};
  for (const [idxSym, names] of Object.entries(IDX_TO_SECTOR))
    for (const n of names) _SECTOR_TO_IDX[n] = idxSym;
  return _SECTOR_TO_IDX;
}
function sectorTvLink(name) {
  const idxSym = _getSectorToIdx()[name];
  if (!idxSym) return '';
  return `<a class="tv-link" href="https://www.tradingview.com/chart/?symbol=${idxToTVSym(idxSym)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="ดูกลุ่มใน TradingView">↗</a>`;
}

// ── Stage helpers ──────────────────────────────────────────────
function getStage(s) {
  if (s.stage != null) return s.stage;
  // client-side fallback: approximate ด้วย ema200_slope_pct หรือ returns
  const above = s.above_ema200;
  if (above == null) return null;
  // ถ้ายังไม่มี ema200_slope_pct (ก่อน Quick Update) คืน null — ไม่ guess สูตร
  const slope = s.ema200_slope_pct ?? null;
  if (slope == null) return null;
  if (above  && slope >= 0) return 2;
  if (above  && slope <  0) return 3;
  if (!above && slope >= -1.5) return 1;
  return 4;
}

function stageBadge(stage) {
  if (stage == null) return '<span class="text2">—</span>';
  const cfg = {
    1: { label: 'S1 Basing',    color: '#58a6ff' },
    2: { label: 'S2 ✅',        color: '#3fb950' },
    3: { label: 'S3 Topping',   color: '#e3b341' },
    4: { label: 'S4 Declining', color: '#f85149' },
  };
  const c = cfg[stage] || { label: '—', color: 'var(--text2)' };
  return `<span style="font-size:10px;font-weight:700;color:${c.color}">${c.label}</span>`;
}

// ── Market Regime ──────────────────────────────────────────────
function calcMarketRegime(stocks) {
  const n = stocks.length || 1;
  const pct200 = stocks.filter(s => s.above_ema200).length / n * 100;
  const pct50  = stocks.filter(s => s.above_ema50).length  / n * 100;
  const pctRS80 = stocks.filter(s => (s.rs_score||0) >= 80).length / n * 100;
  const pctPos1m = stocks.filter(s => (s.ret_1m||0) > 0).length / n * 100;
  const score = Math.round(pct200 * 0.35 + pct50 * 0.25 + pctRS80 * 0.25 + pctPos1m * 0.15);
  const capped = Math.min(100, score);
  let desc = '';
  if (pct200 >= 60 && pctRS80 >= 20)    desc = `EMA200: ${pct200.toFixed(0)}% · RS80+: ${pctRS80.toFixed(0)}%`;
  else if (pct200 < 40) desc = `ระวัง: เพียง ${pct200.toFixed(0)}% อยู่เหนือ EMA200`;
  else desc = `EMA200: ${pct200.toFixed(0)}% · EMA50: ${pct50.toFixed(0)}%`;
  return { score: capped, desc };
}

function pct(v, dec=2) {
  if (v == null) return '<span class="text2">—</span>';
  const c = v > 0 ? "green" : v < 0 ? "red" : "text2";
  return `<span class="${c}">${v > 0 ? "+" : ""}${v.toFixed(dec)}%</span>`;
}
function rsColor(rs) {
  if (rs == null) return "text2";
  if (rs >= 90) return "green";
  if (rs >= 70) return "blue";
  if (rs >= 50) return "yellow";
  return "red";
}
function emaBadge(above) {
  if (above == null) return '<span class="text2">—</span>';
  return above
    ? '<span class="badge badge-green">▲</span>'
    : '<span class="badge badge-red">▼</span>';
}

function rvolHtml(s) {
  if (!s.vol_today || !s.vol_avg20 || s.vol_avg20 === 0) return '<span class="text2">—</span>';
  const rv   = s.vol_today / s.vol_avg20;
  const pct  = rv * 100;
  const cls  = pct >= 300 ? 'red' : pct >= 200 ? 'green' : pct >= 150 ? 'yellow' : 'text2';
  const icon = pct >= 300 ? ' 🔥' : pct >= 200 ? ' ⚡' : '';
  const dvol = s.price && s.vol_today ? s.price * s.vol_today : null;
  const tip  = [
    `RVOL: ${rv.toFixed(2)}x`,
    `วันนี้: ${_fmtVolRaw(s.vol_today)} หุ้น`,
    `เฉลี่ย 20วัน: ${_fmtVolRaw(s.vol_avg20)} หุ้น`,
    dvol ? `มูลค่าซื้อขาย: ${_fmtVolRaw(dvol)} บาท` : '',
    '',
    '>1.5x = เริ่มผิดปกติ (เหลือง)',
    '>2x  = Volume สูง ⚡ (เขียว)',
    '>3x  = Volume Spike 🔥 (แดง)',
  ].filter(Boolean).join('&#10;');
  return `<span class="${cls}" style="font-weight:600;cursor:help" title="${tip}">${rv.toFixed(1)}x${icon}</span>`;
}

function computeSectorRanks() {
  if (!DATA) return {};
  const bySector = {};
  DATA.stocks.forEach(s => {
    const sec = s.sector || 'Unknown';
    if (!bySector[sec]) bySector[sec] = [];
    bySector[sec].push(s);
  });
  const rankMap = {};
  Object.values(bySector).forEach(group => {
    const sorted = [...group].sort((a, b) => (b.rs_score ?? -1) - (a.rs_score ?? -1));
    sorted.forEach((s, i) => {
      rankMap[s.symbol] = { rank: i + 1, total: sorted.length };
    });
  });
  return rankMap;
}

function secRankHtml(sr) {
  if (!sr) return '—';
  const ratio = sr.rank / sr.total;
  const cls = ratio <= 0.25 ? 'green' : ratio <= 0.5 ? 'yellow' : 'text2';
  return `<span class="${cls}" style="font-weight:600">${sr.rank}</span><span class="text2" style="font-size:10px">/${sr.total}</span>`;
}
function fmtValuation(val, type) {
  if (val == null) return '—';
  if (type === 'pe') {
    if (val <= 0) return `<span class="red" style="font-weight:600">N/A</span>`;
    const cls = val < 15 ? 'green' : val < 25 ? 'yellow' : 'red';
    return `<span class="${cls}" style="font-weight:600">${val.toFixed(1)}x</span>`;
  }
  if (type === 'pbv') {
    if (val <= 0) return `<span class="red" style="font-weight:600">N/A</span>`;
    const cls = val < 1 ? 'green' : val < 2 ? 'yellow' : 'red';
    return `<span class="${cls}" style="font-weight:600">${val.toFixed(2)}x</span>`;
  }
  return val.toFixed(2);
}

// ============================================================
// FEAR & GREED INDEX
// ============================================================
function calcFGI(stocks) {
  const n = stocks.length;
  if (!n) return { score: 50, c1: 50, c2: 50, c3: 50, c4: 50, c5: 50 };
  const c1 = stocks.filter(s => s.above_ema50).length / n * 100;
  const c2 = stocks.filter(s => (s.rs_score || 0) >= 50).length / n * 100;
  const c3 = stocks.filter(s => s.above_ema200).length / n * 100;
  const c4 = stocks.filter(s => (s.ret_3m || 0) > 0).length / n * 100;
  const w1m = stocks.filter(s => s.ret_1m != null);
  const avg1m = w1m.length ? w1m.reduce((a, s) => a + s.ret_1m, 0) / w1m.length : 0;
  const c5 = Math.max(0, Math.min(100, (avg1m + 15) / 30 * 100));
  const score = Math.round((c1 + c2 + c3 + c4 + c5) / 5);
  return { score, c1, c2, c3, c4, c5 };
}

// ============================================================
// OVERVIEW
// ============================================================
function renderOverview() {
  if (!DATA) return;
  const stocks = DATA.stocks;
  const total  = stocks.length;

  // --- stat cards ---
  const above50  = stocks.filter(s => s.above_ema50).length;
  const above200 = stocks.filter(s => s.above_ema200).length;
  const rs80     = stocks.filter(s => (s.rs_score||0) >= 80).length;
  const rs90     = stocks.filter(s => (s.rs_score||0) >= 90).length;

  const stocks1m = stocks.filter(s => s.ret_1m != null);
  const stocks1d = stocks.filter(s => s.ret_1d != null);
  const avgRet1m = stocks1m.length ? (stocks1m.reduce((a,s) => a + s.ret_1m, 0) / stocks1m.length).toFixed(2) : "0.00";
  const avgRet1d = stocks1d.length ? (stocks1d.reduce((a,s) => a + s.ret_1d, 0) / stocks1d.length).toFixed(2) : "0.00";

  const statColor = v => v > 0 ? "var(--green)" : v < 0 ? "var(--red)" : "var(--text2)";

  // Market Regime
  const regime = calcMarketRegime(stocks);
  const regimeColor = regime.score >= 65 ? '#3fb950' : regime.score >= 40 ? '#e3b341' : '#f85149';
  const regimeLabel = regime.score >= 65 ? 'Bull Market' : regime.score >= 40 ? 'Neutral' : 'Bear Market';
  const regimeIcon  = regime.score >= 65 ? '🟢' : regime.score >= 40 ? '🟡' : '🔴';

  document.getElementById("stat-cards").innerHTML = `
    <div class="card">
      <div class="card-title">หุ้นทั้งหมด</div>
      <div class="stat-val">${total}</div>
      <div class="stat-label">SET + mai</div>
      <div class="stat-sub">RS 80+ : <span class="green">${rs80} ตัว</span></div>
    </div>
    <div class="card">
      <div class="card-title">Avg Return 1D</div>
      <div class="stat-val" style="color:${statColor(avgRet1d)}">${avgRet1d > 0 ? "+" : ""}${avgRet1d}%</div>
      <div class="stat-label">ค่าเฉลี่ยทั้งตลาด</div>
    </div>
    <div class="card">
      <div class="card-title">Market Regime</div>
      <div class="stat-val" style="color:${regimeColor}">${regimeIcon} ${regime.score}</div>
      <div class="stat-label" style="color:${regimeColor};font-weight:700">${regimeLabel}</div>
      <div class="stat-sub" style="font-size:10px">${regime.desc}</div>
    </div>
    <div class="card">
      <div class="card-title">Market Breadth</div>
      <div class="stat-val blue">${Math.round(above50/total*100)}%</div>
      <div class="stat-label">% เหนือ EMA50</div>
      <div class="stat-sub">${Math.round(above200/total*100)}% เหนือ EMA200</div>
    </div>
  `;

  // --- Fear & Greed Index ---
  const fgi = calcFGI(stocks);
  const fgiColor = s => s >= 75 ? '#3fb950' : s >= 55 ? '#6fca6f' : s >= 45 ? '#d29922' : s >= 25 ? '#e07830' : '#f85149';
  const fgiLabel = s => s >= 75 ? 'Extreme Greed' : s >= 55 ? 'Greed' : s >= 45 ? 'Neutral' : s >= 25 ? 'Fear' : 'Extreme Fear';

  const fgiPt = (s, cx, cy, r) => {
    const a = Math.PI - (s / 100) * Math.PI;
    return [cx + r * Math.cos(a), cy - r * Math.sin(a)];
  };
  const fgiArc = (s1, s2, color, cx, cy, r) => {
    const [x1, y1] = fgiPt(s1, cx, cy, r);
    const [x2, y2] = fgiPt(s2, cx, cy, r);
    return `<path d="M${x1.toFixed(1)} ${y1.toFixed(1)} A${r} ${r} 0 0 0 ${x2.toFixed(1)} ${y2.toFixed(1)}" stroke="${color}" stroke-width="12" fill="none" stroke-linecap="butt"/>`;
  };
  const gcx = 80, gcy = 76, gr = 62;
  const [nx, ny] = fgiPt(fgi.score, gcx, gcy, gr * 0.82);
  const gaugeHTML = `<svg width="160" height="90" viewBox="0 0 160 90" style="display:block;margin:0 auto">
    <path d="M${gcx-gr} ${gcy} A${gr} ${gr} 0 0 0 ${gcx+gr} ${gcy}" stroke="var(--bg3)" stroke-width="13" fill="none"/>
    ${fgiArc(0,20,'#f85149',gcx,gcy,gr)}
    ${fgiArc(20,40,'#e07830',gcx,gcy,gr)}
    ${fgiArc(40,60,'#d29922',gcx,gcy,gr)}
    ${fgiArc(60,80,'#6fca6f',gcx,gcy,gr)}
    ${fgiArc(80,100,'#3fb950',gcx,gcy,gr)}
    <line x1="${gcx}" y1="${gcy}" x2="${nx.toFixed(1)}" y2="${ny.toFixed(1)}" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/>
    <circle cx="${gcx}" cy="${gcy}" r="5" fill="#fff"/>
    <text x="13" y="${gcy+13}" fill="#f85149" font-size="8" font-family="Segoe UI,system-ui,sans-serif" text-anchor="middle" font-weight="600">Fear</text>
    <text x="147" y="${gcy+13}" fill="#3fb950" font-size="8" font-family="Segoe UI,system-ui,sans-serif" text-anchor="middle" font-weight="600">Greed</text>
  </svg>`;

  const compBarFGI = (label, value) => {
    const w = Math.round(value);
    const c = value >= 60 ? '#3fb950' : value >= 40 ? '#d29922' : '#f85149';
    return `<div style="margin-bottom:8px">
      <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px">
        <span style="color:var(--text2)">${label}</span>
        <span style="color:${c};font-weight:600">${value.toFixed(0)}</span>
      </div>
      <div style="background:var(--bg3);border-radius:3px;height:4px;overflow:hidden">
        <div style="width:${w}%;background:${c};height:100%;border-radius:3px"></div>
      </div>
    </div>`;
  };

  document.getElementById('fgi-container').innerHTML = `
    <div class="card">
      <div style="display:flex;align-items:flex-start;gap:28px;flex-wrap:wrap">
        <div style="text-align:center;min-width:160px">
          <div class="card-title">SET Fear &amp; Greed Index</div>
          ${gaugeHTML}
          <div style="font-size:28px;font-weight:700;color:${fgiColor(fgi.score)};line-height:1;margin-top:-2px">${fgi.score}</div>
          <div style="font-size:13px;font-weight:700;color:${fgiColor(fgi.score)};margin-top:4px">${fgiLabel(fgi.score)}</div>
          <div style="font-size:10px;color:var(--text2);margin-top:5px">คำนวณจากข้อมูลตลาดจริง</div>
        </div>
        <div style="flex:1;min-width:220px;padding-top:26px">
          <div style="font-size:10px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">Components — 0 = Fear · 100 = Greed</div>
          ${compBarFGI('Market Breadth (% เหนือ EMA50)', fgi.c1)}
          ${compBarFGI('RS Strength (% หุ้น RS &ge; 50)', fgi.c2)}
          ${compBarFGI('Market Breadth (% เหนือ EMA200)', fgi.c3)}
          ${compBarFGI('Positive 3M Momentum', fgi.c4)}
          ${compBarFGI('Avg 1M Return (±15% range)', fgi.c5)}
        </div>
      </div>
    </div>`;

  // --- breadth bars ---
  const above20  = stocks.filter(s => s.above_ema20).length;
  const bars = [
    { label:"EMA20",  n: above20,  total, color:"#bc8cff" },
    { label:"EMA50",  n: above50,  total, color:"var(--blue)" },
    { label:"EMA200", n: above200, total, color:"var(--green)" },
  ];
  document.getElementById("breadth-bars").innerHTML = bars.map(b => {
    const pct = Math.round(b.n/b.total*100);
    return `
      <div class="breadth-row">
        <div class="breadth-label">${b.label}</div>
        <div class="breadth-track"><div class="breadth-fill" style="width:${pct}%;background:${b.color}"></div></div>
        <div class="breadth-pct" style="color:${b.color}">${pct}%</div>
      </div>
      <div style="font-size:10px;color:var(--text2);margin:-4px 0 8px 80px">${b.n} จาก ${b.total} ตัว</div>
    `;
  }).join("");

  // --- RS distribution ---
  const bins = [
    {label:"0-19",  min:0,  max:20,  color:"var(--red)"},
    {label:"20-39", min:20, max:40,  color:"#e05a20"},
    {label:"40-59", min:40, max:60,  color:"var(--yellow)"},
    {label:"60-69", min:60, max:70,  color:"#6ea8fe"},
    {label:"70-79", min:70, max:80,  color:"var(--blue)"},
    {label:"80-89", min:80, max:90,  color:"#5fca7f"},
    {label:"90-99", min:90, max:100, color:"var(--green)"},
  ];
  const binCounts = bins.map(b => stocks.filter(s => (s.rs_score||0) >= b.min && (s.rs_score||0) < b.max).length);
  const maxBin = Math.max(...binCounts);
  document.getElementById("rs-dist-bars").innerHTML = bins.map((b,i) => {
    const h = Math.round(binCounts[i]/maxBin*56) + 4;
    return `<div class="rs-bar" style="background:${b.color};height:${h}px" title="${b.label}: ${binCounts[i]} ตัว"></div>`;
  }).join("");
  document.getElementById("rs-dist-labels").innerHTML = bins.map(b => `<div class="rs-lbl">${b.label}</div>`).join("");
  document.getElementById("rs-dist-summary").textContent =
    `RS 90+ : ${binCounts[6]} ตัว  |  RS 80+ : ${binCounts[5]+binCounts[6]} ตัว  |  RS 70+ : ${binCounts[4]+binCounts[5]+binCounts[6]} ตัว`;

  // --- gainers/losers ---
  const sorted1d = stocks.filter(s => s.ret_1d != null).sort((a,b) => b.ret_1d - a.ret_1d);
  const gainers = sorted1d.slice(0,10);
  const losers  = sorted1d.slice(-10).reverse();

  const miniTbl = (rows, isGainer) => `
    <thead><tr><th${colTip('symbol')}>Symbol</th><th${colTip('sector')}>Sector</th><th class="r"${colTip('ret_1d')}>1D%</th><th class="r"${colTip('rs_score')}>RS</th></tr></thead>
    <tbody>${rows.map(s => `
      <tr>
        <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
        <td class="text2">${s.sector || "—"}</td>
        <td class="r">${pct(s.ret_1d)}</td>
        <td class="r"><span class="${rsColor(s.rs_score)}">${s.rs_score ?? "—"}</span></td>
      </tr>`).join("")}
    </tbody>`;
  document.getElementById("tbl-gainers").innerHTML = miniTbl(gainers, true);
  document.getElementById("tbl-losers").innerHTML  = miniTbl(losers, false);

  // --- industry bars ---
  const industries = [...DATA.industries].sort((a,b) => (b.ret_1m||0)-(a.ret_1m||0));
  const maxAbs = Math.max(...industries.map(ig => Math.abs(ig.ret_1m||0)));
  document.getElementById("industry-bars").innerHTML = industries.map(ig => {
    const r = ig.ret_1m || 0;
    const w = Math.round(Math.abs(r)/maxAbs*100);
    return `
      <div class="sec-bar-row">
        <div class="sec-name">${ig.name}</div>
        <div class="sec-track">
          ${r >= 0
            ? `<div class="sec-fill-pos" style="width:${w}%"></div>`
            : `<div class="sec-fill-neg" style="width:${w}%"></div>`}
        </div>
        <div class="sec-val ${r>0?'green':r<0?'red':'text2'}">${r>0?"+":""}${r.toFixed(1)}%</div>
      </div>`;
  }).join("");

  // --- RS leaders preview ---
  // กัน penny stock ออกจาก RS leaders preview — tick เดียว = ±33% ทำให้ติดอันดับปลอม
  const leaders = [...stocks].filter(s => s.rs_score != null && !_dqIsPenny(s))
    .sort((a,b) => (b.rs_score||0)-(a.rs_score||0)).slice(0,10);
  document.getElementById("tbl-rs-preview").innerHTML = `
    <thead><tr><th${colTip('rs_score')}>RS</th><th${colTip('symbol')}>Symbol</th><th${colTip('sector')}>Sector</th><th class="r"${colTip('ret_1m')}>1M%</th><th class="r"${colTip('ret_3m')}>3M%</th></tr></thead>
    <tbody>${leaders.map(s => `
      <tr>
        <td><span class="${rsColor(s.rs_score)}">${s.rs_score}</span></td>
        <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
        <td class="text2" style="font-size:11px">${s.sector||"—"}</td>
        <td class="r">${pct(s.ret_1m)}</td>
        <td class="r">${pct(s.ret_3m)}</td>
      </tr>`).join("")}
    </tbody>`;
}

// ============================================================
// ROTATION
// ============================================================
function setRotView(v, btn) {
  rotView = v;
  document.querySelectorAll("#page-rotation .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById(rotTimeframe === 'long' ? 'rot-tf-long' : 'rot-tf-short')?.classList.add("active");
  renderRotation();
}

function setRotTimeframe(tf, btn) {
  rotTimeframe = tf;
  document.getElementById('rot-tf-long')?.classList.toggle('active', tf === 'long');
  document.getElementById('rot-tf-short')?.classList.toggle('active', tf === 'short');
  const isShort = tf === 'short';
  const yLabel = document.getElementById('rot-ylabel');
  const sub    = document.getElementById('rot-subtitle');
  if (yLabel) yLabel.textContent = isShort ? '↑ 1W Return % (Momentum) ↑' : '↑ 1M Return % (Momentum) ↑';
  if (sub)    sub.textContent    = isShort
    ? 'แกน X = 1M Return (Trend) · แกน Y = 1W Return (Momentum) · ลูกศรบอกทิศ 1–4 สัปดาห์ล่าสุด'
    : 'แกน X = 3M Return (Trend) · แกน Y = 1M Return (Momentum)';
  renderRotation();
}

// ============================================================
// ROTATION QUADRANT ALERTS — sector เปลี่ยน quadrant (ยืนยัน 3 วันทำการ)
// ============================================================
let _rotAlertsData = null;

const _QUAD_COLOR = { Leading: '#3fb950', Improving: '#58a6ff',
                      Weakening: '#e3b341', Lagging: '#f85149' };

function _quadSpan(q) {
  return `<span style="color:${_QUAD_COLOR[q] || 'var(--text2)'};font-weight:700">${q}</span>`;
}

async function renderRotAlerts() {
  const el = document.getElementById('rot-alerts');
  if (!el) return;
  try {
    if (!_rotAlertsData) {
      const r = await fetch('/api/rotation-alerts');
      _rotAlertsData = await r.json();
    }
    const d = _rotAlertsData;
    const trans   = (d.transitions || []).slice(0, 8);
    const pending = (d.pending || []).filter(p => p.days >= 2);  // โชว์เฉพาะใกล้ยืนยัน
    if (trans.length === 0 && pending.length === 0) { el.style.display = 'none'; return; }

    const transHtml = trans.map(t =>
      `<div style="padding:3px 0">
         <span style="color:var(--text2);font-size:11px">${t.date}</span>
         &nbsp;<b>${t.name}</b> <span style="color:var(--text2);font-size:10px">(${t.type})</span>:
         ${_quadSpan(t.from)} → ${_quadSpan(t.to)}
       </div>`).join('');
    const pendHtml = pending.map(p =>
      `<div style="padding:3px 0;color:var(--text2)">
         ⏳ <b style="color:var(--text)">${p.name}</b>
         <span style="font-size:10px">(${p.type})</span>
         กำลังเข้า ${_quadSpan(p.to)} — ยืนยันแล้ว ${p.days}/${p.need} วัน
       </div>`).join('');

    el.innerHTML =
      `<div class="card" style="padding:10px 16px;font-size:12px">
         <div style="font-weight:700;margin-bottom:4px">🔔 Quadrant Changes
           <span class="scr-tip-icon" style="font-size:10px;width:15px;height:15px;margin-left:6px;vertical-align:middle">?<div class="scr-tip-box" style="width:280px">
             นับจากแกน Long (X=3M%, Y=1M%) — ต้องอยู่ quadrant ใหม่ครบ
             <b>${d.rules?.confirm_days ?? 3} วันทำการติดต่อกัน</b>ถึงยืนยัน
             (ค่าในช่วง ±${d.rules?.dead_zone_pct ?? 0.3}% รอบแกน = ไม่นับ กัน flip-flop)
           </div></span>
         </div>
         ${trans.length ? transHtml : '<div style="color:var(--text2);padding:3px 0">ยังไม่มีการเปลี่ยน quadrant ที่ยืนยันแล้ว</div>'}
         ${pendHtml}
       </div>`;
    el.style.display = 'block';
  } catch (e) {
    el.style.display = 'none';
  }
}

function renderRotation() {
  if (!DATA) return;
  renderRotAlerts();
  const data = rotView === "sector" ? DATA.sectors : DATA.industries;
  const sorted = [...data].sort((a,b) => (b.ret_1m||0)-(a.ret_1m||0));
  const maxAbs = Math.max(...sorted.map(s => Math.abs(s.ret_1m||0)), 1);

  // --- Sector Ranking Bars ---
  // --- Scatter Plot ---
  drawRotationScatter(sorted);

  // --- Multi-Timeframe (left panel) ---
  renderRotMulti();

  // --- Right tabs ---
  renderRotTab(_curRotTab || 'heat');

  // --- Momentum Playbook ---
  renderRotPlaybook(data);

  // --- Appendix ---
  renderRotAppendix();
}

function renderRotPlaybook(data) {
  const el = document.getElementById('rot-playbook');
  if (!el || !data) return;

  // กรองเฉพาะ sector/industry ที่มีข้อมูล ret_3m และ ret_1m ครบ
  const validData = data.filter(s => s.ret_3m != null && s.ret_1m != null);

  const quadrants = [
    {
      key: 'leading',
      label: 'Leading',
      emoji: '🚀',
      desc: 'Trend ดี + Momentum แรง — ถือต่อ/เพิ่ม',
      color: '#3fb950',
      bg: 'rgba(63,185,80,0.08)',
      border: 'rgba(63,185,80,0.3)',
      filter: s => s.ret_3m > 0 && s.ret_1m > 0,
      sort: (a,b) => b.ret_1m - a.ret_1m,
    },
    {
      key: 'improving',
      label: 'Improving',
      emoji: '📈',
      desc: 'กำลัง Recover — จับจังหวะเข้า risk/reward ดี',
      color: '#58a6ff',
      bg: 'rgba(88,166,255,0.08)',
      border: 'rgba(88,166,255,0.3)',
      filter: s => s.ret_3m <= 0 && s.ret_1m > 0,
      sort: (a,b) => b.ret_1m - a.ret_1m,
    },
    {
      key: 'weakening',
      label: 'Weakening',
      emoji: '⚠️',
      desc: 'Trend ดีแต่ Momentum ชะลอ — ระวัง/ลด position',
      color: '#e3b341',
      bg: 'rgba(227,179,65,0.08)',
      border: 'rgba(227,179,65,0.3)',
      filter: s => s.ret_3m > 0 && s.ret_1m <= 0,
      sort: (a,b) => b.ret_1m - a.ret_1m,
    },
    {
      key: 'lagging',
      label: 'Lagging',
      emoji: '🔴',
      desc: 'Trend แย่ + Momentum อ่อน — หลีกเลี่ยง/Short',
      color: '#f85149',
      bg: 'rgba(248,81,73,0.08)',
      border: 'rgba(248,81,73,0.3)',
      filter: s => s.ret_3m <= 0 && s.ret_1m <= 0,
      sort: (a,b) => b.ret_1m - a.ret_1m,
    },
  ];

  const pc = v => v == null ? '—' : `${v>0?'+':''}${v.toFixed(1)}%`;
  const rsC = v => v >= 70 ? '#3fb950' : v >= 50 ? '#e3b341' : '#f85149';

  const isSector = rotView === 'sector';
  const cols = quadrants.map(q => {
    const items = [...validData].filter(q.filter).sort(q.sort);
    const rows = items.map(s => {
      const clickFn = isSector
        ? `openSectorPage('${s.name.replace(/'/g,"\\'")}')`
        : `openSectorModal('${s.name.replace(/'/g,"\\'")}')`;
      return `
      <div style="display:flex;align-items:center;gap:6px;padding:4px 6px;border-radius:4px;cursor:pointer;transition:background .12s"
           onmouseover="this.style.background='rgba(255,255,255,0.05)'" onmouseout="this.style.background=''"
           onclick="${clickFn}">
        <span style="font-size:11px;font-weight:600;color:${q.color};min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${s.name}">${s.name}</span>
        <span style="font-size:10px;color:${(s.ret_1m||0)>=0?'#3fb950':'#f85149'};min-width:42px;text-align:right;font-weight:600">${pc(s.ret_1m)}</span>
        <span style="font-size:10px;min-width:28px;text-align:right;color:${rsC(s.avg_rs||0)}">${s.avg_rs!=null?Math.round(s.avg_rs):'—'}</span>
      </div>`; }).join('');

    return `
      <div style="flex:1;min-width:200px;background:${q.bg};border:1px solid ${q.border};border-radius:8px;padding:12px 14px">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">
          <span style="font-size:14px">${q.emoji}</span>
          <span style="font-size:13px;font-weight:700;color:${q.color}">${q.label}</span>
          <span style="font-size:10px;color:var(--text2);margin-left:auto;background:rgba(255,255,255,0.06);border-radius:10px;padding:1px 8px">${items.length}</span>
        </div>
        <div style="font-size:10px;color:var(--text2);margin-bottom:10px;line-height:1.4">${q.desc}</div>
        <div style="display:flex;font-size:9px;color:var(--text2);padding:0 6px 4px;border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:4px">
          <span style="flex:1">Sector / Industry</span>
          <span style="min-width:42px;text-align:right">1M%</span>
          <span style="min-width:28px;text-align:right">RS</span>
        </div>
        ${items.length ? rows : `<div style="font-size:11px;color:var(--text2);padding:8px 6px">—</div>`}
      </div>`;
  }).join('');

  el.innerHTML = `
    <div style="margin-bottom:10px;display:flex;align-items:center;gap:8px">
      <span style="font-size:13px;font-weight:700;color:#c8d0dc">🧭 Momentum Playbook</span>
      <span style="font-size:10px;color:var(--text2)">— ตำแหน่ง ${rotView === 'sector' ? 'Sector' : 'Industry'} แต่ละ Quadrant</span>
    </div>
    <div style="display:flex;gap:10px;flex-wrap:wrap">${cols}</div>`;
}

let _curRotTab = 'heat';

function setRotTab(tab, btn) {
  _curRotTab = tab;
  document.querySelectorAll('.rot-tab-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  renderRotTab(tab);
}

function renderRotTab(tab) {
  if (!DATA) return;
  if (tab === 'heat') renderRotHeat();
  else renderRotRVOL();
}

let _multiSortKey = 'ret_1w';

function renderRotMulti(sortKey) {
  if (sortKey) _multiSortKey = sortKey;
  const data = rotView === 'sector' ? DATA.sectors : DATA.industries;
  const sorted = [...data].sort((a,b) => (b[_multiSortKey]||0)-(a[_multiSortKey]||0));
  const pc = (v) => v == null ? '<span class="text2">—</span>'
    : `<span class="${v>0?'green':v<0?'red':'text2'}">${v>0?'+':''}${v.toFixed(1)}%</span>`;
  const label = rotView === 'sector' ? 'Sector' : 'Industry';
  const cols = [
    { key:'ret_1d', label:'1D%' },
    { key:'ret_1w', label:'1W%' }, { key:'ret_1m', label:'1M%' },
    { key:'ret_3m', label:'3M%' }, { key:'ret_6m', label:'6M%' },
    { key:'ret_1y', label:'1Y%' }, { key:'avg_rs',  label:'RS'  },
  ];
  const th = (col) => {
    const active = _multiSortKey === col.key;
    return `<th class="r" style="cursor:pointer;${active?'color:#58a6ff':''}"${colTip(col.key)}
      onclick="renderRotMulti('${col.key}')">${col.label}${active?' ↑':''}</th>`;
  };
  const el = document.getElementById('rot-multi-content');
  el.innerHTML = `
    <div style="font-size:10px;color:var(--text2);margin-bottom:6px">Multi-Timeframe · ${sorted.length} ${label} · คลิก header เพื่อเรียง</div>
    <table class="tbl" style="width:100%;font-size:11px">
      <thead><tr>
        <th style="cursor:default">${label}</th>
        ${cols.map(th).join('')}
      </tr></thead>
      <tbody>${sorted.map(s => { const _is = _getSectorToIdx()[s.name]; return `<tr class="heat-row" style="cursor:${_is?'pointer':'default'}" ${_is?`onclick="openIdxChartModal('${_is}')"`:''}">
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${s.name}">${rotName(s.name)}${sectorTvLink(s.name)}</td>
        <td class="r">${pc(s.ret_1d)}</td>
        <td class="r">${pc(s.ret_1w)}</td>
        <td class="r">${pc(s.ret_1m)}</td>
        <td class="r">${pc(s.ret_3m)}</td>
        <td class="r">${pc(s.ret_6m)}</td>
        <td class="r">${pc(s.ret_1y)}</td>
        <td class="r text2">${s.avg_rs!=null?Math.round(s.avg_rs):'—'}</td>
      </tr>`; }).join('')}</tbody>
    </table>`;
}

function renderRotAppendix() {
  const el = document.getElementById('rot-appendix');
  if (!el || !DATA) return;

  // จัดกลุ่มหุ้นตาม sector หรือ industry ตาม rotView
  const groupKey = rotView === 'sector' ? 'sector' : 'industry';
  const groups = {};
  DATA.stocks.forEach(s => {
    const g = s[groupKey] || 'Other';
    if (!groups[g]) groups[g] = [];
    groups[g].push(s);
  });

  // เรียง group ตามค่าเฉลี่ย RS ของหุ้นในกลุ่ม (สูงสุดก่อน)
  const sorted = Object.entries(groups).sort((a, b) => {
    const avgRs = arr => arr.reduce((s, x) => s + (x.rs_score || 0), 0) / arr.length;
    return avgRs(b[1]) - avgRs(a[1]);
  });

  const rsCol = v => v >= 80 ? '#3fb950' : v >= 60 ? '#e3b341' : '#8b949e';
  const retCol = v => v > 0 ? '#3fb950' : v < 0 ? '#f85149' : '#8b949e';

  const cards = sorted.map(([name, stocks]) => {
    // เรียงหุ้นใน group ตาม RS สูงสุดก่อน
    const sorted_stocks = [...stocks].sort((a, b) => (b.rs_score || 0) - (a.rs_score || 0));
    const avgRs = Math.round(stocks.reduce((s, x) => s + (x.rs_score || 0), 0) / stocks.length);
    const ret1m = stocks.filter(s => s.ret_1m != null);
    const avgRet1m = ret1m.length ? ret1m.reduce((s, x) => s + x.ret_1m, 0) / ret1m.length : null;

    const chips = sorted_stocks.map(s => {
      const rs = s.rs_score || 0;
      const bg = rs >= 80 ? 'rgba(63,185,80,0.15)' : rs >= 60 ? 'rgba(227,179,65,0.12)' : 'rgba(139,148,158,0.1)';
      const border = rs >= 80 ? 'rgba(63,185,80,0.35)' : rs >= 60 ? 'rgba(227,179,65,0.3)' : 'rgba(139,148,158,0.2)';
      const ret = s.ret_1d != null ? `${s.ret_1d > 0 ? '+' : ''}${s.ret_1d.toFixed(1)}%` : '';
      const retC = s.ret_1d != null ? retCol(s.ret_1d) : '#8b949e';
      return `<span title="${s.symbol} · RS ${rs} · 1M ${s.ret_1m != null ? (s.ret_1m > 0 ? '+' : '') + s.ret_1m.toFixed(1) + '%' : '—'} · ราคา ${s.price || '—'}"
        onclick="openChartModal('${s.symbol}')"
        style="display:inline-flex;align-items:center;gap:3px;background:${bg};border:1px solid ${border};
               border-radius:4px;padding:2px 6px;font-size:10px;cursor:pointer;white-space:nowrap;
               transition:opacity .12s;margin:2px"
        onmouseover="this.style.opacity='.7'" onmouseout="this.style.opacity='1'">
        <span style="font-weight:700;color:#c8d0dc">${s.symbol}</span>
        ${ret ? `<span style="color:${retC}">${ret}</span>` : ''}
      </span>`;
    }).join('');

    const avgRetStr = avgRet1m != null
      ? `<span style="color:${retCol(avgRet1m)};font-weight:600">${avgRet1m > 0 ? '+' : ''}${avgRet1m.toFixed(1)}%</span>`
      : '';

    return `
      <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:8px;padding:12px 14px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;cursor:pointer"
             onclick="openSectorPage('${name.replace(/'/g,"\\'")}')">
          <span style="font-size:12px;font-weight:700;color:#c8d0dc;flex:1">${name}</span>
          <span style="font-size:10px;color:var(--text2)">${stocks.length} หุ้น</span>
          ${avgRetStr}
          <span style="font-size:10px;color:${rsCol(avgRs)};font-weight:600">RS ${avgRs}</span>
        </div>
        <div style="display:flex;flex-wrap:wrap;gap:0">${chips}</div>
      </div>`;
  }).join('');

  const label = rotView === 'sector' ? 'Sector' : 'Industry Group';
  el.innerHTML = `
    <div style="margin-bottom:10px;display:flex;align-items:center;gap:8px">
      <span style="font-size:13px;font-weight:700;color:#c8d0dc">📋 Appendix — ${label} Constituents</span>
      <span style="font-size:10px;color:var(--text2)">สีชิป: <span style="color:#3fb950">RS≥80</span> · <span style="color:#e3b341">RS≥60</span> · <span style="color:#8b949e">RS&lt;60</span> · ตัวเลข = 1D% · hover = รายละเอียด</span>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:10px">${cards}</div>`;
}

function renderRotHeat() {
  const data = rotView === 'sector' ? DATA.sectors : DATA.industries;
  const sorted = [...data].sort((a,b) => (b.ret_3m||0)-(a.ret_3m||0));
  const periods = ['ret_1w','ret_1m','ret_3m','ret_6m','ret_1y'];
  const labels  = ['1W','1M','3M','6M','1Y'];

  function heatBg(v) {
    if (v == null) return 'rgba(255,255,255,0.05)';
    const intensity = Math.min(Math.abs(v)/30, 1);
    if (v > 0) return `rgba(63,185,80,${0.12 + intensity*0.55})`;
    return `rgba(248,81,73,${0.12 + intensity*0.55})`;
  }
  function heatTxt(v) {
    if (v == null) return '—';
    return (v>0?'+':'') + v.toFixed(1)+'%';
  }

  const el = document.getElementById('rot-tab-content');
  el.innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:11px">
      <thead><tr>
        <th style="text-align:left;padding:4px 6px;color:var(--text2);font-weight:600">Sector</th>
        ${labels.map(l=>`<th style="text-align:center;padding:4px 6px;color:var(--text2);font-weight:600">${l}</th>`).join('')}
      </tr></thead>
      <tbody>${sorted.map(s => { const _is = _getSectorToIdx()[s.name]; return `<tr class="heat-row" style="cursor:${_is?'pointer':'default'}" ${_is?`onclick="openIdxChartModal('${_is}')"`:''}">
        <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--fg)" title="${s.name}">${rotName(s.name)}${sectorTvLink(s.name)}</td>
        ${periods.map(p=>{
          const v = s[p];
          return `<td style="text-align:center;padding:3px 4px">
            <span class="heat-cell" style="background:${heatBg(v)};color:${v==null?'var(--text2)':v>=0?'#3fb950':'#f85149'}">${heatTxt(v)}</span>
          </td>`;
        }).join('')}
      </tr>`; }).join('')}</tbody>
    </table>`;
}

function renderRotRVOL() {
  if (!DATA || !DATA.stocks) return;
  const stocks = DATA.stocks
    .filter(s => s.vol_today > 0 && s.vol_avg20 > 0)
    .map(s => ({ ...s, rvol: s.vol_today / s.vol_avg20 }))
    .sort((a,b) => b.rvol - a.rvol)
    .slice(0,30);

  const el = document.getElementById('rot-tab-content');
  el.innerHTML = `
    <table class="tbl" style="width:100%;font-size:11px">
      <thead><tr>
        <th>#</th><th${colTip('symbol')}>Symbol</th><th${colTip('sector')}>Sector</th>
        <th class="r"${colTip('price')}>ราคา</th><th class="r"${colTip('ret_1d')}>1D%</th>
        <th class="r" title="ปริมาณซื้อขายวันนี้ (จำนวนหุ้น)">Vol Today</th><th class="r"${colTip('rvol')}>RVOL</th>
      </tr></thead>
      <tbody>${stocks.map((s,i) => {
        const rv = s.rvol;
        const icon = rv>=3?'🔥':rv>=2?'⚡':'';
        const rCls = rv>=3?'red':rv>=2?'green':rv>=1.5?'yellow':'text2';
        const d1 = s.ret_1d ?? s.chg_1d ?? null;
        return `<tr>
          <td class="text2">${i+1}</td>
          <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
          <td class="text2" style="max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.sector||'—'}</td>
          <td class="r">${s.price!=null?s.price.toFixed(2):'—'}</td>
          <td class="r ${d1!=null?(d1>0?'green':d1<0?'red':'text2'):'text2'}">${d1!=null?(d1>0?'+':'')+d1.toFixed(2)+'%':'—'}</td>
          <td class="r text2">${_fmtVolRaw(s.vol_today)}</td>
          <td class="r ${rCls}">${rv.toFixed(1)}x${icon}</td>
        </tr>`;
      }).join('')}</tbody>
    </table>`;
}

function drawRotationScatter(sectors) {
  const canvas = document.getElementById("rotation-map");
  if (!canvas) return;

  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth || canvas.parentElement?.offsetWidth || 960;
  const H = 580;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  canvas.style.height = H + "px";  // ไม่ lock style.width ให้ CSS width:100% จัดการ
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  const PAD = { top: 44, right: 28, bottom: 56, left: 60 };
  const PW = W - PAD.left - PAD.right;
  const PH = H - PAD.top  - PAD.bottom;

  const isShort = rotTimeframe === 'short';
  // ไม่ plot กลุ่มที่ค่าแกนเป็น null (เช่น sector สมาชิก < 3 ตัวที่ validation
  // ตัดค่าเฉลี่ยทิ้ง) — เดิม || 0 ทำให้ไปกองที่จุด (0,0) หลอกตา
  const _hasAxes = s => isShort
    ? (s.ret_1m != null && s.ret_1w != null)
    : (s.ret_3m != null && s.ret_1m != null);
  const _rotExcluded = sectors.filter(s => !_hasAxes(s)).map(s => s.name);
  sectors = sectors.filter(_hasAxes);
  if (sectors.length === 0) return;
  const getX = s => isShort ? (s.ret_1m || 0) : (s.ret_3m || 0);
  const getY = s => isShort ? (s.ret_1w || 0) : (s.ret_1m || 0);
  const xs = sectors.map(getX);
  const ys = sectors.map(getY);

  // Symlog scale — รองรับค่าลบ, compress outlier ที่ห่างจาก 0 มากๆ
  // ค่าใน ±SL_C (%) จะเกือบ linear, นอกจากนั้น log compress
  const SL_C = 3;
  function symlog(v)    { return Math.sign(v) * Math.log10(1 + Math.abs(v) / SL_C); }
  function _pct(arr, p) {
    const sorted = [...arr].sort((a,b) => a-b);
    const idx = (p/100) * (sorted.length - 1);
    const lo = Math.floor(idx), hi = Math.ceil(idx);
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  }
  // แกน X: ซ้ายแค่ -50%, ขวาขยายเต็มที่ตามข้อมูล
  const sxs  = xs.map(symlog), sys = ys.map(symlog);
  const sxMin = Math.min(...sxs), sxMax = Math.max(...sxs);
  const syMin = Math.min(...sys), syMax = Math.max(...sys);
  // cap ขึ้นกับ timeframe — short mode ใช้ range เล็กกว่า
  const xCapLo = isShort ? -15 : -30, xCapHi = isShort ? 20 : 60;
  const yCapLo = isShort ? -10 : -30, yCapHi = isShort ? 10 : 60;
  const xLow  = Math.min(sxMin, symlog(xCapLo)) * 1.08;
  const xHigh = Math.max(sxMax * 1.20, symlog(xCapHi));
  const yLow  = Math.min(syMin, symlog(yCapLo)) * 1.08;
  const yHigh = Math.max(syMax * 1.20, symlog(yCapHi));
  const xRange = xHigh - xLow, yRange = yHigh - yLow;

  const toX = v => PAD.left + (symlog(v) - xLow) / xRange * PW;
  const toY = v => PAD.top  + (yHigh - symlog(v)) / yRange * PH;

  const palette = [
    "#58a6ff","#3fb950","#ffa657","#f85149","#d2a8ff","#79c0ff","#56d364",
    "#e3b341","#ff7b72","#bc8cff","#1f9e75","#87ceeb","#ffb347","#ff6eb4",
    "#7ee787","#40e0d0","#ff8c42","#9370db","#20b2aa","#ffd700","#00bcd4",
    "#ff69b4","#98fb98","#dda0dd","#f0e68c","#87cefa","#a8e6cf","#90ee90",
    "#ffcba4","#c3b1e1",
  ];
  const nameToColor = {};
  sectors.forEach((s,i) => { nameToColor[s.name] = palette[i % palette.length]; });

  const BUBBLE_R = 10;
  const MARGIN = BUBBLE_R + 3;
  const points = sectors.map(s => {
    const ox = toX(getX(s)), oy = toY(getY(s));
    return {
      x: ox, y: oy, ox, oy,
      isOutlier: false,
      R: BUBBLE_R,
      name: s.name,
      count: s.count || 0,
      ret_1w: s.ret_1w, ret_1m: s.ret_1m, ret_3m: s.ret_3m, ret_6m: s.ret_6m, ret_1y: s.ret_1y,
      avg_rs: s.avg_rs,
      color: nameToColor[s.name],
    };
  });

  // Iterative collision resolution — ผลัก bubble ที่ซ้อนกันออก + spring ดึงกลับตำแหน่งจริง
  const MIN_DIST = BUBBLE_R * 2 + 3;   // ระยะขั้นต่ำระหว่างศูนย์กลาง (diameter + gap 3px)
  const SPRING  = 0.08;                 // แรงดึงกลับตำแหน่งเดิม (0=ไม่ดึง, 1=ดึงเต็มที่)
  for (let iter = 0; iter < 25; iter++) {
    for (let i = 0; i < points.length; i++) {
      for (let j = i + 1; j < points.length; j++) {
        const dx = points[j].x - points[i].x;
        const dy = points[j].y - points[i].y;
        const dist = Math.hypot(dx, dy) || 0.01;
        if (dist < MIN_DIST) {
          const push = (MIN_DIST - dist) / 2;
          const nx = dx / dist, ny = dy / dist;
          points[i].x -= nx * push;  points[i].y -= ny * push;
          points[j].x += nx * push;  points[j].y += ny * push;
        }
      }
      // Spring — ดึงกลับตำแหน่งข้อมูลจริง
      points[i].x += (points[i].ox - points[i].x) * SPRING;
      points[i].y += (points[i].oy - points[i].y) * SPRING;
    }
  }

  function hexAlpha(hex, a) {
    const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
    return `rgba(${r},${g},${b},${a})`;
  }

  function redraw(highlightName) {
    ctx.clearRect(0, 0, W, H);
    const ox = toX(0), oy = toY(0);

    // Quadrant fills + labels
    const quads = [
      { x:PAD.left, y:PAD.top,  w:ox-PAD.left,    h:oy-PAD.top,    fill:"rgba(20,50,100,0.18)", label:"Improving", lx:"left",  ly:"top",    tc:"#4f9cf0" },
      { x:ox,       y:PAD.top,  w:PAD.left+PW-ox, h:oy-PAD.top,    fill:"rgba(10,60,25,0.18)",  label:"Leading",   lx:"right", ly:"top",    tc:"#3fb950" },
      { x:PAD.left, y:oy,       w:ox-PAD.left,    h:PAD.top+PH-oy, fill:"rgba(60,10,10,0.18)",  label:"Lagging",   lx:"left",  ly:"bottom", tc:"#f85149" },
      { x:ox,       y:oy,       w:PAD.left+PW-ox, h:PAD.top+PH-oy, fill:"rgba(60,38,5,0.16)",   label:"Weakening", lx:"right", ly:"bottom", tc:"#e3b341" },
    ];
    quads.forEach(q => {
      ctx.fillStyle = q.fill;
      ctx.fillRect(q.x, q.y, q.w, q.h);
      ctx.save();
      ctx.font = "bold 13px Segoe UI, sans-serif";
      ctx.textAlign = q.lx === "left" ? "left" : "right";
      const lx = q.lx === "left" ? q.x + 12 : q.x + q.w - 12;
      const ly = q.ly === "top" ? q.y + 24 : q.y + q.h - 12;
      // background pill (roundRect with fallback for older browsers)
      const tw = ctx.measureText(q.label).width;
      const px = 8;
      const rx = q.lx === "left" ? lx - px : lx - tw - px;
      const ry = ly - 14, rw = tw + px*2, rh = 20, rr = 4;
      ctx.globalAlpha = 0.45;
      ctx.fillStyle = q.tc;
      ctx.beginPath();
      if (ctx.roundRect) {
        ctx.roundRect(rx, ry, rw, rh, rr);
      } else {
        ctx.rect(rx, ry, rw, rh);
      }
      ctx.fill();
      // text
      ctx.globalAlpha = 1;
      ctx.fillStyle = '#ffffff';
      ctx.fillText(q.label, lx, ly);
      ctx.restore();
    });

    // Center axis lines
    ctx.save();
    ctx.strokeStyle = "rgba(139,148,158,0.45)";
    ctx.lineWidth = 1;
    ctx.setLineDash([5, 4]);
    ctx.beginPath(); ctx.moveTo(ox, PAD.top); ctx.lineTo(ox, PAD.top+PH); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(PAD.left, oy); ctx.lineTo(PAD.left+PW, oy); ctx.stroke();
    ctx.setLineDash([]);
    ctx.restore();

    // Grid lines + tick labels — filter ใน symlog space
    const xTicks = [-60,-50,-40,-30,-20,-10,-5,0,5,10,20,30,40,50,60].filter(v => symlog(v) > xLow && symlog(v) < xHigh);
    const yTicks = [-40,-30,-25,-20,-15,-10,-5,0,5,10,15,20,25,30,40].filter(v => symlog(v) > yLow && symlog(v) < yHigh);
    ctx.save();
    ctx.strokeStyle = "rgba(48,54,61,0.7)";
    ctx.lineWidth = 0.4;
    ctx.setLineDash([2,4]);
    xTicks.forEach(v => { if (v!==0) { ctx.beginPath(); ctx.moveTo(toX(v),PAD.top); ctx.lineTo(toX(v),PAD.top+PH); ctx.stroke(); } });
    yTicks.forEach(v => { if (v!==0) { ctx.beginPath(); ctx.moveTo(PAD.left,toY(v)); ctx.lineTo(PAD.left+PW,toY(v)); ctx.stroke(); } });
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(139,148,158,0.7)"; ctx.font = "10px Segoe UI, sans-serif";
    ctx.textAlign = "center";
    xTicks.forEach(v => { if (v!==0) ctx.fillText((v>0?"+":"")+v+"%", toX(v), PAD.top+PH+16); });
    ctx.textAlign = "right";
    yTicks.forEach(v => { if (v!==0) ctx.fillText((v>0?"+":"")+v+"%", PAD.left-7, toY(v)+4); });
    ctx.restore();


    // Trail for highlighted point only
    if (highlightName) {
      const p = points.find(p => p.name === highlightName);
      if (p) {
        const anchors = [];
        if (isShort) {
          // Short mode: (ret_3m, ret_1m) → (ret_1m, ret_1w)
          if (p.ret_3m != null) anchors.push({ rx: p.ret_3m, ry: p.ret_1m || 0 });
          anchors.push({ rx: p.ret_1m || 0, ry: p.ret_1w || 0 });
        } else {
          // Long mode: (ret_1y, ret_6m) → (ret_6m, ret_3m) → (ret_3m, ret_1m)
          if (p.ret_1y != null && p.ret_6m != null) anchors.push({ rx: p.ret_1y, ry: p.ret_6m });
          if (p.ret_6m != null) anchors.push({ rx: p.ret_6m, ry: p.ret_3m || 0 });
          anchors.push({ rx: p.ret_3m || 0, ry: p.ret_1m || 0 });
        }
        const trail = anchors.map(a => ({ x: toX(a.rx), y: toY(a.ry) }));
        for (let i = 1; i < trail.length; i++) {
          const prev = trail[i-1], curr = trail[i];
          const t0 = (i-1)/(trail.length-1), t1 = i/(trail.length-1);
          const alpha0 = 0.15 + t0 * 0.5, alpha1 = 0.15 + t1 * 0.65;
          ctx.save();
          const grad = ctx.createLinearGradient(prev.x, prev.y, curr.x, curr.y);
          grad.addColorStop(0, hexAlpha(p.color, alpha0));
          grad.addColorStop(1, hexAlpha(p.color, alpha1));
          ctx.strokeStyle = grad; ctx.lineWidth = 2.5;
          ctx.beginPath(); ctx.moveTo(prev.x, prev.y); ctx.lineTo(curr.x, curr.y); ctx.stroke();
          // ghost dot at previous position
          ctx.globalAlpha = 0.25; ctx.fillStyle = p.color;
          ctx.beginPath(); ctx.arc(prev.x, prev.y, p.R * 0.5, 0, Math.PI*2); ctx.fill();
          // arrowhead at 65% along each segment
          const ax = prev.x + (curr.x - prev.x) * 0.65;
          const ay = prev.y + (curr.y - prev.y) * 0.65;
          const angle = Math.atan2(curr.y - prev.y, curr.x - prev.x);
          const sz = 7;
          ctx.globalAlpha = 0.5 + t1 * 0.45;
          ctx.fillStyle = p.color;
          ctx.beginPath();
          ctx.moveTo(ax + Math.cos(angle) * sz, ay + Math.sin(angle) * sz);
          ctx.lineTo(ax + Math.cos(angle + 2.4) * sz * 0.6, ay + Math.sin(angle + 2.4) * sz * 0.6);
          ctx.lineTo(ax + Math.cos(angle - 2.4) * sz * 0.6, ay + Math.sin(angle - 2.4) * sz * 0.6);
          ctx.closePath(); ctx.fill();
          ctx.restore();
        }
      }
    }

    // Sort: dimmed first so active renders on top
    const ordered = [...points].sort((a,b) => {
      if (!highlightName) return 0;
      return (a.name === highlightName ? 1 : 0) - (b.name === highlightName ? 1 : 0);
    });

    // Draw bubbles
    ordered.forEach(p => {
      const isDim = highlightName && p.name !== highlightName;
      ctx.save();
      if (isDim) {
        ctx.globalAlpha = 0.1;
        ctx.fillStyle = "#3d444d";
        ctx.beginPath(); ctx.arc(p.x, p.y, p.R, 0, Math.PI*2); ctx.fill();
        ctx.strokeStyle = "#4d5562"; ctx.lineWidth = 1;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.R, 0, Math.PI*2); ctx.stroke();
      } else {
        // Glow
        ctx.shadowColor = p.color; ctx.shadowBlur = 12;
        ctx.fillStyle = hexAlpha(p.color, 0.2);
        ctx.beginPath(); ctx.arc(p.x, p.y, p.R, 0, Math.PI*2); ctx.fill();
        ctx.shadowBlur = 0;
        // Stroke ring
        ctx.strokeStyle = p.color; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.R, 0, Math.PI*2); ctx.stroke();
        // Subtle inner fill gradient
        const grad = ctx.createRadialGradient(p.x-p.R*0.3, p.y-p.R*0.3, 0, p.x, p.y, p.R);
        grad.addColorStop(0, hexAlpha(p.color, 0.35));
        grad.addColorStop(1, hexAlpha(p.color, 0.08));
        ctx.fillStyle = grad;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.R, 0, Math.PI*2); ctx.fill();
        // RS Score inside bubble
        if (p.avg_rs != null) {
          ctx.fillStyle = "#ffffff";
          ctx.font = `bold ${p.R >= 16 ? 11 : 9}px Segoe UI, sans-serif`;
          ctx.textAlign = "center"; ctx.textBaseline = "middle";
          ctx.fillText(Math.round(p.avg_rs), p.x, p.y);
          ctx.textBaseline = "alphabetic";
        }
        // Outlier indicator — แสดงลูกศรชี้ทิศที่ฟองอยู่นอกสเกล
        if (p.isOutlier) {
          const angle = Math.atan2(p.outDirY, p.outDirX);
          const tipX = p.x + Math.cos(angle) * (p.R + 8);
          const tipY = p.y + Math.sin(angle) * (p.R + 8);
          const sz = 6;
          ctx.save();
          ctx.globalAlpha = 0.9;
          ctx.fillStyle = p.color;
          ctx.strokeStyle = p.color; ctx.lineWidth = 1.5;
          ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.lineTo(tipX, tipY); ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(tipX, tipY);
          ctx.lineTo(tipX - Math.cos(angle - 0.5) * sz, tipY - Math.sin(angle - 0.5) * sz);
          ctx.lineTo(tipX - Math.cos(angle + 0.5) * sz, tipY - Math.sin(angle + 0.5) * sz);
          ctx.closePath(); ctx.fill();
          ctx.restore();
        }
        // Direction arrow: long=(ret_6m,ret_3m)→(ret_3m,ret_1m), short=(ret_3m,ret_1m)→(ret_1m,ret_1w)
        if (!highlightName) {
          const prevCx = isShort ? toX(p.ret_3m ?? p.ret_1m) : toX(p.ret_6m ?? p.ret_3m);
          const prevCy = isShort ? toY(p.ret_1m || 0)        : toY(p.ret_3m || 0);
          const dx = p.x - prevCx;
          const dy = p.y - prevCy;
          const len = Math.hypot(dx, dy);
          if (len > 0.5) {
            const angle = Math.atan2(dy, dx);
            const tip = p.R + 6, sz = 5;
            const tx = p.x + Math.cos(angle) * tip;
            const ty = p.y + Math.sin(angle) * tip;
            ctx.save();
            ctx.globalAlpha = 0.75;
            ctx.fillStyle = p.color;
            ctx.beginPath();
            ctx.moveTo(tx, ty);
            ctx.lineTo(tx - Math.cos(angle - 0.5) * sz, ty - Math.sin(angle - 0.5) * sz);
            ctx.lineTo(tx - Math.cos(angle + 0.5) * sz, ty - Math.sin(angle + 0.5) * sz);
            ctx.closePath(); ctx.fill();
            ctx.restore();
          }
        }
      }
      ctx.restore();
    });

    // Labels — แสดงเฉพาะตอน hover/pin
    if (highlightName) {
      const p = points.find(p => p.name === highlightName);
      if (p) {
        ctx.font = "bold 11px Segoe UI, sans-serif";
        const short = p.name.length > 22 ? p.name.slice(0,20)+"…" : p.name;
        const lw = ctx.measureText(short).width + 14;
        const lh = 18;
        const OFFSET = p.R + 10;

        // arrow direction (from prev position → current bubble)
        const prevCx = isShort ? toX(p.ret_3m ?? p.ret_1m) : toX(p.ret_6m ?? p.ret_3m);
        const prevCy = isShort ? toY(p.ret_1m || 0) : toY(p.ret_3m || 0);
        const arrowAngle = Math.atan2(p.y - prevCy, p.x - prevCx);

        // 8 candidate directions — try each, score by: fits canvas + away from arrow
        const dirs8 = [[1,0],[-1,0],[0,-1],[0,1],[0.7,-0.7],[-0.7,-0.7],[0.7,0.7],[-0.7,0.7]];
        let bestLx = p.x + OFFSET, bestLy = p.y - lh/2, bestScore = -Infinity;
        for (const [dx, dy] of dirs8) {
          let lx = p.x + dx * OFFSET + (dx >= 0 ? 0 : -lw);
          let ly = p.y + dy * OFFSET + (dy <= 0 ? -lh : 0);
          // clamp to canvas
          lx = Math.max(PAD.left + 2, Math.min(W - lw - 4, lx));
          ly = Math.max(PAD.top + 2, Math.min(H - PAD.bottom - lh - 2, ly));
          // penalise directions close to arrow direction
          const dirAngle = Math.atan2(dy, dx);
          let angleDiff = Math.abs(dirAngle - arrowAngle);
          if (angleDiff > Math.PI) angleDiff = 2 * Math.PI - angleDiff;
          const score = Math.PI - angleDiff; // smaller diff = more aligned with arrow direction = label goes in front, trail stays behind
          if (score > bestScore) { bestScore = score; bestLx = lx; bestLy = ly; }
        }

        // Connector line
        const labelCx = bestLx + lw/2, labelCy = bestLy + lh/2;
        const dist = Math.hypot(labelCx - p.x, labelCy - p.y);
        if (dist > p.R + 8) {
          const edgeX = p.x + (labelCx - p.x) / dist * (p.R + 1);
          const edgeY = p.y + (labelCy - p.y) / dist * (p.R + 1);
          ctx.save();
          ctx.strokeStyle = hexAlpha(p.color, 0.6);
          ctx.lineWidth = 1.2; ctx.setLineDash([3,3]);
          ctx.beginPath(); ctx.moveTo(edgeX, edgeY); ctx.lineTo(labelCx, labelCy); ctx.stroke();
          ctx.setLineDash([]);
          ctx.restore();
        }

        // Label pill
        ctx.save();
        ctx.fillStyle = "rgba(13,17,23,0.92)";
        if (ctx.roundRect) { ctx.beginPath(); ctx.roundRect(bestLx, bestLy, lw, lh, 4); ctx.fill(); }
        else { ctx.fillRect(bestLx, bestLy, lw, lh); }
        ctx.strokeStyle = hexAlpha(p.color, 0.85);
        ctx.lineWidth = 1.5;
        if (ctx.roundRect) { ctx.beginPath(); ctx.roundRect(bestLx, bestLy, lw, lh, 4); ctx.stroke(); }
        else { ctx.strokeRect(bestLx, bestLy, lw, lh); }
        ctx.fillStyle = "#ffffff";
        ctx.textAlign = "left"; ctx.textBaseline = "middle";
        ctx.fillText(short, bestLx + 7, bestLy + lh/2);
        ctx.textBaseline = "alphabetic";
        ctx.restore();
      }
    }

    ctx.globalAlpha = 1;
  }

  // ---- Legend chips ----
  let pinnedSector = null;
  const legendEl = document.getElementById("rot-legend");
  if (legendEl) {
    legendEl.innerHTML = [...points].sort((a,b) => (b.ret_3m||0) - (a.ret_3m||0)).map(p => {
      const safeId = "rchip_" + p.name.replace(/[^a-zA-Z0-9]/g, "_");
      const label  = p.name.length > 22 ? p.name.slice(0,20)+"…" : p.name;
      return `<div class="rot-chip" id="${safeId}" data-sector="${p.name.replace(/"/g,'&quot;')}"
                   style="color:${p.color};opacity:0.65">
                <span class="rot-chip-sq" style="background:${p.color}"></span>${label}
              </div>`;
    }).join("")
    + (_rotExcluded.length
       ? `<div style="font-size:11px;color:var(--text2);font-style:italic;padding:4px 8px"
               title="กลุ่มที่มีหุ้นน้อยกว่า 3 ตัว — ค่าเฉลี่ยไม่ represent จึงไม่แสดงบนแผนที่">
            ⚠ ไม่แสดง ${_rotExcluded.length} กลุ่ม (ข้อมูลไม่พอ): ${_rotExcluded.join(", ")}
          </div>`
       : "");
    legendEl.querySelectorAll(".rot-chip").forEach(chip => {
      chip.addEventListener("click", () => {
        const name = chip.dataset.sector;
        pinnedSector = pinnedSector === name ? null : name;
        legendEl.querySelectorAll(".rot-chip").forEach(c => {
          c.classList.remove("pinned");
          c.style.opacity = pinnedSector ? "0.3" : "0.65";
        });
        if (pinnedSector) { chip.classList.add("pinned"); chip.style.opacity = "1"; }
        redraw(pinnedSector);
      });
    });
  }

  redraw(null);

  // Tooltip
  let tooltipDiv = document.getElementById("rot-tooltip");
  if (!tooltipDiv) {
    tooltipDiv = document.createElement("div");
    tooltipDiv.id = "rot-tooltip";
    tooltipDiv.style.cssText = `
      position:fixed;background:rgba(22,27,34,0.97);border:1px solid #30363d;
      color:#e6edf3;padding:10px 14px;border-radius:8px;font-size:12px;
      pointer-events:none;display:none;z-index:999;line-height:1.7;
      box-shadow:0 6px 16px rgba(0,0,0,0.5);min-width:160px;
    `;
    document.body.appendChild(tooltipDiv);
  }

  function showTooltip(hit, e) {
    const fmt = v => v != null ? `<span style="color:${v>=0?'#3fb950':'#f85149'};font-weight:600">${v>0?'+':''}${v.toFixed(1)}%</span>` : '<span style="color:#6e7681">—</span>';
    tooltipDiv.innerHTML = `
      <div style="font-weight:700;color:${hit.color};margin-bottom:6px;font-size:13px">${hit.name}</div>
      <div style="display:grid;grid-template-columns:auto 1fr;gap:1px 12px;font-size:11px;color:#8b949e">
        <span>1M</span>${fmt(hit.ret_1m)}
        <span>3M</span>${fmt(hit.ret_3m)}
        <span>6M</span>${fmt(hit.ret_6m)}
        <span>1Y</span>${fmt(hit.ret_1y)}
      </div>
      <div style="margin-top:6px;padding-top:6px;border-top:1px solid #21262d;font-size:11px;display:flex;gap:14px">
        <span style="color:#8b949e">Avg RS <span style="color:#58a6ff;font-weight:700">${hit.avg_rs!=null?Math.round(hit.avg_rs):'—'}</span></span>
        <span style="color:#8b949e">${hit.count} หุ้น</span>
      </div>
    `;
    tooltipDiv.style.visibility = "hidden";
    tooltipDiv.style.display = "block";
    const tw = tooltipDiv.offsetWidth, th = tooltipDiv.offsetHeight;
    const vw = window.innerWidth, vh = window.innerHeight;

    // คำนวณทิศที่ trail ยืดออกไป (bubble → oldest trail point)
    // แล้ววาง tooltip ฝั่งตรงข้ามเพื่อไม่บัง trail
    let tailCanvasX = hit.x, tailCanvasY = hit.y;
    if (isShort) {
      if (hit.ret_3m != null) { tailCanvasX = toX(hit.ret_3m); tailCanvasY = toY(hit.ret_1m || 0); }
    } else {
      if (hit.ret_1y != null && hit.ret_6m != null) {
        tailCanvasX = toX(hit.ret_1y); tailCanvasY = toY(hit.ret_6m);
      } else if (hit.ret_6m != null) {
        tailCanvasX = toX(hit.ret_6m); tailCanvasY = toY(hit.ret_3m || 0);
      }
    }
    const tailGoesLeft = tailCanvasX < hit.x; // trail ยืดไปซ้าย → tooltip ขวา
    const tailGoesUp   = tailCanvasY < hit.y; // trail ยืดขึ้นบน  → tooltip ล่าง

    let left = tailGoesLeft ? e.clientX + 16 : e.clientX - tw - 16;
    let top  = tailGoesUp   ? e.clientY + 16 : e.clientY - th - 8;
    // fallback ถ้าชนขอบ
    if (left + tw > vw - 8) left = e.clientX - tw - 16;
    if (left < 8)            left = e.clientX + 16;
    if (top + th > vh - 8)  top  = e.clientY - th - 8;
    if (top < 8)             top  = e.clientY + 16;

    tooltipDiv.style.left = left + "px";
    tooltipDiv.style.top  = top  + "px";
    tooltipDiv.style.visibility = "visible";
  }

  canvas.onmousemove = function(e) {
    const r2 = canvas.getBoundingClientRect();
    const scaleX = (W*dpr)/r2.width/dpr, scaleY = (H*dpr)/r2.height/dpr;
    const mx = (e.clientX-r2.left)*scaleX, my = (e.clientY-r2.top)*scaleY;
    const hit = points.find(p => Math.hypot(p.x-mx, p.y-my) <= p.R + 4);
    if (hit) {
      redraw(hit.name); showTooltip(hit, e);
      canvas.style.cursor = "pointer";
    } else if (pinnedSector) {
      redraw(pinnedSector); tooltipDiv.style.display = "none";
      canvas.style.cursor = "crosshair";
    } else {
      redraw(null); tooltipDiv.style.display = "none";
      canvas.style.cursor = "crosshair";
    }
  };
  canvas.onmouseleave = () => { redraw(pinnedSector); tooltipDiv.style.display = "none"; };
  canvas.onclick = function(e) {
    const r2 = canvas.getBoundingClientRect();
    const scaleX = (W*dpr)/r2.width/dpr, scaleY = (H*dpr)/r2.height/dpr;
    const mx = (e.clientX-r2.left)*scaleX, my = (e.clientY-r2.top)*scaleY;
    const hit = points.find(p => Math.hypot(p.x-mx, p.y-my) <= p.R + 4);
    if (hit) openSectorModal(hit.name);
  };
}


// ============================================================
// RS LEADERS
// ============================================================
function renderRSLeaders() {
  if (!DATA) return;
  const stocks = DATA.stocks.filter(s => (s.rs_score||0) >= 80);

  // stat cards
  const rs90 = stocks.filter(s => s.rs_score >= 90).length;
  const rs80 = stocks.filter(s => s.rs_score >= 80 && s.rs_score < 90).length;
  const avgRS = stocks.length ? Math.round(stocks.reduce((a,s) => a+(s.rs_score||0),0)/stocks.length) : 0;
  const avgRet1m = stocks.length ? (stocks.reduce((a,s)=>a+(s.ret_1m||0),0)/stocks.length).toFixed(1) : '0.0';

  document.getElementById("rs-stat-cards").innerHTML = `
    <div class="card"><div class="card-title">RS 90-99</div><div class="stat-val green">${rs90}</div><div class="stat-label">หุ้น</div></div>
    <div class="card"><div class="card-title">RS 80-89</div><div class="stat-val blue">${rs80}</div><div class="stat-label">หุ้น</div></div>
    <div class="card"><div class="card-title">Avg RS Score</div><div class="stat-val">${avgRS}</div><div class="stat-label">เฉลี่ย RS 80+</div></div>
    <div class="card"><div class="card-title">Avg 1M Return</div><div class="stat-val ${avgRet1m>0?'green':'red'}">${avgRet1m>0?"+":""}${avgRet1m}%</div><div class="stat-label">เฉลี่ย RS 80+</div></div>
  `;

  // sector filter buttons
  const sectors = ["ALL", ...new Set(stocks.map(s=>s.sector||"—").sort())];
  document.getElementById("rs-sector-filters").innerHTML = sectors.map(s =>
    `<button class="filter-btn ${s===rsFilter?'active':''}" onclick="setRSFilter('${s}',this)">${s}</button>`
  ).join("");

  renderRSTable();
}

function setRSFilter(val, btn) {
  rsFilter = val;
  document.querySelectorAll("#rs-sector-filters .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  renderRSTable();
}

function renderRSTable() {
  if (!DATA) return;
  let stocks = DATA.stocks.filter(s => (s.rs_score||0) >= 80);
  if (rsFilter !== "ALL") stocks = stocks.filter(s => s.sector === rsFilter);
  stocks.sort((a,b) => (b.rs_score||0)-(a.rs_score||0));

  document.getElementById("rs-tbody").innerHTML = stocks.map(s => `
    <tr>
      <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score}</span></td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}${dqBadge(s)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector||"—"}</td>
      <td class="r">${s.price?.toFixed(2) ?? "—"}</td>
      <td class="r">${pct(s.ret_1d)}</td>
      <td class="r">${pct(s.ret_1w)}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r">${pct(s.ret_ytd)}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
    </tr>`).join("");
}

// ── RS Leaders tab switch ──────────────────────────────────────
let _rsTab = 'leaders';
function setRsTab(tab, btn) {
  _rsTab = tab;
  document.querySelectorAll("#page-rs-leaders .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById("rs-panel-leaders").style.display  = tab === 'leaders'  ? '' : 'none';
  document.getElementById("rs-panel-emerging").style.display = tab === 'emerging' ? '' : 'none';
  if (tab === 'emerging') renderEmergingLeaders();
}

function renderEmergingLeaders() {
  if (!DATA) return;
  // หุ้นที่ RS กำลังไต่ขึ้น: RS 35-79 + ret_1m > 3% + above_ema50
  // ถ้ามี rs_momentum ใช้เลย, ถ้าไม่มีใช้ rs_score + momentum proxy
  let stocks = DATA.stocks.filter(s => {
    const rs = s.rs_score || 0;
    return rs >= 35 && rs < 80 && (s.ret_1m || 0) >= 3 && s.above_ema50 === true;
  });
  // เรียงตาม rs_momentum ถ้ามี, ไม่มีเรียงตาม ret_1m
  stocks.sort((a, b) => {
    const am = a.rs_momentum ?? (a.ret_1m || 0);
    const bm = b.rs_momentum ?? (b.ret_1m || 0);
    return bm - am;
  });
  stocks = stocks.slice(0, 100);

  document.getElementById("emerging-tbody").innerHTML = stocks.map(s => {
    const mom = s.rs_momentum != null
      ? `<span style="color:${s.rs_momentum>0?'#3fb950':'#f85149'};font-weight:700">${s.rs_momentum>0?'+':''}${s.rs_momentum}</span>`
      : `<span class="text2" title="กด Quick Update เพื่อคำนวณ">—</span>`;
    return `<tr>
      <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score}</span></td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector||"—"}</td>
      <td class="r">${s.price?.toFixed(2) ?? "—"}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r">${mom}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
    </tr>`;
  }).join("");
}

// ── Stage Analysis ─────────────────────────────────────────────
let _stageFilter = 'all';

function setStageFilter(val, btn) {
  _stageFilter = val;
  document.querySelectorAll("#page-stage .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  renderStageTable();
}

function renderStage() {
  if (!DATA) return;
  const stocks = DATA.stocks;
  const counts = {1:0, 2:0, 3:0, 4:0, null:0};
  stocks.forEach(s => { const st = getStage(s); counts[st] = (counts[st]||0)+1; });

  document.getElementById("stage-stat-cards").innerHTML = `
    <div class="card" style="border-top:3px solid #3fb950">
      <div class="card-title">Stage 2 — Advancing</div>
      <div class="stat-val green">${counts[2]||0}</div>
      <div class="stat-label">หุ้น ✅ โซนซื้อ</div>
    </div>
    <div class="card" style="border-top:3px solid #58a6ff">
      <div class="card-title">Stage 1 — Basing</div>
      <div class="stat-val blue">${counts[1]||0}</div>
      <div class="stat-label">หุ้น — รอสัญญาณ</div>
    </div>
    <div class="card" style="border-top:3px solid #e3b341">
      <div class="card-title">Stage 3 — Topping</div>
      <div class="stat-val yellow">${counts[3]||0}</div>
      <div class="stat-label">หุ้น — ระวัง</div>
    </div>
    <div class="card" style="border-top:3px solid #f85149">
      <div class="card-title">Stage 4 — Declining</div>
      <div class="stat-val red">${counts[4]||0}</div>
      <div class="stat-label">หุ้น — หลีกเลี่ยง</div>
    </div>
  `;
  renderStageTable();
}

function renderStageTable() {
  if (!DATA) return;
  let stocks = DATA.stocks.map(s => ({ ...s, _stage: getStage(s) }));
  if (_stageFilter !== 'all') stocks = stocks.filter(s => s._stage === +_stageFilter);
  // Priority: Stage 2 (โซนซื้อ) → 1 (Basing) → 3 (Topping) → 4 (Declining) → null
  const stagePrio = { 2:1, 1:2, 3:3, 4:4 };
  stocks.sort((a, b) => {
    const pa = stagePrio[a._stage] ?? 9, pb = stagePrio[b._stage] ?? 9;
    if (pa !== pb) return pa - pb;
    return (b.rs_score||0) - (a.rs_score||0);
  });

  document.getElementById("stage-count").textContent = `แสดง ${stocks.length} หุ้น`;
  document.getElementById("stage-tbody").innerHTML = stocks.map(s => {
    const slope = s.ema200_slope_pct;
    const slopeHtml = slope != null
      ? `<span style="color:${slope>=0?'#3fb950':slope>=-1.5?'#e3b341':'#f85149'};font-size:11px">${slope>0?'+':''}${slope.toFixed(2)}%</span>`
      : `<span class="text2" style="font-size:10px">Quick Update</span>`;
    const atr = s.atr14_pct != null ? s.atr14_pct.toFixed(2)+'%' : '—';
    return `<tr>
      <td>${stageBadge(s._stage)}</td>
      <td class="r"><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score??'—'}</span></td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector||'—'}</td>
      <td class="r">${s.price?.toFixed(2)??"—"}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
      <td class="r">${slopeHtml}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r" style="font-size:11px;color:var(--text2)">${atr}</td>
    </tr>`;
  }).join("");
}

// ── 52W New High Chart ─────────────────────────────────────────
let _nhLoaded = false;
function resetNhCache() { _nhLoaded = false; }  // call after Quick Update
async function loadNewHighChart() {
  if (_nhLoaded) return;
  try {
    const r = await fetch("/api/market-internals");
    if (!r.ok) { document.getElementById("new-high-loading").textContent = "ไม่สามารถโหลดได้"; return; }
    const d = await r.json();
    if (d.error) { document.getElementById("new-high-loading").textContent = d.error; return; }
    _nhLoaded = true;
    document.getElementById("new-high-loading").style.display = "none";
    const canvas = document.getElementById("new-high-chart");
    canvas.style.display = "block";
    drawNewHighChart(canvas, d);
  } catch(e) {
    document.getElementById("new-high-loading").textContent = "โหลดไม่สำเร็จ: " + e.message;
  }
}

function drawNewHighChart(canvas, d) {
  const W = canvas.offsetWidth || 800;
  const H = 140;
  canvas.width  = W;
  canvas.height = H;
  const ctx = canvas.getContext("2d");
  const NH = d.new_highs, NL = d.new_lows, dates = d.dates;
  const n  = NH.length;
  if (!n) return;

  const maxVal = Math.max(...NH, ...NL, 1);
  const pad = { l: 40, r: 10, t: 10, b: 28 };
  const cW = W - pad.l - pad.r;
  const cH = H - pad.t - pad.b;

  const toX = i => pad.l + (i / (n - 1)) * cW;
  const toY = v => pad.t + cH - (v / maxVal) * cH;

  ctx.clearRect(0, 0, W, H);

  // grid lines
  ctx.strokeStyle = "rgba(139,148,158,0.15)";
  ctx.lineWidth = 1;
  [0.25, 0.5, 0.75, 1].forEach(f => {
    const y = pad.t + cH * (1 - f);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.fillStyle = "rgba(139,148,158,0.6)";
    ctx.font = "9px Segoe UI,system-ui,sans-serif";
    ctx.textAlign = "right";
    ctx.fillText(Math.round(maxVal * f), pad.l - 4, y + 3);
  });

  // New High area
  ctx.beginPath();
  NH.forEach((v, i) => i === 0 ? ctx.moveTo(toX(i), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.lineTo(toX(n-1), toY(0)); ctx.lineTo(toX(0), toY(0)); ctx.closePath();
  ctx.fillStyle = "rgba(63,185,80,0.15)"; ctx.fill();
  ctx.beginPath();
  NH.forEach((v, i) => i === 0 ? ctx.moveTo(toX(i), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.strokeStyle = "#3fb950"; ctx.lineWidth = 2; ctx.stroke();

  // New Low area
  ctx.beginPath();
  NL.forEach((v, i) => i === 0 ? ctx.moveTo(toX(i), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.lineTo(toX(n-1), toY(0)); ctx.lineTo(toX(0), toY(0)); ctx.closePath();
  ctx.fillStyle = "rgba(248,81,73,0.12)"; ctx.fill();
  ctx.beginPath();
  NL.forEach((v, i) => i === 0 ? ctx.moveTo(toX(i), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.strokeStyle = "#f85149"; ctx.lineWidth = 1.5; ctx.stroke();

  // X axis labels — แสดงทุก 2 สัปดาห์
  ctx.fillStyle = "rgba(139,148,158,0.8)";
  ctx.font = "9px Segoe UI,system-ui,sans-serif"; ctx.textAlign = "center";
  const step = Math.max(1, Math.round(n / 10));
  dates.forEach((dt, i) => {
    if (i % step === 0 || i === n - 1) {
      ctx.fillText(dt.slice(5), toX(i), H - 6);
    }
  });
}

// ============================================================
// SECTORS
// ============================================================
function openSectorPage(name) {
  const idxSym = _getSectorToIdx()[name];
  if (idxSym) { openIdxChartModal(idxSym); }
  else { openSectorModal(name); }
}
function setSectorView(v, btn) {
  sectorView = v;
  document.querySelectorAll("#sector-view-btns .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  renderSectors();
}
function sortSectors(key) {
  if (sectorSort.key === key) sectorSort.dir *= -1;
  else { sectorSort.key = key; sectorSort.dir = -1; }
  renderSectorTable();
}

// ============================================================
// SECTOR RANK VIEW — เทียบอันดับข้าม 1M/3M/6M/1Y ในสเกลเดียว
// ============================================================
let sectorMode = 'pct';                         // 'pct' | 'rank'
let sectorRankSort = { key: 'avg', dir: 1 };    // อันดับน้อย = ดี -> default ascending

function setSectorMode(m, btn) {
  sectorMode = m;
  document.querySelectorAll('#sector-mode-btns .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const pctCard  = document.getElementById('sector-pct-card');
  const rankCard = document.getElementById('sector-rank-card');
  if (pctCard)  pctCard.style.display  = m === 'pct'  ? '' : 'none';
  if (rankCard) rankCard.style.display = m === 'rank' ? '' : 'none';
  renderSectors();
}

function sortSectorRank(key) {
  if (sectorRankSort.key === key) {
    sectorRankSort.dir *= -1;
  } else {
    sectorRankSort.key = key;
    sectorRankSort.dir = key === 'mom' ? -1 : 1;  // mom เริ่มจากบวกมาก (rotation in) ก่อน
  }
  renderSectorRankTable();
}

function _rankBadge(rank, total, pct) {
  if (rank == null) return '<span style="color:var(--text2)">—</span>';
  const q = rank / total;
  const c = q <= 0.25 ? '#3fb950' : q <= 0.5 ? '#e3b341' : q <= 0.75 ? '#f0883e' : '#f85149';
  const pctTxt = pct != null
    ? ` <span style="font-size:10px;color:var(--text2)">${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%</span>` : '';
  return `<span style="background:${c}22;color:${c};border:1px solid ${c}55;` +
         `padding:1px 6px;border-radius:4px;font-weight:700;font-size:11px">#${rank}</span>${pctTxt}`;
}

// Heat strip: ช่องสี 5 ช่อง (1Y→1W) ตามโซนอันดับ — สีชุดเดียวกับ _rankBadge
// เขียว = top 25%, เหลือง = 25-50%, ส้ม = 50-75%, แดง = bottom 25%
const _TRAJ_LABELS = ['1Y', '6M', '3M', '1M', '1W'];
function _rankSpark(seq, total) {
  if (seq.every(v => v == null)) return '<span style="color:var(--text2)">—</span>';
  const cells = seq.map((v, i) => {
    const lbl = _TRAJ_LABELS[i];
    if (v == null) {
      return `<span title="${lbl}: ไม่มีข้อมูล" style="flex:1;height:16px;border-radius:3px;` +
             `background:rgba(255,255,255,0.05);border:1px solid var(--border)"></span>`;
    }
    const q = v / total;
    const c = q <= 0.25 ? '#3fb950' : q <= 0.5 ? '#e3b341' : q <= 0.75 ? '#f0883e' : '#f85149';
    // ช่องสุดท้าย (1W = ล่าสุด) แสดงเลขอันดับกำกับ — ช่องอื่นดูจาก hover
    const num = i === seq.length - 1
      ? `<span style="font-size:10px;font-weight:700;color:#0d1117">#${v}</span>` : '';
    return `<span title="${lbl}: อันดับ ${v}/${total}" style="flex:1;height:16px;border-radius:3px;` +
           `background:${c};display:inline-flex;align-items:center;justify-content:center">${num}</span>`;
  }).join('');
  return `<span style="display:inline-flex;gap:3px;width:230px;vertical-align:middle">${cells}</span>`;
}

function renderSectorRankTable() {
  if (!DATA) return;
  const el = document.getElementById('sector-rank-tbody');
  if (!el) return;
  const groups = [...(sectorView === 'sector' ? DATA.sectors : DATA.industries)];

  // จัดอันดับต่อ horizon (1 = return สูงสุด) — กลุ่มที่ค่าเป็น null ไม่ถูกจัด
  const H = [['ret_1w', 'r1w'], ['ret_1m', 'r1m'], ['ret_3m', 'r3m'], ['ret_6m', 'r6m'], ['ret_1y', 'r1y']];
  const ranks = {}, totals = {};
  groups.forEach(g => ranks[g.name] = {});
  H.forEach(([f, k]) => {
    const valid = groups.filter(g => g[f] != null).sort((a, b) => b[f] - a[f]);
    valid.forEach((g, i) => ranks[g.name][k] = i + 1);
    totals[k] = valid.length;
  });

  const rows = groups.map(g => {
    const r = ranks[g.name];
    const seq = [r.r1y, r.r6m, r.r3m, r.r1m, r.r1w];  // เรียงตาม trajectory ซ้าย->ขวา
    // avg ใช้ 4 horizon เดิม (1M/3M/6M/1Y) — 1W มีไว้เพิ่มความละเอียดปลายเส้น spark เท่านั้น
    const have = [r.r1y, r.r6m, r.r3m, r.r1m].filter(x => x != null);
    return {
      g, name: g.name,
      r1m: r.r1m ?? null, r3m: r.r3m ?? null, r6m: r.r6m ?? null, r1y: r.r1y ?? null,
      avg: have.length === 4 ? +(have.reduce((a, b) => a + b, 0) / 4).toFixed(1) : null,
      mom: (r.r1m != null && r.r1y != null) ? r.r1y - r.r1m : null,
      seq,
    };
  });

  const { key, dir } = sectorRankSort;
  rows.sort((a, b) => {
    const av = a[key], bv = b[key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;                       // ข้อมูลไม่พอ -> ท้ายตารางเสมอ
    if (bv == null) return -1;
    return (av - bv) * dir;
  });

  el.innerHTML = rows.map(r => {
    const abbr = SECTOR_ABBR[r.name] || "";
    const abbrTag = abbr && abbr !== "—"
      ? ` <span style="font-size:10px;color:var(--blue);background:#0d2847;padding:1px 5px;border-radius:3px;margin-left:4px">${abbr}</span>` : "";
    let momHtml = '<span style="color:var(--text2)">—</span>';
    if (r.mom != null) {
      const fire = r.mom >= 10 ? ' 🔥' : r.mom <= -10 ? ' ❄️' : '';
      const c = r.mom > 0 ? '#3fb950' : r.mom < 0 ? '#f85149' : 'var(--text2)';
      momHtml = `<span style="color:${c};font-weight:${fire ? 700 : 400}">` +
                `${r.mom > 0 ? '+' : ''}${r.mom}${fire}</span>`;
    }
    return `
    <tr style="cursor:pointer" onclick="openSectorPage('${r.name.replace(/'/g, "\\'")}')">
      <td style="font-size:12px"><strong>${r.name}</strong>${abbrTag}</td>
      <td class="r">${_rankBadge(r.r1m, totals.r1m, r.g.ret_1m)}</td>
      <td class="r">${_rankBadge(r.r3m, totals.r3m, r.g.ret_3m)}</td>
      <td class="r">${_rankBadge(r.r6m, totals.r6m, r.g.ret_6m)}</td>
      <td class="r">${_rankBadge(r.r1y, totals.r1y, r.g.ret_1y)}</td>
      <td class="r" style="font-weight:700">${r.avg ?? '—'}</td>
      <td class="r">${momHtml}</td>
      <td>${_rankSpark(r.seq, Math.max(totals.r1m || 1, totals.r1y || 1))}</td>
    </tr>`;
  }).join('');
}

function renderSectors() {
  if (sectorMode === 'rank') renderSectorRankTable();
  else renderSectorTable();
}
function renderSectorTable() {
  if (!DATA) return;

  // นับหุ้นที่ทำ 52W High ใหม่ (price >= high_52w) ต่อ sector/industry
  const groupKey = sectorView === 'sector' ? 'sector' : 'industry';
  const nhMap = {};
  DATA.stocks.forEach(s => {
    const key = s[groupKey];
    if (!key) return;
    if (!nhMap[key]) nhMap[key] = 0;
    if (s.high_52w > 0 && s.price >= s.high_52w) nhMap[key]++;
  });

  // คำนวณค่าเฉลี่ย 1M% ของหุ้นทุกตัวในตลาด
  const stocks1m = DATA.stocks.filter(s => s.ret_1m != null);
  const mktRet1m = stocks1m.length ? stocks1m.reduce((a, s) => a + s.ret_1m, 0) / stocks1m.length : 0;

  const data = [...(sectorView === "sector" ? DATA.sectors : DATA.industries)].map(s => ({
    ...s,
    newHighCount: nhMap[s.name] ?? 0,
    newHighPct:   s.count > 0 ? (nhMap[s.name] ?? 0) / s.count * 100 : 0,
    vs_set_1m:   s.ret_1m != null ? s.ret_1m - mktRet1m : null,
  }));
  data.sort((a,b) => {
    const av = a[sectorSort.key], bv = b[sectorSort.key];
    return ((bv ?? -Infinity) - (av ?? -Infinity)) * sectorSort.dir;
  });

  document.getElementById("sectors-tbody").innerHTML = data.map((s,i) => {
    const abbr = SECTOR_ABBR[s.name] || "";
    const abbrTag = abbr && abbr !== "—" ? ` <span style="font-size:10px;color:var(--blue);background:#0d2847;padding:1px 5px;border-radius:3px;margin-left:4px">${abbr}</span>` : "";
    const nhRatio = s.count > 0 ? s.newHighCount / s.count : 0;
    const nhColor = nhRatio >= 0.25 ? 'green' : nhRatio >= 0.1 ? 'yellow' : 'text2';
    return `
    <tr style="cursor:pointer" onclick="openSectorPage('${s.name.replace(/'/g,"\\'")}')">
      <td class="text2">${i+1}</td>
      <td><strong>${s.name}</strong>${abbrTag}${sectorTvLink(s.name)}</td>
      <td class="r text2">${s.count}</td>
      <td class="r">${pct(s.ret_1d,2)}</td>
      <td class="r">${pct(s.ret_1w,2)}</td>
      <td class="r">${pct(s.ret_1m,2)}</td>
      <td class="r">${s.vs_set_1m != null ? `<span style="color:${s.vs_set_1m>0?'var(--green)':s.vs_set_1m<0?'var(--red)':'var(--text2)'}; font-weight:${Math.abs(s.vs_set_1m)>=2?'700':'400'}">${s.vs_set_1m>0?'+':''}${s.vs_set_1m.toFixed(1)}%</span>` : '—'}</td>
      <td class="r">${pct(s.ret_3m,2)}</td>
      <td class="r">${pct(s.ret_1y,2)}</td>
      <td class="r"><span class="${rsColor(s.avg_rs)}">${s.avg_rs!=null?Math.round(s.avg_rs):"—"}</span></td>
      <td class="r">${s.pct_above_ema50!=null?s.pct_above_ema50+"%":"—"}</td>
      <td class="r"><span class="${nhColor}" style="font-weight:600;cursor:pointer;text-decoration:underline dotted" onclick="event.stopPropagation();openSectorModal('${s.name.replace(/'/g,"\\'")}',true)">${s.newHighCount}</span><span class="text2" style="font-size:10px">/${s.count}</span></td>
      <td class="r">${fmtValuation(s.avg_pe, 'pe')}</td>
      <td class="r">${fmtValuation(s.avg_pbv, 'pbv')}</td>
      <td class="r">${s.avg_div_yield != null ? `<span class="${s.avg_div_yield >= 4 ? 'green' : s.avg_div_yield >= 2 ? 'yellow' : 'text2'}" style="font-weight:600">${s.avg_div_yield.toFixed(1)}%</span>` : '—'}</td>
    </tr>`;
  }).join("");
}

// ============================================================
// STOCKS ALL
// ============================================================
function sortStocks(key) {
  if (stockSort.key === key) stockSort.dir *= -1;
  else { stockSort.key = key; stockSort.dir = -1; }
  // อัปเดต header indicator
  const headers = {
    rs_score:'sh-rs', symbol:'sh-sym', price:'sh-px',
    ret_1d:'sh-1d', ret_1w:'sh-1w', ret_1m:'sh-1m', ret_3m:'sh-3m', ret_ytd:'sh-ytd',
    mkt_cap:'sh-cap', vol_today:'sh-vol',
    above_ema50_n:'sh-e50', above_ema200_n:'sh-e200', ath_pct:'sh-ath',
    atr14_pct:'sh-atr', _stage:'sh-stage'
  };
  document.querySelectorAll('#tbl-stocks th').forEach(th => {
    const txt = th.textContent.replace(/[↑↓↕]/g,'').trim();
    th.textContent = txt + (th.id && headers[stockSort.key]===th.id ? (stockSort.dir===-1?' ↓':' ↑') : ' ↕');
  });
  renderStocksTable();
}
function filterStocks() {
  stockSearch = document.getElementById("stock-search").value.toLowerCase();
  renderStocksTable();
}
function setIndustryFilter(val, btn) {
  industryFilter = val;
  sectorFilter = "ALL";
  document.querySelectorAll("#industry-filter-btns .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  _rebuildSectorBtns();
  renderStocksTable();
}

function setSectorFilter(val, btn) {
  sectorFilter = val;
  document.querySelectorAll("#sector-filter-btns .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  renderStocksTable();
}

function setStocksStageFilter(val, btn) {
  stageFilter = val;
  document.querySelectorAll("#stage-filter-btns .filter-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  renderStocksTable();
}

function setDqOnlyFilter(v) {
  dqOnlyFilter = v;
  renderStocksTable();
}

// เรียกจาก banner คุณภาพข้อมูล — พาไปหน้า "หุ้นทั้งหมด" พร้อมกรองเฉพาะหุ้นที่ติด badge ⚠
function goToDqStocks() {
  const btn = [...document.querySelectorAll(".nav-btn")].find(b => b.getAttribute("onclick")?.includes("'stocks'"));
  showPage("stocks", btn);
  setDqOnlyFilter(true);
}

function _rebuildSectorBtns() {
  const base = industryFilter === "ALL" ? DATA.stocks : DATA.stocks.filter(s => s.industry === industryFilter);
  const sectors = ["ALL", ...new Set(base.map(s => s.sector || "Unknown").sort())];
  document.getElementById("sector-filter-btns").innerHTML = sectors.map(s =>
    `<button class="filter-btn ${s===sectorFilter?'active':''}" onclick="setSectorFilter('${s.replace(/'/g,"\\'")}',this)">${s}</button>`
  ).join("");
}

function renderStocks() {
  if (!DATA) return;
  const industries = ["ALL", ...new Set(DATA.stocks.map(s => s.industry || "Unknown").sort())];
  document.getElementById("industry-filter-btns").innerHTML = industries.map(ig =>
    `<button class="filter-btn ${ig===industryFilter?'active':''}" onclick="setIndustryFilter('${ig.replace(/'/g,"\\'")}',this)">${ig}</button>`
  ).join("");
  _rebuildSectorBtns();
  renderStocksTable();
}

function renderStocksTable() {
  if (!DATA) return;
  let stocks = [...DATA.stocks];
  if (industryFilter !== "ALL") stocks = stocks.filter(s => s.industry === industryFilter);
  if (sectorFilter   !== "ALL") stocks = stocks.filter(s => s.sector   === sectorFilter);
  if (stockSearch) stocks = stocks.filter(s =>
    s.symbol.toLowerCase().includes(stockSearch)
  );
  if (dqOnlyFilter) stocks = stocks.filter(s => s.dq && s.dq.rs_eligible === false);
  const dqChip = document.getElementById("dq-only-chip");
  if (dqChip) dqChip.style.display = dqOnlyFilter ? "inline-flex" : "none";
  // แปลง bool -> number เพื่อ sort ได้ + คำนวณ _stage ไว้ล่วงหน้า
  stocks = stocks.map(s => ({
    ...s,
    above_ema50_n:  s.above_ema50  === true ? 1 : s.above_ema50  === false ? 0 : null,
    above_ema200_n: s.above_ema200 === true ? 1 : s.above_ema200 === false ? 0 : null,
    _stage: getStage(s),
  }));
  if (stageFilter !== "ALL") stocks = stocks.filter(s => s._stage === stageFilter || s._stage === Number(stageFilter));

  stocks.sort((a,b) => {
    const av = a[stockSort.key], bv = b[stockSort.key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1; if (bv == null) return -1;
    if (typeof av === 'string') return av.localeCompare(bv) * stockSort.dir;
    return (bv - av) * stockSort.dir;
  });

  document.getElementById("stocks-count").textContent = `แสดง ${stocks.length} จาก ${DATA.total} ตัว`;

  // แสดงทีละ 100 แบบ lazy load
  let stockPage = 1;
  const PAGE_SIZE = 100;

  window._currentStockList = stocks;
  window._reRenderStockBadges = () => renderStockRows(window._currentStockList, window._stockPage || 1);

  function renderStockRows(list, page) {
    const slice = list.slice(0, page * PAGE_SIZE);
    document.getElementById("stocks-tbody").innerHTML = slice.map(s => `
      <tr data-sym="${s.symbol}">
        <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score??"-"}</span></td>
        <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}${dqBadge(s)}${insiderBadge(s.symbol)}${shortBadge(s.symbol)}${nvdrBadge(s.symbol)}</td>
        <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
        <td style="font-size:11px">${s.industry||"—"}</td>
        <td style="font-size:11px">${s.sector||"—"}</td>
        <td class="r">${s.price?.toFixed(2)??"—"}</td>
        <td class="r">${pct(s.ret_1d)}</td>
        <td class="r">${pct(s.ret_1w)}</td>
        <td class="r">${pct(s.ret_1m)}</td>
        <td class="r">${pct(s.ret_3m)}</td>
        <td class="r">${pct(s.ret_ytd)}</td>
        <td class="r"><canvas class="spark-canvas" width="60" height="24" style="display:block"></canvas></td>
        <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
        <td class="r" style="font-size:11px">${rvolHtml(s)}</td>
        <td class="r">${emaBadge(s.above_ema50)}</td>
        <td class="r">${emaBadge(s.above_ema200)}</td>
        <td class="r"><span class="${s.ath_pct != null ? (s.ath_pct >= -5 ? 'green' : s.ath_pct >= -20 ? 'yellow' : s.ath_pct >= -40 ? 'text2' : 'red') : 'text2'}" style="font-size:11px">${s.ath_pct != null ? s.ath_pct.toFixed(1)+'%' : '—'}</span></td>
        <td class="r" style="font-size:11px;color:var(--text2)">${s.atr14_pct != null ? s.atr14_pct.toFixed(2)+'%' : '—'}</td>
        <td class="r">${stageBadge(s._stage)}</td>
      </tr>`).join("");
    renderSparklinesInTable("stocks-tbody", window._currentStockList);

    if (slice.length < list.length) {
      document.getElementById("stocks-tbody").innerHTML +=
        `<tr><td colspan="19" style="text-align:center;padding:14px">
          <button onclick="loadMoreStocks()" style="background:var(--bg3);border:1px solid var(--border);color:var(--text);padding:7px 20px;border-radius:6px;cursor:pointer;font-size:13px">
            โหลดเพิ่ม (${slice.length}/${list.length} ตัว)
          </button>
        </td></tr>`;
    }
  }

  window._currentStockList = stocks;
  window._stockPage = 1;
  renderStockRows(stocks, 1);

  window.loadMoreStocks = function() {
    window._stockPage++;
    renderStockRows(window._currentStockList, window._stockPage);
  };
}

// ============================================================
// WATCHLIST
// ============================================================
function _wlPopulateSymList() {
  const dl = document.getElementById("wl-sym-list");
  if (dl && dl.children.length === 0) {
    const setSyms = (DATA?.stocks || []).map(s => s.symbol).sort();
    const drSyms  = (_drData || []).map(s => s.sym).sort();
    dl.innerHTML =
      setSyms.map(s => `<option value="${s}" label="${s} (SET)">`).join("") +
      drSyms.map(s => `<option value="${s}" label="${s} (DR/DRx)">`).join("");
  }
}

function addToWatchlist() {
  let raw = document.getElementById("wl-input").value.trim().toUpperCase();
  if (!raw) return;

  // ถ้าพิมพ์ "DR:AAPL" ให้เก็บตรง ๆ
  // ถ้าพิมพ์ "AAPL" → ตรวจว่าอยู่ใน _drData → เติม "DR:" ให้อัตโนมัติ
  let sym = raw;
  if (!sym.startsWith("DR:")) {
    const matchesDR  = (_drData || []).some(s => s.sym === sym);
    const matchesSET = (DATA?.stocks || []).some(s => s.symbol === sym);
    if (matchesDR && !matchesSET) sym = "DR:" + sym;
  }

  if (!watchlist.includes(sym)) {
    watchlist.push(sym);
    localStorage.setItem("set_wl", JSON.stringify(watchlist));
  }
  document.getElementById("wl-input").value = "";
  renderWatchlist();
}
function removeFromWatchlist(sym) {
  watchlist = watchlist.filter(s => s !== sym);
  localStorage.setItem("set_wl", JSON.stringify(watchlist));
  renderWatchlist();
}
function _wlAlertCell(sym) {
  const alerts = _loadAlerts();
  const active = alerts.filter(a => a.symbol === sym && !a.triggered);
  if (active.length === 0) {
    return `<button class="wl-alert-btn" onclick="openWlAlertModal('${sym}')" title="ตั้งแจ้งเตือนราคา">+</button>`;
  }
  const tags = active.map(a => {
    const condTh = a.condition === "above" ? "↑" : "↓";
    return `<span class="wl-alert-tag ${a.condition}" onclick="openWlAlertModal('${sym}')" title="${a.condition==='above'?'แจ้งเมื่อราคาขึ้นถึง':'แจ้งเมื่อราคาลงถึง'} ${a.targetPrice}">${condTh}${a.targetPrice.toFixed(2)}</span>`;
  }).join(" ");
  return tags;
}

function renderWatchlist() {
  _wlPopulateSymList();
  if (!DATA) {
    document.getElementById("wl-tbody").innerHTML =
      `<tr><td colspan="17"><div class="empty">กำลังโหลดข้อมูล...</div></td></tr>`;
    return;
  }
  const stockMap = Object.fromEntries(DATA.stocks.map(s => [s.symbol, s]));
  const drMap    = Object.fromEntries((_drData || []).map(s => [s.sym, s]));

  // ถ้ามี symbol ใน watchlist ที่ไม่เจอใน SET และ DR ยังไม่โหลด → fetch DR แล้ว re-render
  if (!_drData) {
    const hasUnknown = watchlist.some(sym => {
      const clean = sym.startsWith("DR:") ? sym.slice(3) : sym;
      return !stockMap[sym] && !stockMap[clean];
    });
    if (hasUnknown) {
      document.getElementById("wl-tbody").innerHTML =
        `<tr><td colspan="17"><div class="empty" style="font-size:12px">กำลังโหลดข้อมูล DR/DRx...</div></td></tr>`;
      fetch('/api/dr')
        .then(r => r.json())
        .then(d => {
          if (d.stocks) {
            _drData = d.stocks;
            _drLoaded = true;
          }
          renderWatchlist();
        })
        .catch(() => renderWatchlist());
      return;
    }
  }

  if (watchlist.length === 0) {
    document.getElementById("wl-tbody").innerHTML =
      `<tr><td colspan="17"><div class="empty">ยังไม่มีหุ้นใน Watchlist<br>พิมพ์ Symbol (เช่น PTT, AAPL, NVDA) แล้วกด + เพิ่ม</div></td></tr>`;
    return;
  }

  const rows = watchlist.map(sym => {
    // ── DR row ──
    if (sym.startsWith("DR:")) {
      const under = sym.slice(3);
      const d = drMap[under];
      if (!d) return `
        <tr>
          <td class="text2">—</td>
          <td><strong style="color:var(--blue)">${under}</strong>
            <span style="font-size:9px;background:rgba(88,166,255,.15);color:var(--blue);border-radius:3px;padding:1px 4px;margin-left:4px">DR</span>
          </td>
          <td colspan="14" class="text2">ยังไม่โหลดข้อมูล DR — ไปหน้า DR ก่อน</td>
          <td><button class="wl-del-btn" onclick="removeFromWatchlist('${sym}')">✕</button></td>
        </tr>`;

      const chgCls = (d.chg ?? 0) >= 0 ? "green" : "red";
      const tvSym  = yfToTVSym(d.yf);
      const tvHref = `https://www.tradingview.com/chart/?symbol=${tvSym}&interval=D`;
      return `
        <tr data-sym="${sym}" data-dr-close='${JSON.stringify(d.close100||[])}'>
          <td><span class="${rsColor(d.rs_score)}" style="font-weight:700">${d.rs_score??"-"}</span></td>
          <td>
            <strong class="sym-link" style="color:var(--blue)" onclick="openDRChartModal('${under}')">${under}</strong>
            <span style="font-size:9px;background:rgba(88,166,255,.15);color:var(--blue);border-radius:3px;padding:1px 4px;margin-left:3px">DR</span>
            <a class="tv-link" href="${tvHref}" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗</a>
          </td>
          <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${d.name}</td>
          <td style="font-size:11px;color:var(--text2)">${d.region}</td>
          <td class="r"><span style="font-size:12px;font-weight:600">${_drFmtPrice(d.price)}</span>
            <br><span class="${chgCls}" style="font-size:10px">${(d.chg??0)>=0?"+":""}${(d.chg??0).toFixed(2)}%</span>
          </td>
          <td class="r text2" style="font-size:11px">—</td>
          <td class="r">${pct(d.ret_1w)}</td>
          <td class="r">${pct(d.ret_1m)}</td>
          <td class="r">${pct(d.ret_3m)}</td>
          <td class="r">${pct(d.ret_ytd)}</td>
          <td class="r">${pct(d.ret_1y)}</td>
          <td class="r"><canvas class="spark-canvas wl-dr-spark" width="60" height="24" style="display:block"></canvas></td>
          <td class="r" style="font-size:11px">${_drFmtCap(d.mkt_cap)}</td>
          <td class="r text2">—</td>
          <td class="r text2">—</td>
          <td class="r" style="white-space:nowrap">${_wlAlertCell(sym)}</td>
          <td><button class="wl-del-btn" onclick="removeFromWatchlist('${sym}')">✕</button></td>
        </tr>`;
    }

    // ── SET row (หรือ DR ที่เก็บโดยไม่มี prefix — migrate อัตโนมัติ) ──
    const s = stockMap[sym];
    if (!s) {
      // ตรวจว่าเป็น DR underlying ที่ไม่มี prefix
      const drMatch = drMap[sym];
      if (drMatch) {
        // migrate: เปลี่ยน sym → DR:sym ใน localStorage แล้ว re-render
        const idx = watchlist.indexOf(sym);
        if (idx !== -1) {
          watchlist[idx] = "DR:" + sym;
          localStorage.setItem("set_wl", JSON.stringify(watchlist));
        }
        // render ในรอบนี้เป็น DR row โดยใช้ key ใหม่
        const newSym = "DR:" + sym;
        const under = sym;
        const d = drMatch;
        const chgCls2 = (d.chg ?? 0) >= 0 ? "green" : "red";
        const tvSym2  = yfToTVSym(d.yf);
        const tvHref2 = `https://www.tradingview.com/chart/?symbol=${tvSym2}&interval=D`;
        return `
          <tr data-sym="${newSym}" data-dr-close='${JSON.stringify(d.close100||[])}'>
            <td><span class="${rsColor(d.rs_score)}" style="font-weight:700">${d.rs_score??"-"}</span></td>
            <td>
              <strong class="sym-link" style="color:var(--blue)" onclick="openDRChartModal('${under}')">${under}</strong>
              <span style="font-size:9px;background:rgba(88,166,255,.15);color:var(--blue);border-radius:3px;padding:1px 4px;margin-left:3px">DR</span>
              <a class="tv-link" href="${tvHref2}" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗</a>
            </td>
            <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${d.name}</td>
            <td style="font-size:11px;color:var(--text2)">${d.region}</td>
            <td class="r"><span style="font-size:12px;font-weight:600">${_drFmtPrice(d.price)}</span>
              <br><span class="${chgCls2}" style="font-size:10px">${(d.chg??0)>=0?"+":""}${(d.chg??0).toFixed(2)}%</span>
            </td>
            <td class="r text2" style="font-size:11px">—</td>
            <td class="r">${pct(d.ret_1w)}</td>
            <td class="r">${pct(d.ret_1m)}</td>
            <td class="r">${pct(d.ret_3m)}</td>
            <td class="r">${pct(d.ret_ytd)}</td>
            <td class="r">${pct(d.ret_1y)}</td>
            <td class="r"><canvas class="spark-canvas wl-dr-spark" width="60" height="24" style="display:block"></canvas></td>
            <td class="r" style="font-size:11px">${_drFmtCap(d.mkt_cap)}</td>
            <td class="r text2">—</td>
            <td class="r text2">—</td>
            <td class="r" style="white-space:nowrap">${_wlAlertCell(newSym)}</td>
            <td><button class="wl-del-btn" onclick="removeFromWatchlist('${newSym}')">✕</button></td>
          </tr>`;
      }
      return `
        <tr>
          <td class="text2">—</td><td><strong>${sym}</strong></td>
          <td colspan="14" class="text2">ไม่พบข้อมูล</td>
          <td><button class="wl-del-btn" onclick="removeFromWatchlist('${sym}')">✕</button></td>
        </tr>`;
    }
    return `
      <tr data-sym="${s.symbol}">
        <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score??"-"}</span></td>
        <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
        <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
        <td style="font-size:11px">${s.sector||"—"}</td>
        <td class="r">${s.price?.toFixed(2)??"—"}</td>
        <td class="r">${pct(s.ret_1d)}</td>
        <td class="r">${pct(s.ret_1w)}</td>
        <td class="r">${pct(s.ret_1m)}</td>
        <td class="r">${pct(s.ret_3m)}</td>
        <td class="r">${pct(s.ret_ytd)}</td>
        <td class="r">${pct(s.ret_1y)}</td>
        <td class="r"><canvas class="spark-canvas" width="60" height="24" style="display:block"></canvas></td>
        <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
        <td class="r">${emaBadge(s.above_ema50)}</td>
        <td class="r">${emaBadge(s.above_ema200)}</td>
        <td class="r" style="white-space:nowrap">${_wlAlertCell(s.symbol)}</td>
        <td><button class="wl-del-btn" onclick="removeFromWatchlist('${sym}')">✕</button></td>
      </tr>`;
  }).join("");

  document.getElementById("wl-tbody").innerHTML = rows;

  // sparklines: SET
  renderSparklinesInTable("wl-tbody", DATA.stocks.filter(s => watchlist.includes(s.symbol)));

  // sparklines: DR — วาดจาก close100 ที่ฝังใน data-dr-close
  requestAnimationFrame(() => {
    document.querySelectorAll("#wl-tbody tr[data-dr-close]").forEach(row => {
      const canvas = row.querySelector(".wl-dr-spark");
      if (!canvas) return;
      try {
        const closes = JSON.parse(row.dataset.drClose || "[]");
        if (closes.length >= 2) {
          // สร้าง price_history format ที่ drawSparkline รองรับ ([date, price])
          const history = closes.map((p, i) => [i, p]);
          const ret = closes.length >= 2 ? (closes[closes.length-1] - closes[closes.length-22]) / closes[closes.length-22] * 100 : null;
          drawSparkline(canvas, history, ret);
        }
      } catch(e) {}
    });
  });
}

// ============================================================
// NAV
// ============================================================
function showPage(id, btn) {
  document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".nav-btn").forEach(b => b.classList.remove("active"));
  document.getElementById("page-"+id).classList.add("active");
  if (btn) btn.classList.add("active");
  // Re-draw canvas charts once the page is visible and has real dimensions
  if (id === "sectors")      renderSectors();
  if (id === "rotation")     renderRotation();
  if (id === "fundamentals") renderFundamentals();
  if (id === "dr")           loadDRPage();
  if (id === "financials")   initFinPage();
  if (id === "valuation")    loadValuationPage();
  if (id === "insider")      loadInsiderPage();
  if (id === "short")        loadShortPage();
  if (id === "flow")         loadFlowPage();
  if (id === "indices")      loadIndicesPage();
  if (id === "stage")        renderStage();
  if (id === "ema-breadth")  loadBreadthCharts();
  if (id === "overview")     { setTimeout(() => { if (!_nhLoaded) loadNewHighChart(); }, 100); }
}


// ============================================================
// SECTOR ABBREVIATION MAP
// ============================================================
const SECTOR_ABBR = {
  "Agro & Food Industry":                  "AGRO",
  "Agribusiness":                          "AGRI",
  "Food & Beverage":                       "FOOD",
  "Fashion":                               "FASHION",
  "Home & Office Products":                "HOME",
  "Personal Products & Pharmaceuticals":   "PERSON",
  "Banking":                               "BANK",
  "Finance & Securities":                  "FIN",
  "Insurance":                             "INSUR",
  "Automotive":                            "AUTO",
  "Industrial Materials & Machinery":      "IMM",
  "Paper & Printing Materials":            "PAPER",
  "Petrochemicals & Chemicals":            "PETRO",
  "Packaging":                             "PKG",
  "Steel and Metal Products":              "STEEL",
  "Construction Materials":                "CONMAT",
  "Construction Services":                 "CONS",
  "Property Development":                  "PROP",
  "Property Fund & REITs":                 "PF&REIT",
  "Energy & Utilities":                    "ENERG",
  "Commerce":                              "COMM",
  "Health Care Services":                  "HELTH",
  "Media & Publishing":                    "MEDIA",
  "Professional Services":                 "PROF",
  "Tourism & Leisure":                     "TOURISM",
  "Transportation & Logistics":            "TRANS",
  "Electronic Components":                 "ETRON",
  "Information & Communication Technology":"ICT",
  "-":                                     "—",
  "Unknown":                               "—",
};

function openSectorModal(sectorName, nhOnly = false) {
  if (!DATA) return;
  const abbr = SECTOR_ABBR[sectorName] || "—";
  let stocks = DATA.stocks
    .filter(s => s.sector === sectorName || s.industry === sectorName);
  if (nhOnly) stocks = stocks.filter(s => s.high_52w > 0 && s.price >= s.high_52w);
  stocks.sort((a,b) => (b.rs_score||0)-(a.rs_score||0));

  document.getElementById("modal-title").textContent = nhOnly ? `${sectorName} — New Highs` : sectorName;
  document.getElementById("modal-abbr").textContent  = abbr;

  // stats summary
  const n = stocks.length;
  const avg1m  = n ? (stocks.reduce((a,s)=>a+(s.ret_1m||0),0)/n).toFixed(1) : null;
  const avg1d  = n ? (stocks.reduce((a,s)=>a+(s.ret_1d||0),0)/n).toFixed(1) : null;
  const avgRS  = n ? Math.round(stocks.reduce((a,s)=>a+(s.rs_score||0),0)/n) : null;
  const pctEma = n ? Math.round(stocks.filter(s=>s.above_ema50).length/n*100) : null;

  document.getElementById("modal-stats").innerHTML = `
    <div class="modal-stat"><div class="modal-stat-val">${n}</div><div class="modal-stat-lbl">หุ้น</div></div>
    <div class="modal-stat"><div class="modal-stat-val ${avg1d>0?'green':avg1d<0?'red':''}">${avg1d!=null?(avg1d>0?'+':'')+avg1d+'%':'—'}</div><div class="modal-stat-lbl">Avg 1D</div></div>
    <div class="modal-stat"><div class="modal-stat-val ${avg1m>0?'green':avg1m<0?'red':''}">${avg1m!=null?(avg1m>0?'+':'')+avg1m+'%':'—'}</div><div class="modal-stat-lbl">Avg 1M</div></div>
    <div class="modal-stat"><div class="modal-stat-val ${rsColor(avgRS)}">${avgRS??'—'}</div><div class="modal-stat-lbl">Avg RS</div></div>
    <div class="modal-stat"><div class="modal-stat-val">${pctEma??'—'}%</div><div class="modal-stat-lbl">% &gt;EMA50</div></div>
  `;

  document.getElementById("modal-tbody").innerHTML = stocks.map(s => `
    <tr>
      <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score??'—'}</span></td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td class="r">${s.price?.toFixed(2)??'—'}</td>
      <td class="r">${pct(s.ret_1d)}</td>
      <td class="r">${pct(s.ret_1w)}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r">${pct(s.ret_1y)}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
    </tr>`).join("");

  document.getElementById("sector-modal").classList.add("open");
  document.body.style.overflow = "hidden";
}

function closeModal() {
  document.getElementById("sector-modal").classList.remove("open");
  document.body.style.overflow = "";
}

document.addEventListener("keydown", e => { if(e.key==="Escape") { closeModal(); closeChartModal(); closeStockPopup(); } });

// ============================================================
// STOCK QUICK POPUP
// ============================================================
let _spopSym = null;

function openStockPopup(sym, evt) {
  evt.stopPropagation();
  _spopSym = sym;

  const popup = document.getElementById('stock-popup');
  const rect  = evt.currentTarget.getBoundingClientRect();
  const vw    = window.innerWidth, vh = window.innerHeight;

  // Reset
  document.getElementById('spop-loading').style.display = 'block';
  document.getElementById('spop-canvas').style.display  = 'none';
  document.getElementById('spop-fin-note').style.display = 'none';
  popup.style.display = 'block';

  // Stock name
  document.getElementById('spop-sym').textContent  = sym;
  const found = (DATA?.stocks || []).find(s => s.symbol === sym);
  document.getElementById('spop-name').textContent = found?.name || '';

  // Position: prefer below, then above; prefer right, then left
  const pw = 260, ph = 220;
  let top  = rect.bottom + 6;
  let left = rect.left;
  if (top + ph > vh - 8)  top  = rect.top - ph - 6;
  if (left + pw > vw - 8) left = vw - pw - 8;
  if (top < 8)            top  = 8;
  popup.style.top  = top  + 'px';
  popup.style.left = left + 'px';

  // Fetch mini financials
  fetch(`/api/financials/${encodeURIComponent(sym)}`)
    .then(r => r.json())
    .then(d => {
      if (_spopSym !== sym) return;
      if (d.error) { document.getElementById('spop-loading').textContent = 'ไม่มีข้อมูลงบการเงิน'; return; }
      document.getElementById('spop-loading').style.display = 'none';
      document.getElementById('spop-canvas').style.display  = 'block';
      document.getElementById('spop-fin-note').style.display = 'block';
      document.getElementById('spop-fin-note').textContent   = d.currency || '';
      _drawSpopChart(d);
    })
    .catch(() => { if (_spopSym === sym) document.getElementById('spop-loading').textContent = 'ไม่มีข้อมูลงบการเงิน'; });

  setTimeout(() => document.addEventListener('click', _spopOutside, { once: true }), 80);
}

function _spopOutside(e) {
  if (!document.getElementById('stock-popup').contains(e.target)) closeStockPopup();
}

function closeStockPopup() {
  _spopSym = null;
  document.getElementById('stock-popup').style.display = 'none';
  document.removeEventListener('click', _spopOutside);
}

function _spopGoFin(sym) {
  const drStocks = _drData || [];
  const isDR = drStocks.some(s => s.sym === sym);
  showPage('financials');
  if (isDR) {
    setFinTab('dr',  document.getElementById('fin-tab-dr-btn'));
    document.getElementById('fin-sym-dr').value = sym;
  } else {
    setFinTab('set', document.getElementById('fin-tab-set-btn'));
    document.getElementById('fin-sym-set').value = sym;
  }
  setTimeout(searchFinancials, 150);
}

function _drawSpopChart(d) {
  const canvas = document.getElementById('spop-canvas');
  const ctx    = canvas.getContext('2d');
  const dpr    = window.devicePixelRatio || 1;
  const W = 228, H = 90;
  canvas.width  = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);

  // Collect last 4 years
  const allDates = new Set();
  [d.income, d.balance, d.cashflow].forEach(sec => Object.values(sec).forEach(r => Object.keys(r).forEach(k => allDates.add(k))));
  const years = [...allDates].sort().slice(-4);
  if (!years.length) return;

  const getVal = (keys) => {
    for (const k of keys) {
      const row = d.income[k] || d.balance[k] || d.cashflow[k];
      if (row) { const v = years.map(y => row[y] ?? null); if (v.some(x => x != null)) return v; }
    }
    return years.map(() => null);
  };
  const revVals = getVal(['Total Revenue','Revenue','Revenues','Net Revenue']);
  const niVals  = getVal(['Net Income','Net Income Common Stockholders','Net Income From Continuing Operations']);

  const allVals = [...revVals, ...niVals].filter(v => v != null);
  if (!allVals.length) return;
  const maxV = Math.max(...allVals, 0);
  const minV = Math.min(...allVals, 0);
  const range = maxV - minV || 1;

  const PAD = { top: 8, bottom: 22, left: 4, right: 4 };
  const PW = W - PAD.left - PAD.right;
  const PH = H - PAD.top - PAD.bottom;
  const n  = years.length;
  const gw = PW / (n * 2 + (n - 1) * 0.5);
  const toY = v => PAD.top + PH - ((v - minV) / range) * PH;
  const zero = toY(0);

  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = '#0d1117'; ctx.fillRect(0, 0, W, H);

  // Zero line
  if (minV < 0) { ctx.strokeStyle = '#30363d'; ctx.lineWidth = 0.5; ctx.beginPath(); ctx.moveTo(PAD.left, zero); ctx.lineTo(W - PAD.right, zero); ctx.stroke(); }

  const _finFmtShort = v => {
    if (v == null) return '';
    const a = Math.abs(v);
    if (a >= 1e12) return (v/1e12).toFixed(1) + 'T';
    if (a >= 1e9)  return (v/1e9).toFixed(1) + 'B';
    if (a >= 1e6)  return (v/1e6).toFixed(0) + 'M';
    return (v/1e3).toFixed(0) + 'K';
  };

  years.forEach((yr, i) => {
    const cx  = PAD.left + i * (gw * 2 + gw * 0.5) + gw;
    const rv  = revVals[i], nv = niVals[i];

    // Revenue bar (blue)
    if (rv != null) {
      const y = toY(Math.max(rv, 0)), h = Math.abs(toY(Math.min(rv, 0)) - toY(Math.max(rv, 0)));
      ctx.fillStyle = '#1f6feb';
      ctx.fillRect(cx - gw, rv >= 0 ? y : zero, gw, Math.max(h, 1));
    }
    // Net Income bar (green/red)
    if (nv != null) {
      const y = toY(Math.max(nv, 0)), h = Math.abs(toY(Math.min(nv, 0)) - toY(Math.max(nv, 0)));
      ctx.fillStyle = nv >= 0 ? '#238636' : '#da3633';
      ctx.fillRect(cx, nv >= 0 ? y : zero, gw, Math.max(h, 1));
    }

    // Year label
    ctx.fillStyle = '#8b949e'; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(yr.slice(0, 4), cx, H - PAD.bottom + 12);

    // NI label on top bar
    if (nv != null) {
      ctx.fillStyle = nv >= 0 ? '#3fb950' : '#f85149';
      ctx.font = '8px sans-serif'; ctx.textAlign = 'center';
      const labelY = toY(Math.max(nv, 0)) - 3;
      if (labelY > PAD.top + 6) ctx.fillText(_finFmtShort(nv), cx + gw / 2, labelY);
    }
  });

  // Legend
  ctx.font = '8px sans-serif'; ctx.textAlign = 'left';
  ctx.fillStyle = '#1f6feb'; ctx.fillRect(PAD.left, H - PAD.bottom + 16, 8, 6);
  ctx.fillStyle = '#8b949e'; ctx.fillText('Revenue', PAD.left + 10, H - PAD.bottom + 22);
  ctx.fillStyle = '#238636'; ctx.fillRect(PAD.left + 70, H - PAD.bottom + 16, 8, 6);
  ctx.fillStyle = '#8b949e'; ctx.fillText('Net Income', PAD.left + 80, H - PAD.bottom + 22);
}

// ============================================================
// SCREENER
// ============================================================
let _scrStocks  = [];
let _scrSortCol = 'rs_score';
let _scrSortDir = 1; // 1 = desc (มากไปน้อย), -1 = asc (น้อยไปมาก)

const _SCR_STR  = new Set(['symbol','name','sector']);
const _SCR_BOOL = new Set(['above_ema50','above_ema200','above_ema20']);

function setScrSort(col) {
  if (_scrSortCol === col) _scrSortDir *= -1;
  else { _scrSortCol = col; _scrSortDir = 1; }
  renderScrTable();
}

function renderScrTable() {
  const secRanks = computeSectorRanks();
  const withRange = _scrStocks.map(s => {
    const fromHigh = s.high_52w > 0 ? (s.price - s.high_52w) / s.high_52w * 100 : null;
    const fromLow  = s.low_52w  > 0 ? (s.price - s.low_52w)  / s.low_52w  * 100 : null;
    const rvol     = (s.vol_today && s.vol_avg20 > 0) ? s.vol_today / s.vol_avg20 * 100 : null;
    const sr = secRanks[s.symbol];
    return { ...s, fromHigh, fromLow, rvol, sec_rank: sr?.rank ?? null, sec_total: sr?.total ?? null };
  });
  const sorted = [...withRange].sort((a, b) => {
    const col = _scrSortCol;
    if (_SCR_BOOL.has(col)) return ((b[col]?1:0) - (a[col]?1:0)) * _scrSortDir;
    if (_SCR_STR.has(col))  return ((a[col]??'').localeCompare(b[col]??'')) * _scrSortDir;
    return ((b[col]??-Infinity) - (a[col]??-Infinity)) * _scrSortDir;
  });

  function th(col, label, cls='') {
    const active = _scrSortCol === col;
    const arrow  = active ? (_scrSortDir === 1 ? '↓' : '↑') : '↕';
    const c = (cls ? cls+' ' : '') + 'sortable';
    return `<th class="${c}"${colTip(col)} onclick="setScrSort('${col}')">${label}<span class="sort-ind${active?' on':''}">${arrow}</span></th>`;
  }

  document.getElementById('screener-results').innerHTML = `
    <div class="card"><table class="tbl" style="table-layout:fixed;width:100%">
      <colgroup>
        <col style="width:36px"><!-- RS -->
        <col style="width:60px"><!-- SEC.RANK -->
        <col style="width:72px"><!-- SYMBOL -->
        <col style="width:130px"><!-- ชื่อ -->
        <col style="width:90px"><!-- SECTOR -->
        <col style="width:46px"><!-- ราคา -->
        <col style="width:44px"><!-- 1D% -->
        <col style="width:44px"><!-- 1W% -->
        <col style="width:44px"><!-- 1M% -->
        <col style="width:44px"><!-- 3M% -->
        <col style="width:44px"><!-- 6M% -->
        <col style="width:44px"><!-- 1Y% -->
        <col style="width:44px"><!-- YTD% -->
        <col style="width:56px"><!-- %HIGH -->
        <col style="width:48px"><!-- %LOW -->
        <col style="width:36px"><!-- P/E -->
        <col style="width:36px"><!-- P/BV -->
        <col style="width:40px"><!-- DIV% -->
        <col style="width:52px"><!-- RVOL -->
        <col style="width:60px"><!-- MKT CAP -->
        <col style="width:44px"><!-- EMA50 -->
        <col style="width:44px"><!-- EMA200 -->
      </colgroup>
      <thead><tr>
        ${th('rs_score','RS')}${th('sec_rank','SEC.RANK','r')}${th('symbol','SYMBOL')}${th('name','ชื่อ')}${th('sector','SECTOR')}
        ${th('price','ราคา','r')}${th('ret_1d','1D%','r')}${th('ret_1w','1W%','r')}${th('ret_1m','1M%','r')}
        ${th('ret_3m','3M%','r')}${th('ret_6m','6M%','r')}${th('ret_1y','1Y%','r')}${th('ret_ytd','YTD%','r')}
        ${th('fromHigh','%HIGH','r')}${th('fromLow','%LOW','r')}
        ${th('pe','P/E','r')}${th('pbv','P/BV','r')}${th('div_yield','DIV%','r')}
        ${th('rvol','RVOL','r')}${th('mkt_cap','MKT CAP','r')}${th('above_ema50','EMA50','r')}${th('above_ema200','EMA200','r')}
      </tr></thead>
      <tbody>${sorted.map(s => `
        <tr>
          <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score ?? '—'}</span></td>
          <td class="r">${secRankHtml(s.sec_rank ? {rank:s.sec_rank,total:s.sec_total} : null)}</td>
          <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
          <td style="font-size:11px;color:var(--text2);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
          <td style="font-size:11px">${s.sector || '—'}</td>
          <td class="r">${s.price?.toFixed(2) ?? '—'}</td>
          <td class="r">${pct(s.ret_1d)}</td>
          <td class="r">${pct(s.ret_1w)}</td>
          <td class="r">${pct(s.ret_1m)}</td>
          <td class="r">${pct(s.ret_3m)}</td>
          <td class="r">${pct(s.ret_6m)}</td>
          <td class="r">${pct(s.ret_1y)}</td>
          <td class="r">${pct(s.ret_ytd)}</td>
          <td class="r">${s.fromHigh == null ? '—' : `<span class="${s.fromHigh >= -2 ? 'green' : s.fromHigh >= -5 ? 'yellow' : 'text2'}">${s.fromHigh >= 0 ? 'NEW HIGH' : s.fromHigh.toFixed(1)+'%'}</span>`}</td>
          <td class="r">${s.fromLow  == null ? '—' : `<span class="${s.fromLow  <= 2  ? 'red'   : s.fromLow  <= 5  ? 'yellow' : 'text2'}">${s.fromLow  <= 0.5  ? 'NEW LOW'  : s.fromLow.toFixed(1)+'%'}</span>`}</td>
          <td class="r text2">${s.pe        != null ? s.pe.toFixed(1)        : '—'}</td>
          <td class="r text2">${s.pbv       != null ? s.pbv.toFixed(2)       : '—'}</td>
          <td class="r">${s.div_yield != null ? `<span class="${s.div_yield >= 5 ? 'green' : s.div_yield >= 3 ? 'yellow' : 'text2'}">${s.div_yield.toFixed(2)}%</span>` : '—'}</td>
          <td class="r">${rvolHtml(s)}</td>
          <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
          <td class="r">${emaBadge(s.above_ema50)}</td>
          <td class="r">${emaBadge(s.above_ema200)}</td>
        </tr>`).join('')}
      </tbody>
    </table></div>`;
}

function toggleScrGuide() {
  const box = document.getElementById('scr-guide-box');
  box.style.display = box.style.display === 'none' ? '' : 'none';
}

// ============================================================
// TECHNICAL SIGNAL PRE-COMPUTATION
// ============================================================
function _sma(arr, n) {
  if (arr.length < n) return null;
  return arr.slice(-n).reduce((a,b)=>a+b,0)/n;
}
function _ema(arr, period) {
  if (arr.length < period) return null;
  const k = 2/(period+1);
  let e = arr.slice(0,period).reduce((a,b)=>a+b)/period;
  for (let i=period; i<arr.length; i++) e = arr[i]*k + e*(1-k);
  return e;
}
function _rsi(arr, period=14) {
  // Wilder's Smoothed RSI — ต้องการ warmup period*2 แท่ง
  if (arr.length < period * 2 + 1) return null;
  // seed ด้วย SMA ของ period แรก
  let ag = 0, al = 0;
  for (let i = 1; i <= period; i++) {
    const d = arr[i] - arr[i-1];
    if (d > 0) ag += d; else al -= d;
  }
  ag /= period; al /= period;
  // Wilder's smoothing สำหรับแท่งที่เหลือ
  for (let i = period + 1; i < arr.length; i++) {
    const d = arr[i] - arr[i-1];
    ag = (ag * (period-1) + (d > 0 ? d : 0)) / period;
    al = (al * (period-1) + (d < 0 ? -d : 0)) / period;
  }
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

// ============================================================
// MONTHLY REMINDER — เตือนอัปเดต P/E & P/BV (Table_PE.xls/Table_PBV.xls)
// ตั้งแต่วันที่ 5 ของเดือน จนกว่าจะกดอัปเดตจริง (เช็คจาก mtime ของ set_market_stats.json)
// เตือนสูงสุดวันละครั้งต่อ browser (ปิดแล้วเงียบจนถึงพรุ่งนี้)
// ============================================================
function _todayStr() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
async function checkPeReminder() {
  if (IS_STATIC) return; // เวอร์ชันเว็บอัปเดตอัตโนมัติ ไม่ต้องเตือนวางไฟล์
  const now = new Date();
  if (now.getDate() < 5) return;
  const todayStr = _todayStr();
  if (localStorage.getItem('peReminderDismissedDate') === todayStr) return;
  try {
    const r = await fetch('/api/market-stats-meta');
    const d = await r.json();
    const curYM = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    const updatedYM = d.updated_at ? d.updated_at.slice(0, 7) : null;
    if (updatedYM === curYM) return; // อัปเดตเดือนนี้ไปแล้ว
    document.getElementById('pe-reminder-modal').classList.add('open');
  } catch (e) { /* เงียบ — ไม่ใช่ฟีเจอร์หลัก */ }
}
function dismissPeReminder() {
  localStorage.setItem('peReminderDismissedDate', _todayStr());
  document.getElementById('pe-reminder-modal').classList.remove('open');
}
function goToPeUpdate() {
  document.getElementById('pe-reminder-modal').classList.remove('open');
  const btn = [...document.querySelectorAll('.nav-btn')].find(b => b.getAttribute('onclick')?.includes("'valuation'"));
  showPage('valuation', btn);
}

// ============================================================
// DATA QUALITY — badges ต่อหุ้น + banner ระดับ dataset
// ============================================================
const DQ_FLAG_DEFS = [
  ['stale',      'red',    s => `หยุดเทรดมา ${s.dq_stale_days || '?'} วัน — ไม่ถูกนำเข้า RS Rank และค่าเฉลี่ยกลุ่ม`],
  ['no_trade',   'red',    () => 'ไม่มีการซื้อขาย (volume = 0 ต่อเนื่อง ≥ 5 วัน — หุ้นติด SP ที่แหล่งข้อมูลยังส่งแท่งราคาค้างมา) — ไม่ถูกนำเข้า RS Rank และค่าเฉลี่ยกลุ่ม'],
  ['thin',       'yellow', () => 'เทรดเบาบาง (21 แท่ง > 45 วัน) — return ระยะยาวหน่วยเวลาเพี้ยน ไม่ถูกนำเข้า RS Rank'],
  ['suspect_ca', 'yellow', () => 'ราคาเคลื่อนเกิน ceiling ±30% — สงสัย corporate action พักออกจาก RS Rank รอบนี้'],
  ['penny',      'gray',   () => 'ราคาต่ำกว่า 0.10 บาท — 1 tick = ±10% ขึ้นไป return/RS อาจเพี้ยนจาก tick เดียว'],
  ['short_hist', 'gray',   () => 'ข้อมูลไม่ถึง 1 ปี (IPO ใหม่) — RS คำนวณจากช่วงเวลาสั้นกว่าหุ้นอื่น'],
];
function dqBadge(s) {
  const f = (s.dq && s.dq.flags) || [];
  if (!f.length) return '';
  const hits = DQ_FLAG_DEFS.filter(([flag]) => f.includes(flag));
  if (!hits.length) return '';
  const sevRank = { red: 2, yellow: 1, gray: 0 };
  const sev = hits.reduce((worst, [, s2]) => sevRank[s2] > sevRank[worst] ? s2 : worst, 'gray');
  const title = hits.map(([, , msg]) => msg(s)).join(' | ');
  // badge เดียวรวมทุก flag — สีตามความรุนแรงสูงสุด (red > yellow > gray) ลด icon ที่ต้องจำ เหลือแค่ ⚠
  return `<span class="dq-badge dq-${sev}" title="${title}">⚠</span>`;
}

function _dqIsPenny(s) { return ((s.dq && s.dq.flags) || []).includes('penny'); }

function reopenDqBanner() {
  sessionStorage.removeItem('dqBannerDismissed');
  renderDqBanner();
}
function dismissDqBanner() {
  sessionStorage.setItem('dqBannerDismissed', '1');
  renderDqBanner();
}
function renderDqBanner() {
  const el = document.getElementById('dq-banner');
  if (!el || !DATA) return;
  const msgs = [];

  // D2: dataset stale — data_as_of เก่ากว่าวันนี้เกิน 5 วันปฏิทิน (~3 วันทำการ)
  if (DATA.data_as_of) {
    const gap = Math.round((Date.now() - new Date(DATA.data_as_of).getTime()) / 86400000);
    if (gap > 5) msgs.push(`⚠️ ข้อมูลอาจไม่เป็นปัจจุบัน — ราคาล่าสุดคือวันที่ <b>${DATA.data_as_of}</b> (${gap} วันก่อน) ลองกด Quick Update`);
  }

  // D1: สรุปหุ้นที่ถูกกันออกจาก RS Rank
  const dq = DATA.dq_summary;
  if (dq && dq.rs_excluded > 0) {
    const c = dq.counts || {};
    const detail = [
      c.stale      ? `พักเทรด ${c.stale}` : null,
      c.no_trade   ? `ไม่มีเทรด (SP) ${c.no_trade}` : null,
      c.thin       ? `เทรดเบาบาง ${c.thin}` : null,
      c.suspect_ca ? `สงสัย CA ${c.suspect_ca}` : null,
      c.no_data    ? `ข้อมูลไม่ครบ ${c.no_data}` : null,
    ].filter(Boolean).join(', ');
    // หมายเหตุ: ผลรวมในวงเล็บอาจมากกว่าจำนวนกันออกจริง เพราะหุ้นบางตัวเข้าเงื่อนไขซ้อนกันมากกว่า 1 เหตุผล
    msgs.push(`ℹ️ RS Rank คำนวณจาก <b>${dq.rs_universe}</b> หุ้น — กันออก ${dq.rs_excluded} ตัว (เหตุผล: ${detail} — บางตัวเข้าเงื่อนไขซ้อนกันมากกว่า 1 อย่าง) <a href="#" onclick="goToDqStocks(); return false;" style="color:inherit;text-decoration:underline;font-weight:600">ดูรายชื่อหุ้น →</a>`);
  }

  if (!msgs.length) {
    el.style.cssText = 'display:none';
    el.innerHTML = '';
    return;
  }

  if (sessionStorage.getItem('dqBannerDismissed') === '1') {
    // ปิดไปแล้ว — เหลือ chip เล็กๆ ให้กดเปิดอ่านใหม่ได้ตลอด แทนที่จะหายไปเลย
    el.style.cssText = 'display:block; margin:8px 16px 0; padding:0; background:none; border:none';
    el.innerHTML = `<button onclick="reopenDqBanner()"
      style="background:none;border:1px dashed var(--border);color:var(--text2);border-radius:6px;padding:3px 10px;font-size:11px;cursor:pointer">
      ℹ️ มีแจ้งเตือนคุณภาพข้อมูล (ปิดไว้) — คลิกเพื่อดูอีกครั้ง
    </button>`;
    return;
  }

  el.style.cssText = '';
  el.innerHTML = `<div style="display:flex;align-items:flex-start;gap:8px">
    <div style="flex:1">${msgs.join('<br>')}</div>
    <button onclick="dismissDqBanner()"
      title="ปิดข้อความนี้ (จะเหลือปุ่มเล็กให้กดเปิดดูใหม่ได้ตลอด)"
      style="background:none;border:none;color:inherit;opacity:.7;cursor:pointer;font-size:14px;line-height:1;padding:0 2px">✕</button>
  </div>`;
  el.style.display = 'block';
}

function _enrichTechSignals(stocks) {
  stocks.forEach(s => {
    const ph = s.price_history;
    if (!ph || ph.length < 15) return;
    const p = ph.map(x=>x[1]);
    const pv = p.slice(0,-1); // T-1 prices

    // SMA crossover 10/50 and 10/200
    const s10=_sma(p,10), s10v=_sma(pv,10);
    const s50=_sma(p,50), s50v=_sma(pv,50);
    const s200=_sma(p,200), s200v=_sma(pv,200);
    s._sma_cross50  = s10!=null&&s50!=null&&s10v!=null&&s50v!=null  && s10>=s50  && s10v<s50v;
    s._sma_cross200 = s10!=null&&s200!=null&&s10v!=null&&s200v!=null && s10>=s200 && s10v<s200v;

    // EMA crossover 10/50 and 10/200
    const e10=_ema(p,10), e10v=_ema(pv,10);
    const e50=_ema(p,50), e50v=_ema(pv,50);
    const e200=_ema(p,200), e200v=_ema(pv,200);
    s._ema_cross50  = e10!=null&&e50!=null&&e10v!=null&&e50v!=null   && e10>=e50  && e10v<e50v;
    s._ema_cross200 = e10!=null&&e200!=null&&e10v!=null&&e200v!=null  && e10>=e200 && e10v<e200v;

    // RSI Rebound: RSI14 ตัดขึ้น 45 วันนี้ + above SMA200 + above EMA200
    const r=_rsi(p), rv=_rsi(pv);
    const aboveSma200 = s200!=null && p[p.length-1] >= s200;
    s._rsi_rebound = r!=null&&rv!=null && r>=45 && rv<45 && !!s.above_ema200 && aboveSma200;

    // 52W High breakout
    s._new_52w_high = s.high_52w>0 && s.price>=s.high_52w;

    // Bullish High Volume: ราคาขึ้น + RVOL5d≥200% + Value≥10M บาท (ตาม reference criteria)
    let rvol5d = null;
    if (s.vol_history && s.vol_history.length >= 6) {
      const v5 = s.vol_history.slice(-6, -1).reduce((a,b)=>a+b,0) / 5; // -6 to -1 = 5 วันก่อนวันนี้
      rvol5d = v5 > 0 && s.vol_today ? s.vol_today / v5 * 100 : null;
    } else if (s.vol_avg20 > 0 && s.vol_today) {
      rvol5d = s.vol_today / s.vol_avg20 * 100; // fallback ก่อน Full Refresh
    }
    const valueK = (s.price && s.vol_today) ? s.price * s.vol_today / 1000 : 0;
    s._bullish_vol = (s.ret_1d??0)>=0 && rvol5d!=null && rvol5d>=200 && valueK>=10000;
  });
}

function applyPreset(name) {
  // ล้างทุก field โดยใช้ list เดียวกับ persistent settings
  _SCR_FIELDS.forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  _SCR_CHECKS.forEach(id => { const el = document.getElementById(id); if (el) el.checked = false; });
  const mk = document.getElementById('scr-market'); if (mk) mk.value = 'ALL';
  const ind = document.getElementById('scr-industry'); if (ind) ind.value = 'ALL';

  if (name === 'onset') {
    document.getElementById('scr-rs-min').value  = '50';
    document.getElementById('scr-1m').value      = '3';
    document.getElementById('scr-ema50').checked  = true;
  } else if (name === 'strong_momentum') {
    document.getElementById('scr-rs-min').value  = '70';
    document.getElementById('scr-1m').value      = '5';
    document.getElementById('scr-3m').value      = '10';
    document.getElementById('scr-ema50').checked  = true;
    document.getElementById('scr-ema200').checked = true;
  } else if (name === 'breakout') {
    document.getElementById('scr-rs-min').value  = '60';
    document.getElementById('scr-rvol').value    = '150';
    document.getElementById('scr-ema50').checked  = true;
  } else if (name === 'near_ath') {
    document.getElementById('scr-ath-dist').value = '10';
    document.getElementById('scr-rs-min').value   = '50';
  } else if (name === 'high_yield_rs') {
    document.getElementById('scr-rs-min').value = '60';
    document.getElementById('scr-dy').value     = '3';
  } else if (name === 'low_pbv_reversal') {
    document.getElementById('scr-pbv').value    = '1.5';
    document.getElementById('scr-rs-min').value = '45';
    document.getElementById('scr-1m').value     = '0';
  } else if (name === 'backtest_strategy') {
    document.getElementById('scr-backtest').checked = true;
  }
  runScreener();
}

function runScreener() {
  if (!DATA) return;
  saveScreenerSettings();
  const rsMin     = parseFloat(document.getElementById('scr-rs-min').value);
  const ret1m     = parseFloat(document.getElementById('scr-1m').value);
  const ret3m     = parseFloat(document.getElementById('scr-3m').value);
  const ret1d     = parseFloat(document.getElementById('scr-1d').value);
  const retYtd    = parseFloat(document.getElementById('scr-ytd').value);
  const capMin    = parseFloat(document.getElementById('scr-cap').value) * 1e9;
  const priceMin   = parseFloat(document.getElementById('scr-price-min').value);
  const priceMax   = parseFloat(document.getElementById('scr-price-max').value);
  const fromHighMax = parseFloat(document.getElementById('scr-from-high').value);
  const athDistMax  = parseFloat(document.getElementById('scr-ath-dist').value);
  const fromLowMax  = parseFloat(document.getElementById('scr-from-low').value);
  const peMax       = parseFloat(document.getElementById('scr-pe').value);
  const pbvMax      = parseFloat(document.getElementById('scr-pbv').value);
  const dyMin       = parseFloat(document.getElementById('scr-dy').value);
  const rvolMin     = parseFloat(document.getElementById('scr-rvol').value);
  const rvolDays    = parseInt(document.getElementById('scr-rvol-days').value) || 20;
  const ret1w     = parseFloat(document.getElementById('scr-1w').value);
  const ret6m     = parseFloat(document.getElementById('scr-6m').value);
  const ret1y     = parseFloat(document.getElementById('scr-1y').value);
  const ema20     = document.getElementById('scr-ema20').checked;
  const ema50     = document.getElementById('scr-ema50').checked;
  const ema200    = document.getElementById('scr-ema200').checked;
  const goldenX   = document.getElementById('scr-golden-cross').checked;
  const mkt       = document.getElementById('scr-market').value;
  const industry  = document.getElementById('scr-industry').value;
  const sig52h      = document.getElementById('scr-new52h').checked;
  const sigSmaCr50  = document.getElementById('scr-sma-cross50').checked;
  const sigSmaCr200 = document.getElementById('scr-sma-cross200').checked;
  const sigEmaCr50  = document.getElementById('scr-ema-cross50').checked;
  const sigEmaCr200 = document.getElementById('scr-ema-cross200').checked;
  const sigRsiReb   = document.getElementById('scr-rsi-rebound').checked;
  const sigBullVol  = document.getElementById('scr-bullish-vol').checked;
  const sigBacktest = document.getElementById('scr-backtest')?.checked || false;

  // เกณฑ์ backtest: sector ที่ 1M momentum > 0 (quadrant Leading/Improving)
  // — ตรรกะเดียวกับ sector filter ใน backtest_rs_rrg.py
  let _btSectors = null;
  if (sigBacktest) {
    _btSectors = new Set(
      (DATA.sectors || []).filter(g => (g.ret_1m ?? -1) > 0).map(g => g.name));
  }

  _scrStocks = DATA.stocks.filter(s => {
    if (sigBacktest) {
      if ((s.rs_score ?? -1) < 90)                          return false;
      if ((s.price ?? 0) < 1)                               return false;
      if (((s.vol_avg20 || 0) * (s.price || 0)) < 5e6)      return false;
      if (!_btSectors.has(s.sector))                        return false;
    }
    if (!isNaN(rsMin)    && (s.rs_score  ?? -1) < rsMin)              return false;
    if (!isNaN(ret1m)    && (s.ret_1m    ?? -Infinity) < ret1m)       return false;
    if (!isNaN(ret3m)    && (s.ret_3m    ?? -Infinity) < ret3m)       return false;
    if (!isNaN(ret1d)    && (s.ret_1d    ?? -Infinity) < ret1d)       return false;
    if (!isNaN(retYtd)   && (s.ret_ytd   ?? -Infinity) < retYtd)      return false;
    if (!isNaN(ret1w)    && (s.ret_1w    ?? -Infinity) < ret1w)       return false;
    if (!isNaN(ret6m)    && (s.ret_6m    ?? -Infinity) < ret6m)       return false;
    if (!isNaN(ret1y)    && (s.ret_1y    ?? -Infinity) < ret1y)       return false;
    if (!isNaN(capMin)   && capMin > 0 && (!s.mkt_cap || s.mkt_cap < capMin)) return false;
    if (!isNaN(priceMin) && (s.price ?? 0) < priceMin)                return false;
    if (!isNaN(priceMax) && priceMax > 0 && (s.price ?? 0) > priceMax) return false;
    if (!isNaN(fromHighMax) && fromHighMax >= 0) {
      const fh = s.high_52w > 0 ? (s.price - s.high_52w) / s.high_52w * 100 : null;
      if (fh == null || fh < -fromHighMax) return false;
    }
    if (!isNaN(athDistMax) && athDistMax >= 0) {
      if (s.ath_pct == null || s.ath_pct < -athDistMax) return false;
    }
    if (!isNaN(fromLowMax) && fromLowMax >= 0) {
      const fl = s.low_52w > 0 ? (s.price - s.low_52w) / s.low_52w * 100 : null;
      if (fl == null || fl > fromLowMax) return false;
    }
    if (!isNaN(peMax)  && peMax  > 0 && (s.pe  == null || s.pe  > peMax))  return false;
    if (!isNaN(pbvMax) && pbvMax > 0 && (s.pbv == null || s.pbv > pbvMax)) return false;
    if (!isNaN(dyMin)  && dyMin  > 0 && (s.div_yield == null || s.div_yield < dyMin)) return false;
    if (!isNaN(rvolMin) && rvolMin > 0) {
      let rv = null;
      if (s.vol_today && s.vol_history?.length) {
        const slice = s.vol_history.slice(0, -1).slice(-rvolDays);
        const avg = slice.length ? slice.reduce((a,b) => a+b, 0) / slice.length : 0;
        if (avg > 0) rv = s.vol_today / avg * 100;
      } else if (s.vol_today && s.vol_avg20) {
        rv = s.vol_today / s.vol_avg20 * 100;
      }
      if (!rv || rv < rvolMin) return false;
    }
    if (ema20  && !s.above_ema20)  return false;
    if (ema50  && !s.above_ema50)  return false;
    if (ema200 && !s.above_ema200) return false;
    if (goldenX && !(s.ema50 > s.ema200)) return false;
    if (sig52h      && !s._new_52w_high)  return false;
    if (sigSmaCr50  && !s._sma_cross50)   return false;
    if (sigSmaCr200 && !s._sma_cross200)  return false;
    if (sigEmaCr50  && !s._ema_cross50)   return false;
    if (sigEmaCr200 && !s._ema_cross200)  return false;
    if (sigRsiReb   && !s._rsi_rebound)   return false;
    if (sigBullVol  && !s._bullish_vol)   return false;
    if (mkt !== 'ALL' && s.market !== mkt) return false;
    if (industry !== 'ALL' && s.industry !== industry) return false;
    return true;
  });

  // reset sort to RS desc on every new search
  _scrSortCol = 'rs_score';
  _scrSortDir = 1;

  document.getElementById('screener-count').textContent = `พบ ${_scrStocks.length} หุ้น`;
  const csvBtn = document.getElementById('btn-export-csv');
  if (csvBtn) csvBtn.style.display = _scrStocks.length > 0 ? '' : 'none';
  if (_scrStocks.length === 0) {
    document.getElementById('screener-results').innerHTML = '<div class="empty">ไม่พบหุ้นที่ตรงเงื่อนไข ลองปรับเงื่อนไขใหม่</div>';
    return;
  }
  renderScrTable();
}

function resetScreener() {
  _SCR_FIELDS.forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  _SCR_CHECKS.forEach(id => { const el = document.getElementById(id); if (el) el.checked = false; });
  document.getElementById('scr-market').value = 'ALL';
  const ind = document.getElementById('scr-industry'); if (ind) ind.value = 'ALL';
  document.getElementById('screener-count').textContent = '';
  document.getElementById('screener-results').innerHTML = '<div class="empty">กำหนดเงื่อนไขแล้วกด ค้นหา</div>';
  const csvBtn = document.getElementById('btn-export-csv');
  if (csvBtn) csvBtn.style.display = 'none';
  localStorage.removeItem(_SCR_LS);
}

// ============================================================
// HEATMAP
// ============================================================
let hmPeriod = 'ret_1d';

function setHmPeriod(key, btn) {
  hmPeriod = key;
  document.querySelectorAll('#page-heatmap .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderHeatmap();
}

function _heatColor(v, cap = 15) {
  if (v == null) return '#21262d';
  const t = Math.min(Math.abs(v) / cap, 1);
  if (v >= 0) return `rgba(63,185,80,${0.15 + t * 0.85})`;
  return `rgba(248,81,73,${0.15 + t * 0.85})`;
}
function _heatColorRS(v) {
  if (v == null) return '#21262d';
  const t = Math.abs(v - 50) / 50;
  if (v >= 50) return `rgba(63,185,80,${0.15 + t * 0.85})`;
  return `rgba(248,81,73,${0.15 + t * 0.85})`;
}
function _heatColorVol(v) {
  if (v == null) return '#21262d';
  const t = Math.min(Math.max(v - 80, 0) / 220, 1);
  return `rgba(88,166,255,${0.1 + t * 0.9})`;
}

const HM_CFG = {
  ret_1d:    { getV:s=>s.ret_1d,   clr:v=>_heatColor(v,15),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>6?'#fff':'var(--text)',              hint:'เขียว = ขึ้น · แดง = ลง' },
  ret_1w:    { getV:s=>s.ret_1w,   clr:v=>_heatColor(v,15),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>6?'#fff':'var(--text)',              hint:'เขียว = ขึ้น · แดง = ลง' },
  ret_1m:    { getV:s=>s.ret_1m,   clr:v=>_heatColor(v,20),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>8?'#fff':'var(--text)',              hint:'เขียว = ขึ้น · แดง = ลง' },
  ret_3m:    { getV:s=>s.ret_3m,   clr:v=>_heatColor(v,30),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>12?'#fff':'var(--text)',             hint:'เขียว = ขึ้น · แดง = ลง' },
  ret_6m:    { getV:s=>s.ret_6m,   clr:v=>_heatColor(v,40),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>16?'#fff':'var(--text)',             hint:'เขียว = ขึ้น · แดง = ลง (6 เดือน)' },
  ret_ytd:   { getV:s=>s.ret_ytd,  clr:v=>_heatColor(v,30),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>12?'#fff':'var(--text)',             hint:'เขียว = ขึ้น · แดง = ลง' },
  ret_1y:    { getV:s=>s.ret_1y,   clr:v=>_heatColor(v,50),  fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0,  txt:v=>Math.abs(v??0)>20?'#fff':'var(--text)',             hint:'เขียว = ขึ้น · แดง = ลง (12 เดือน)' },
  rs_score:  { getV:s=>s.rs_score, clr:v=>_heatColorRS(v),   fmt:v=>'RS '+Math.round(v),            aFmt:v=>'avg RS '+Math.round(v),       aPos:v=>v>=50, txt:v=>(v??50)>70||(v??50)<30?'#fff':'var(--text)',        hint:'เขียว = RS สูง (แข็งแกร่ง) · แดง = RS ต่ำ (อ่อนแอ)' },
  vol_ratio: { getV:s=>(s.vol_today&&s.vol_avg20>0)?s.vol_today/s.vol_avg20*100:null, clr:v=>_heatColorVol(v), fmt:v=>(v/100).toFixed(1)+'x', aFmt:v=>'avg '+(v/100).toFixed(1)+'x', aPos:v=>v>=100, txt:v=>(v??0)>175?'#fff':'var(--text)', hint:'น้ำเงินเข้ม = Volume สูงกว่าเฉลี่ย · น้ำเงินจาง = Volume ต่ำกว่าเฉลี่ย' },
  from_52wh: { getV:s=>s.high_52w>0?(s.price-s.high_52w)/s.high_52w*100:null, clr:v=>_heatColor(v,40), fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>'avg '+(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0, txt:v=>Math.abs(v??0)>16?'#fff':'var(--text)', hint:'เขียว = ใกล้/ทำ 52W High · แดง = ห่างจาก 52W High มาก' },
  ath_dist:  { getV:s=>s.ath_pct??null, clr:v=>_heatColor(v,50), fmt:v=>(v>0?'+':'')+v.toFixed(1)+'%', aFmt:v=>'avg '+(v>0?'+':'')+v.toFixed(1)+'%', aPos:v=>v>=0, txt:v=>Math.abs(v??0)>20?'#fff':'var(--text)', hint:'เขียว = ใกล้/ทำ ATH · แดง = ห่างจาก All-Time High มาก (คำนวณจากข้อมูลที่โหลด)' },
};

function renderHeatmap() {
  if (!DATA) return;
  const cfg = HM_CFG[hmPeriod] || HM_CFG.ret_1d;
  const hintEl = document.getElementById('hm-hint');
  if (hintEl) hintEl.textContent = cfg.hint;

  const groups = {};
  DATA.stocks.forEach(s => {
    const sec = s.sector || 'Unknown';
    if (!groups[sec]) groups[sec] = [];
    groups[sec].push(s);
  });

  const sectorList = Object.entries(groups).sort((a, b) => {
    const avg = arr => { const vs = arr.map(x => cfg.getV(x)).filter(v => v != null); return vs.length ? vs.reduce((s,v)=>s+v,0)/vs.length : -999; };
    return avg(b[1]) - avg(a[1]);
  });

  document.getElementById('heatmap-grid').innerHTML = sectorList.map(([sec, stocks]) => {
    const sorted = [...stocks].sort((a, b) => (cfg.getV(b) ?? -999) - (cfg.getV(a) ?? -999));
    const withVal = sorted.filter(s => cfg.getV(s) != null);
    const avg = withVal.length ? withVal.reduce((s, x) => s + cfg.getV(x), 0) / withVal.length : null;
    const cells = sorted.map(s => {
      const v   = cfg.getV(s);
      const bg  = cfg.clr(v);
      const txt = cfg.txt(v);
      const lbl = v != null ? cfg.fmt(v) : '—';
      return `<div class="hm-cell" style="background:${bg};color:${txt}" title="${s.symbol} ${lbl}" onclick="openChartModal('${s.symbol}')">
        <span style="font-size:11px;font-weight:700;line-height:1">${s.symbol.slice(0,5)}</span>
        <span style="font-size:10px;line-height:1;opacity:0.92">${lbl}</span>
      </div>`;
    }).join('');
    const avgClass = avg != null ? (cfg.aPos(avg) ? 'green' : 'red') : 'text2';
    const avgDisp  = avg != null ? cfg.aFmt(avg) : '—';
    return `
      <div style="margin-bottom:14px">
        <div style="font-size:11px;font-weight:600;color:var(--text2);margin-bottom:4px;display:flex;align-items:center;gap:8px">
          ${sec}
          <span class="${avgClass}">${avgDisp}</span>
          <span class="text2" style="font-size:10px">${stocks.length} หุ้น</span>
        </div>
        <div style="display:flex;flex-wrap:wrap;gap:2px">${cells}</div>
      </div>`;
  }).join('');
}

// ============================================================
// EMA BREADTH BY SECTOR
// ============================================================
let emaBreadthView = 'sector';

function setEMABreadthView(v, btn) {
  emaBreadthView = v;
  document.querySelectorAll('#page-ema-breadth .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderEMABreadth();
}

// ============================================================
// MARKET BREADTH CHARTS — % above EMA / NH-NL / McClellan
// ช่วงเวลาเลือกได้: 1Y / 3Y / 5Y / All — cache แยกต่อ range ในฝั่ง client ด้วย
// ============================================================
let _breadthData = null;
let _breadthRange = '1y';
let _breadthCacheByRange = {};
let _breadthLoading = false;

function setBreadthRange(range, btn) {
  if (_breadthRange === range) return;
  _breadthRange = range;
  document.querySelectorAll('#breadth-range-btns .filter-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  loadBreadthCharts();
}

async function loadBreadthCharts() {
  // จับ range ณ ตอนเริ่ม fetch — กัน race: ถ้า user สลับปุ่มระหว่างโหลด
  // ข้อมูลเก่าต้องเก็บลง cache ใต้ key ที่ขอจริง ไม่ใช่ key ปัจจุบัน
  const rng = _breadthRange;
  const cached = _breadthCacheByRange[rng];
  if (cached) { _breadthData = cached; drawBreadthCharts(); return; }
  if (_breadthLoading) return;
  _breadthLoading = true;
  ['bc-ema', 'bc-nhnl', 'bc-mcc'].forEach(id => {
    const loading = document.getElementById(id + '-loading');
    const canvas  = document.getElementById(id);
    if (loading) { loading.style.display = 'block'; loading.textContent = 'กำลังโหลด...'; }
    if (canvas)  canvas.style.display = 'none';
  });
  try {
    const r = await fetch('/api/breadth?range=' + encodeURIComponent(rng));
    const d = await r.json();
    if (d.error) throw new Error(d.error);
    _breadthCacheByRange[rng] = d;
    if (rng === _breadthRange) {   // user ยังอยู่ range เดิม — วาดได้
      _breadthData = d;
      drawBreadthCharts();
    }
  } catch (e) {
    ['bc-ema', 'bc-nhnl', 'bc-mcc'].forEach(id => {
      const el = document.getElementById(id + '-loading');
      if (el) el.textContent = 'โหลดไม่สำเร็จ: ' + e.message;
    });
  } finally {
    _breadthLoading = false;
    // ถ้าระหว่างโหลด user สลับไป range อื่นที่ยังไม่มี cache — โหลดต่อให้เลย
    if (_breadthRange !== rng && !_breadthCacheByRange[_breadthRange]) loadBreadthCharts();
  }
}

function _bcSetup(id) {
  const loading = document.getElementById(id + '-loading');
  const canvas  = document.getElementById(id);
  if (!canvas) return null;
  // reset เป็น 100% ก่อนวัดทุกครั้ง — กันค่า px เก่า (จากรอบที่วาดตอนแท็บซ่อน) ค้างทับ
  canvas.style.width = '100%';
  canvas.style.display = 'block';
  const W = canvas.offsetWidth, H = canvas.offsetHeight || 170;
  if (!W) {
    // แท็บยังซ่อนอยู่ วัดความกว้างจริงไม่ได้ — ข้ามไป ให้ showPage วาดตอนเปิดแท็บ
    canvas.style.display = 'none';
    return null;
  }
  if (loading) loading.style.display = 'none';
  const dpr = window.devicePixelRatio || 1;
  canvas.width = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(dpr, dpr);
  return { canvas, ctx, W, H,
           pad: { l: 34, r: 6, t: 6, b: 18 } };
}

function _bcAxes(c, yMin, yMax, dates, refs) {
  const { ctx, W, H, pad } = c;
  const cH = H - pad.t - pad.b;
  const toY = v => pad.t + (1 - (v - yMin) / (yMax - yMin)) * cH;
  ctx.font = '9px sans-serif'; ctx.fillStyle = '#8b949e';
  (refs || []).forEach(rv => {
    const y = toY(rv);
    ctx.strokeStyle = 'rgba(139,148,158,.25)'; ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.setLineDash([]);
    ctx.textAlign = 'right'; ctx.fillText(String(rv), pad.l - 4, y + 3);
  });
  // label เดือนคร่าวๆ 4 จุด
  ctx.textAlign = 'center';
  for (let i = 0; i < 4; i++) {
    const idx = Math.floor(i * (dates.length - 1) / 3);
    const x = pad.l + idx / (dates.length - 1) * (W - pad.l - pad.r);
    ctx.fillText(dates[idx].slice(5, 7) + '/' + dates[idx].slice(2, 4), x, H - 5);
  }
  return toY;
}

function _bcLine(c, data, toY, color) {
  const { ctx, W, pad } = c;
  const cW = W - pad.l - pad.r;
  ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.beginPath();
  let started = false;
  data.forEach((v, i) => {
    if (v == null) return;
    const x = pad.l + i / (data.length - 1) * cW, y = toY(v);
    started ? ctx.lineTo(x, y) : ctx.moveTo(x, y); started = true;
  });
  ctx.stroke();
}

function _bcBars(c, data, toY, zero) {
  const { ctx, W, pad } = c;
  const cW = W - pad.l - pad.r, y0 = toY(zero);
  const bw = Math.max(1, cW / data.length - 0.5);
  data.forEach((v, i) => {
    if (v == null) return;
    const x = pad.l + i / (data.length - 1) * cW;
    ctx.fillStyle = v >= zero ? 'rgba(63,185,80,.75)' : 'rgba(248,81,73,.75)';
    const y = toY(v);
    ctx.fillRect(x, Math.min(y, y0), bw, Math.max(1, Math.abs(y - y0)));
  });
}

function drawBreadthCharts() {
  const d = _breadthData;
  if (!d) return;
  const n = d.dates.length;

  // 1) % above EMA50/200
  let c = _bcSetup('bc-ema');
  if (c) {
    const toY = _bcAxes(c, 0, 100, d.dates, [20, 50, 80]);
    _bcLine(c, d.pct_above_ema50,  toY, '#e3b341');
    _bcLine(c, d.pct_above_ema200, toY, '#3fb950');
    document.getElementById('bc-ema-now').innerHTML =
      ` — <span style="color:#e3b341">EMA50: ${d.pct_above_ema50[n-1]}%</span> · ` +
      `<span style="color:#3fb950">EMA200: ${d.pct_above_ema200[n-1]}%</span>`;
  }

  // 2) NH - NL net bars
  c = _bcSetup('bc-nhnl');
  if (c) {
    const net = d.nh.map((h, i) => h - d.nl[i]);
    const m = Math.max(...net.map(Math.abs), 5);
    const toY = _bcAxes(c, -m, m, d.dates, [0]);
    _bcBars(c, net, toY, 0);
    const r = d.nhnl_ratio[n-1];
    document.getElementById('bc-nhnl-now').textContent =
      ` — NH ${d.nh[n-1]} / NL ${d.nl[n-1]}` + (r != null ? ` (ratio ${r})` : '');
  }

  // 3) McClellan Oscillator
  c = _bcSetup('bc-mcc');
  if (c) {
    const osc = d.mcclellan_osc;
    const m = Math.max(...osc.filter(v => v != null).map(Math.abs), 50);
    const toY = _bcAxes(c, -m, m, d.dates, [-70, 0, 70]);
    _bcBars(c, osc, toY, 0);
    const cur = osc[n-1], sum = d.mcclellan_sum[n-1];
    document.getElementById('bc-mcc-now').innerHTML =
      ` — <span style="color:${cur >= 0 ? '#3fb950' : '#f85149'}">${cur}</span>` +
      ` · summation ${sum >= 0 ? '+' : ''}${sum}`;
  }
}

// ============================================================
// MARKET REGIME LIGHT — ไฟ 3 โซนบน nav จาก % หุ้นเหนือ EMA200
// (เกณฑ์โซนมาจากผล backtest — ดูรายละเอียดที่ /backtest-report)
// ============================================================
async function loadRegimeLight() {
  const wrap  = document.getElementById('regime-wrap');
  const light = document.getElementById('regime-light');
  const tip   = document.getElementById('regime-tip');
  if (!wrap || !light) return;
  try {
    if (!_breadthData) {
      const r = await fetch('/api/breadth');
      const d = await r.json();
      if (d.error) return;
      _breadthData = d;
    }
    const arr = _breadthData.pct_above_ema200;
    const val = arr[arr.length - 1];
    if (val == null) return;

    let cls, icon, label, desc;
    if (val >= 50)      { cls = 'regime-on';  icon = '🟢'; label = 'Risk-On';
      desc = 'หุ้นเกินครึ่งตลาดอยู่เหนือ EMA200 — ช่วงที่สัญญาณ RS สูงทำงานได้ดีตามประวัติศาสตร์'; }
    else if (val >= 30) { cls = 'regime-mid'; icon = '🟡'; label = 'Caution';
      desc = 'โซน whipsaw (แบบปี 2024 ใน backtest) — ตลาดแกว่ง ระวังการเข้าออกถี่ พิจารณาลดขนาด position'; }
    else                { cls = 'regime-off'; icon = '🔴'; label = 'Risk-Off';
      desc = 'ทุก threshold ที่ backtest ทดสอบเห็นตรงกันว่าช่วงแบบนี้ (เช่น 2018–2019) การถือเงินสดคุ้มกว่า'; }

    light.className = cls;
    light.innerHTML = `${icon} ${label} <span style="opacity:.75">${val.toFixed(0)}%</span>`;
    tip.innerHTML =
      `<b>Market Regime: ${icon} ${label}</b><br>
       <span style="color:var(--text2)">% หุ้นเหนือ EMA200 ทั้งตลาด = <b>${val.toFixed(1)}%</b></span><br><br>
       ${desc}<br><br>
       <span style="color:#3fb950">🟢 ≥ 50</span> Risk-On ·
       <span style="color:#e3b341">🟡 30–50</span> Caution ·
       <span style="color:#f85149">🔴 &lt; 30</span> Risk-Off<br><br>
       <span style="color:var(--text2);font-size:11px">เกณฑ์มาจาก backtest RS+RRG 10.5 ปี (2016–2026) —
       regime filter เปลี่ยนปี 2018/2019 จาก −36.6%/−25.3% เป็น +4.5%/+7.1%
       แต่มีข้อจำกัดสำคัญ (survivorship bias ฯลฯ)</span><br>
       <a href="/backtest-report" target="_blank" rel="noopener">📄 อ่านรายงาน backtest ฉบับเต็ม + ข้อจำกัด →</a><br>
       <span style="color:var(--text2);font-size:10px">ข้อมูลบริบท ไม่ใช่คำแนะนำการลงทุน · คลิกที่ไฟเพื่อเปิดรายงาน</span>`;
    wrap.style.display = 'inline-block';
  } catch (e) { /* เงียบ — ไฟไม่ขึ้นแต่ dashboard ทำงานปกติ */ }
}

function renderEMABreadth() {
  if (!DATA) return;
  loadBreadthCharts();
  const key = emaBreadthView;
  const groups = {};
  DATA.stocks.forEach(s => {
    const g = s[key] || 'Unknown';
    if (!groups[g]) groups[g] = [];
    groups[g].push(s);
  });

  const rows = Object.entries(groups).map(([name, stocks]) => {
    const n    = stocks.length;
    const p20  = Math.round(stocks.filter(s => s.above_ema20  === true).length / n * 100);
    const p50  = Math.round(stocks.filter(s => s.above_ema50  === true).length / n * 100);
    const p200 = Math.round(stocks.filter(s => s.above_ema200 === true).length / n * 100);
    const score = Math.round((p20 + p50 + p200) / 3);
    return { name, n, p20, p50, p200, score };
  }).sort((a, b) => b.score - a.score);

  const bar = (v) => {
    const c = v >= 60 ? 'var(--green)' : v >= 40 ? 'var(--yellow)' : 'var(--red)';
    const tc = v >= 60 ? 'green' : v >= 40 ? 'yellow' : 'red';
    return `<div style="display:flex;align-items:center;gap:6px">
      <div style="flex:1;height:6px;background:var(--bg3);border-radius:3px;overflow:hidden">
        <div style="width:${v}%;height:100%;background:${c};border-radius:3px"></div>
      </div>
      <span class="${tc}" style="width:38px;text-align:right;font-size:12px;font-weight:600">${v}%</span>
    </div>`;
  };

  document.getElementById('ema-breadth-tbody').innerHTML = rows.map((r, i) => `
    <tr style="cursor:pointer" onclick="openSectorModal('${r.name.replace(/'/g,"\\'")}')">
      <td class="text2">${i + 1}</td>
      <td><strong>${r.name}</strong>${sectorTvLink(r.name)}</td>
      <td class="r text2">${r.n}</td>
      <td style="min-width:140px;padding-right:12px">${bar(r.p20)}</td>
      <td style="min-width:140px;padding-right:12px">${bar(r.p50)}</td>
      <td style="min-width:140px;padding-right:12px">${bar(r.p200)}</td>
      <td class="r"><span class="${rsColor(r.score)}" style="font-weight:700">${r.score}</span></td>
    </tr>`).join('');
}

// ============================================================
// ============================================================
// BREAKOUT RADAR
// ============================================================
let _boRS = 70, _boDist = 10, _boEMA = '50';
let _boSide = 'high';   // 'high' = Breakout/High watch, 'low' = Low watch
let _boSortCol = 'rs_score', _boSortDir = 1;

const _BO_STR  = new Set(['symbol','name','sector']);
const _BO_BOOL = new Set(['above_ema50','above_ema200']);

function setBoSort(col) {
  if (_boSortCol === col) _boSortDir *= -1;
  else { _boSortCol = col; _boSortDir = 1; }
  renderBreakout();
}

// tooltip กลางอธิบายคอลัมน์ตาราง — ใช้ร่วมกันทุกเมนู (hover หัวตารางเพื่อดู)
const _COL_TIPS = {
  rs_score:     'RS Score 0–99 — จัดอันดับความแข็งแกร่งของราคาเทียบหุ้นทั้งตลาด ยิ่งสูงยิ่งแข็งแกร่ง (99 = แข็งสุด)',
  rs_momentum:  'RS ปัจจุบัน − RS เมื่อ 4 สัปดาห์ก่อน — บวก = ความแข็งแกร่งกำลังเร่งขึ้น, ลบ = กำลังแผ่ว',
  sec_rank:     'อันดับ RS ภายใน sector เดียวกัน เช่น 3/25 = RS สูงเป็นอันดับ 3 จาก 25 ตัวในกลุ่ม',
  symbol:       'ชื่อย่อหุ้น — คลิกเพื่อเปิดกราฟ',
  name:         'ชื่อบริษัท',
  sector:       'กลุ่มอุตสาหกรรม',
  industry:     'หมวดธุรกิจ (ละเอียดกว่า sector)',
  price:        'ราคาปิดล่าสุด (บาท)',
  high_52w:     'ราคาสูงสุดใน 52 สัปดาห์ย้อนหลัง (ไม่รวมแท่งวันล่าสุด)',
  low_52w:      'ราคาต่ำสุดใน 52 สัปดาห์ย้อนหลัง (ไม่รวมแท่งวันล่าสุด)',
  ret_1d:       'ผลตอบแทนวันนี้ (เทียบราคาปิดเมื่อวาน)',
  ret_1w:       'ผลตอบแทนย้อนหลัง 1 สัปดาห์',
  ret_1m:       'ผลตอบแทนย้อนหลัง 1 เดือน',
  ret_3m:       'ผลตอบแทนย้อนหลัง 3 เดือน',
  ret_6m:       'ผลตอบแทนย้อนหลัง 6 เดือน',
  ret_1y:       'ผลตอบแทนย้อนหลัง 1 ปี',
  ret_ytd:      'ผลตอบแทนตั้งแต่ต้นปี (Year-to-Date)',
  fromHigh:     '% ห่างจาก 52W High — ติดลบ = ยังต่ำกว่า high, 0 หรือบวก = ทำ new high',
  fromLow:      '% เด้งขึ้นจาก 52W Low — ยิ่งน้อยยิ่งใกล้จุดต่ำสุดของปี',
  pe:           'P/E Ratio = ราคา ÷ กำไรต่อหุ้น — ยิ่งต่ำยิ่งถูก (เทียบภายใน sector เดียวกัน), ติดลบ/ว่าง = ขาดทุน',
  pbv:          'P/BV Ratio = ราคา ÷ มูลค่าทางบัญชีต่อหุ้น — ต่ำกว่า 1 = ซื้อได้ถูกกว่ามูลค่าบัญชี',
  div_yield:    'อัตราเงินปันผลตอบแทนต่อปี (% ของราคาปัจจุบัน)',
  vol_today:    'Relative Volume = volume วันนี้ ÷ ค่าเฉลี่ย 20 วัน — เช่น 2.0x = ซื้อขายคึกคักกว่าปกติ 2 เท่า',
  rvol:         'Relative Volume = volume วันนี้ ÷ ค่าเฉลี่ย 20 วัน — เช่น 2.0x = ซื้อขายคึกคักกว่าปกติ 2 เท่า',
  mkt_cap:      'มูลค่าตลาด = ราคา × จำนวนหุ้นจดทะเบียน',
  above_ema50:  'ราคาอยู่เหนือเส้น EMA 50 วันหรือไม่ (✓ = แนวโน้มระยะกลางขาขึ้น)',
  above_ema200: 'ราคาอยู่เหนือเส้น EMA 200 วันหรือไม่ (✓ = แนวโน้มระยะยาวขาขึ้น)',
  avg_rs:       'ค่าเฉลี่ย RS Score ของหุ้นทุกตัวในกลุ่ม — ใช้เทียบความแข็งแกร่งระหว่างกลุ่ม',
};
const _BO_TIP_RANGE     = 'ตำแหน่งราคาในกรอบ 52 สัปดาห์: 0% = ที่ Low, 100% = ที่ High, เกิน 100% = ทะลุ High เดิม (วัดเป็นสัดส่วนของความกว้างกรอบ High−Low ไม่ใช่ % ของราคา)';
const _BO_TIP_FROM_HIGH = '% ห่างจาก 52W High — ติดลบ = ยังต่ำกว่า high, NEW HIGH = ราคาทะลุ high เดิมแล้ว';
const _BO_TIP_FROM_LOW  = '% เด้งขึ้นจาก 52W Low — ยิ่งน้อยยิ่งใกล้จุดต่ำสุด, NEW LOW = ทำจุดต่ำสุดใหม่';

// คืน attribute title="..." สำหรับใส่ใน <th> — คืน '' ถ้าไม่มีคำอธิบายของคอลัมน์นั้น
function colTip(col) {
  const t = _COL_TIPS[col];
  return t ? ` title="${t}"` : '';
}

function boTh(col, label, cls='', tip=null) {
  const active = _boSortCol === col;
  const arrow  = active ? (_boSortDir === 1 ? '↓' : '↑') : '↕';
  const c = (cls ? cls+' ' : '') + 'sortable';
  const tAttr = tip ? ` title="${tip}"` : colTip(col);
  return `<th class="${c}"${tAttr} onclick="setBoSort('${col}')">${label}<span class="sort-ind${active?' on':''}">${arrow}</span></th>`;
}

function setBO(type, val, btn) {
  if (type === 'rs')   _boRS   = val;
  if (type === 'dist') _boDist = val;
  if (type === 'ema')  _boEMA  = val;
  if (type === 'side') _boSide = val;
  // map type -> id prefix ของกลุ่มปุ่ม (dist ใช้ id "bo-d-*" ไม่ใช่ "bo-dist*"
  // — บั๊กเดิม: prefix ไม่ match ทำให้สี active ของกลุ่มระยะไม่เคยถูกล้าง)
  const prefix = { rs: 'bo-rs-', dist: 'bo-d-', ema: 'bo-ema-', side: 'bo-side-' }[type];
  document.querySelectorAll(`#page-breakout .filter-btn[id^="${prefix}"]`)
    .forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderBreakout();
}

function renderBreakout() {
  if (!DATA) return;
  const isHigh = _boSide === 'high';
  const aLbl   = isHigh ? 'High' : 'Low';
  const secRanks = computeSectorRanks();
  const stocks = DATA.stocks.map(s => {
    const anchor     = isHigh ? s.high_52w : s.low_52w;
    const fromAnchor = anchor > 0 ? (s.price - anchor) / anchor * 100 : null;
    const range      = (s.high_52w ?? 0) - (s.low_52w ?? 0);
    const rangePct   = range > 0 ? Math.round((s.price - s.low_52w) / range * 100) : null;
    const sr = secRanks[s.symbol];
    return { ...s, fromAnchor, rangePct, sec_rank: sr?.rank ?? null, sec_total: sr?.total ?? null };
  }).filter(s => {
    if (s.fromAnchor == null) return false;
    if ((s.rs_score ?? 0) < _boRS) return false;
    // high: ห่างลงจาก High ไม่เกิน X% | low: เด้งขึ้นจาก Low ไม่เกิน X%
    if (isHigh ? (s.fromAnchor < -_boDist) : (s.fromAnchor > _boDist)) return false;
    if (_boEMA === '50'  && !s.above_ema50)  return false;
    if (_boEMA === '200' && (!s.above_ema50 || !s.above_ema200)) return false;
    return true;
  }).sort((a, b) => {
    const col = _boSortCol;
    if (_BO_BOOL.has(col)) return ((b[col]?1:0) - (a[col]?1:0)) * _boSortDir;
    if (_BO_STR.has(col))  return ((a[col]??'').localeCompare(b[col]??'')) * _boSortDir;
    return ((b[col]??-Infinity) - (a[col]??-Infinity)) * _boSortDir;
  });

  const parts = [`${stocks.length} หุ้น`, isHigh ? 'ใกล้ 52W High' : 'ใกล้ 52W Low'];
  if (_boRS > 0)     parts.push(`RS ≥ ${_boRS}`);
  if (_boDist < 100) parts.push(`ห่าง ${aLbl} ≤ ${_boDist}%`);
  if (_boEMA !== 'any') parts.push(_boEMA === '50' ? '> EMA50' : '> EMA50+200');
  document.getElementById('bo-count').textContent = parts.join(' · ');

  const anchorCol = isHigh ? 'high_52w' : 'low_52w';
  document.getElementById('bo-thead').innerHTML = `<tr>
    ${boTh('rs_score','RS')}${boTh('sec_rank','Sec.Rank','r')}${boTh('symbol','Symbol')}${boTh('name','ชื่อ')}${boTh('sector','Sector')}
    ${boTh('price','ราคา','r')}${boTh(anchorCol,'52W '+aLbl,'r')}${boTh('fromAnchor','% จาก '+aLbl,'r', isHigh ? _BO_TIP_FROM_HIGH : _BO_TIP_FROM_LOW)}
    <th title="${_BO_TIP_RANGE}">Range</th>
    ${boTh('ret_1m','1M%','r')}${boTh('ret_3m','3M%','r')}${boTh('ret_ytd','YTD%','r')}
    ${boTh('vol_today','RVOL','r')}${boTh('mkt_cap','MKT CAP','r')}${boTh('above_ema50','EMA50','r')}${boTh('above_ema200','EMA200','r')}
  </tr>`;
  document.getElementById('bo-tbody').innerHTML = stocks.map(s => {
    const fa = s.fromAnchor;
    const faHtml = isHigh
      ? `<span class="${fa >= -2 ? 'green' : fa >= -5 ? 'yellow' : 'text2'}">${fa >= 0 ? 'NEW HIGH' : fa.toFixed(1) + '%'}</span>`
      : `<span class="${fa <= 2 ? 'red' : fa <= 5 ? 'yellow' : 'text2'}">${fa <= 0.5 ? 'NEW LOW' : '+' + fa.toFixed(1) + '%'}</span>`;
    return `
    <tr>
      <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score ?? '—'}</span></td>
      <td class="r">${secRankHtml(s.sec_rank ? {rank:s.sec_rank,total:s.sec_total} : null)}</td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}${dqBadge(s)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector || '—'}</td>
      <td class="r">${s.price?.toFixed(2) ?? '—'}</td>
      <td class="r text2">${(isHigh ? s.high_52w : s.low_52w)?.toFixed(2) ?? '—'}</td>
      <td class="r">${faHtml}</td>
      <td>
        <div class="range-bar-wrap">
          <div class="range-bar-track"><div class="range-bar-fill" style="width:${s.rangePct ?? 0}%"></div></div>
          <div style="font-size:10px;color:var(--text2);text-align:right">${s.rangePct ?? '—'}%</div>
        </div>
      </td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r">${pct(s.ret_ytd)}</td>
      <td class="r">${rvolHtml(s)}</td>
      <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
    </tr>`;
  }).join('');
}

// ============================================================
// MOMENTUM ALIGNMENT
// ============================================================
let _momFilter = 'all4';
let _momSortCol = 'rs_score', _momSortDir = 1;

const _MOM_STR  = new Set(['symbol','name','sector']);
const _MOM_BOOL = new Set(['above_ema50','above_ema200']);

function setMomSort(col) {
  if (_momSortCol === col) _momSortDir *= -1;
  else { _momSortCol = col; _momSortDir = 1; }
  renderMomentum();
}

function momTh(col, label, cls='') {
  const active = _momSortCol === col;
  const arrow  = active ? (_momSortDir === 1 ? '↓' : '↑') : '↕';
  const c = (cls ? cls+' ' : '') + 'sortable';
  return `<th class="${c}"${colTip(col)} onclick="setMomSort('${col}')">${label}<span class="sort-ind${active?' on':''}">${arrow}</span></th>`;
}

function setMomFilter(val, btn) {
  _momFilter = val;
  document.querySelectorAll('#page-momentum .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderMomentum();
}

function renderMomentum() {
  if (!DATA) return;
  const secRanks = computeSectorRanks();
  const stocks = DATA.stocks.filter(s => {
    if (_momFilter === 'all4') return (s.ret_1d ?? -1) > 0 && (s.ret_1w ?? -1) > 0 && (s.ret_1m ?? -1) > 0 && (s.ret_3m ?? -1) > 0;
    if (_momFilter === '3tf')  return (s.ret_1w ?? -1) > 0 && (s.ret_1m ?? -1) > 0 && (s.ret_3m ?? -1) > 0;
    if (_momFilter === '2tf')  return (s.ret_1m ?? -1) > 0 && (s.ret_3m ?? -1) > 0;
    return true;
  }).map(s => {
    const sr = secRanks[s.symbol];
    return { ...s, sec_rank: sr?.rank ?? null, sec_total: sr?.total ?? null };
  }).sort((a, b) => {
    const col = _momSortCol;
    if (_MOM_BOOL.has(col)) return ((b[col]?1:0) - (a[col]?1:0)) * _momSortDir;
    if (_MOM_STR.has(col))  return ((a[col]??'').localeCompare(b[col]??'')) * _momSortDir;
    return ((b[col]??-Infinity) - (a[col]??-Infinity)) * _momSortDir;
  });

  const tfLabel = { all4: 'ทุก 4 Timeframe (1D·1W·1M·3M)', '3tf': '3 Timeframe (1W·1M·3M)', '2tf': '2 Timeframe (1M·3M)' };
  document.getElementById('mom-count').textContent = `${stocks.length} หุ้น — ${tfLabel[_momFilter]}`;
  document.getElementById('mom-thead').innerHTML = `<tr>
    ${momTh('rs_score','RS')}${momTh('sec_rank','Sec.Rank','r')}${momTh('symbol','Symbol')}${momTh('name','ชื่อ')}${momTh('sector','Sector')}
    ${momTh('price','ราคา','r')}
    ${momTh('ret_1d','1D%','r')}${momTh('ret_1w','1W%','r')}${momTh('ret_1m','1M%','r')}${momTh('ret_3m','3M%','r')}${momTh('ret_ytd','YTD%','r')}
    ${momTh('vol_today','RVOL','r')}${momTh('mkt_cap','MKT CAP','r')}${momTh('above_ema50','EMA50','r')}${momTh('above_ema200','EMA200','r')}
  </tr>`;
  document.getElementById('mom-tbody').innerHTML = stocks.map(s => {
    return `
    <tr>
      <td><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score ?? '—'}</span></td>
      <td class="r">${secRankHtml(s.sec_rank ? {rank:s.sec_rank,total:s.sec_total} : null)}</td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector || '—'}</td>
      <td class="r">${s.price?.toFixed(2) ?? '—'}</td>
      <td class="r">${pct(s.ret_1d)}</td>
      <td class="r">${pct(s.ret_1w)}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_3m)}</td>
      <td class="r">${pct(s.ret_ytd)}</td>
      <td class="r">${rvolHtml(s)}</td>
      <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
      <td class="r">${emaBadge(s.above_ema200)}</td>
    </tr>`}).join('');
}

// ============================================================
// PERSISTENT SCREENER SETTINGS
// ============================================================
const _SCR_LS = 'set_scr_v1';
const _SCR_FIELDS = ['scr-rs-min','scr-1m','scr-3m','scr-1d','scr-ytd','scr-cap',
                     'scr-price-min','scr-price-max','scr-from-high','scr-ath-dist','scr-from-low',
                     'scr-pe','scr-pbv','scr-dy','scr-rvol','scr-1w','scr-6m','scr-1y'];
const _SCR_CHECKS = ['scr-ema20','scr-ema50','scr-ema200','scr-golden-cross',
                     'scr-new52h','scr-sma-cross50','scr-sma-cross200',
                     'scr-ema-cross50','scr-ema-cross200','scr-rsi-rebound','scr-bullish-vol',
                     'scr-backtest'];

function _populateScrIndustry() {
  if (!DATA) return;
  const sel = document.getElementById('scr-industry');
  if (!sel) return;
  const industries = [...new Set(DATA.stocks.map(s => s.industry).filter(Boolean))].sort();
  while (sel.options.length > 1) sel.remove(1);
  industries.forEach(ind => {
    const opt = document.createElement('option');
    opt.value = ind; opt.textContent = ind;
    sel.appendChild(opt);
  });
}

function saveScreenerSettings() {
  const s = {};
  _SCR_FIELDS.forEach(id => { const el = document.getElementById(id); if (el) s[id] = el.value; });
  _SCR_CHECKS.forEach(id => { const el = document.getElementById(id); if (el) s[id] = el.checked; });
  const mk = document.getElementById('scr-market'); if (mk) s['scr-market'] = mk.value;
  const ind = document.getElementById('scr-industry'); if (ind) s['scr-industry'] = ind.value;
  localStorage.setItem(_SCR_LS, JSON.stringify(s));
}

function loadScreenerSettings() {
  try {
    const raw = localStorage.getItem(_SCR_LS);
    if (!raw) return;
    const s = JSON.parse(raw);
    _SCR_FIELDS.forEach(id => { if (s[id] != null) { const el = document.getElementById(id); if (el) el.value = s[id]; } });
    _SCR_CHECKS.forEach(id => { if (s[id] != null) { const el = document.getElementById(id); if (el) el.checked = s[id]; } });
    if (s['scr-market']) { const el = document.getElementById('scr-market'); if (el) el.value = s['scr-market']; }
    if (s['scr-industry']) { const el = document.getElementById('scr-industry'); if (el) el.value = s['scr-industry']; }
  } catch(e) {}
}

// autosave ทุกครั้งที่แก้ค่า filter — เดิมบันทึกเฉพาะตอนกด "ค้นหา"
// ทำให้ค่าที่พิมพ์ไว้แต่ยังไม่ได้กดค้นหาหายเมื่อ refresh หน้า
let _scrAutosaveWired = false;
function initScreenerAutosave() {
  if (_scrAutosaveWired) return;
  _scrAutosaveWired = true;
  _SCR_FIELDS.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('input', saveScreenerSettings);
  });
  _SCR_CHECKS.concat(['scr-market', 'scr-industry']).forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', saveScreenerSettings);
  });
}

// ============================================================
// SAVED PRESETS
// ============================================================
const _PRESETS_LS = 'set_scr_presets_v1';

function _loadPresets() {
  try { return JSON.parse(localStorage.getItem(_PRESETS_LS) || '{}'); } catch(e) { return {}; }
}

function saveCurrentPreset() {
  const name = document.getElementById('preset-name-input').value.trim();
  if (!name) return;
  const presets = _loadPresets();
  const s = {};
  _SCR_FIELDS.forEach(id => { const el = document.getElementById(id); if (el) s[id] = el.value; });
  _SCR_CHECKS.forEach(id => { const el = document.getElementById(id); if (el) s[id] = el.checked; });
  const mk = document.getElementById('scr-market'); if (mk) s['scr-market'] = mk.value;
  const ind = document.getElementById('scr-industry'); if (ind) s['scr-industry'] = ind.value;
  presets[name] = s;
  localStorage.setItem(_PRESETS_LS, JSON.stringify(presets));
  document.getElementById('preset-name-input').value = '';
  renderSavedPresets();
}

function loadSavedPreset(name) {
  const presets = _loadPresets();
  const s = presets[name];
  if (!s) return;
  _SCR_FIELDS.forEach(id => { if (s[id] != null) { const el = document.getElementById(id); if (el) el.value = s[id]; } });
  _SCR_CHECKS.forEach(id => { if (s[id] != null) { const el = document.getElementById(id); if (el) el.checked = s[id]; } });
  if (s['scr-market']) { const el = document.getElementById('scr-market'); if (el) el.value = s['scr-market']; }
  if (s['scr-industry']) { const el = document.getElementById('scr-industry'); if (el) el.value = s['scr-industry']; }
  runScreener();
}

function deleteSavedPreset(name) {
  const presets = _loadPresets();
  delete presets[name];
  localStorage.setItem(_PRESETS_LS, JSON.stringify(presets));
  renderSavedPresets();
}

function renderSavedPresets() {
  const presets = _loadPresets();
  const row = document.getElementById('saved-presets-row');
  if (!row) return;
  const names = Object.keys(presets);
  if (names.length === 0) {
    row.innerHTML = '<span style="font-size:11px;color:var(--text2);font-style:italic">ยังไม่มี preset</span>';
    return;
  }
  // ชื่อ preset ต้อง escape เป็น HTML entity — JSON.stringify ให้ double quote
  // ซึ่งชนกับ quote ของ attribute ทำให้ onclick พังเงียบ (บั๊กเดิม)
  const _attrArg = n => JSON.stringify(n)
    .replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  const _escHtml = n => String(n)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  row.innerHTML = names.map(n =>
    `<span class="preset-saved-btn" onclick="loadSavedPreset(${_attrArg(n)})">
      ${_escHtml(n)}<button class="preset-del" onclick="event.stopPropagation();deleteSavedPreset(${_attrArg(n)})" title="ลบ">✕</button>
    </span>`
  ).join('');
}

// ============================================================
// EXPORT CSV
// ============================================================
function exportScreenerCSV() {
  if (!_scrStocks || _scrStocks.length === 0) return;
  const secRanks = computeSectorRanks();
  const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const header = ['Symbol','Name','Sector','RS','Sec.Rank','Price',
                  '1D%','1W%','1M%','3M%','YTD%','1Y%',
                  '% From High','% From Low','P/E','P/BV','Div Yield%',
                  'Mkt Cap (MB)','EMA50','EMA200'].map(esc).join(',');
  const rows = _scrStocks.map(s => {
    const fh = s.high_52w > 0 ? ((s.price - s.high_52w) / s.high_52w * 100).toFixed(2) : '';
    const fl = s.low_52w  > 0 ? ((s.price - s.low_52w)  / s.low_52w  * 100).toFixed(2) : '';
    const sr = secRanks[s.symbol];
    return [
      s.symbol, s.name, s.sector || '',
      s.rs_score ?? '', sr ? `${sr.rank}/${sr.total}` : '',
      s.price?.toFixed(2) ?? '',
      s.ret_1d?.toFixed(2) ?? '', s.ret_1w?.toFixed(2) ?? '',
      s.ret_1m?.toFixed(2) ?? '', s.ret_3m?.toFixed(2) ?? '',
      s.ret_ytd?.toFixed(2) ?? '', s.ret_1y?.toFixed(2) ?? '',
      fh, fl,
      s.pe?.toFixed(1) ?? '', s.pbv?.toFixed(2) ?? '',
      s.div_yield?.toFixed(2) ?? '',
      s.mkt_cap ? Math.round(s.mkt_cap / 1e6) : '',
      s.above_ema50 === true ? 'Y' : s.above_ema50 === false ? 'N' : '',
      s.above_ema200 === true ? 'Y' : s.above_ema200 === false ? 'N' : '',
    ].map(esc).join(',');
  });
  const csv = '﻿' + header + '\n' + rows.join('\n');
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' })),
    download: `SET_Screener_${new Date().toISOString().slice(0,10)}.csv`,
  });
  a.click(); URL.revokeObjectURL(a.href);
}

// ============================================================
// PRICE CHART MODAL
// ============================================================
let _cmStock       = null;
let _cmTf          = '1y';
let _cmHistoryData = null;  // full history from /api/history/<symbol>
let _cmVolumeData  = null;  // volume array aligned with _cmHistoryData
const _CM_TF_BARS  = { '1m': 21, '3m': 63, '6m': 126, '1y': 260, '5y': 1260, 'max': 99999 };

async function setCmTf(tf, btn) {
  _cmTf = tf;
  document.querySelectorAll('#chart-modal .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  if (!_cmStock) return;
  if ((tf === '5y' || tf === 'max') && !_cmHistoryData) {
    await _fetchLongHistory();
  }
  _drawChart(_cmStock, _cmHistoryData);
}

async function _fetchLongHistory() {
  if (!_cmStock) return;
  const canvas = document.getElementById('cm-canvas');
  if (canvas) {
    const dpr = window.devicePixelRatio || 1;
    const W   = canvas.parentElement.clientWidth - 40;
    const H   = 300;
    canvas.width  = W * dpr; canvas.height = H * dpr;
    canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.fillStyle = '#161b22';
    ctx.fillRect(0, 0, W, H);
    ctx.fillStyle = '#8b949e';
    ctx.font = '13px Segoe UI,sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('กำลังโหลด full history...', W / 2, H / 2);
  }
  try {
    const endpoint = _cmStock._isDR
      ? `/api/dr-history/${encodeURIComponent(_cmStock.symbol)}`
      : `/api/history/${encodeURIComponent(_cmStock.symbol)}`;
    const r = await fetch(endpoint);
    const d = await r.json();
    if (d.error || !d.dates) {
      console.warn('history fetch error:', d.error);
      return;
    }
    _cmHistoryData = d.dates.map((date, i) => [date, d.closes[i]]);
    _cmVolumeData  = d.volumes || null;
  } catch (e) {
    console.error('fetch history error:', e);
  }
}

function openDRChartModal(sym) {
  const s = (_drData || []).find(x => x.sym === sym);
  if (!s) return;

  const _cmTitleEl1 = document.getElementById('cm-title');
  _cmTitleEl1.textContent = `${s.sym}  ${s.name}`;
  _cmTitleEl1.title = `${s.sym}  ${s.name}`;
  document.getElementById('cm-sub').textContent   = `${s.ind || '—'} · RS ${s.rs_score ?? '—'} · ${_drExchBadge(s.yf)}`;
  document.getElementById('cm-tv-link').href = `https://www.tradingview.com/chart/?symbol=${yfToTVSym(s.yf)}&interval=D`;

  const mk = (val, lbl, cls = '') =>
    `<div><div class="cm-metric-val ${cls}">${val}</div><div class="cm-metric-lbl">${lbl}</div></div>`;
  const cPct = v => v != null ? (v >= 0 ? '+' : '') + v.toFixed(2) + '%' : '—';
  const pCls = v => v == null ? '' : v >= 0 ? 'green' : 'red';

  const athPctCls = v => v == null ? '' : v >= -5 ? 'green' : v >= -20 ? 'yellow' : 'red';
  const fmtPx = v => v != null ? _drFmtPrice(v) : '—';
  document.getElementById('cm-metrics').innerHTML = [
    mk(_drFmtPrice(s.price), 'ราคา'),
    mk(cPct(s.chg),    '1D%', pCls(s.chg)),
    mk(cPct(s.ret_1w), '1W%', pCls(s.ret_1w)),
    mk(cPct(s.ret_1m), '1M%', pCls(s.ret_1m)),
    mk(cPct(s.ret_3m), '3M%', pCls(s.ret_3m)),
    `<span id="cm-extra-metrics" style="display:contents">` +
      mk(cPct(s.ret_ytd), 'YTD%', pCls(s.ret_ytd)) +
      mk(fmtPx(s.high_52w), '52W High', 'text2') +
      mk(fmtPx(s.low_52w),  '52W Low',  'text2') +
      mk(fmtPx(s.ath),      'ATH',      'text2') +
      mk(s.ath_pct != null ? (s.ath_pct >= 0 ? '+' : '') + s.ath_pct.toFixed(1) + '%' : '—', '% จาก ATH', athPctCls(s.ath_pct)) +
    `</span>`,
    mk(_drFmtCap(s.mkt_cap), 'MKT CAP', 'text2'),
  ].join('');

  _cmStock = { ...s, symbol: s.sym, price_history: null, _isDR: true };
  _cmTf = '1y';
  _cmHistoryData = null;
  _cmVolumeData  = null;
  _cmFinLoaded = null;
  const drFinBtn    = document.getElementById('cm-mode-fin');
  const drStocksBtn = document.getElementById('cm-mode-stocks');
  if (drFinBtn)    drFinBtn.style.display    = 'none';
  if (drStocksBtn) drStocksBtn.style.display = 'none';
  const drFsLink = document.getElementById('cm-factsheet-link');
  if (drFsLink) drFsLink.style.display = 'none';
  const drSetLink = document.getElementById('cm-set-link');
  if (drSetLink) drSetLink.style.display = 'none';
  setCmMode('chart');
  document.querySelectorAll('#chart-modal .filter-btn').forEach(b => b.classList.remove('active'));
  const tfBtn = document.getElementById('cm-tf-1y');
  if (tfBtn) tfBtn.classList.add('active');
  document.getElementById('chart-modal').classList.add('open');
  document.body.style.overflow = 'hidden';

  // ใช้ close100 เป็น initial preview แล้วโหลด full history ใน background
  const c100 = s.close100 || [];
  if (c100.length) {
    const today = new Date();
    const initHistory = c100.map((price, i) => {
      const d = new Date(today);
      d.setDate(d.getDate() - (c100.length - 1 - i));
      return [d.toISOString().slice(0, 10), price];
    });
    requestAnimationFrame(() => _drawChart(_cmStock, initHistory));
  }
  // โหลด full 5Y history จาก yfinance
  _fetchDRFullHistory(sym);
}

async function _fetchDRFullHistory(sym) {
  try {
    const r = await fetch(`/api/dr-history/${encodeURIComponent(sym)}`);
    const d = await r.json();
    if (d.error || !d.dates) return;
    const hist = d.dates.map((date, i) => [date, d.closes[i]]);
    _cmHistoryData = hist;
    if (_cmStock?.symbol === sym) _drawChart(_cmStock, hist);
  } catch(e) { console.error('DR history fetch error:', e); }
}

function _calcOBV(prices, volumes) {
  const obv = new Array(prices.length).fill(0);
  for (let i = 1; i < prices.length; i++) {
    const v = volumes[i] || 0;
    obv[i] = obv[i-1] + (prices[i] > prices[i-1] ? v : prices[i] < prices[i-1] ? -v : 0);
  }
  return obv;
}

function _fmtVolRaw(v) {
  if (!v) return '—';
  if (v >= 1e12) return (v/1e12).toFixed(2) + 'T';
  if (v >= 1e9)  return (v/1e9).toFixed(1)  + 'B';
  if (v >= 1e6)  return (v/1e6).toFixed(1)  + 'M';
  if (v >= 1e3)  return (v/1e3).toFixed(1)  + 'K';
  return v.toLocaleString();
}

function _calcEMA(prices, period) {
  const out = new Array(prices.length).fill(null);
  if (prices.length < period) return out;
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  out[period - 1] = ema;
  for (let i = period; i < prices.length; i++) {
    ema = prices[i] * k + ema * (1 - k);
    out[i] = ema;
  }
  return out;
}

function openChartModal(symbol) {
  if (!DATA) return;
  const s = DATA.stocks.find(x => x.symbol === symbol);
  if (!s || !s.price_history || s.price_history.length < 5) {
    alert(`ไม่มีข้อมูลราคาสำหรับ ${symbol} — ลอง Full Refresh`);
    return;
  }

  const _cmTitleEl2 = document.getElementById('cm-title');
  _cmTitleEl2.textContent = `${s.symbol}  ${s.name}`;
  _cmTitleEl2.title = `${s.symbol}  ${s.name}`;
  document.getElementById('cm-sub').textContent   = `${s.sector || '—'} · RS ${s.rs_score ?? '—'} · ${s.market || ''}`;
  document.getElementById('cm-tv-link').href = `https://www.tradingview.com/chart/?symbol=SET:${s.symbol}&interval=D`;
  const fsLink = document.getElementById('cm-factsheet-link');
  if (fsLink) { fsLink.href = `https://www.set.or.th/th/market/product/stock/quote/${encodeURIComponent(s.symbol.toLowerCase())}/factsheet`; fsLink.style.display = 'inline-flex'; }
  const setLink = document.getElementById('cm-set-link');
  if (setLink) { setLink.href = `https://www.set.or.th/th/market/product/stock/quote/${encodeURIComponent(s.symbol)}/financial-statement/company-highlights`; setLink.style.display = 'inline-flex'; }

  const mk = (val, lbl, cls = '') =>
    `<div><div class="cm-metric-val ${cls}">${val}</div><div class="cm-metric-lbl">${lbl}</div></div>`;
  document.getElementById('cm-metrics').innerHTML = [
    mk(s.price?.toFixed(2) ?? '—', 'ราคา'),
    mk(pct(s.ret_1d), '1D%'),
    mk(pct(s.ret_1w), '1W%'),
    mk(pct(s.ret_1m), '1M%'),
    mk(pct(s.ret_3m), '3M%'),
    mk(pct(s.ret_ytd), 'YTD%'),
    mk(s.high_52w?.toFixed(2) ?? '—', '52W High', 'text2'),
    mk(s.low_52w?.toFixed(2)  ?? '—', '52W Low',  'text2'),
    s.ath != null ? mk(s.ath.toFixed(2), 'ATH', 'text2') : '',
    s.ath_pct != null ? mk((s.ath_pct > 0 ? '+' : '') + s.ath_pct.toFixed(1) + '%', '% จาก ATH', s.ath_pct >= -5 ? 'green' : s.ath_pct >= -20 ? 'yellow' : 'red') : '',
    s.pe        != null ? mk(s.pe.toFixed(1),        'P/E',      'text2') : '',
    s.pbv       != null ? mk(s.pbv.toFixed(2),       'P/BV',     'text2') : '',
    s.div_yield != null ? mk(s.div_yield.toFixed(2)+'%', 'Div Yield', s.div_yield >= 4 ? 'green' : 'text2') : '',
  ].join('');

  _cmStock = s;
  _cmTf = '1y';
  _cmHistoryData = null;
  _cmVolumeData  = null;
  _cmFinLoaded = null;
  _cmParentIdx = null; // not opened from stocks panel
  const finBtn    = document.getElementById('cm-mode-fin');
  const stocksBtn2 = document.getElementById('cm-mode-stocks');
  if (finBtn)     finBtn.style.display     = '';    // restore if hidden by index modal
  if (stocksBtn2) stocksBtn2.style.display = 'none'; // hide stocks btn for normal stocks
  setCmMode('chart');
  document.querySelectorAll('#chart-modal .filter-btn').forEach(b => b.classList.remove('active'));
  const tfBtn = document.getElementById('cm-tf-1y');
  if (tfBtn) tfBtn.classList.add('active');
  document.getElementById('chart-modal').classList.add('open');
  document.body.style.overflow = 'hidden';
  requestAnimationFrame(() => _drawChart(s, null));
  // reset insider section
  const insWrap = document.getElementById('popup-insider-section');
  const insToggle = document.getElementById('popup-insider-toggle');
  if (insWrap) { insWrap.style.display = 'none'; insWrap.innerHTML = ''; }
  if (insToggle) insToggle.textContent = '▶ แสดง';
  _insiderPopupSym = symbol;
  // reset short section
  const shortWrap = document.getElementById('popup-short-section');
  const shortToggle = document.getElementById('popup-short-toggle');
  if (shortWrap) { shortWrap.style.display = 'none'; shortWrap.innerHTML = ''; }
  if (shortToggle) shortToggle.textContent = '▶ แสดง';
  // reset nvdr section
  const nvdrWrap = document.getElementById('popup-nvdr-section');
  const nvdrToggle = document.getElementById('popup-nvdr-toggle');
  if (nvdrWrap) { nvdrWrap.style.display = 'none'; nvdrWrap.innerHTML = ''; }
  if (nvdrToggle) nvdrToggle.textContent = '▶ แสดง';
}

let _cmParentIdx = null; // index sym to return to after closing a stock chart opened from stocks panel
let _insiderPopupSym = null;

function toggleNvdrWrap() {
  const el  = document.getElementById('popup-nvdr-section');
  const tog = document.getElementById('popup-nvdr-toggle');
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = 'block'; tog.textContent = '▼ ซ่อน';
    if (!el.innerHTML && _insiderPopupSym) {
      el.innerHTML = '<div style="color:var(--muted);font-size:11px">กำลังโหลด...</div>';
      loadNvdrData().then(() => { el.innerHTML = renderNvdrPopup(_insiderPopupSym); })
        .catch(e => { el.innerHTML = `<div style="color:var(--red);font-size:11px">โหลดไม่ได้: ${e.message}</div>`; });
    }
  } else { el.style.display = 'none'; tog.textContent = '▶ แสดง'; }
}

function toggleShortWrap() {
  const el  = document.getElementById('popup-short-section');
  const tog = document.getElementById('popup-short-toggle');
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = 'block';
    tog.textContent = '▼ ซ่อน';
    if (!el.innerHTML && _insiderPopupSym) {
      el.innerHTML = '<div style="color:var(--muted);font-size:11px">กำลังโหลด...</div>';
      loadShortData().then(() => { el.innerHTML = renderShortPopup(_insiderPopupSym); })
        .catch(e => { el.innerHTML = `<div style="color:var(--red);font-size:11px">โหลดไม่ได้: ${e.message}</div>`; });
    }
  } else {
    el.style.display = 'none';
    tog.textContent = '▶ แสดง';
  }
}

function toggleInsiderWrap() {
  const el = document.getElementById('popup-insider-section');
  const tog = document.getElementById('popup-insider-toggle');
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = 'block';
    tog.textContent = '▼ ซ่อน';
    if (!el.innerHTML && _insiderPopupSym) loadInsiderForStock(_insiderPopupSym);
  } else {
    el.style.display = 'none';
    tog.textContent = '▶ แสดง';
  }
}

function closeChartModal() {
  const parent = _cmParentIdx;
  _cmParentIdx = null;
  document.getElementById('chart-modal').classList.remove('open');
  document.body.style.overflow = '';
  _cmVolumeData = null;
  setCmMode('chart');
  if (parent) {
    // return to the index chart modal with stocks tab
    setTimeout(() => { openIdxChartModal(parent); setTimeout(() => setCmMode('stocks'), 50); }, 60);
  }
}

let _cmMode = 'chart';
let _cmFinLoaded = null; // sym that was last loaded in fin panel

// mapping from index symbol → sector/industry name(s) in DATA.stocks
const IDX_TO_SECTOR = {
  "^AGRO.BK":      ["Agro & Food Industry"],
  "^CONSUMP.BK":   ["Consumer Products"],
  "^FINCIAL.BK":   ["Financials"],
  "^INDUS.BK":     ["Industrials", "Industrial"],
  "^PROPCON.BK":   ["Property & Construction"],
  "^RESOURC.BK":   ["Resources"],
  "^SERVICE.BK":   ["Services"],
  "^AGRI.BK":      ["Agribusiness"],
  "^FOOD.BK":      ["Food & Beverage"],
  "^FASHION.BK":   ["Fashion"],
  "^HOME.BK":      ["Home & Office Products"],
  "^PERSON.BK":    ["Personal Products & Pharmaceuticals"],
  "^BANK.BK":      ["Banking"],
  "^FIN.BK":       ["Finance & Securities"],
  "^INSUR.BK":     ["Insurance"],
  "^AUTO.BK":      ["Automotive"],
  "^IMM.BK":       ["Industrial Materials & Machinery"],
  "^PAPER.BK":     ["Paper & Printing Materials"],
  "^PETRO.BK":     ["Petrochemicals & Chemicals"],
  "^PKG.BK":       ["Packaging"],
  "^STEEL.BK":     ["Steel and Metal Products"],
  "^ETRON.BK":     ["Electronic Components"],
  "^ICT.BK":       ["Information & Communication Technology"],
  "^CONMAT.BK":    ["Construction Materials"],
  "^PROP.BK":      ["Property Development"],
  "^PFREIT.BK":    ["Property Fund & REITs"],
  "^CONS.BK":      ["Construction Services"],
  "^ENERG.BK":     ["Energy & Utilities"],
  "^COMM.BK":      ["Commerce"],
  "^HELTH.BK":     ["Health Care Services"],
  "^MEDIA.BK":     ["Media & Publishing"],
  "^TOURISM.BK":   ["Tourism & Leisure"],
  "^AGRO-M.BK":    ["Agro & Food Industry -mai"],
  "^CONSUMP-M.BK": ["Consumer Products -mai"],
  "^FINCIAL-M.BK": ["Financials -mai"],
  "^INDUS-M.BK":   ["Industrial -mai"],
  "^PROPCON-M.BK": ["Property & Construction -mai"],
  "^RESOURC-M.BK": ["Resources -mai"],
  "^SERVICE-M.BK": ["Services -mai"],
  "^TECH-M.BK":    ["Technology -mai"],
  "^TECH.BK":      ["Technology"],
  "^TRANS.BK":     ["Transportation & Logistics"],
  "^PROF.BK":      ["Professional Services"],
};

function setCmMode(mode) {
  _cmMode = mode;
  const chartPanel  = document.getElementById('cm-chart-panel');
  const finPanel    = document.getElementById('cm-fin-panel');
  const stocksPanel = document.getElementById('cm-stocks-panel');
  const btnChart    = document.getElementById('cm-mode-chart');
  const btnFin      = document.getElementById('cm-mode-fin');
  const btnStocks   = document.getElementById('cm-mode-stocks');

  const extraMetrics = document.getElementById('cm-extra-metrics');

  // reset all panels + buttons
  chartPanel.style.display  = 'none';
  if (finPanel)    finPanel.style.display    = 'none';
  if (stocksPanel) stocksPanel.style.display = 'none';
  [btnChart, btnFin, btnStocks].forEach(b => {
    if (b) { b.style.background = 'var(--card2)'; b.style.color = 'var(--text2)'; }
  });
  if (extraMetrics) extraMetrics.style.display = 'none';

  if (mode === 'chart') {
    chartPanel.style.display = '';
    btnChart.style.background = 'var(--accent)'; btnChart.style.color = '#fff';
    if (extraMetrics) extraMetrics.style.display = 'contents';
  } else if (mode === 'fin') {
    if (finPanel) { finPanel.style.display = ''; }
    if (btnFin)   { btnFin.style.background = 'var(--accent)'; btnFin.style.color = '#fff'; }
    _loadCmFin();
  } else if (mode === 'stocks') {
    if (stocksPanel) { stocksPanel.style.display = ''; }
    if (btnStocks)   { btnStocks.style.background = 'var(--accent)'; btnStocks.style.color = '#fff'; }
    _renderIdxStocks(_cmStock?.symbol);
  }
}

function _renderIdxStocks(sym) {
  const panel = document.getElementById('cm-stocks-panel');
  if (!panel || !DATA) return;
  const names = IDX_TO_SECTOR[sym] || [];
  if (!names.length) { panel.innerHTML = '<div class="text2" style="padding:20px;text-align:center">ไม่มีข้อมูลหุ้นรายตัวสำหรับดัชนีนี้</div>'; return; }

  let stocks = DATA.stocks.filter(s => names.includes(s.sector) || names.includes(s.industry));
  stocks.sort((a,b) => (b.rs_score||0)-(a.rs_score||0));

  const n = stocks.length;
  const avg1d  = n ? (stocks.reduce((a,s)=>a+(s.ret_1d||0),0)/n).toFixed(1) : null;
  const avg1m  = n ? (stocks.reduce((a,s)=>a+(s.ret_1m||0),0)/n).toFixed(1) : null;
  const avgRS  = n ? Math.round(stocks.reduce((a,s)=>a+(s.rs_score||0),0)/n) : null;
  const pctEma = n ? Math.round(stocks.filter(s=>s.above_ema50).length/n*100) : null;
  const rsC = v => v==null?'':v>=70?'green':v>=40?'':'red';
  const pctFmt = v => v==null?'—':`<span class="${v>0?'green':v<0?'red':''}">${v>0?'+':''}${v}%</span>`;
  const emaBadge = v => v == null ? '<span style="color:var(--text2)">—</span>' : v ? '<span style="color:#3fb950">▲</span>' : '<span style="color:#f85149">▼</span>';

  panel.innerHTML = `
    <div style="display:flex;gap:20px;padding:12px 0 16px;border-bottom:1px solid var(--border);margin-bottom:12px">
      <div class="modal-stat"><div class="modal-stat-val">${n}</div><div class="modal-stat-lbl">หุ้น</div></div>
      <div class="modal-stat"><div class="modal-stat-val ${avg1d>0?'green':avg1d<0?'red':''}">${avg1d!=null?(+avg1d>0?'+':'')+avg1d+'%':'—'}</div><div class="modal-stat-lbl">Avg 1D</div></div>
      <div class="modal-stat"><div class="modal-stat-val ${avg1m>0?'green':avg1m<0?'red':''}">${avg1m!=null?(+avg1m>0?'+':'')+avg1m+'%':'—'}</div><div class="modal-stat-lbl">Avg 1M</div></div>
      <div class="modal-stat"><div class="modal-stat-val ${rsC(avgRS)}">${avgRS??'—'}</div><div class="modal-stat-lbl">Avg RS</div></div>
      <div class="modal-stat"><div class="modal-stat-val">${pctEma??'—'}%</div><div class="modal-stat-lbl">%&gt;EMA50</div></div>
    </div>
    <div style="overflow-x:auto">
    <table class="sector-table" style="width:100%">
      <thead><tr>
        <th${colTip('rs_score')}>RS</th><th${colTip('symbol')}>Symbol</th><th${colTip('name')}>ชื่อ</th>
        <th class="r"${colTip('price')}>ราคา</th><th class="r"${colTip('ret_1d')}>1D%</th><th class="r"${colTip('ret_1w')}>1W%</th>
        <th class="r"${colTip('ret_1m')}>1M%</th><th class="r"${colTip('ret_3m')}>3M%</th><th class="r"${colTip('ret_1y')}>1Y%</th>
        <th class="r"${colTip('above_ema50')}>EMA50</th><th class="r"${colTip('above_ema200')}>EMA200</th>
      </tr></thead>
      <tbody>${stocks.map(s=>`
        <tr>
          <td><span class="${rsC(s.rs_score)}" style="font-weight:700">${s.rs_score??'—'}</span></td>
          <td><strong class="sym-link" onclick="openChartModal('${s.symbol}');_cmParentIdx='${sym}'">${s.symbol}</strong>${tvLink(s.symbol)}</td>
          <td style="font-size:11px;color:var(--text2);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
          <td class="r">${s.price?.toFixed(2)??'—'}</td>
          <td class="r">${pctFmt(s.ret_1d)}</td>
          <td class="r">${pctFmt(s.ret_1w)}</td>
          <td class="r">${pctFmt(s.ret_1m)}</td>
          <td class="r">${pctFmt(s.ret_3m)}</td>
          <td class="r">${pctFmt(s.ret_1y)}</td>
          <td class="r">${emaBadge(s.above_ema50)}</td>
          <td class="r">${emaBadge(s.above_ema200)}</td>
        </tr>`).join('')}
      </tbody>
    </table>
    </div>`;
}

function _loadCmFin() {
  const sym = _cmStock?.symbol;
  if (!sym) return;
  if (_cmFinLoaded === sym) return; // already loaded
  _cmFinLoaded = null;
  document.getElementById('cm-fin-loading').style.display = '';
  document.getElementById('cm-fin-body').innerHTML = '';

  fetch(`/api/financials/${encodeURIComponent(sym)}`)
    .then(r => r.json())
    .then(d => {
      document.getElementById('cm-fin-loading').style.display = 'none';
      if (d.error) { document.getElementById('cm-fin-body').innerHTML = `<div style="color:var(--text2);padding:16px">${d.error}</div>`; return; }
      _cmFinLoaded = sym;
      document.getElementById('cm-fin-body').innerHTML = _renderCmFin(d);
      requestAnimationFrame(() => _drawCmFinChart(d));
    })
    .catch(e => {
      document.getElementById('cm-fin-loading').style.display = 'none';
      document.getElementById('cm-fin-body').innerHTML = `<div style="color:var(--red);padding:16px">โหลดไม่สำเร็จ: ${e.message}</div>`;
    });
}

function _renderCmFin(d) {
  const inc = d.income, bal = d.balance, cf = d.cashflow;
  const ti  = d.ttm_income || {}, tb = d.ttm_balance || {}, tc = d.ttm_cashflow || {};
  const cur = d.currency || '';

  const allDates = new Set();
  [inc, bal, cf].forEach(sec => Object.values(sec).forEach(r => Object.keys(r).forEach(k => allDates.add(k))));
  const years = [...allDates].sort().slice(-4);
  if (!years.length) return '<div style="color:var(--text2);padding:16px">ไม่มีข้อมูลงบการเงิน</div>';

  const yLabels = years.map(y => y.slice(0,4));

  // TTM lookup: income/cashflow คือ flow (sum 4Q), balance คือ snapshot ล่าสุด
  const getTTM = (keys, isBalance = false) => {
    const src = isBalance ? tb : { ...ti, ...tc };
    for (const k of keys) {
      if (src[k] != null) return src[k];
    }
    return null;
  };

  const hasTTM = Object.keys(ti).length > 0 || Object.keys(tb).length > 0;

  const getRow = (keys) => {
    for (const k of keys) {
      const row = inc[k] || bal[k] || cf[k];
      if (row && years.some(y => row[y] != null)) return years.map(y => row[y] ?? null);
    }
    return null;
  };

  const fmt = v => {
    if (v == null) return '<span style="color:var(--text2)">—</span>';
    const a = Math.abs(v);
    let s = a >= 1e12 ? (v/1e12).toFixed(2)+'T' : a >= 1e9 ? (v/1e9).toFixed(2)+'B' : a >= 1e6 ? (v/1e6).toFixed(1)+'M' : (v/1e3).toFixed(0)+'K';
    return `<span class="${v>=0?'green':'red'}">${v<0?'':'+'}${s}</span>`;
  };
  const fmtN = v => {
    if (v == null) return '<span style="color:var(--text2)">—</span>';
    const a = Math.abs(v);
    let s = a >= 1e12 ? (v/1e12).toFixed(2)+'T' : a >= 1e9 ? (v/1e9).toFixed(2)+'B' : a >= 1e6 ? (v/1e6).toFixed(1)+'M' : (v/1e3).toFixed(0)+'K';
    return `<span>${s}</span>`;
  };

  const tblRows = [
    { label: 'Revenue',        keys: ['Total Revenue','Revenue','Revenues','Net Revenue'] },
    { label: 'Gross Profit',   keys: ['Gross Profit'] },
    { label: 'EBITDA',         keys: ['EBITDA','Normalized EBITDA'] },
    { label: 'Net Income',     keys: ['Net Income','Net Income Common Stockholders'] },
    { label: 'Net Margin %',   keys: null },
    { label: 'EPS',            keys: ['Basic EPS','Diluted EPS'] },
    { label: 'Total Assets',   keys: ['Total Assets'] },
    { label: 'Total Debt',     keys: ['Total Debt','Long Term Debt And Capital Lease Obligation'] },
    { label: 'Equity',         keys: ['Stockholders Equity','Common Stock Equity'] },
    { label: 'Op. Cash Flow',  keys: ['Operating Cash Flow','Cash Flow From Continuing Operating Activities'] },
    { label: 'Free Cash Flow', keys: ['Free Cash Flow'] },
  ];

  const revValsForMargin = getRow(['Total Revenue','Revenue','Revenues','Net Revenue']);
  const niValsForMargin  = getRow(['Net Income','Net Income Common Stockholders']);
  const marginRow = (revValsForMargin && niValsForMargin)
    ? years.map((_,i) => (revValsForMargin[i] && niValsForMargin[i] != null) ? niValsForMargin[i] / revValsForMargin[i] * 100 : null)
    : null;

  const thStyle  = 'padding:6px 10px;text-align:right;font-size:11px;color:var(--text2);font-weight:400;border-bottom:1px solid var(--border)';
  const thTTM    = 'padding:6px 10px;text-align:right;font-size:11px;color:var(--accent);font-weight:600;border-bottom:1px solid var(--border);border-left:1px solid var(--border)';
  const tdStyle  = 'padding:6px 10px;text-align:right;font-size:12px;border-bottom:1px solid var(--border)';
  const tdTTM    = 'padding:6px 10px;text-align:right;font-size:12px;border-bottom:1px solid var(--border);border-left:1px solid var(--border);background:rgba(88,166,255,0.04)';
  const tlStyle  = 'padding:6px 10px;font-size:11px;color:var(--text2);border-bottom:1px solid var(--border);white-space:nowrap';

  const isBalanceRow = label => ['Total Assets','Total Debt','Equity'].includes(label);

  let rows = '';
  for (const {label, keys} of tblRows) {
    if (label === 'Net Margin %') {
      if (!marginRow) continue;
      const ttmRev = getTTM(['Total Revenue','Revenue','Revenues','Net Revenue']);
      const ttmNI  = getTTM(['Net Income','Net Income Common Stockholders']);
      const ttmMgn = (ttmRev && ttmNI != null) ? ttmNI / ttmRev * 100 : null;
      const cells = marginRow.map((v,i) => {
        const s = v == null ? '<span style="color:var(--text2)">—</span>'
          : `<span style="color:${v>=0?'var(--green)':'var(--red)'}">${v.toFixed(1)}%</span>`;
        return `<td style="${tdStyle}">${s}</td>`;
      }).join('');
      const ttmMgnColor = ttmMgn == null ? '' : ttmMgn >= 0 ? 'var(--green)' : 'var(--red)';
      const ttmMgnHtml  = ttmMgn != null ? `<span style="color:${ttmMgnColor};font-weight:600">${ttmMgn.toFixed(1)}%</span>` : '<span style="color:var(--text2)">—</span>';
      const ttmCell = hasTTM ? `<td style="${tdTTM}">${ttmMgnHtml}</td>` : '';
      rows += `<tr><td style="${tlStyle}">Net Margin %</td>${cells}${ttmCell}</tr>`;
      continue;
    }
    const vals = getRow(keys);
    const isBalance = isBalanceRow(label);
    const ttmVal = hasTTM && keys ? getTTM(keys, isBalance) : null;
    if (!vals && ttmVal == null) continue;
    const displayVals = vals || years.map(() => null);
    const cells = displayVals.map((v,i) => {
      const prev = i > 0 ? displayVals[i-1] : null;
      if (label === 'EPS') {
        const s = v == null ? '<span style="color:var(--text2)">—</span>'
          : `<span>${Math.abs(v) >= 1000 ? (v/1000).toFixed(2)+'K' : v.toFixed(2)}</span>`;
        return `<td style="${tdStyle}">${s}</td>`;
      }
      const cls = (v != null && prev != null) ? (v > prev ? 'green' : v < prev ? 'red' : '') : '';
      return `<td style="${tdStyle};color:${cls==='green'?'var(--green)':cls==='red'?'var(--red)':'var(--fg)'}">${fmtN(v)}</td>`;
    }).join('');
    let ttmCell = '';
    if (hasTTM) {
      if (label === 'EPS') {
        const s = ttmVal == null ? '<span style="color:var(--text2)">—</span>'
          : `<span style="font-weight:600">${Math.abs(ttmVal) >= 1000 ? (ttmVal/1000).toFixed(2)+'K' : ttmVal.toFixed(2)}</span>`;
        ttmCell = `<td style="${tdTTM}">${s}</td>`;
      } else {
        const lastAnnual = displayVals.filter(v => v != null).at(-1) ?? null;
        const cls = (ttmVal != null && lastAnnual != null) ? (ttmVal > lastAnnual ? 'green' : ttmVal < lastAnnual ? 'red' : '') : '';
        const ttmColor = cls === 'green' ? 'var(--green)' : cls === 'red' ? 'var(--red)' : 'var(--fg)';
        ttmCell = `<td style="${tdTTM};color:${ttmColor};font-weight:600">${fmtN(ttmVal)}</td>`;
      }
    }
    rows += `<tr><td style="${tlStyle}">${label}</td>${cells}${ttmCell}</tr>`;
  }

  const ttmHeader = hasTTM ? `<th style="${thTTM}">TTM</th>` : '';

  return `
    <div style="font-size:11px;color:var(--text2);margin-bottom:8px">${d.name} · ${cur}</div>
    <div style="position:relative;height:220px;margin-bottom:14px"><canvas id="cm-fin-chart"></canvas></div>
    <div style="overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead><tr>
        <th style="${thStyle};text-align:left">รายการ</th>
        ${yLabels.map(y=>`<th style="${thStyle}">${y}</th>`).join('')}
        ${ttmHeader}
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div>
    <div style="font-size:10px;color:var(--text2);margin-top:10px">หน่วย: ${cur} · ข้อมูลจาก Yahoo Finance${hasTTM ? ' · TTM = Trailing 12 Months' : ''}</div>
  `;
}

let _cmFinChartInst = null;

function _drawCmFinChart(d) {
  const canvas = document.getElementById('cm-fin-chart');
  if (!canvas) return;

  if (_cmFinChartInst) { _cmFinChartInst.destroy(); _cmFinChartInst = null; }

  const inc = d.income, bal = d.balance, cf = d.cashflow;
  const allDates = new Set();
  [inc, bal, cf].forEach(sec => Object.values(sec).forEach(r => Object.keys(r).forEach(k => allDates.add(k))));
  const years = [...allDates].sort().slice(-4);
  if (!years.length) return;

  const getVal = keys => {
    for (const k of keys) {
      const row = inc[k] || bal[k] || cf[k];
      if (row) { const v = years.map(y => row[y] ?? null); if (v.some(x => x != null)) return v; }
    }
    return years.map(() => null);
  };
  const revVals    = getVal(['Total Revenue','Revenue','Revenues','Net Revenue']);
  const niVals     = getVal(['Net Income','Net Income Common Stockholders','Net Income From Continuing Operations']);
  const marginVals = years.map((_, i) =>
    (revVals[i] != null && niVals[i] != null && revVals[i] !== 0)
      ? parseFloat((niVals[i] / revVals[i] * 100).toFixed(2)) : null
  );

  const fmtShort = v => {
    if (v == null) return '';
    const a = Math.abs(v);
    if (a >= 1e12) return (v/1e12).toFixed(1)+'T';
    if (a >= 1e9)  return (v/1e9).toFixed(2)+'B';
    if (a >= 1e6)  return (v/1e6).toFixed(0)+'M';
    return (v/1e3).toFixed(0)+'K';
  };

  _cmFinChartInst = new Chart(canvas, {
    data: {
      labels: years.map(y => y.slice(0,4)),
      datasets: [
        {
          type: 'bar',
          label: 'Revenue',
          data: revVals,
          backgroundColor: 'rgba(31,111,235,0.85)',
          borderColor: '#1f6feb',
          borderWidth: 1,
          borderRadius: 4,
          yAxisID: 'yRev',
          order: 2,
        },
        {
          type: 'line',
          label: 'Net Margin %',
          data: marginVals,
          borderColor: '#e8b84b',
          backgroundColor: 'rgba(232,184,75,0.15)',
          borderWidth: 2.5,
          pointRadius: 5,
          pointHoverRadius: 7,
          pointBackgroundColor: marginVals.map(v => v == null ? 'transparent' : v >= 0 ? '#3fb950' : '#f85149'),
          pointBorderColor: marginVals.map(v => v == null ? 'transparent' : v >= 0 ? '#3fb950' : '#f85149'),
          tension: 0.3,
          yAxisID: 'yMargin',
          order: 1,
          spanGaps: true,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          labels: { color: '#8b949e', font: { size: 11 }, boxWidth: 14, padding: 16 }
        },
        tooltip: {
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          callbacks: {
            label: ctx => {
              if (ctx.dataset.yAxisID === 'yRev') return ` Revenue: ${fmtShort(ctx.raw)}`;
              if (ctx.raw == null) return null;
              return ` Net Margin: ${ctx.raw.toFixed(1)}%`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#8b949e', font: { size: 11 } },
          grid: { color: '#1e2736' }
        },
        yRev: {
          type: 'linear',
          position: 'left',
          ticks: { color: '#5a6476', font: { size: 10 }, callback: v => fmtShort(v) },
          grid: { color: '#1e2736' }
        },
        yMargin: {
          type: 'linear',
          position: 'right',
          ticks: { color: '#e8b84b', font: { size: 10 }, callback: v => v + '%' },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

function _drawChart(s, historyOverride = null) {
  const canvas = document.getElementById('cm-canvas');
  if (!canvas) return;
  const priceHist = historyOverride || (s && s.price_history);
  if (!priceHist || !priceHist.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W   = canvas.parentElement.clientWidth - 40;

  const PAD = { top: 16, right: 16, bottom: 36, left: 56 };
  const PW  = W - PAD.left - PAD.right;

  // EMA (computed on full history for accuracy)
  const fullPrices = priceHist.map(p => p[1]);
  const e20full  = _calcEMA(fullPrices, 20);
  const e50full  = _calcEMA(fullPrices, 50);
  const e200full = _calcEMA(fullPrices, 200);
  const bars  = _CM_TF_BARS[_cmTf] || 9999;
  const start = Math.max(0, priceHist.length - bars);
  const dates  = priceHist.slice(start).map(p => p[0]);
  const prices = priceHist.slice(start).map(p => p[1]);
  const e20  = e20full.slice(start);
  const e50  = e50full.slice(start);
  const e200 = e200full.slice(start);

  // Volume & OBV
  const volFull = _cmVolumeData;
  const volumes = volFull ? volFull.slice(start) : null;
  const HAS_VOL = !!(volumes && volumes.some(v => v > 0));
  let obv = null;
  if (HAS_VOL && volFull && fullPrices.length === volFull.length) {
    obv = _calcOBV(fullPrices, volFull).slice(start);
  }

  // Canvas dimensions
  const PRICE_H = 248;
  const VOL_H   = HAS_VOL ? 60 : 0;
  const GAP     = HAS_VOL ? 8  : 0;
  const H = PAD.top + PRICE_H + GAP + VOL_H + PAD.bottom;
  canvas.width  = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const PH   = PRICE_H;
  const n    = Math.max(prices.length, 2);
  const toX  = i => PAD.left + (i / (n - 1)) * PW;
  const toY  = v => PAD.top + (1 - (v - yLo) / (yHi - yLo)) * PH;

  const allV = [...prices, ...e20, ...e50, ...e200].filter(v => v != null);
  const yLo  = Math.min(...allV) * 0.995;
  const yHi  = Math.max(...allV) * 1.005;

  ctx.fillStyle = '#161b22';
  ctx.fillRect(0, 0, W, H);

  // Y grid + labels
  ctx.setLineDash([2, 4]); ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const v = yLo + (i / 4) * (yHi - yLo);
    const y = toY(v);
    ctx.strokeStyle = 'rgba(48,54,61,0.9)';
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + PW, y); ctx.stroke();
    ctx.fillStyle = '#8b949e'; ctx.font = '10px Segoe UI,sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(v.toFixed(2), PAD.left - 4, y + 3);
  }

  // X axis labels + vertical grid lines
  const monthStarts = [];
  let _lastMo = '';
  dates.forEach((d, i) => {
    const mo = d.slice(0, 7);
    if (mo !== _lastMo) { monthStarts.push({ mo, i }); _lastMo = mo; }
  });
  const _maxLbls = Math.max(3, Math.floor(PW / 55));
  const _rawStep = monthStarts.length / _maxLbls;
  const _step = [1, 2, 3, 6, 12, 24, 36, 60].find(s => s >= _rawStep) || 60;
  ctx.fillStyle = '#8b949e'; ctx.font = '10px Segoe UI,sans-serif';
  let _lastLblX = -999;
  monthStarts.forEach(({ mo, i }, idx) => {
    if (idx % _step !== 0) return;
    const [yr, mn] = mo.split('-');
    const label = _step >= 12 ? "'" + yr.slice(2) : mn + '/' + yr.slice(2);
    const rawX = toX(i);
    const lw = ctx.measureText(label).width;
    const x = Math.max(PAD.left + lw/2 + 2, Math.min(PAD.left + PW - lw/2 - 2, rawX));
    if (x - _lastLblX < 42) return;
    _lastLblX = x;
    ctx.strokeStyle = 'rgba(48,54,61,0.5)';
    ctx.setLineDash([2, 4]);
    ctx.beginPath(); ctx.moveTo(rawX, PAD.top); ctx.lineTo(rawX, PAD.top + PH + GAP + VOL_H); ctx.stroke();
    ctx.setLineDash([]);
    ctx.textAlign = 'center';
    ctx.fillText(label, x, H - PAD.bottom + 14);
  });
  ctx.setLineDash([]);

  const drawLine = (vals, color, w = 1) => {
    ctx.beginPath(); ctx.strokeStyle = color; ctx.lineWidth = w;
    let go = false;
    vals.forEach((v, i) => {
      if (v == null) { go = false; return; }
      go ? ctx.lineTo(toX(i), toY(v)) : (ctx.moveTo(toX(i), toY(v)), go = true);
    });
    ctx.stroke();
  };

  // ATH line
  const _athVal = Math.max(...priceHist.map(p => p[1]).filter(v => v > 0));
  if (_athVal > 0) {
    const athY = toY(_athVal);
    if (athY >= PAD.top && athY <= PAD.top + PH) {
      ctx.setLineDash([3, 6]); ctx.lineWidth = 0.8; ctx.strokeStyle = '#a78bfa';
      ctx.beginPath(); ctx.moveTo(PAD.left, athY); ctx.lineTo(PAD.left + PW, athY); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#a78bfa'; ctx.font = '9px Segoe UI,sans-serif'; ctx.textAlign = 'right';
      ctx.fillText('ATH  ' + _athVal.toFixed(2), PAD.left + PW - 2, athY - 3);
    }
  }

  // EMA lines
  drawLine(e200, '#f85149', 1);
  drawLine(e50,  '#ffa657', 1);
  drawLine(e20,  '#bc8cff', 1);

  // Price area fill
  ctx.beginPath();
  prices.forEach((v, i) => i === 0 ? ctx.moveTo(toX(0), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.lineTo(toX(n - 1), PAD.top + PH); ctx.lineTo(toX(0), PAD.top + PH); ctx.closePath();
  const g = ctx.createLinearGradient(0, PAD.top, 0, PAD.top + PH);
  g.addColorStop(0, 'rgba(88,166,255,0.2)'); g.addColorStop(1, 'rgba(88,166,255,0.01)');
  ctx.fillStyle = g; ctx.fill();

  // Price line + dot
  drawLine(prices, '#58a6ff', 1.5);
  ctx.beginPath(); ctx.arc(toX(n - 1), toY(prices[n - 1]), 3, 0, Math.PI * 2);
  ctx.fillStyle = '#58a6ff'; ctx.fill();

  // ── Volume sub-panel ──────────────────────────────────────────
  if (HAS_VOL) {
    const VT   = PAD.top + PRICE_H + GAP;   // volume panel top y
    const maxV = Math.max(...volumes.filter(v => v > 0)) || 1;
    const avgV = volumes.reduce((a, b) => a + b, 0) / (volumes.length || 1);
    const slotW = PW / n;
    const bw    = Math.max(1, slotW * 0.65);

    // separator line
    ctx.strokeStyle = 'rgba(48,54,61,0.8)'; ctx.lineWidth = 0.5;
    ctx.beginPath(); ctx.moveTo(PAD.left, VT); ctx.lineTo(PAD.left + PW, VT); ctx.stroke();

    // volume bars (colored by price direction)
    volumes.forEach((v, i) => {
      if (!v) return;
      const bh  = Math.max(1, (v / maxV) * (VOL_H - 2));
      const bx  = toX(i);
      const up  = prices[i] >= (i > 0 ? prices[i - 1] : prices[i]);
      ctx.fillStyle = up ? 'rgba(63,185,80,0.45)' : 'rgba(248,81,73,0.45)';
      ctx.fillRect(bx - bw / 2, VT + VOL_H - bh, bw, bh);
    });

    // avg volume dashed line
    const avgY = VT + VOL_H - (avgV / maxV) * (VOL_H - 2);
    ctx.strokeStyle = 'rgba(255,166,87,0.6)'; ctx.lineWidth = 0.8; ctx.setLineDash([3, 4]);
    ctx.beginPath(); ctx.moveTo(PAD.left, avgY); ctx.lineTo(PAD.left + PW, avgY); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = 'rgba(255,166,87,0.7)'; ctx.font = '8px Segoe UI,sans-serif'; ctx.textAlign = 'left';
    ctx.fillText('avg', PAD.left + 2, avgY - 2);

    // OBV line (normalized to fit volume panel)
    if (obv) {
      const obvMin = Math.min(...obv), obvMax = Math.max(...obv);
      const obvRng = obvMax - obvMin || 1;
      const toOBVy = v => VT + VOL_H - 2 - ((v - obvMin) / obvRng) * (VOL_H - 4);
      ctx.beginPath(); ctx.strokeStyle = 'rgba(188,140,255,0.85)'; ctx.lineWidth = 1;
      let go = false;
      obv.forEach((v, i) => {
        const y = toOBVy(v);
        go ? ctx.lineTo(toX(i), y) : (ctx.moveTo(toX(i), y), go = true);
      });
      ctx.stroke();
      ctx.fillStyle = 'rgba(188,140,255,0.85)'; ctx.font = '8px Segoe UI,sans-serif'; ctx.textAlign = 'left';
      ctx.fillText('OBV', PAD.left + 2, VT + 9);
    }

    // VOL label
    ctx.fillStyle = '#8b949e'; ctx.font = '8px Segoe UI,sans-serif'; ctx.textAlign = 'left';
    ctx.fillText('VOL', PAD.left + 2, VT + VOL_H - 2);
    // last volume value
    const lastV = volumes[volumes.length - 1];
    if (lastV) {
      ctx.textAlign = 'right';
      ctx.fillText(_fmtVolRaw(lastV), PAD.left + PW, VT + VOL_H - 2);
    }
  }
}

// ============================================================
// FINANCIALS PAGE
// ============================================================
let _finTab  = 'set';
let _finData = null;

function initFinPage() {
  // populate SET datalist once DATA is ready
  const dl = document.getElementById('fin-set-datalist');
  if (dl && DATA && !dl.childElementCount) {
    DATA.stocks.forEach(s => {
      const o = document.createElement('option');
      o.value = s.symbol; o.label = s.name;
      dl.appendChild(o);
    });
  }
  // populate DR datalist — fetch if not loaded yet
  const drd = document.getElementById('fin-dr-datalist');
  if (drd && !drd.childElementCount) {
    const _fillDrDl = (stocks) => {
      if (drd.childElementCount) return;
      stocks.forEach(s => {
        const o = document.createElement('option');
        o.value = s.sym; o.label = s.name;
        drd.appendChild(o);
      });
    };
    if (_drData && _drData.length) {
      _fillDrDl(_drData);
    } else {
      fetch('/api/dr').then(r => r.json()).then(d => {
        if (d.stocks) { _drData = _drData || d.stocks; _fillDrDl(d.stocks); }
      }).catch(() => {});
    }
  }
}

function setFinTab(tab, btn) {
  _finTab = tab;
  document.querySelectorAll('#fin-tab-set-btn,#fin-tab-dr-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('fin-search-set').style.display = tab === 'set' ? '' : 'none';
  document.getElementById('fin-search-dr').style.display  = tab === 'dr'  ? '' : 'none';
  document.getElementById('fin-result').innerHTML = '';
  initFinPage();
}

async function searchFinancials() {
  const inputId = _finTab === 'set' ? 'fin-sym-set' : 'fin-sym-dr';
  const hintId  = _finTab === 'set' ? 'fin-set-hint' : 'fin-dr-hint';
  const sym = (document.getElementById(inputId).value || '').trim().toUpperCase();
  if (!sym) return;
  const hint = document.getElementById(hintId);
  hint.textContent = 'กำลังโหลด...';
  document.getElementById('fin-result').innerHTML = '<div class="empty" style="padding:24px">กำลังดึงข้อมูลงบการเงิน...</div>';
  try {
    const r = await fetch(`/api/financials/${encodeURIComponent(sym)}`);
    const d = await r.json();
    if (d.error) throw new Error(d.error);
    _finData = d;
    hint.textContent = `${d.currency} · cache 24h`;
    _renderFinancials(d);
  } catch(e) {
    hint.textContent = '';
    document.getElementById('fin-result').innerHTML = `<div class="empty" style="padding:24px;color:var(--red)">⚠ ${e.message}</div>`;
  }
}

function _finFmt(v) {
  if (v == null || isNaN(v)) return '—';
  const abs = Math.abs(v), sign = v < 0 ? '-' : '';
  if (abs >= 1e12) return sign + (abs/1e12).toFixed(2) + 'T';
  if (abs >= 1e9)  return sign + (abs/1e9).toFixed(2) + 'B';
  if (abs >= 1e6)  return sign + (abs/1e6).toFixed(1) + 'M';
  if (abs >= 1e3)  return sign + (abs/1e3).toFixed(0) + 'K';
  return sign + abs.toFixed(2);
}

function _finPct(v) {
  if (v == null || isNaN(v)) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
}

function _finGetRow(dict, keys) {
  for (const k of keys) {
    if (dict[k]) return dict[k];
  }
  return null;
}

function _finColCls(v, prev) {
  if (v == null || prev == null) return '';
  return v > prev ? 'green' : v < prev ? 'red' : '';
}

function _renderFinancials(d) {
  const inc = d.income, bal = d.balance, cf = d.cashflow;

  // Get sorted years (oldest → newest)
  const allDates = new Set();
  [inc, bal, cf].forEach(section => Object.values(section).forEach(row => Object.keys(row).forEach(k => allDates.add(k))));
  const years = [...allDates].sort().slice(-4);
  const yearLabels = years.map(y => y.slice(0, 4));

  // Helper: render one table section
  function _finTable(title, rows) {
    const headerCells = yearLabels.map(y => `<th class="r" style="min-width:90px">${y}</th>`).join('');
    let rowHtml = '';
    for (const { label, keys, calc } of rows) {
      let vals = years.map(() => null);
      if (calc) {
        vals = calc(inc, bal, cf, years);
      } else if (keys) {
        for (const key of keys) {
          const found = inc[key] || bal[key] || cf[key];
          if (found) { vals = years.map(y => found[y] ?? null); break; }
        }
      }
      if (vals.every(v => v === null)) continue;
      const cells = vals.map((v, i) => {
        const cls = i > 0 ? _finColCls(v, vals[i-1]) : '';
        return `<td class="r ${cls}" style="font-size:12px">${_finFmt(v)}</td>`;
      }).join('');
      rowHtml += `<tr><td style="font-size:12px;padding:5px 8px;white-space:nowrap">${label}</td>${cells}</tr>`;
    }
    if (!rowHtml) return '';
    return `
    <div style="margin-top:16px">
      <div style="font-size:13px;font-weight:600;color:var(--blue);margin-bottom:6px">${title}</div>
      <div style="overflow-x:auto"><table class="tbl" style="min-width:400px">
        <thead><tr><th style="min-width:160px">รายการ</th>${headerCells}</tr></thead>
        <tbody>${rowHtml}</tbody>
      </table></div>
    </div>`;
  }

  const incomeRows = [
    { label: 'Revenue',            keys: ['Total Revenue','Revenue'] },
    { label: 'Gross Profit',       keys: ['Gross Profit'] },
    { label: 'Gross Margin %',     keys: null, calc: (inc,_b,_c,yrs) => {
        const rev = _finGetRow(inc, ['Total Revenue','Revenue']);
        const gp  = _finGetRow(inc, ['Gross Profit']);
        return yrs.map(y => (rev&&gp&&rev[y]&&gp[y]) ? gp[y]/rev[y]*100 : null);
    }},
    { label: 'Operating Income',   keys: ['Operating Income','EBIT'] },
    { label: 'EBITDA',             keys: ['EBITDA','Normalized EBITDA'] },
    { label: 'Net Income',         keys: ['Net Income','Net Income Common Stockholders'] },
    { label: 'Net Margin %',       keys: null, calc: (inc,_b,_c,yrs) => {
        const rev = _finGetRow(inc, ['Total Revenue','Revenue']);
        const ni  = _finGetRow(inc, ['Net Income','Net Income Common Stockholders']);
        return yrs.map(y => (rev&&ni&&rev[y]&&ni[y]) ? ni[y]/rev[y]*100 : null);
    }},
    { label: 'EPS (Diluted)',      keys: ['Diluted EPS','Basic EPS'] },
  ];

  const balRows = [
    { label: 'Total Assets',      keys: ['Total Assets'] },
    { label: 'Cash & Equiv.',     keys: ['Cash And Cash Equivalents','Cash Cash Equivalents And Short Term Investments','Cash And Short Term Investments'] },
    { label: 'Total Debt',        keys: ['Total Debt','Long Term Debt'] },
    { label: 'Equity',            keys: ['Stockholders Equity','Common Stock Equity','Total Equity Gross Minority Interest'] },
    { label: 'Debt/Equity',       keys: null, calc: (_i,bal,_c,yrs) => {
        const dbt = _finGetRow(bal, ['Total Debt','Long Term Debt']);
        const eq  = _finGetRow(bal, ['Stockholders Equity','Common Stock Equity','Total Equity Gross Minority Interest']);
        return yrs.map(y => (dbt&&eq&&eq[y]&&eq[y]!==0) ? dbt[y]/eq[y] : null);
    }},
  ];

  const cfRows = [
    { label: 'Operating CF',      keys: ['Operating Cash Flow','Cash From Operating Activities'] },
    { label: 'CapEx',             keys: ['Capital Expenditure','Capital Expenditures'] },
    { label: 'Free Cash Flow',    keys: ['Free Cash Flow'] },
    { label: 'Dividends Paid',    keys: ['Common Stock Dividend Paid','Cash Dividends Paid'] },
  ];

  // Revenue + Net Income chart
  const revData  = _finGetRow(inc, ['Total Revenue','Revenue']);
  const niData   = _finGetRow(inc, ['Net Income','Net Income Common Stockholders']);
  const revVals  = years.map(y => revData?.[y] ?? null);
  const niVals   = years.map(y => niData?.[y]  ?? null);
  const chartId  = 'fin-chart-' + Date.now();

  const typeLabel = d.type === 'set'
    ? `<span style="background:#1f6feb22;color:#58a6ff;padding:2px 8px;border-radius:4px;font-size:11px">🇹🇭 SET</span>`
    : `<span style="background:#3fb95022;color:#3fb950;padding:2px 8px;border-radius:4px;font-size:11px">🌏 DR</span>`;

  document.getElementById('fin-result').innerHTML = `
  <div class="card" style="margin-top:12px">
    <div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:4px">
      <span style="font-size:18px;font-weight:700">${d.sym}</span>
      <a class="tv-link" href="https://www.tradingview.com/chart/?symbol=${d.type==='set'?'SET:'+d.sym:yfToTVSym(d.yf)}&interval=D" target="_blank" rel="noopener" title="ดูใน TradingView">↗</a>
      ${d.type==='set' ? `<a href="https://www.set.or.th/th/market/product/stock/quote/${encodeURIComponent(d.sym)}/financial-statement/company-highlights" target="_blank" rel="noopener" title="ดูงบการเงินบนเว็บ SET" style="font-size:11px;color:#b09030;border:1px solid #b09030;border-radius:4px;padding:1px 7px;text-decoration:none;vertical-align:middle">SET ↗</a>` : ''}
      ${typeLabel}
      <span style="font-size:13px;color:var(--text2)">${d.name}</span>
      <span style="font-size:11px;color:var(--text2);margin-left:auto">สกุลเงิน: <strong>${d.currency}</strong></span>
    </div>
    <div style="font-size:11px;color:var(--text2);margin-bottom:12px">yfinance: ${d.yf} · ข้อมูลรายปี (Annual)</div>

    ${years.length ? `
    <canvas id="${chartId}" style="width:100%;max-width:680px;height:180px;display:block"></canvas>
    ` : ''}

    ${_finTable('📊 งบกำไรขาดทุน (Income Statement)', incomeRows)}
    ${_finTable('📋 งบดุล (Balance Sheet)', balRows)}
    ${_finTable('💵 กระแสเงินสด (Cash Flow)', cfRows)}

    <div style="font-size:10px;color:var(--text2);margin-top:16px">
      ⚠ ข้อมูลจาก Yahoo Finance อาจไม่ครบถ้วนสำหรับหุ้นบางตัว โดยเฉพาะ VN/EU — ใช้เพื่อดูแนวโน้มเท่านั้น
    </div>
  </div>`;

  // Draw chart after DOM is ready
  requestAnimationFrame(() => {
    const cv = document.getElementById(chartId);
    if (cv && (revVals.some(v=>v!=null) || niVals.some(v=>v!=null))) {
      _drawFinChart(cv, yearLabels, revVals, niVals, d.currency);
    }
  });
}

function _drawFinChart(canvas, labels, revVals, niVals, currency) {
  const dpr = window.devicePixelRatio || 1;
  const W   = Math.min(canvas.parentElement.clientWidth, 680);
  const H   = 180;
  canvas.width  = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const PAD = { top: 24, right: 16, bottom: 36, left: 60 };
  const PW  = W - PAD.left - PAD.right;
  const PH  = H - PAD.top  - PAD.bottom;

  ctx.fillStyle = '#161b22';
  ctx.fillRect(0, 0, W, H);

  const n = labels.length;
  if (n === 0) return;

  const allVals = [...revVals, ...niVals].filter(v => v != null);
  const minV = Math.min(0, ...allVals);
  const maxV = Math.max(...allVals) * 1.1 || 1;
  const toY  = v => PAD.top + (1 - (v - minV) / (maxV - minV)) * PH;
  const barW = Math.floor(PW / n * 0.35);
  const gap  = PW / n;

  // Grid lines
  ctx.setLineDash([2,4]); ctx.lineWidth = 0.5; ctx.strokeStyle = 'rgba(48,54,61,0.8)';
  [0, 0.25, 0.5, 0.75, 1].forEach(t => {
    const y = PAD.top + t * PH;
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + PW, y); ctx.stroke();
    const v = maxV - t * (maxV - minV);
    ctx.fillStyle = '#8b949e'; ctx.font = '9px Segoe UI,sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(_finFmt(v), PAD.left - 3, y + 3);
  });
  ctx.setLineDash([]);

  const zero = toY(0);

  labels.forEach((lbl, i) => {
    const cx = PAD.left + i * gap + gap / 2;

    // Revenue bar (blue)
    if (revVals[i] != null) {
      const y = toY(revVals[i]), h = Math.abs(zero - y);
      ctx.fillStyle = 'rgba(88,166,255,0.55)';
      ctx.fillRect(cx - barW - 2, Math.min(y, zero), barW, Math.max(h, 1));
    }
    // Net Income bar (green/red)
    if (niVals[i] != null) {
      const y = toY(niVals[i]), h = Math.abs(zero - y);
      ctx.fillStyle = niVals[i] >= 0 ? 'rgba(63,185,80,0.7)' : 'rgba(248,81,73,0.7)';
      ctx.fillRect(cx + 2, Math.min(y, zero), barW, Math.max(h, 1));
    }

    // X label
    ctx.fillStyle = '#8b949e'; ctx.font = '10px Segoe UI,sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(lbl, cx, H - PAD.bottom + 14);
  });

  // Zero line
  if (minV < 0) {
    ctx.strokeStyle = 'rgba(139,148,158,0.4)'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(PAD.left, zero); ctx.lineTo(PAD.left + PW, zero); ctx.stroke();
  }

  // Legend
  ctx.fillStyle = 'rgba(88,166,255,0.7)';  ctx.fillRect(PAD.left, 6, 10, 9);
  ctx.fillStyle = '#ccc'; ctx.font = '10px Segoe UI,sans-serif'; ctx.textAlign = 'left';
  ctx.fillText('Revenue', PAD.left + 13, 14);
  ctx.fillStyle = 'rgba(63,185,80,0.8)'; ctx.fillRect(PAD.left + 72, 6, 10, 9);
  ctx.fillText('Net Income', PAD.left + 85, 14);
}

// ============================================================
// VALUATION BAND PAGE
// ============================================================
function _bandZone(cur, b) {
  if (cur == null) return { label: '—', color: 'var(--text2)' };
  if (cur < b.m2sd) return { label: 'ต่ำกว่า -2SD  (Undervalued มาก)', color: '#22c55e' };
  if (cur < b.m1sd) return { label: '-2SD ถึง -1SD  (Undervalued)',      color: '#86efac' };
  if (cur < b.avg)  return { label: '-1SD ถึง AVG  (ต่ำกว่าค่าเฉลี่ย)', color: '#facc15' };
  if (cur < b.p1sd) return { label: 'AVG ถึง +1SD  (สูงกว่าค่าเฉลี่ย)', color: '#fb923c' };
  if (cur < b.p2sd) return { label: '+1SD ถึง +2SD  (Overvalued)',        color: '#f87171' };
  return               { label: 'สูงกว่า +2SD  (Overvalued มาก)',         color: '#ef4444' };
}

async function searchBand() {
  const sym = (document.getElementById('band-input').value || '').trim().toUpperCase();
  if (!sym) return;
  document.getElementById('band-loading').style.display = 'block';
  document.getElementById('band-result').style.display  = 'none';
  document.getElementById('band-error').style.display   = 'none';
  try {
    const res  = await fetch(`/api/band/${encodeURIComponent(sym)}`);
    const data = await res.json();
    document.getElementById('band-loading').style.display = 'none';
    if (data.error) {
      document.getElementById('band-error').style.display   = 'block';
      document.getElementById('band-error').textContent     = '⚠ ' + data.error;
      return;
    }
    _renderBandResult(data);
  } catch(e) {
    document.getElementById('band-loading').style.display = 'none';
    document.getElementById('band-error').style.display   = 'block';
    document.getElementById('band-error').textContent     = '⚠ เกิดข้อผิดพลาด: ' + e.message;
  }
}

function _renderBandResult(data) {
  document.getElementById('band-result').style.display = 'block';
  document.getElementById('band-sym-title').textContent = data.symbol;
  const cacheTag = document.getElementById('band-cache-tag');
  if (data.cached_at) {
    cacheTag.textContent = `cache ${data.cached_at}`;
    cacheTag.style.display = 'inline';
  } else {
    cacheTag.style.display = 'none';
  }
  ['pe','pbv'].forEach(type => {
    const d   = data[type];
    const card = document.getElementById(`band-${type}-card`);
    if (!d) { card.style.display = 'none'; return; }
    card.style.display = 'block';
    const zone = _bandZone(d.current, d);
    const fmtN = v => v != null ? v : '—';
    document.getElementById(`band-${type}-metrics`).innerHTML = `
      <div class="band-metric"><div class="band-metric-val" style="color:${zone.color}">${fmtN(d.current)}</div><div class="band-metric-lbl">ปัจจุบัน</div></div>
      <div class="band-metric"><div class="band-metric-val" style="color:#22c55e">${fmtN(d.m2sd)}</div><div class="band-metric-lbl">-2SD</div></div>
      <div class="band-metric"><div class="band-metric-val" style="color:#86efac">${fmtN(d.m1sd)}</div><div class="band-metric-lbl">-1SD</div></div>
      <div class="band-metric"><div class="band-metric-val" style="color:#94a3b8">${fmtN(d.avg)}</div><div class="band-metric-lbl">AVG</div></div>
      <div class="band-metric"><div class="band-metric-val" style="color:#fb923c">${fmtN(d.p1sd)}</div><div class="band-metric-lbl">+1SD</div></div>
      <div class="band-metric"><div class="band-metric-val" style="color:#f87171">${fmtN(d.p2sd)}</div><div class="band-metric-lbl">+2SD</div></div>
      <div class="band-zone-badge" style="color:${zone.color};border:1px solid ${zone.color}30">${zone.label}</div>
    `;
    requestAnimationFrame(() => _drawBandChart(`band-${type}-canvas`, d));
  });
}

function _drawBandChart(canvasId, d) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const history = (d.history || []).filter(r => r.val != null);
  if (!history.length) return;

  const dpr = window.devicePixelRatio || 1;
  const W   = canvas.offsetWidth || 600;
  const H   = 260;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const PAD = { top:16, right:96, bottom:34, left:44 };
  const cW  = W - PAD.left - PAD.right;
  const cH  = H - PAD.top  - PAD.bottom;

  const vals = history.map(r => r.val);
  const rawMin = Math.min(...vals, d.m2sd, d.current ?? Infinity);
  const rawMax = Math.max(...vals, d.p2sd, d.current ?? -Infinity);
  const pad    = (rawMax - rawMin) * 0.08;
  const minV   = rawMin - pad;
  const maxV   = rawMax + pad;
  const range  = maxV - minV;

  const toX = i => PAD.left + (i / Math.max(history.length - 1, 1)) * cW;
  const toY = v => PAD.top  + cH - ((v - minV) / range) * cH;

  // Zone fills
  const zoneFills = [
    [minV,    d.m2sd, 'rgba(34,197,94,0.13)'],
    [d.m2sd,  d.m1sd, 'rgba(134,239,172,0.09)'],
    [d.m1sd,  d.avg,  'rgba(250,204,21,0.07)'],
    [d.avg,   d.p1sd, 'rgba(251,146,60,0.09)'],
    [d.p1sd,  d.p2sd, 'rgba(248,113,113,0.12)'],
    [d.p2sd,  maxV,   'rgba(239,68,68,0.17)'],
  ];
  zoneFills.forEach(([lo, hi, color]) => {
    const y1 = toY(Math.min(hi, maxV));
    const y2 = toY(Math.max(lo, minV));
    if (y2 > y1) { ctx.fillStyle = color; ctx.fillRect(PAD.left, y1, cW, y2 - y1); }
  });

  // SD dashed lines
  const sdLines = [
    [d.m2sd, `-2SD  ${d.m2sd}`, '#22c55e'],
    [d.m1sd, `-1SD  ${d.m1sd}`, '#86efac'],
    [d.avg,  `AVG  ${d.avg}`,   '#94a3b8'],
    [d.p1sd, `+1SD  ${d.p1sd}`, '#fb923c'],
    [d.p2sd, `+2SD  ${d.p2sd}`, '#f87171'],
  ];
  ctx.font = '10px sans-serif';
  sdLines.forEach(([val, label, color]) => {
    if (val < minV || val > maxV) return;
    const y = toY(val);
    ctx.setLineDash([4,4]); ctx.lineWidth = 1; ctx.strokeStyle = color;
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + cW, y); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = color; ctx.textAlign = 'left';
    ctx.fillText(label, PAD.left + cW + 4, y + 4);
  });

  // Current value highlight line
  if (d.current != null && d.current >= minV && d.current <= maxV) {
    const zone = _bandZone(d.current, d);
    const y = toY(d.current);
    ctx.setLineDash([]); ctx.lineWidth = 1.5; ctx.strokeStyle = zone.color + 'cc';
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + cW, y); ctx.stroke();
  }

  // Historical value line
  ctx.setLineDash([]); ctx.lineWidth = 2; ctx.strokeStyle = '#60a5fa'; ctx.lineJoin = 'round';
  ctx.beginPath();
  history.forEach((r, i) => {
    const x = toX(i), y = toY(r.val);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();

  // X axis labels (every Jan / Jul)
  ctx.fillStyle = '#64748b'; ctx.font = '10px sans-serif'; ctx.textAlign = 'center';
  history.forEach((r, i) => {
    if (!r.month) return;
    const m = r.month.split('/')[0];
    if (m === '01' || m === '07') ctx.fillText(r.month, toX(i), H - PAD.bottom + 13);
  });

  // Y axis labels
  ctx.textAlign = 'right'; ctx.fillStyle = '#64748b';
  for (let i = 0; i <= 4; i++) {
    const v = minV + (range / 4) * i;
    ctx.fillText(v.toFixed(1), PAD.left - 4, toY(v) + 4);
  }
}

// ============================================================
// FUNDAMENTAL VIEW
// ============================================================
let _fundView    = 'all';
let _fundSortCol = 'div_yield';
let _fundSortDir = 1;
const _FUND_STR  = new Set(['symbol','name','sector']);

function setFundView(v, btn) {
  _fundView = v;
  document.querySelectorAll('#page-fundamentals .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderFundTable();
}

function fundTh(col, label, cls='') {
  const active = _fundSortCol === col;
  const arrow  = active ? (_fundSortDir === 1 ? '↓' : '↑') : '↕';
  const c = (cls ? cls+' ' : '') + 'sortable';
  return `<th class="${c}"${colTip(col)} onclick="setFundSort('${col}')">${label}<span class="sort-ind${active?' on':''}">${arrow}</span></th>`;
}

function setFundSort(col) {
  if (_fundSortCol === col) _fundSortDir *= -1;
  else { _fundSortCol = col; _fundSortDir = 1; }
  renderFundTable();
}

function renderFundamentals() {
  if (!DATA) return;
  // สร้าง sector dropdown
  const sectors = ['ALL', ...new Set(DATA.stocks.map(s => s.sector || 'Unknown').sort())];
  const sel = document.getElementById('fund-sector-filter');
  if (sel) {
    const cur = sel.value;
    sel.innerHTML = sectors.map(s => `<option value="${s}"${s===cur?' selected':''}>${s}</option>`).join('');
  }
  // stat cards
  const stocks = DATA.stocks;
  const withPE  = stocks.filter(s => s.pe  != null).length;
  const withPBV = stocks.filter(s => s.pbv != null).length;
  const withDY  = stocks.filter(s => s.div_yield != null).length;
  const avgDY   = withDY ? (stocks.filter(s=>s.div_yield!=null).reduce((a,s)=>a+s.div_yield,0)/withDY).toFixed(2) : '—';
  document.getElementById('fund-stat-cards').innerHTML = `
    <div class="card"><div class="card-title">มีข้อมูล P/E</div><div class="stat-val">${withPE}</div><div class="stat-label">จาก ${stocks.length} หุ้น</div></div>
    <div class="card"><div class="card-title">มีข้อมูล P/BV</div><div class="stat-val">${withPBV}</div><div class="stat-label">จาก ${stocks.length} หุ้น</div></div>
    <div class="card"><div class="card-title">มีข้อมูล Div Yield</div><div class="stat-val">${withDY}</div><div class="stat-label">จาก ${stocks.length} หุ้น</div></div>
    <div class="card"><div class="card-title">Avg Div Yield</div><div class="stat-val green">${avgDY}%</div><div class="stat-label">เฉลี่ยตลาด</div></div>
  `;
  renderFundTable();
}

function renderFundTable() {
  if (!DATA) return;
  const secFilter = document.getElementById('fund-sector-filter')?.value || 'ALL';
  const q = (document.getElementById('fund-search')?.value || '').trim().toUpperCase();
  let stocks = DATA.stocks.filter(s => {
    if (secFilter !== 'ALL' && s.sector !== secFilter) return false;
    if (q && !s.symbol.toUpperCase().includes(q) && !(s.name || '').toUpperCase().includes(q)) return false;
    if (_fundView === 'high_yield') return s.div_yield != null && s.div_yield >= 3;
    if (_fundView === 'low_pbv')    return s.pbv != null && s.pbv < 1;
    if (_fundView === 'low_pe')     return s.pe  != null && s.pe  < 15;
    if (_fundView === 'has_data')   return s.pe  != null && s.pbv != null && s.div_yield != null;
    return true;
  });
  stocks = [...stocks].sort((a, b) => {
    if (_FUND_STR.has(_fundSortCol)) return ((a[_fundSortCol]??'').localeCompare(b[_fundSortCol]??'')) * _fundSortDir;
    return ((b[_fundSortCol]??-Infinity) - (a[_fundSortCol]??-Infinity)) * _fundSortDir;
  });

  document.getElementById('fund-count').textContent = `แสดง ${stocks.length} หุ้น`;
  document.getElementById('fund-thead').innerHTML = `<tr>
    ${fundTh('rs_score','RS','r')}${fundTh('symbol','Symbol')}${fundTh('name','ชื่อ')}${fundTh('sector','Sector')}
    ${fundTh('price','ราคา','r')}${fundTh('ret_1d','1D%','r')}${fundTh('ret_1m','1M%','r')}${fundTh('ret_ytd','YTD%','r')}
    ${fundTh('pe','P/E','r')}${fundTh('pbv','P/BV','r')}${fundTh('div_yield','Div Yield%','r')}
    ${fundTh('mkt_cap','MKT CAP','r')}${fundTh('above_ema50','EMA50','r')}
  </tr>`;
  document.getElementById('fund-tbody').innerHTML = stocks.map(s => `
    <tr>
      <td class="r"><span class="${rsColor(s.rs_score)}" style="font-weight:700">${s.rs_score ?? '—'}</span></td>
      <td><strong class="sym-link" onclick="openChartModal('${s.symbol}')">${s.symbol}</strong>${tvLink(s.symbol)}</td>
      <td style="font-size:11px;color:var(--text2);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name}</td>
      <td style="font-size:11px">${s.sector || '—'}</td>
      <td class="r">${s.price?.toFixed(2) ?? '—'}</td>
      <td class="r">${pct(s.ret_1d)}</td>
      <td class="r">${pct(s.ret_1m)}</td>
      <td class="r">${pct(s.ret_ytd)}</td>
      <td class="r">${fmtValuation(s.pe, 'pe')}</td>
      <td class="r">${fmtValuation(s.pbv, 'pbv')}</td>
      <td class="r">${s.div_yield != null
        ? `<span class="${s.div_yield >= 6 ? 'green' : s.div_yield >= 3 ? 'yellow' : 'text2'}" style="font-weight:600">${s.div_yield.toFixed(2)}%</span>`
        : '<span class="text2">—</span>'}</td>
      <td class="r" style="font-size:11px">${fmtCap(s.mkt_cap, s.is_reit)}</td>
      <td class="r">${emaBadge(s.above_ema50)}</td>
    </tr>`).join('');
}

// ============================================================
// INIT
// ============================================================
loadData();

// ============================================================
// DR / DRx PAGE
// ============================================================

const _DR_LOGO_DOMAIN = {
  // US
  'AAPL':'apple.com','ABBV':'abbvie.com','ABNB':'airbnb.com','ADBE':'adobe.com',
  'AFRM':'affirm.com','AMAT':'appliedmaterials.com','AMD':'amd.com','AMGN':'amgen.com',
  'AMZN':'amazon.com','ANET':'arista.com','APLD':'applieddigital.com','ASML':'asml.com',
  'ASTS':'ast-science.com','AVGO':'broadcom.com','AXP':'americanexpress.com',
  'BAC':'bankofamerica.com','BDX':'bd.com','BKNG':'booking.com','BLK':'blackrock.com',
  'BA':'boeing.com','BRK-B':'berkshirehathaway.com','CCJ':'cameco.com',
  'CEG':'constellationenergy.com','CME':'cmegroup.com','COHR':'coherent.com',
  'COIN':'coinbase.com','COST':'costco.com','CRM':'salesforce.com','CRSP':'crisprtx.com',
  'CRWD':'crowdstrike.com','CRWV':'coreweave.com','CSCO':'cisco.com','DASH':'doordash.com',
  'DDOG':'datadoghq.com','DELL':'dell.com','DIS':'disney.com','DUOL':'duolingo.com',
  'EOSE':'eose.com','EL':'elcompanies.com','EXPE':'expedia.com','FCX':'fcx.com',
  'RACE':'ferrari.com','GDS':'gds-services.com','GEV':'gevernova.com','GCT':'gigacloudtech.com',
  'GLD':'spdrgoldshares.com','GOOG':'google.com','GOOGL':'google.com','GRAB':'grab.com',
  'GS':'goldmansachs.com','HIMS':'forhims.com','HOOD':'robinhood.com','IBM':'ibm.com',
  'INTC':'intel.com','IONQ':'ionq.com','ISRG':'intuitivesurgical.com',
  'JEPI':'jpmorgan.com','JGRO':'jpmorgan.com','JNJ':'jnj.com','KLAC':'kla.com',
  'KO':'coca-cola.com','LITE':'lumentum.com','LLY':'lilly.com','LRCX':'lamresearch.com',
  'LULU':'lululemon.com','MA':'mastercard.com','MELI':'mercadolibre.com','META':'meta.com',
  'MU':'micron.com','MNSO':'miniso.com','MNST':'monsterbevcorp.com','MP':'mpmaterials.com',
  'MRVL':'marvell.com','MS':'morganstanley.com','MSFT':'microsoft.com',
  'NBIS':'nebius.com','NDAQ':'nasdaq.com','NEE':'nexteraenergy.com','NEM':'newmont.com',
  'NET':'cloudflare.com','NFLX':'netflix.com','NKE':'nike.com','NOW':'servicenow.com',
  'NVDA':'nvidia.com','NVTS':'navitassemi.com','ON':'onsemi.com','ONON':'on.com',
  'ORCL':'oracle.com','PANW':'paloaltonetworks.com','PEP':'pepsico.com','PFE':'pfizer.com',
  'PLTR':'palantir.com','PYPL':'paypal.com','QCOM':'qualcomm.com','QQQM':'invesco.com',
  'RBLX':'roblox.com','REMX':'vaneck.com','RGTI':'rigetti.com','RKLB':'rocketlabusa.com',
  'SBUX':'starbucks.com','STX':'seagate.com','SHOP':'shopify.com','SIL':'globalxetfs.com',
  'SMCI':'supermicro.com','SNDK':'sandisk.com','SNOW':'snowflake.com','SOFI':'sofi.com',
  'SPYM':'ssga.com','SPAB':'ssga.com','XLC':'ssga.com','XLE':'ssga.com','XLF':'ssga.com',
  'XLV':'ssga.com','XLK':'ssga.com','SPOT':'spotify.com','SNPS':'synopsys.com',
  'TEL':'te.com','TER':'teradyne.com','TME':'tencentmusic.com','TCOM':'trip.com',
  'TRV':'travelers.com','TSLA':'tesla.com','UBER':'uber.com','UNH':'unitedhealthgroup.com',
  'V':'visa.com','VRT':'vertiv.com','VT':'vanguard.com','WMT':'walmart.com',
  'GSUS':'goldmansachs.com','ANET':'arista.com','GEV':'gevernova.com',
  // HK / China
  '1299.HK':'aia.com','2020.HK':'antagroup.com','9988.HK':'alibaba.com',
  '9888.HK':'baidu.com','9626.HK':'bilibili.com','6082.HK':'biren.com',
  '1211.HK':'bydglobal.com','3750.HK':'catl.com','0941.HK':'chinamobileltd.com',
  '3968.HK':'cmbchina.com','0388.HK':'hkex.com','3032.HK':'hsbc.com',
  '9660.HK':'horizon-robotics.com','1347.HK':'huahong-semi.com',
  '1398.HK':'icbc.com.cn','9618.HK':'jd.com','6618.HK':'jdhealth.com',
  '3888.HK':'kingsoft.com','1024.HK':'kuaishou.com','0992.HK':'lenovo.com',
  '1318.HK':'mgccos.com','3690.HK':'meituan.com','000333.SZ':'midea.com',
  '2097.HK':'mixuebingcheng.com','600519.SS':'moutai.com.cn',
  '9999.HK':'netease.com','9633.HK':'nongfuspring.com','0857.HK':'petrochina.com.cn',
  '2318.HK':'pingan.com','9992.HK':'popmart.com','0020.HK':'sensetime.com',
  '1177.HK':'sinobiopharma.com','0981.HK':'smics.com','2727.HK':'shanghai-electric.com',
  '2382.HK':'sunny-optical.com','0700.HK':'tencent.com','9880.HK':'ubtrobot.com',
  '2269.HK':'wuxibiologics.com','2359.HK':'wuxiapptec.com','1810.HK':'mi.com',
  '9868.HK':'xpeng.com','9688.HK':'zailaboratory.com','2899.HK':'zijinmining.com',
  '0175.HK':'geely.com','1772.HK':'ganfengligroup.com','6690.HK':'haier.com',
  '3692.HK':'hansoh.com','600900.SS':'cypc.com.cn','2238.HK':'en.gac.com.cn',
  '002230.SZ':'iflytek.com','3888.HK':'kingsoft.com','6181.HK':'laopugold.com',
  '688100.SS':'montagemi.com','002371.SZ':'naura.com','688256.SS':'cambricon.com',
  '000625.SZ':'changan.com.cn','159682.SZ':'csop.com.hk','6680.HK':'jlmag.com',
  '300308.SZ':'giglight.com',
  // Japan
  '7936.T':'asics.com','6146.T':'disco.co.jp','6954.T':'fanuc.com',
  '6501.T':'hitachi.com','7267.T':'honda.com','8001.T':'itochu.com',
  '7860.T':'avex.co.jp','6861.T':'keyence.com','285A.T':'kioxia.com',
  '9766.T':'konami.com','8058.T':'mitsubishi.com','8306.T':'mufg.jp',
  '1321.T':'nikkoam.com','7974.T':'nintendo.com','8136.T':'sanrio.com',
  '8316.T':'smfg.co.jp','9984.T':'softbank.com','6758.T':'sony.com',
  '3563.T':'food-and-life.co.jp','7203.T':'toyota.com','9983.T':'fastretailing.com',
  '2638.T':'globalxetfs.com','2644.T':'globalxetfs.com',
  '1545.T':'nikkoam.com','2015.T':'globalxetfs.com',
  // Europe
  'RMS.PA':'hermes.com','OR.PA':'loreal.com','MC.PA':'lvmh.com',
  'NVO':'novonordisk.com','SNY':'sanofi.com','SMSWLD.MI':'ishares.com',
  // Singapore
  'D05.SI':'dbs.com','S68.SI':'sgx.com','C6L.SI':'singaporeair.com',
  'Z74.SI':'singtel.com','Y92.SI':'thaibev.com','U11.SI':'uob.com.sg',
  'U96.SI':'sembcorp.com','V03.SI':'venture.com.sg','N6M.SI':'blackrock.com',
  'QK9.SI':'blackrock.com','MBH.SI':'blackrock.com',
  // Vietnam
  'FPT.VN':'fpt.com.vn','VCB.VN':'vietcombank.com.vn','MWG.VN':'thegioididong.com',
  'MSN.VN':'masangroup.com','VNM.VN':'vinamilk.com.vn','VHM.VN':'vinhomes.vn',
  'HPG.VN':'hoaphat.com.vn','GAS.VN':'pvgas.com.vn',
  // Taiwan
  '2395.TW':'advantech.com',
};

function _drLogoUrl(yf) {
  const d = _DR_LOGO_DOMAIN[yf];
  return d ? `https://www.google.com/s2/favicons?domain=${d}&sz=64` : null;
}
function _drLogoFallback(img, color, initials) {
  img.style.display = 'none';
  const fb = img.nextElementSibling;
  if (fb) { fb.style.display = 'flex'; }
}

let _drData      = null;
let _drLoaded    = false;
let _drRegion    = 'ALL';
let _drView      = 'table';
let _drSort      = 'rs_desc';
let _drSearch    = '';

const DR_REGION_LABEL = { US:'🇺🇸 USA', HK:'🇭🇰 HK / China', JP:'🇯🇵 Japan', ASEAN:'🌏 ASEAN', EU:'🇪🇺 Europe', SG:'🇸🇬 Singapore', VN:'🇻🇳 Vietnam', TW:'🇹🇼 Taiwan' };

function _drSymColor(sym) {
  const palette = ['#1f6feb','#3fb950','#d29922','#bc8cff','#f0883e','#58a6ff','#56d364','#e3b341'];
  let h = 0;
  for (const c of sym) h = (h * 31 + c.charCodeAt(0)) & 0x7fffffff;
  return palette[h % palette.length];
}

function _drFmtCap(v) {
  if (!v || v <= 0) return '—';
  if (v >= 1e12) return (v / 1e12).toFixed(2) + 'T';
  if (v >= 1e9)  return (v / 1e9).toFixed(1) + 'B';
  if (v >= 1e6)  return (v / 1e6).toFixed(1) + 'M';
  return v.toLocaleString();
}

function _drFmtPrice(p) {
  if (p == null) return '—';
  if (p >= 10000) return p.toLocaleString('en-US', {maximumFractionDigits: 0});
  if (p >= 1000)  return p.toLocaleString('en-US', {maximumFractionDigits: 1});
  if (p >= 100)   return p.toFixed(2);
  if (p >= 10)    return p.toFixed(3);
  return p.toFixed(4);
}

function loadDRPage() {
  if (_drLoaded && _drData) { renderDRTable(); return; }
  document.getElementById('dr-status').textContent = 'กำลังดึงข้อมูล...';
  document.getElementById('dr-table-wrap').innerHTML =
    '<div class="dr-loading"><span class="dr-load-spin"></span>กำลังโหลดข้อมูล DR/DRx — อาจใช้เวลา 1–2 นาที...</div>';

  fetch('/api/dr')
    .then(r => r.json())
    .then(d => {
      if (d.error) throw new Error(d.error);
      _drData   = d.stocks || [];
      _drLoaded = true;
      const ts = d.ts ? d.ts.replace('T', ' ').slice(0, 16) : '—';
      const drTotal = _drData.reduce((s, x) => s + (x.drs?.length || 0), 0);
      document.querySelector('#page-dr .page-sub').textContent =
        `ราคาและ Performance ของ Underlying Stocks ที่มี DR/DRx เทรดบน SET (${drTotal} DR จาก ${_drData.length} หุ้นต่างประเทศ)`;
      document.getElementById('dr-status').innerHTML =
        `อัปเดต: ${ts} &nbsp;|&nbsp; ${_drData.length} underlying stocks &nbsp;|&nbsp; cache 4 ชั่วโมง`;
      _updateDRRegionCounts();
      renderDRTable();
      // รีเฟรช datalist และ watchlist ถ้าเปิดอยู่
      const wlDl = document.getElementById("wl-sym-list");
      if (wlDl) wlDl.innerHTML = "";  // reset ให้ _wlPopulateSymList rebuild ใหม่
      if (document.getElementById("page-watchlist")?.classList.contains("active")) renderWatchlist();
    })
    .catch(e => {
      document.getElementById('dr-table-wrap').innerHTML =
        `<div class="empty">เกิดข้อผิดพลาด: ${e.message}</div>`;
    });
}

function reloadDRPage() {
  fetch('/api/dr-full-refresh', { method: 'POST' }).catch(() => {});
  _drLoaded = false;
  _drData   = null;
  loadDRPage();
}

function drQuickUpdate() {
  const btn = document.getElementById('dr-quick-btn');
  if (!_drData) { loadDRPage(); return; }
  if (btn) { btn.disabled = true; btn.textContent = '⏳ กำลังอัปเดต...'; }
  const statusEl = document.getElementById('dr-status');

  fetch('/api/dr-quick-update', { method: 'POST' })
    .then(r => r.json())
    .then(() => {
      const poll = setInterval(() => {
        fetch('/api/dr-quick-status').then(r => r.json()).then(s => {
          if (!s.running) {
            clearInterval(poll);
            if (s.error) {
              if (statusEl) statusEl.textContent = 'อัปเดตไม่สำเร็จ: ' + s.error;
              if (btn) { btn.disabled = false; btn.textContent = '⚡ อัปเดตราคา'; }
            } else {
              fetch('/api/dr').then(r => r.json()).then(d => {
                if (d.stocks) {
                  _drData = d.stocks;
                  const ts = d.ts ? d.ts.replace('T',' ').slice(0,16) : '—';
                  if (statusEl) statusEl.innerHTML =
                    `อัปเดต: ${ts} &nbsp;|&nbsp; ${_drData.length} stocks &nbsp;|&nbsp; <span style="color:var(--green)">✓ อัปเดตราคาสำเร็จ</span>`;
                  _updateDRRegionCounts();
                  renderDRTable();
                }
                if (btn) { btn.disabled = false; btn.textContent = '⚡ อัปเดตราคา'; }
              }).catch(e => {
                if (statusEl) statusEl.textContent = 'โหลด DR ไม่ได้: ' + e.message;
                if (btn) { btn.disabled = false; btn.textContent = '⚡ อัปเดตราคา'; }
              });
            }
          }
        }).catch(e => {
          clearInterval(poll);
          if (statusEl) statusEl.textContent = 'ตรวจสอบสถานะไม่ได้: ' + e.message;
          if (btn) { btn.disabled = false; btn.textContent = '⚡ อัปเดตราคา'; }
        });
      }, 1500);
    })
    .catch(e => {
      if (btn) { btn.disabled = false; btn.textContent = '⚡ อัปเดตราคา'; }
      if (statusEl) statusEl.textContent = 'เกิดข้อผิดพลาด: ' + e.message;
    });
}

function _updateDRRegionCounts() {
  if (!_drData) return;
  const counts = {};
  _drData.forEach(s => { counts[s.region] = (counts[s.region] || 0) + 1; });
  document.querySelectorAll('#dr-region-btns .dr-region-count-badge').forEach(el => {
    const region = el.dataset.region;
    const n = region === 'ALL' ? _drData.length : (counts[region] || 0);
    el.textContent = `(${n})`;
  });
}

function setDRRegion(r, btn) {
  _drRegion = r;
  document.querySelectorAll('#dr-region-btns .filter-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (_drData) renderDRTable();
}

function setDRSort(s, btn) {
  _drSort = s;
  document.querySelectorAll('#dr-sort-btns .filter-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (_drData) renderDRTable();
}

function filterDR() {
  _drSearch = document.getElementById('dr-search').value.toLowerCase().trim();
  if (_drData) renderDRTable();
}

function _sortDR(arr) {
  return [...arr].sort((a, b) => {
    if (_drSort === 'rs_desc')     return (b.rs_score ?? -1)  - (a.rs_score ?? -1);
    if (_drSort === 'chg_desc')    return (b.chg ?? -999)     - (a.chg ?? -999);
    if (_drSort === 'chg_asc')     return (a.chg ?? 999)      - (b.chg ?? 999);
    if (_drSort === 'ret_1m_desc') return (b.ret_1m ?? -999)  - (a.ret_1m ?? -999);
    if (_drSort === 'ret_1y_desc') return (b.ret_1y ?? -999)  - (a.ret_1y ?? -999);
    if (_drSort === 'mkt_cap')     return (b.mkt_cap || 0)    - (a.mkt_cap || 0);
    return a.sym.localeCompare(b.sym);
  });
}

function setDRView(v, btn) {
  _drView = v;
  document.querySelectorAll('.dr-view-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  if (_drData) renderDRTable();
}

function _drExchBadge(yf) {
  if (!yf) return '';
  if (yf.endsWith('.HK'))  return '🇭🇰 HKEX';
  if (yf.endsWith('.T'))   return '🇯🇵 TSE';
  if (yf.endsWith('.SI'))  return '🇸🇬 SGX';
  if (yf.endsWith('.VN'))  return '🇻🇳 HOSE';
  if (yf.endsWith('.TW'))  return '🇹🇼 TWSE';
  if (yf.endsWith('.PA'))  return '🇫🇷 EPA';
  if (yf.endsWith('.MI'))  return '🇮🇹 BIT';
  if (yf.endsWith('.SS'))  return '🇨🇳 SSE';
  if (yf.endsWith('.SZ'))  return '🇨🇳 SZSE';
  return '🇺🇸 US';
}

function _drCardGrid(stocks) {
  return stocks.map(s => {
    const chgCls  = s.chg >= 0 ? 'green' : 'red';
    const chgStr  = s.chg != null ? (s.chg >= 0 ? '+' : '') + s.chg.toFixed(2) + '%' : '—';
    const color   = _drSymColor(s.sym);
    const initials = s.yf.slice(0, 4);
    const logoUrl = _drLogoUrl(s.yf);
    const logoHtml = logoUrl
      ? `<img src="${logoUrl}" class="dr-card-logo-img" onerror="_drLogoFallback(this)"><div class="dr-card-logo" style="background:${color};display:none">${initials}</div>`
      : `<div class="dr-card-logo" style="background:${color}">${initials}</div>`;
    const badges = s.drs.map(d =>
      `<a class="dr-badge${d.endsWith('X') ? ' dr-badge-x' : ''}" href="https://www.tradingview.com/chart/?symbol=SET:${encodeURIComponent(d)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="เปิด ${d} ใน TradingView">${d}</a>`
    ).join('');
    const closeData = JSON.stringify(s.close100 || []);
    const cPct = v => v != null ? `<span class="${v>=0?'green':'red'}">${v>=0?'+':''}${v.toFixed(1)}%</span>` : '—';
    const rsDisp = s.rs_score != null ? `<span class="${rsColor(s.rs_score)}" style="font-weight:700">RS ${s.rs_score}</span>` : '';
    return `<div class="dr-card" onclick="openDRChartModal('${s.sym}')" style="cursor:pointer">
      <div class="dr-card-header">
        ${logoHtml}
        <div>
          <div class="dr-card-sym" style="color:var(--blue)">${s.yf}</div>
          <span class="dr-card-exch">${_drExchBadge(s.yf)}</span>
        </div>
        ${rsDisp ? `<div style="margin-left:auto;font-size:11px">${rsDisp}</div>` : ''}
        <a class="tv-link" href="https://www.tradingview.com/chart/?symbol=${yfToTVSym(s.yf)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="ดูใน TradingView" style="margin-left:${rsDisp?'6px':'auto'}">↗</a>
      </div>
      <div class="dr-card-name">${s.name}</div>
      <div class="dr-card-price-row">
        <span class="dr-card-price">${_drFmtPrice(s.price)}</span>
        <span class="dr-card-chg ${chgCls}">${chgStr}</span>
      </div>
      <div style="display:flex;gap:6px;font-size:10px;color:var(--text2);margin:3px 0 2px">
        <span>1M ${cPct(s.ret_1m)}</span><span>3M ${cPct(s.ret_3m)}</span><span>1Y ${cPct(s.ret_1y)}</span>
      </div>
      <canvas class="dr-card-chart dr-trend-cv" data-close='${JSON.stringify(s.close100||[])}' width="200" height="52"></canvas>
      <div class="dr-card-badges">${badges}</div>
    </div>`;
  }).join('');
}

function renderDRTable() {
  if (!_drData) return;

  let stocks = _drData;
  if (_drRegion !== 'ALL') stocks = stocks.filter(s => s.region === _drRegion);
  if (_drSearch) {
    stocks = stocks.filter(s =>
      s.sym.toLowerCase().includes(_drSearch) ||
      s.name.toLowerCase().includes(_drSearch) ||
      s.ind.toLowerCase().includes(_drSearch) ||
      s.drs.some(d => d.toLowerCase().includes(_drSearch))
    );
  }

  const wrap = document.getElementById('dr-table-wrap');
  if (!stocks.length) { wrap.innerHTML = '<div class="empty">ไม่พบข้อมูลที่ตรงกับเงื่อนไข</div>'; return; }

  if (_drView === 'cards') {
    let html = '<div class="dr-card-grid">';
    if (_drRegion === 'ALL') {
      for (const region of ['US','HK','JP','EU','SG','VN','TW']) {
        const rs = _sortDR(stocks.filter(s => s.region === region));
        if (!rs.length) continue;
        html += `<div class="dr-region-hdr-card">${DR_REGION_LABEL[region] || region} — ${rs.length} stocks</div>`;
        html += _drCardGrid(rs);
      }
    } else {
      html += _drCardGrid(_sortDR(stocks));
    }
    html += '</div>';
    wrap.innerHTML = html;
    requestAnimationFrame(() => _drawAllDRCharts());
    return;
  }

  const thead = `<thead><tr>
    <th style="width:112px" title="ชื่อย่อหุ้นต่างประเทศ (underlying) — คลิกเพื่อเปิดกราฟ">Symbol</th>
    <th class="r" style="width:80px" title="ราคาปิดล่าสุด (สกุลเงินท้องถิ่นของตลาดนั้น)">Close</th>
    <th class="r" style="width:64px"${colTip('ret_1d')}>CHG%</th>
    <th class="r" style="width:42px" title="RS Score 0–99 — จัดอันดับเทียบภายในกลุ่ม underlying ทั้ง 84 ตัว ยิ่งสูงยิ่งแข็งแกร่ง">RS</th>
    <th class="r" style="width:58px"${colTip('ret_1w')}>1W%</th>
    <th class="r" style="width:58px"${colTip('ret_1m')}>1M%</th>
    <th class="r" style="width:58px"${colTip('ret_3m')}>3M%</th>
    <th class="r" style="width:58px"${colTip('ret_1y')}>1Y%</th>
    <th style="min-width:150px"${colTip('name')}>Company</th>
    <th style="min-width:180px" title="DR/DRx ที่อ้างอิงหุ้นตัวนี้ ซื้อขายได้บน SET ด้วยเงินบาท">DR Tickers บน SET</th>
    <th style="min-width:140px"${colTip('industry')}>Industry</th>
    <th class="r" style="width:72px"${colTip('mkt_cap')}>MKT CAP</th>
    <th style="width:108px;text-align:center">Trend (100D)</th>
    <th style="width:136px;text-align:center">Pattern (30D)</th>
    <th class="r" style="width:52px" title="ตั้งราคาแจ้งเตือน">🔔</th>
  </tr></thead>`;

  let tbody = '';
  if (_drRegion === 'ALL') {
    for (const region of ['US','HK','JP','ASEAN','EU','SG','VN','TW']) {
      const rs = _sortDR(stocks.filter(s => s.region === region));
      if (!rs.length) continue;
      tbody += `<tr><td colspan="15" class="dr-region-hdr">${DR_REGION_LABEL[region] || region} <span class="dr-region-count">${rs.length} stocks</span></td></tr>`;
      tbody += _drRows(rs);
    }
  } else {
    tbody = _drRows(_sortDR(stocks));
  }

  wrap.innerHTML = `<table class="tbl dr-tbl">${thead}<tbody>${tbody}</tbody></table>`;
  requestAnimationFrame(() => _drawAllDRCharts());
}

function _drRows(stocks) {
  return stocks.map(s => {
    const chgCls   = s.chg >= 0 ? 'green' : 'red';
    const chgStr   = s.chg != null ? (s.chg >= 0 ? '+' : '') + s.chg.toFixed(2) + '%' : '—';
    const color    = _drSymColor(s.sym);
    const initials = s.yf.slice(0, 4);
    const badges   = s.drs.map(d =>
      `<a class="dr-badge${d.endsWith('X') ? ' dr-badge-x' : ''}" href="https://www.tradingview.com/chart/?symbol=SET:${encodeURIComponent(d)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="เปิด ${d} ใน TradingView">${d}</a>`
    ).join('');

    const logoUrl = _drLogoUrl(s.yf);
    const logoHtml = logoUrl
      ? `<img src="${logoUrl}" class="dr-logo-img" onerror="_drLogoFallback(this)"><div class="dr-logo" style="background:${color};display:none">${initials}</div>`
      : `<div class="dr-logo" style="background:${color}">${initials}</div>`;

    const drPct = v => v != null
      ? `<span class="${v >= 0 ? 'green' : 'red'}" style="font-size:11px">${v >= 0 ? '+' : ''}${v.toFixed(1)}%</span>`
      : '<span style="color:var(--text2);font-size:11px">—</span>';
    const rsNum = s.rs_score != null
      ? `<span class="${rsColor(s.rs_score)}" style="font-weight:700;font-size:11px">${s.rs_score}</span>`
      : '<span style="color:var(--text2);font-size:11px">—</span>';

    return `<tr>
      <td>
        <div class="dr-sym-cell" onclick="openDRChartModal('${s.sym}')" style="cursor:pointer" title="ดูกราฟ ${s.yf}">
          ${logoHtml}
          <span class="dr-sym-text" style="color:var(--blue)">${s.yf}</span>
          <a class="tv-link" href="https://www.tradingview.com/chart/?symbol=${yfToTVSym(s.yf)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="ดูใน TradingView">↗</a>
        </div>
      </td>
      <td class="r" style="font-size:12px;font-weight:600">${_drFmtPrice(s.price)}</td>
      <td class="r ${chgCls}" style="font-size:12px;font-weight:600">${chgStr}</td>
      <td class="r">${rsNum}</td>
      <td class="r">${drPct(s.ret_1w)}</td>
      <td class="r">${drPct(s.ret_1m)}</td>
      <td class="r">${drPct(s.ret_3m)}</td>
      <td class="r">${drPct(s.ret_1y)}</td>
      <td style="font-size:11px">${s.name}${s.yf ? '<br><span style="font-size:10px;color:var(--text2)">(' + s.yf + ')</span>' : ''}</td>
      <td style="font-size:9.5px;line-height:1.8">${badges}</td>
      <td style="font-size:11px;color:var(--text2)">${s.ind}</td>
      <td class="r" style="font-size:11px">${_drFmtCap(s.mkt_cap)}</td>
      <td style="padding:4px 5px">
        <canvas class="dr-trend-cv" data-close='${JSON.stringify(s.close100||[])}' width="104" height="36" style="display:block;width:104px;height:36px"></canvas>
      </td>
      <td style="padding:4px 5px">
        <canvas class="dr-candle-cv" data-ohlc='${JSON.stringify(s.ohlc30||[])}' width="130" height="36" style="display:block;width:130px;height:36px"></canvas>
      </td>
      <td class="r" style="white-space:nowrap">${_wlAlertCell("DR:" + s.sym)}</td>
    </tr>`;
  }).join('');
}

function _drawAllDRCharts() {
  document.querySelectorAll('.dr-trend-cv').forEach(cv => {
    try { const d = JSON.parse(cv.dataset.close||'[]'); if (d.length>=2) _drawDRSparkline(cv,d); } catch(e) {}
  });
  document.querySelectorAll('.dr-candle-cv').forEach(cv => {
    try { const d = JSON.parse(cv.dataset.ohlc||'[]'); if (d.length>=2) _drawDRCandles(cv,d); } catch(e) {}
  });
}

function _drawDRSparkline(canvas, prices) {
  const dpr = window.devicePixelRatio || 1;
  const W = 104, H = 36;
  canvas.width = W * dpr; canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const min = Math.min(...prices), max = Math.max(...prices), rng = max - min || 1;
  const isUp = prices[prices.length-1] >= prices[0];
  const col  = isUp ? '#3fb950' : '#f85149';
  const pad  = 3;

  ctx.clearRect(0, 0, W, H);
  ctx.beginPath();
  prices.forEach((p, i) => {
    const x = (i / (prices.length-1)) * W;
    const y = H - pad - ((p - min) / rng) * (H - pad*2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo(W, H); ctx.lineTo(0, H); ctx.closePath();
  ctx.fillStyle = isUp ? 'rgba(63,185,80,0.12)' : 'rgba(248,81,73,0.12)';
  ctx.fill();
}

function _drawDRCandles(canvas, ohlc) {
  const dpr = window.devicePixelRatio || 1;
  const W = 130, H = 36;
  canvas.width = W * dpr; canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);

  const hasVol = ohlc.length > 0 && ohlc[0].length >= 5 && ohlc.some(d => d[4] > 0);
  const priceH = hasVol ? Math.floor(H * 0.70) : H;
  const volH   = hasVol ? H - priceH - 1 : 0;

  const hiVal = Math.max(...ohlc.map(c => c[1]));
  const loVal = Math.min(...ohlc.map(c => c[2]));
  const rng   = hiVal - loVal || 1;
  const pad   = 2, n = ohlc.length;
  const slotW = W / n;
  const cw    = Math.max(1, Math.floor(slotW * 0.65));
  const toY   = v => priceH - pad - ((v - loVal) / rng) * (priceH - pad*2);

  ohlc.forEach(([o, h, l, c], i) => {
    const cx  = i * slotW + slotW / 2;
    const col = c >= o ? '#3fb950' : '#f85149';
    ctx.strokeStyle = col; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(cx, toY(h)); ctx.lineTo(cx, toY(l)); ctx.stroke();
    const top = toY(Math.max(o,c)), bot = toY(Math.min(o,c));
    ctx.fillStyle = col;
    ctx.fillRect(cx - cw/2, top, cw, Math.max(1, bot - top));
  });

  if (hasVol) {
    const vols  = ohlc.map(d => d[4] || 0);
    const maxVol = Math.max(...vols) || 1;
    const volTop = priceH + 1;
    ohlc.forEach(([o,,, c], i) => {
      const v  = vols[i];
      const bh = Math.max(1, Math.round((v / maxVol) * (volH - 1)));
      const cx = i * slotW + slotW / 2;
      ctx.fillStyle = c >= o ? 'rgba(63,185,80,0.5)' : 'rgba(248,81,73,0.5)';
      ctx.fillRect(cx - cw/2, volTop + volH - bh, cw, bh);
    });
  }
}

// ============================================================
// INDICES PAGE
// ============================================================
let _idxData    = null;
let _idxGroup   = 'ALL';
let _idxSortKey = 'ret_1d';
let _idxView    = 'cards';
let _idxFilterReversal = false;

// ── Tooltip system ─────────────────────────────────────────────
function showIdxTT(html, e) {
  const tt = document.getElementById('idx-tt');
  if (!tt) return;
  tt.innerHTML = html; tt.style.display = 'block'; _moveIdxTT(e);
}
function hideIdxTT() {
  const tt = document.getElementById('idx-tt');
  if (tt) tt.style.display = 'none';
}
function _moveIdxTT(e) {
  const tt = document.getElementById('idx-tt');
  if (!tt) return;
  const vw = window.innerWidth, vh = window.innerHeight;
  let x = e.clientX + 14, y = e.clientY + 14;
  if (x + 270 > vw) x = e.clientX - 274;
  if (y + 160 > vh) y = e.clientY - 164;
  tt.style.left = x + 'px'; tt.style.top = y + 'px';
}
document.addEventListener('mousemove', e => {
  const tt = document.getElementById('idx-tt');
  if (tt && tt.style.display !== 'none') _moveIdxTT(e);
});

function toggleIdxReversal(btn) {
  _idxFilterReversal = !_idxFilterReversal;
  btn.classList.toggle('active', _idxFilterReversal);
  renderIdxGrid();
}

function toggleIdxGuide() {
  const box = document.getElementById('idx-guide-box');
  const btn = document.getElementById('idx-guide-btn');
  const open = box.style.display === 'none';
  box.style.display = open ? '' : 'none';
  btn.style.background = open ? '#1f6feb44' : '#1f6feb22';
}

async function loadIndicesPage() {
  if (_idxData) { renderIdxGrid(); return; }
  const grid = document.getElementById('idx-grid');
  grid.innerHTML = '<div style="color:var(--text2);padding:20px">กำลังโหลดข้อมูลดัชนี...</div>';
  try {
    const res = await fetch('/api/indices?t=' + Date.now());
    const d   = await res.json();
    if (d.error) {
      grid.innerHTML = `<div style="padding:20px">
        <div class="text2" style="margin-bottom:12px">${d.error}</div>
        <button onclick="loadIndicesPage()" style="padding:6px 16px;border-radius:6px;border:1px solid var(--border);background:#238636;color:#fff;cursor:pointer;font-size:12px">
          ⟳ ดาวน์โหลดข้อมูล (ใช้เวลา ~30 วินาที)
        </button>
      </div>`;
      return;
    }
    _idxData = d;
    _setIdxUpdated();
    renderIdxGrid();
  } catch(e) {
    grid.innerHTML = `<div style="color:var(--red)">โหลดไม่ได้: ${e.message}</div>`;
  }
}

function _setIdxUpdated() {
  const sample = _idxData && Object.values(_idxData)[0];
  const el = document.getElementById('idx-updated');
  if (el && sample?.updated_at) el.textContent = 'อัปเดต: ' + sample.updated_at;
}


function setIdxGroup(g, btn) {
  _idxGroup = g;
  document.querySelectorAll('#idx-group-row .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderIdxGrid();
}

function setIdxSort(key, btn) {
  _idxSortKey = key;
  document.querySelectorAll('#idx-sort-row .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  // sync active button by id when called programmatically
  const byId = document.getElementById('idx-sort-btn-' + key);
  if (byId && byId !== btn) { btn.classList.remove('active'); byId.classList.add('active'); }
  renderIdxGrid();
}

function setIdxView(view, btn) {
  _idxView = view;
  document.querySelectorAll('#idx-view-cards,#idx-view-heatmap').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  // sort label หายไปเมื่อ heatmap (period ทำหน้าที่แทน)
  const lbl = document.getElementById('idx-sort-label');
  if (lbl) lbl.textContent = view === 'heat' ? 'สี/เรียงตาม:' : 'เรียงตาม:';
  renderIdxGrid();
}

const isReversal = (i) => i.ret_1m > 0 && (i.ret_6m == null || i.ret_6m < 15) && (i.mom == null || i.mom > 1);

function renderIdxHeatmap(items) {
  const grid = document.getElementById('idx-grid');
  grid.style.display = 'block';
  grid.style.gridTemplateColumns = '';
  grid.style.gap = '';
  const pct = (v) => v == null ? '—' : (v > 0 ? '+' : '') + v.toFixed(1) + '%';
  const groupOrder = ['SET_INDICES','SET_INDUSTRY','SET_SECTORS','MAI_INDUSTRY'];
  const groupLabel = {
    SET_INDICES:  'SET Indices', SET_INDUSTRY: 'Industry Group',
    SET_SECTORS:  'Sector (SET)', MAI_INDUSTRY: 'mai'
  };

  const byGroup = {};
  items.forEach(idx => {
    const g = idx.group || 'OTHER';
    if (!byGroup[g]) byGroup[g] = [];
    byGroup[g].push(idx);
  });

  const v = (idx) => idx[_idxSortKey] ?? null;
  const isRS  = _idxSortKey === 'rs_set';
  const isMom = _idxSortKey === 'mom';
  const cap = { ret_1d:10, ret_1w:15, ret_1m:20, ret_3m:30, ret_6m:40, ret_1y:50, mom:15 }[_idxSortKey] || 20;

  const html = groupOrder.filter(g => byGroup[g]?.length).map(g => {
    const sorted = [...byGroup[g]].sort((a,b) => (v(b)??-999)-(v(a)??-999));
    const cells = sorted.map(idx => {
      const val = v(idx);
      const bg  = isRS ? _heatColorRS(val) : _heatColor(val, cap);
      const txtCol = isRS
        ? ((val??50) > 70 || (val??50) < 30 ? '#fff' : 'var(--text)')
        : (Math.abs(val??0) > cap*0.6 ? '#fff' : 'var(--text)');
      const label = isRS ? (val != null ? 'RS '+val : '—')
                  : isMom ? (val != null ? (val>0?'+':'')+val.toFixed(1) : '—')
                  : pct(val);
      const revDot = isReversal && isReversal(idx) ? ' 🔄' : '';
      return `<div onclick="openIdxChartModal('${idx.sym}')"
        title="${idx.name} ${label}"
        style="background:${bg};color:${txtCol};border-radius:6px;padding:10px 8px;cursor:pointer;
               min-width:80px;text-align:center;transition:opacity .15s"
        onmouseover="this.style.opacity='.8'" onmouseout="this.style.opacity='1'">
        <div style="font-size:11px;font-weight:700;margin-bottom:3px">${idx.sym.replace(/^\^/,'').replace(/\.BK$/,'')}${revDot}</div>
        <div style="font-size:10px;color:${txtCol};opacity:.85;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:80px">${idx.name}</div>
        <div style="font-size:12px;font-weight:700">${label}</div>
      </div>`;
    }).join('');
    const avg = sorted.filter(x=>v(x)!=null).reduce((s,x)=>s+v(x),0) / (sorted.filter(x=>v(x)!=null).length||1);
    const avgStr = isNaN(avg) ? '—' : isRS ? 'RS ' + Math.round(avg) : pct(avg);
    const avgCls = isRS ? (avg >= 50 ? 'green' : 'red') : (avg >= 0 ? 'green' : 'red');
    return `<div style="margin-bottom:20px">
      <div style="font-size:11px;font-weight:600;color:var(--text2);margin-bottom:8px;display:flex;align-items:center;gap:8px">
        ${groupLabel[g]}
        <span class="${avgCls}">${avgStr}</span>
        <span class="text2" style="font-size:10px">${sorted.length} ดัชนี</span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:6px">${cells}</div>
    </div>`;
  }).join('');

  document.getElementById('idx-grid').innerHTML = html || '<div class="text2">ไม่มีข้อมูล</div>';
}

function renderIdxGrid() {
  if (!_idxData) return;
  let items = Object.values(_idxData);
  if (_idxGroup !== 'ALL') items = items.filter(x => x.group === _idxGroup);

  // คำนวณ Momentum Score = ret_1m − (ret_6m ÷ 6)
  items = items.map(i => ({
    ...i,
    mom: (i.ret_1m != null && i.ret_6m != null)
      ? +((i.ret_1m - i.ret_6m / 6).toFixed(2))
      : (i.ret_1m != null ? i.ret_1m : null)
  }));

  // Filter กลับตัว
  if (_idxFilterReversal) {
    items = items.filter(i =>
      i.ret_1m != null && i.ret_1m > 0 &&
      (i.ret_6m == null || i.ret_6m < 15) &&
      (i.mom == null || i.mom > 1)
    );
  }

  items.sort((a,b) => (b[_idxSortKey]??-999) - (a[_idxSortKey]??-999));

  if (_idxView === 'heat') { renderIdxHeatmap(items); return; }

  // cards view — reset grid layout
  const grid = document.getElementById('idx-grid');
  grid.style.display = 'grid';
  grid.style.gridTemplateColumns = 'repeat(auto-fill,minmax(260px,1fr))';
  grid.style.gap = '12px';

  const pct = (v) => v == null ? '—'
    : `<span class="${v>0?'green':v<0?'red':'text2'}">${v>0?'+':''}${v.toFixed(1)}%</span>`;

  // RS trend arrow จาก rs_history
  const rsTrend = (hist) => {
    if (!hist || hist.length < 2) return '<span style="color:var(--text2);font-size:11px">ยังไม่มีประวัติ</span>';
    const last = hist[hist.length-1].rs;
    const prev = hist[Math.max(0, hist.length-5)].rs;
    const diff = last - prev;
    const weeks = hist.length >= 5 ? '4 สัปดาห์' : `${hist.length-1} ครั้ง`;
    if (diff >= 5)  return `<span style="color:var(--green);font-weight:700" title="RS ขึ้น +${diff} ใน${weeks}">↑ +${diff}</span>`;
    if (diff <= -5) return `<span style="color:var(--red);font-weight:700"   title="RS ลง ${diff} ใน${weeks}">↓ ${diff}</span>`;
    return `<span style="color:#e3b341;font-weight:700" title="RS ทรงตัว (${diff>=0?'+':''}${diff}) ใน${weeks}">→ ${diff>=0?'+':''}${diff}</span>`;
  };

  // Momentum bar
  const momBar = (m) => {
    if (m == null) return '<span class="text2">—</span>';
    const c = m > 3 ? 'var(--green)' : m > 0 ? '#3fb950' : m > -3 ? '#e3b341' : 'var(--red)';
    const w = Math.min(100, Math.abs(m) * 5);
    const sign = m > 0 ? '+' : '';
    return `<div>
      <span style="font-size:12px;font-weight:700;color:${c}">${sign}${m.toFixed(1)}</span>
      <div style="height:3px;border-radius:2px;background:var(--border);margin-top:2px">
        <div style="height:3px;border-radius:2px;width:${w}%;background:${c};transition:width .3s"></div>
      </div>
    </div>`;
  };

  // RS bar
  const rsBar2 = (v) => {
    if (v == null) return '<span class="text2">—</span>';
    const c = v >= 70 ? 'var(--green)' : v >= 40 ? '#e3b341' : 'var(--red)';
    return `<div>
      <span style="font-size:12px;font-weight:700;color:${c}">${v}</span>
      <div style="height:3px;border-radius:2px;background:var(--border);margin-top:2px">
        <div style="height:3px;border-radius:2px;width:${v}%;background:${c};transition:width .3s"></div>
      </div>
    </div>`;
  };

  const cards = items.map(idx => {
    const spark = `<canvas class="idx-spark" id="idxspark_${idx.sym.replace(/[^a-z0-9]/gi,'_')}"></canvas>`;
    const revBadge = isReversal(idx)
      ? `<span style="font-size:9px;background:#1f6feb33;color:#58a6ff;border:1px solid #1f6feb;border-radius:4px;padding:1px 5px;margin-left:6px"
           onmouseenter="showIdxTT('<b>🔄 สัญญาณกลับตัว</b><br>• 1M% &gt; 0 ✓ เดือนนี้เริ่มเป็นบวก<br>• 6M% &lt; 15 ✓ ยังไม่วิ่งไปไกล<br>• Momentum &gt; 1 ✓ momentum กำลังเร่ง<br><br>ตรวจสอบกราฟว่าตัดขึ้น EMA50 หรือยัง',event)" onmouseleave="hideIdxTT()">🔄 กลับตัว</span>`
      : '';
    return `
      <div class="idx-card" onclick="openIdxChartModal('${idx.sym}')">
        <div style="display:flex;align-items:center;justify-content:space-between">
          <div class="idx-card-sym">${idx.sym}</div>${revBadge}
        </div>
        <div class="idx-card-name" title="${idx.name}" style="display:flex;align-items:center;gap:4px">
          <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${idx.name}</span>
          <a class="tv-link" href="https://www.tradingview.com/chart/?symbol=${idxToTVSym(idx.sym)}&interval=D" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="ดูใน TradingView" style="flex-shrink:0">↗</a>
        </div>
        <div class="idx-card-val">${idx.last != null ? idx.last.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) : '—'}</div>
        ${spark}
        <div class="idx-rets">
          <div class="idx-ret-cell"><div class="idx-ret-lbl">1D%</div><div class="idx-ret-val">${pct(idx.ret_1d)}</div></div>
          <div class="idx-ret-cell"><div class="idx-ret-lbl">1W%</div><div class="idx-ret-val">${pct(idx.ret_1w)}</div></div>
          <div class="idx-ret-cell"><div class="idx-ret-lbl">1M%</div><div class="idx-ret-val">${pct(idx.ret_1m)}</div></div>
          <div class="idx-ret-cell"><div class="idx-ret-lbl">3M%</div><div class="idx-ret-val">${pct(idx.ret_3m)}</div></div>
          <div class="idx-ret-cell"><div class="idx-ret-lbl">6M%</div><div class="idx-ret-val">${pct(idx.ret_6m)}</div></div>
          <div class="idx-ret-cell"><div class="idx-ret-lbl">1Y%</div><div class="idx-ret-val">${pct(idx.ret_1y)}</div></div>
        </div>
        <div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);display:grid;grid-template-columns:1fr 1fr;gap:8px">
          <div onmouseenter="showIdxTT('<b>Momentum Score</b><br>สูตร: 1M% − (6M%÷6)<br>บวกมาก = momentum เพิ่งเร่งขึ้น<br>ลบมาก = momentum กำลังชะลอ<br><br>&gt;3 แรงมาก · 1–3 ดีขึ้น · &lt;0 ชะลอ',event)" onmouseleave="hideIdxTT()">
            <div class="idx-ret-lbl">Momentum</div>
            ${momBar(idx.mom)}
          </div>
          <div onmouseenter="showIdxTT('<b>RS (vs SET)</b><br>Relative Strength เทียบหุ้น SET 930 ตัว<br>0=อ่อนสุด · 99=แรงสุด<br>≥70 แรง · 40–69 กลาง · &lt;40 อ่อน',event)" onmouseleave="hideIdxTT()">
            <div class="idx-ret-lbl">RS SET</div>
            ${rsBar2(idx.rs_set)}
          </div>
        </div>
        <div style="margin-top:6px;display:flex;align-items:center;gap:6px"
          onmouseenter="showIdxTT('<b>RS Trend</b><br>ทิศทาง RS เทียบ 4 สัปดาห์ก่อน<br>↑ RS กำลังดีขึ้น (สัญญาณดี)<br>→ RS ทรงตัว<br>↓ RS กำลังอ่อนลง<br><br>ข้อมูลสะสมหลังกด Quick Update ทุกครั้ง',event)" onmouseleave="hideIdxTT()">
          <div class="idx-ret-lbl">RS Trend</div>
          ${rsTrend(idx.rs_history)}
        </div>
      </div>`;
  }).join('');

  document.getElementById('idx-grid').innerHTML = cards || '<div class="text2">ไม่มีข้อมูล</div>';

  requestAnimationFrame(() => {
    items.forEach(idx => {
      const id  = 'idxspark_' + idx.sym.replace(/[^a-z0-9]/gi,'_');
      const cvs = document.getElementById(id);
      if (cvs && idx.closes?.length > 1) _drawIdxSpark(cvs, idx.closes.slice(-60), idx.ret_1d);
    });
  });
}

function _drawIdxSpark(canvas, closes, ret1d) {
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth || 232, H = 48;
  canvas.width  = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  const mn = Math.min(...closes), mx = Math.max(...closes);
  const rng = mx - mn || 1;
  const toX = i => i / (closes.length-1) * W;
  const toY = v => H - (v - mn) / rng * (H-4) - 2;
  const color = ret1d >= 0 ? '#3fb950' : '#f85149';
  ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.lineJoin = 'round';
  ctx.beginPath();
  closes.forEach((v,i) => i===0 ? ctx.moveTo(toX(i),toY(v)) : ctx.lineTo(toX(i),toY(v)));
  ctx.stroke();
  const grad = ctx.createLinearGradient(0,0,0,H);
  grad.addColorStop(0, ret1d>=0 ? 'rgba(63,185,80,0.25)' : 'rgba(248,81,73,0.25)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.lineTo(W, H); ctx.lineTo(0, H); ctx.closePath(); ctx.fill();
}

function openIdxChartModal(sym) {
  // ถ้ายังไม่มีข้อมูล indices ให้โหลดก่อนแล้วค่อยเปิด modal
  if (!_idxData) {
    loadIndicesPage().then(() => { if (_idxData) openIdxChartModal(sym); });
    return;
  }
  const idx = _idxData?.[sym];
  if (!idx || !idx.closes?.length) {
    // ไม่มีกราฟ index → เปิด sector modal (รายการหุ้น) แทน
    const sectorName = (IDX_TO_SECTOR[sym] || [])[0];
    if (sectorName) { openSectorModal(sectorName); return; }
    window.open(`https://www.tradingview.com/chart/?symbol=${idxToTVSym(sym)}&interval=D`, '_blank');
    return;
  }

  // build [[date,price],...] history
  const hist = idx.dates.map((d,i) => [d, idx.closes[i]]);

  // fake stock object — enough for _drawChart
  const fakeStock = {
    symbol: sym,
    name:   idx.name,
    price_history: hist,
    ath: null,
    _isIndex: true,
  };

  // populate modal header
  const _cmTitleEl3 = document.getElementById('cm-title');
  _cmTitleEl3.textContent = idx.name;
  _cmTitleEl3.title = idx.name;
  document.getElementById('cm-sub').textContent   = sym;
  document.getElementById('cm-tv-link').href = `https://www.tradingview.com/chart/?symbol=${idxToTVSym(sym)}&interval=D`;

  const mk = (val, lbl, cls='') =>
    `<div><div class="cm-metric-val ${cls}">${val}</div><div class="cm-metric-lbl">${lbl}</div></div>`;
  const pp = (v) => v==null ? '—'
    : `<span class="${v>0?'green':v<0?'red':''}">${v>0?'+':''}${v.toFixed(2)}%</span>`;
  document.getElementById('cm-metrics').innerHTML = [
    mk(idx.last?.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) ?? '—', 'ราคา'),
    mk(pp(idx.ret_1d), '1D%'),
    mk(pp(idx.ret_1w), '1W%'),
    mk(pp(idx.ret_1m), '1M%'),
    mk(pp(idx.ret_3m), '3M%'),
    mk(pp(idx.ret_6m), '6M%'),
    mk(pp(idx.ret_1y), '1Y%'),
  ].join('');

  // hide งบการเงิน, show/hide หุ้นในกลุ่ม based on mapping
  const finBtn    = document.getElementById('cm-mode-fin');
  const stocksBtn = document.getElementById('cm-mode-stocks');
  if (finBtn)    finBtn.style.display    = 'none';
  if (stocksBtn) stocksBtn.style.display = IDX_TO_SECTOR[sym]?.length ? '' : 'none';
  const fsLinkIdx = document.getElementById('cm-factsheet-link');
  if (fsLinkIdx) fsLinkIdx.style.display = 'none';
  const setLinkIdx = document.getElementById('cm-set-link');
  if (setLinkIdx) setLinkIdx.style.display = 'none';

  // set chart state
  _cmStock       = fakeStock;
  _cmTf          = '1y';
  _cmHistoryData = hist;   // full history already loaded
  _cmVolumeData  = null;
  _cmFinLoaded   = null;

  setCmMode('chart');
  document.querySelectorAll('#chart-modal .filter-btn').forEach(b => b.classList.remove('active'));
  const tfBtn = document.getElementById('cm-tf-1y');
  if (tfBtn) tfBtn.classList.add('active');

  document.getElementById('chart-modal').classList.add('open');
  document.body.style.overflow = 'hidden';
  requestAnimationFrame(() => _drawChart(fakeStock, hist));
}


/* ═════════ inline block boundary ═════════ */


const _floatContents = {
  obv: `<strong style="color:#bc8cff">OBV — On-Balance Volume</strong><br><br>
สะสม volume ตามทิศทางราคา:<br>
&bull; ราคา <span style="color:#3fb950">ขึ้น</span> → OBV + volume วันนั้น<br>
&bull; ราคา <span style="color:#f85149">ลง</span> → OBV − volume วันนั้น<br><br>
<strong>ดูอะไร?</strong> ดู <em>ทิศทาง</em> ไม่ใช่ตัวเลข<br><br>
<span style="color:#3fb950">▲ Bullish Divergence</span><br>
ราคาลงต่ำใหม่ แต่ OBV ไม่ลงตาม<br>→ แรงขายเริ่มหมด อาจกลับตัวขึ้น<br><br>
<span style="color:#f85149">▼ Bearish Divergence</span><br>
ราคาขึ้นสูงใหม่ แต่ OBV ไม่ขึ้นตาม<br>→ volume ไม่ confirm อาจกลับตัวลง<br><br>
<span style="color:#8b949e">✦ ใช้ได้ดีกับหุ้น Liquid ที่ volume สม่ำเสมอ</span>`,
};

function showFloatPopup(el, key) {
  const popup = document.getElementById('float-popup');
  const body  = document.getElementById('float-popup-body');
  body.innerHTML = _floatContents[key] || '';
  popup.style.display = 'block';
  // position near clicked element
  const r = el.getBoundingClientRect();
  const pw = 280, ph = popup.offsetHeight || 260;
  let left = r.left;
  let top  = r.bottom + 8;
  if (left + pw > window.innerWidth - 8) left = window.innerWidth - pw - 8;
  if (top + ph > window.innerHeight - 8) top = r.top - ph - 8;
  popup.style.left = left + 'px';
  popup.style.top  = top  + 'px';
}

document.addEventListener('click', e => {
  const popup = document.getElementById('float-popup');
  if (popup && !popup.contains(e.target) && !e.target.closest('[onclick*="showFloatPopup"]')) {
    popup.style.display = 'none';
  }
});


/* ═════════ inline block boundary ═════════ */


// ═══════════════════════════════════════════════
//  STOCK VALUATION (cross-sectional)
// ═══════════════════════════════════════════════
let _valStockData = null;
let _valSecSort = 'pe';

async function _loadStockValStats() {
  if (_valStockData) return _valStockData;
  const r = await fetch('/api/stock-valuation-stats?t=' + Date.now());
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  _valStockData = await r.json();
  if (_valStockData.error) throw new Error(_valStockData.error);
  return _valStockData;
}

function _zColor(z) {
  if (z === null || z === undefined) return 'rgba(255,255,255,0.25)';
  if (z >  2) return '#dc503c';
  if (z >  1) return '#dca032';
  if (z > -1) return '#c8d0dc';
  if (z > -2) return '#96c850';
  return '#3ab464';
}
function _zLabel(z, isPE) {
  if (z === null || z === undefined) return '—';
  const s = (z >= 0 ? '+' : '') + z.toFixed(2) + 'σ';
  if (isPE) {
    if (z >  2) return s + ' แพงผิดปกติ';
    if (z >  1) return s + ' แพง';
    if (z > -1) return s + ' ปานกลาง';
    if (z > -2) return s + ' ถูก';
    return s + ' ถูกผิดปกติ';
  } else {
    if (z >  2) return s + ' Overvalued';
    if (z >  1) return s + ' สูง';
    if (z > -1) return s + ' ปานกลาง';
    if (z > -2) return s + ' Undervalued';
    return s + ' Undervalued มาก';
  }
}

function _sdBar(val, avg, std, min, max) {
  // draw a mini bar showing where val sits between avg-2σ and avg+2σ
  const lo = avg - 2*std, hi = avg + 2*std;
  const pct = Math.max(0, Math.min(100, (val - lo) / (hi - lo) * 100));
  const z = std ? (val - avg)/std : 0;
  const col = _zColor(z);
  return `<div style="position:relative;background:var(--bg-card2);border-radius:3px;height:5px;width:80px;display:inline-block;vertical-align:middle">
    <div style="position:absolute;left:50%;top:0;width:1px;height:100%;background:rgba(255,255,255,0.2)"></div>
    <div style="position:absolute;left:${pct}%;top:-1px;width:6px;height:7px;border-radius:2px;background:${col};transform:translateX(-50%)"></div>
  </div>`;
}

async function searchValStock(query) {
  query = (query || '').trim().toUpperCase();
  const el = document.getElementById('val-stock-result');
  if (!query) { el.innerHTML = ''; return; }

  el.innerHTML = '<span style="color:var(--muted);font-size:12px">กำลังโหลด...</span>';
  let data;
  try {
    data = await _loadStockValStats();
  } catch(e) {
    el.innerHTML = `<span style="color:var(--red)">โหลดข้อมูลไม่สำเร็จ: ${e.message}<br>ลอง restart Flask server แล้วกดค้นหาใหม่</span>`;
    return;
  }
  const stock = data.stocks.find(s => s.symbol === query || s.symbol === query + '.BK');
  if (!stock) {
    // fuzzy match
    const matches = data.stocks.filter(s =>
      s.symbol.includes(query) || (s.name||'').toUpperCase().includes(query)
    ).slice(0, 5);
    if (!matches.length) { el.innerHTML = `<span style="color:var(--muted)">ไม่พบหุ้น "${query}"</span>`; return; }
    el.innerHTML = `<div style="color:var(--muted);font-size:12px;margin-bottom:6px">พบ ${matches.length} รายการ:</div>` +
      matches.map(m => `<button onclick="document.getElementById('val-stock-input').value='${m.symbol}';searchValStock('${m.symbol}')"
        style="background:var(--bg-card2);border:1px solid var(--border);color:#c8d0dc;padding:4px 10px;border-radius:4px;cursor:pointer;margin:2px;font-size:12px">
        ${m.symbol} <span style="color:var(--muted)">${(m.name||'').slice(0,30)}</span>
      </button>`).join('');
    return;
  }

  const sec = data.sectors[stock.sector] || {};
  const mkt = data.market;

  function metricCard(label, val, mktDist, secDist, isPE) {
    if (!val) return `<div style="padding:12px;background:var(--bg-card2);border-radius:8px;min-width:160px">
      <div style="font-size:11px;color:var(--muted)">${label}</div>
      <div style="color:var(--muted)">—</div></div>`;
    const zMkt = mktDist && mktDist.std ? (val - mktDist.avg)/mktDist.std : null;
    const zSec = secDist && secDist.std ? (val - secDist.avg)/secDist.std : null;
    return `<div style="padding:14px;background:var(--bg-card2);border-radius:8px;min-width:180px">
      <div style="font-size:11px;color:var(--muted);margin-bottom:4px">${label}</div>
      <div style="font-size:22px;font-weight:700">${val.toFixed(2)}x</div>
      <div style="margin-top:8px">
        <div style="font-size:11px;color:var(--muted)">เทียบตลาด (${mktDist?mktDist.n:'?'} หุ้น)</div>
        <div style="font-size:13px;font-weight:600;color:${_zColor(zMkt)}">${_zLabel(zMkt, isPE)}</div>
        ${mktDist ? _sdBar(val, mktDist.avg, mktDist.std, mktDist.min, mktDist.max) : ''}
        <div style="font-size:10px;color:var(--muted);margin-top:2px">avg=${mktDist?mktDist.avg:'?'}x ±σ=${mktDist?mktDist.std:'?'}</div>
      </div>
      <div style="margin-top:10px">
        <div style="font-size:11px;color:var(--muted)">เทียบ ${stock.sector} (${secDist?secDist.n:'?'} หุ้น)</div>
        <div style="font-size:13px;font-weight:600;color:${_zColor(zSec)}">${_zLabel(zSec, isPE)}</div>
        ${secDist ? _sdBar(val, secDist.avg, secDist.std, secDist.min, secDist.max) : ''}
        <div style="font-size:10px;color:var(--muted);margin-top:2px">avg=${secDist?secDist.avg:'?'}x ±σ=${secDist?secDist.std:'?'}</div>
      </div>
    </div>`;
  }

  el.innerHTML = `
    <div style="font-weight:600;font-size:14px;margin-bottom:10px">
      ${stock.symbol} <span style="font-size:12px;color:var(--muted)">${stock.name||''}</span>
      <span style="font-size:11px;color:var(--muted);margin-left:8px">Sector: ${stock.sector}</span>
    </div>
    <div style="display:flex;gap:12px;flex-wrap:wrap">
      ${metricCard('P/E Ratio', stock.pe, mkt.pe, sec.pe, true)}
      ${metricCard('P/BV Ratio', stock.pbv, mkt.pbv, sec.pbv, false)}
    </div>`;
}

function setValSecSort(by, btn) {
  _valSecSort = by;
  document.querySelectorAll('#page-valuation .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderValSectorTable();
}

// ── SD Zone Browser ──────────────────────────────────────────
let _vsdMetric = 'pe';   // 'pe' | 'pbv'
let _vsdScope  = 'mkt';  // 'mkt' | 'sec'
let _vsdZone   = null;

function resetVsdZone() {
  _vsdZone = null;
  document.querySelectorAll('#page-valuation .card .filter-btn[onclick^="showVsdZone"]').forEach(b => b.classList.remove('active'));
  document.getElementById('vsd-result').innerHTML = '';
}

function setVsdMetric(m, btn) {
  _vsdMetric = m;
  document.getElementById('vsd-pe-btn').classList.toggle('active',  m === 'pe');
  document.getElementById('vsd-pbv-btn').classList.toggle('active', m === 'pbv');
  if (_vsdZone) showVsdZone(_vsdZone);
}
function setVsdScope(s, btn) {
  _vsdScope = s;
  document.getElementById('vsd-mkt-btn').classList.toggle('active', s === 'mkt');
  document.getElementById('vsd-sec-btn').classList.toggle('active', s === 'sec');
  if (_vsdZone) showVsdZone(_vsdZone);
}

async function showVsdZone(zone, btn) {
  _vsdZone = zone;
  // highlight active zone button
  document.querySelectorAll('#page-valuation .card .filter-btn[onclick^="showVsdZone"]').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  const el = document.getElementById('vsd-result');
  el.innerHTML = '<span style="color:var(--muted);font-size:12px">กำลังโหลด...</span>';

  let data;
  try { data = await _loadStockValStats(); }
  catch(e) { el.innerHTML = `<span style="color:var(--red)">${e.message}</span>`; return; }

  // zone ranges: zone key → [lo, hi) exclusive on hi
  const ranges = {
    '+3': [2,    Infinity],
    '+2': [1,    2],
    '+1': [0,    1],
    '-1': [-1,   0],
    '-2': [-2,  -1],
    '-3': [-Infinity, -2],
  };
  const [lo, hi] = ranges[zone] || [0, 1];

  const zoneColors = {
    '+3':'#dc503c', '+2':'#dca032', '+1':'#e0d060',
    '-1':'#96c850', '-2':'#3ab464', '-3':'#1a9455',
  };
  const zoneLabels = {
    '+3':'แพงผิดปกติมาก (> +2σ)', '+2':'แพงผิดปกติ (+1σ ถึง +2σ)',
    '+1':'แพงกว่าค่าเฉลี่ย (0 ถึง +1σ)',
    '-1':'ถูกกว่าค่าเฉลี่ย (-1σ ถึง 0)', '-2':'ถูกผิดปกติ (-2σ ถึง -1σ)',
    '-3':'ถูกมากผิดปกติ (< -2σ)',
  };

  const zField = `${_vsdMetric}_z_${_vsdScope}`;
  const valField = _vsdMetric;

  // filter & sort
  let stocks = data.stocks.filter(s => {
    const z = s[zField];
    return z !== null && z !== undefined && z >= lo && z < hi;
  });
  // sort: positive zones → highest z first (แพงสุด); negative zones → lowest z first (ถูกสุด)
  stocks.sort((a,b) => zone.startsWith('+')
    ? (b[zField] ?? 0) - (a[zField] ?? 0)
    : (a[zField] ?? 0) - (b[zField] ?? 0));

  if (!stocks.length) {
    el.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:8px 0">ไม่พบหุ้นใน zone นี้</div>`;
    return;
  }

  const scopeLabel = _vsdScope === 'mkt' ? 'เทียบตลาดรวม' : 'เทียบ Sector';
  const col = zoneColors[zone];

  // group by sector for easy reading
  const bySector = {};
  stocks.forEach(s => {
    const sec = s.sector || 'Other';
    (bySector[sec] = bySector[sec] || []).push(s);
  });

  // Stats header
  const mktDist = data.market[_vsdMetric];
  const distInfo = mktDist
    ? `avg=${mktDist.avg}x ±σ=${mktDist.std}x | zone นี้: ${lo === -Infinity ? '< ' + hi*-1 : lo >= 0 ? '> +'+lo+'σ' : lo+'σ ~ '+(hi > 0 ? '+'+hi : hi)+'σ'}`
    : '';

  el.innerHTML = `
    <div style="margin-bottom:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
      <span style="font-size:13px;font-weight:700;color:${col}">${zoneLabels[zone]}</span>
      <span style="font-size:11px;color:var(--muted)">${_vsdMetric.toUpperCase()} ${scopeLabel} · ${stocks.length} หุ้น</span>
    </div>
    ${Object.entries(bySector).sort((a,b) => b[1].length - a[1].length).map(([sec, ss]) => `
      <div style="margin-bottom:12px">
        <div style="font-size:11px;color:var(--muted);font-weight:600;margin-bottom:5px;
          border-bottom:1px solid rgba(255,255,255,0.06);padding-bottom:3px">
          ${sec} <span style="font-weight:400">(${ss.length})</span>
        </div>
        <div style="display:flex;flex-wrap:wrap;gap:5px">
          ${ss.map(s => {
            const z = s[zField];
            const v = s[valField];
            const zStr = z !== null ? (z >= 0 ? '+' : '') + z.toFixed(2) + 'σ' : '—';
            const tip = `${s.name||s.symbol} | ${_vsdMetric.toUpperCase()}=${v != null ? v.toFixed(1)+'x' : '—'} | z=${zStr}`.replace(/"/g,'&quot;');
            return `<button
              onclick="openChartModal('${s.symbol}')"
              title="${tip}"
              style="background:var(--bg-card2);border:1px solid ${col}30;border-radius:6px;
                     padding:5px 10px;cursor:pointer;font-size:12px;color:#c8d0dc;
                     display:flex;flex-direction:column;align-items:flex-start;gap:1px;
                     transition:border-color .15s;min-width:80px"
              onmouseover="this.style.borderColor='${col}';this.style.background='${col}18'"
              onmouseout="this.style.borderColor='${col}30';this.style.background='var(--bg-card2)'">
              <span style="font-weight:700;font-size:13px">${s.symbol}</span>
              <span style="color:${col};font-size:10px">${zStr}</span>
              <span style="color:var(--muted);font-size:10px">${v != null ? v.toFixed(1)+'x' : '—'}</span>
            </button>`;
          }).join('')}
        </div>
      </div>
    `).join('')}`;
}

async function openValSectorModal(secName) {
  const data = await _loadStockValStats();
  const secStats = data.sectors[secName] || {};
  const stocks = data.stocks
    .filter(s => s.sector === secName)
    .sort((a,b) => (a.pe_z_sec ?? 999) - (b.pe_z_sec ?? 999)); // cheapest first

  // patch the existing modal with valuation-focused content
  document.getElementById('modal-title').textContent = secName;
  document.getElementById('modal-abbr').textContent  = `${stocks.length} หุ้น`;

  // stats bar: sector PE/PBV summary
  const pe  = secStats.pe;
  const pbv = secStats.pbv;
  document.getElementById('modal-stats').innerHTML = [
    pe  ? `<div class="modal-stat"><div class="modal-stat-val">${pe.median.toFixed(1)}x</div><div class="modal-stat-lbl">PE Median</div></div>` : '',
    pe  ? `<div class="modal-stat"><div class="modal-stat-val">${pe.avg.toFixed(1)}±${pe.std.toFixed(1)}</div><div class="modal-stat-lbl">PE avg±σ</div></div>` : '',
    pbv ? `<div class="modal-stat"><div class="modal-stat-val">${pbv.median.toFixed(1)}x</div><div class="modal-stat-lbl">PBV Median</div></div>` : '',
    pbv ? `<div class="modal-stat"><div class="modal-stat-val">${pbv.avg.toFixed(1)}±${pbv.std.toFixed(1)}</div><div class="modal-stat-lbl">PBV avg±σ</div></div>` : '',
    `<div class="modal-stat"><div class="modal-stat-val">${stocks.length}</div><div class="modal-stat-lbl">หุ้น</div></div>`,
  ].join('');

  function zBadge(z) {
    if (z === null || z === undefined) return '<span style="color:var(--muted)">—</span>';
    const col = _zColor(z);
    const s = (z >= 0 ? '+' : '') + z.toFixed(2) + 'σ';
    return `<span style="color:${col};font-weight:600;font-size:11px">${s}</span>`;
  }
  function valCell(val, z) {
    if (!val) return '<td class="r" style="color:var(--muted)">—</td>';
    return `<td class="r">${val.toFixed(1)}<br>${zBadge(z)}</td>`;
  }

  // Replace modal table with valuation columns
  const modalBody = document.querySelector('#sector-modal .modal-body');
  modalBody.innerHTML = `
    <table class="tbl tbl-clickable" style="width:100%">
      <thead><tr>
        <th${colTip('symbol')}>Symbol</th>
        <th${colTip('name')}>ชื่อ</th>
        <th class="r" style="color:#dca032" title="P/E ของหุ้น เทียบกับค่าเฉลี่ย sector — ต่ำกว่า -1σ = ถูกผิดปกติ, สูงกว่า +1σ = แพงผิดปกติ">PE<br><span style="font-size:9px;color:var(--muted)">vs sector</span></th>
        <th class="r" style="color:#5ab4ff" title="P/BV ของหุ้น เทียบกับค่าเฉลี่ย sector — ต่ำกว่า -1σ = ถูกผิดปกติ, สูงกว่า +1σ = แพงผิดปกติ">PBV<br><span style="font-size:9px;color:var(--muted)">vs sector</span></th>
        <th class="r"${colTip('rs_score')}>RS</th>
        <th class="r"${colTip('price')}>ราคา</th>
        <th class="r"${colTip('ret_1d')}>1D%</th>
        <th class="r"${colTip('ret_1m')}>1M%</th>
      </tr></thead>
      <tbody>
      ${stocks.map(s => {
        const main = (typeof DATA !== 'undefined') ? DATA.stocks.find(x => x.symbol === s.symbol) : null;
        const rs    = main?.rs_score;
        const price = main?.price;
        const r1d   = main?.ret_1d;
        const r1m   = main?.ret_1m;
        const rsCol = rs >= 80 ? 'color:#3ab464' : rs >= 60 ? 'color:#96c850' : rs ? 'color:var(--muted)' : '';
        const r1dCol = r1d > 0 ? 'color:#3ab464' : r1d < 0 ? 'color:#dc503c' : '';
        const r1mCol = r1m > 0 ? 'color:#3ab464' : r1m < 0 ? 'color:#dc503c' : '';
        return `<tr>
          <td><strong class="sym-link" onclick="closeModal();openChartModal('${s.symbol}')">${s.symbol}</strong></td>
          <td style="font-size:11px;color:var(--text2);max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.name||''}</td>
          ${valCell(s.pe, s.pe_z_sec)}
          ${valCell(s.pbv, s.pbv_z_sec)}
          <td class="r" style="${rsCol}">${rs ?? '—'}</td>
          <td class="r">${price != null ? price.toFixed(2) : '—'}</td>
          <td class="r" style="${r1dCol}">${r1d != null ? (r1d>0?'+':'')+r1d.toFixed(1)+'%' : '—'}</td>
          <td class="r" style="${r1mCol}">${r1m != null ? (r1m>0?'+':'')+r1m.toFixed(1)+'%' : '—'}</td>
        </tr>`;
      }).join('')}
      </tbody>
    </table>
    <div style="font-size:10px;color:var(--muted);padding:8px 0">
      เรียงตาม PE z-score (ถูกสุดก่อน) · z-score เทียบกับ sector เดียวกัน
    </div>`;

  document.getElementById('sector-modal').classList.add('open');
  document.body.style.overflow = 'hidden';
}

async function renderValSectorTable() {
  const el = document.getElementById('val-sector-table');
  if (!el) return;
  el.innerHTML = '<span style="color:var(--muted);font-size:12px">กำลังโหลด...</span>';
  let data;
  try {
    data = await _loadStockValStats();
  } catch(e) {
    el.innerHTML = `<span style="color:var(--red)">โหลดไม่สำเร็จ: ${e.message} — กรุณา restart Flask</span>`;
    return;
  }

  let rows = Object.entries(data.sectors).map(([sec, v]) => ({ sec, ...v }));
  if (_valSecSort === 'pe')  rows.sort((a,b) => (a.pe?.avg||999) - (b.pe?.avg||999));
  if (_valSecSort === 'pbv') rows.sort((a,b) => (a.pbv?.avg||999) - (b.pbv?.avg||999));
  if (_valSecSort === 'n')   rows.sort((a,b) => b.n_stocks - a.n_stocks);

  const mkt = data.market;

  function bandCell(dist) {
    if (!dist) return '<td colspan="4" style="color:var(--muted);text-align:center">—</td>';
    const {avg, std, median, n} = dist;
    const b = dist.bands;
    const zMkt_pe = mkt.pe && mkt.pe.std ? (avg - mkt.pe.avg)/mkt.pe.std : null;
    return `
      <td style="text-align:right">${avg.toFixed(1)}<span style="color:var(--muted);font-size:10px"> ±${std.toFixed(1)}</span></td>
      <td style="text-align:right;color:#dca032">${b['+1σ'].toFixed(1)}</td>
      <td style="text-align:right;color:#96c850">${Math.max(0,b['-1σ']).toFixed(1)}</td>
      <td style="text-align:right;color:#5ab4ff;font-size:10px">${median.toFixed(1)}</td>`;
  }

  el.innerHTML = `
  <table style="width:100%;border-collapse:collapse;font-size:12px">
    <thead>
      <tr style="border-bottom:1px solid var(--border);color:var(--muted);font-size:11px">
        <th style="text-align:left;padding:5px 8px">Sector</th>
        <th style="text-align:center;padding:5px" colspan="4">P/E Ratio</th>
        <th style="text-align:center;padding:5px" colspan="4">P/BV Ratio</th>
        <th style="text-align:right;padding:5px">หุ้น</th>
      </tr>
      <tr style="border-bottom:1px solid var(--border);color:var(--muted);font-size:10px">
        <th></th>
        <th title="ค่าเฉลี่ย ± ส่วนเบี่ยงเบนมาตรฐาน (σ) ของหุ้นทุกตัวใน sector">avg±σ</th><th style="color:#dca032" title="เพดานบน = avg + 1σ — หุ้นที่สูงกว่านี้ถือว่าแพงผิดปกติเทียบกลุ่ม">+1σ</th><th style="color:#96c850" title="พื้นล่าง = avg − 1σ — หุ้นที่ต่ำกว่านี้ถือว่าถูกผิดปกติเทียบกลุ่ม">-1σ</th><th style="color:#5ab4ff" title="ค่ามัธยฐาน (median) — ตัวแทนกลางของกลุ่ม ทนต่อ outlier กว่า avg">med</th>
        <th title="ค่าเฉลี่ย ± ส่วนเบี่ยงเบนมาตรฐาน (σ) ของหุ้นทุกตัวใน sector">avg±σ</th><th style="color:#dca032" title="เพดานบน = avg + 1σ — หุ้นที่สูงกว่านี้ถือว่าแพงผิดปกติเทียบกลุ่ม">+1σ</th><th style="color:#96c850" title="พื้นล่าง = avg − 1σ — หุ้นที่ต่ำกว่านี้ถือว่าถูกผิดปกติเทียบกลุ่ม">-1σ</th><th style="color:#5ab4ff" title="ค่ามัธยฐาน (median) — ตัวแทนกลางของกลุ่ม ทนต่อ outlier กว่า avg">med</th>
        <th></th>
      </tr>
    </thead>
    <tbody>
    ${rows.map((r,i) => `
      <tr style="border-bottom:1px solid rgba(255,255,255,0.04);${i%2?'background:rgba(255,255,255,0.02)':''}">
        <td style="padding:5px 8px">
          <span onclick="openValSectorModal('${r.sec.replace(/'/g,"\\'")}',this)"
            style="font-weight:500;cursor:pointer;color:#c8d0dc;border-bottom:1px dashed rgba(90,180,255,0.4)"
            onmouseover="this.style.color='#5ab4ff'" onmouseout="this.style.color='#c8d0dc'">
            ${r.sec}
          </span>
        </td>
        ${bandCell(r.pe)}
        ${bandCell(r.pbv)}
        <td style="text-align:right;color:var(--muted);padding:5px 8px">${r.n_stocks}</td>
      </tr>`).join('')}
    </tbody>
    <tfoot>
      <tr style="border-top:1px solid var(--border);font-weight:600;color:#5ab4ff">
        <td style="padding:6px 8px">ตลาดรวม</td>
        ${mkt.pe ? `<td style="text-align:right">${mkt.pe.avg.toFixed(1)}<span style="color:var(--muted);font-size:10px"> ±${mkt.pe.std.toFixed(1)}</span></td>
          <td style="text-align:right;color:#dca032">${mkt.pe.bands['+1σ'].toFixed(1)}</td>
          <td style="text-align:right;color:#96c850">${Math.max(0,mkt.pe.bands['-1σ']).toFixed(1)}</td>
          <td style="text-align:right;color:#5ab4ff;font-size:10px">${mkt.pe.median.toFixed(1)}</td>` : '<td colspan="4">—</td>'}
        ${mkt.pbv ? `<td style="text-align:right">${mkt.pbv.avg.toFixed(1)}<span style="color:var(--muted);font-size:10px"> ±${mkt.pbv.std.toFixed(1)}</span></td>
          <td style="text-align:right;color:#dca032">${mkt.pbv.bands['+1σ'].toFixed(1)}</td>
          <td style="text-align:right;color:#96c850">${Math.max(0,mkt.pbv.bands['-1σ']).toFixed(1)}</td>
          <td style="text-align:right;color:#5ab4ff;font-size:10px">${mkt.pbv.median.toFixed(1)}</td>` : '<td colspan="4">—</td>'}
        <td style="text-align:right;padding:6px 8px">${rows.reduce((a,r)=>a+r.n_stocks,0)}</td>
      </tr>
    </tfoot>
  </table>`;
}

// ═══════════════════════════════════════════════
//  VALUATION PAGE (INDEX P/E & PBV)
// ═══════════════════════════════════════════════
let _valData = null;
let _valPeriod = 'ALL';

// ── Global tooltip popup ─────────────────────
(function(){
  const pop = document.createElement('div');
  pop.id = '_vtip-popup';
  document.body.appendChild(pop);
  document.addEventListener('mousemove', e => {
    const el = e.target.closest('[data-vtip]');
    if (!el) { pop.style.display='none'; return; }
    pop.innerHTML = el.dataset.vtip;
    pop.style.display = 'block';
    const vw = window.innerWidth, vh = window.innerHeight;
    const pw = pop.offsetWidth + 16, ph = pop.offsetHeight + 16;
    let x = e.clientX + 14, y = e.clientY + 14;
    if (x + pw > vw) x = e.clientX - pw + 2;
    if (y + ph > vh) y = e.clientY - ph + 2;
    pop.style.left = x + 'px'; pop.style.top = y + 'px';
  });
  document.addEventListener('mouseleave', () => pop.style.display='none', true);
})();

async function refreshMarketStats() {
  const btn = document.getElementById('val-refresh-btn');
  const status = document.getElementById('val-refresh-status');
  btn.disabled = true;
  btn.textContent = '⟳ กำลังอัพเดท...';
  status.textContent = '';
  status.style.color = 'var(--muted)';

  try {
    const r = await fetch('/api/refresh-market-stats', { method: 'POST' });
    const d = await r.json();

    if (!d.ok) {
      status.textContent = '✗ ' + d.error;
      status.style.color = 'var(--red)';
      btn.disabled = false;
      btn.innerHTML = '⟳ อัพเดทข้อมูล P/E &amp; P/BV';
      return;
    }

    if (!d.new_data) {
      status.textContent = `✓ ข้อมูลเป็นปัจจุบันแล้ว (ล่าสุด: ${d.new_latest})`;
      status.style.color = '#96c850';
    } else {
      status.textContent = `✓ อัพเดทสำเร็จ! ${d.old_latest} → ${d.new_latest} | P/E=${d.pe_current}x (${d.pe_zscore > 0 ? '+' : ''}${d.pe_zscore}σ) | P/BV=${d.pbv_current}x`;
      status.style.color = '#3ab464';
      // reload chart data
      _valData = null;
      renderValuation();
    }
  } catch(e) {
    status.textContent = '✗ ไม่สามารถเชื่อมต่อ server: ' + e.message;
    status.style.color = 'var(--red)';
  }

  btn.disabled = false;
  btn.innerHTML = '⟳ อัพเดทข้อมูล P/E &amp; P/BV';
}

async function loadValuationPage() {
  // Load both in parallel
  const [_1, _2] = await Promise.allSettled([
    (async () => {
      if (_valData) { renderValuation(); return; }
      try {
        const r = await fetch('/api/market-stats?t=' + Date.now());
        if (!r.ok) throw new Error('ไม่พบข้อมูล');
        _valData = await r.json();
        renderValuation();
      } catch(e) {
        document.getElementById('val-summary').innerHTML =
          '<div style="color:var(--red);padding:16px">ไม่พบ set_market_stats.json — รัน import_market_stats.py ก่อน</div>';
      }
    })(),
    renderValSectorTable(),
  ]);
}

function setValPeriod(p, btn) {
  _valPeriod = p;
  document.querySelectorAll('#page-valuation .filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderValuation();
}

function filterByPeriod(dates, vals, period) {
  if (period === 'ALL') return { dates, vals };
  const years = { '1Y':1, '2Y':2, '3Y':3, '5Y':5, '10Y':10, '20Y':20, '30Y':30 }[period] || 999;
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - years);
  const cutStr = cutoff.toISOString().slice(0,7);
  const idx = dates.findIndex(d => d >= cutStr);
  if (idx < 0) return { dates, vals };
  return { dates: dates.slice(idx), vals: vals.slice(idx) };
}

const VAL_PERIOD_LABEL = { ALL:'ตลอดกาล', '30Y':'30 ปี', '20Y':'20 ปี', '10Y':'10 ปี', '5Y':'5 ปี', '3Y':'3 ปี', '2Y':'2 ปี', '1Y':'1 ปี' };

// ตัด leading/trailing null ออกก่อน filter period (ซีรีส์ mai เริ่มมีข้อมูลช้ากว่า SET)
function _trimNulls(dates, vals) {
  let lo = 0, hi = vals.length - 1;
  while (lo <= hi && vals[lo] === null) lo++;
  while (hi >= lo && vals[hi] === null) hi--;
  return { dates: dates.slice(lo, hi + 1), vals: vals.slice(lo, hi + 1) };
}

// คำนวณ avg/std/percentile/zscore/bands จากช่วงเวลาที่เลือกจริง (ไม่ใช่ตลอดกาลเสมอ)
// current = ค่าล่าสุด (จุดท้ายของ array ที่กรองแล้ว) ส่วน avg/std/percentile scope ตามช่วงที่เลือก
function _calcStatsClient(vals) {
  const v = vals.filter(x => x !== null && x !== undefined && !Number.isNaN(x));
  if (!v.length) return {};
  const arr = [...v].sort((a, b) => a - b);
  const avg = v.reduce((a, b) => a + b, 0) / v.length;
  const variance = v.reduce((a, b) => a + (b - avg) ** 2, 0) / v.length;
  const std = Math.sqrt(variance);
  const current = v[v.length - 1];
  const round2 = x => Math.round(x * 100) / 100;
  const pct = Math.round((arr.filter(x => x <= current).length / arr.length) * 1000) / 10;
  return {
    current: round2(current), min: round2(arr[0]), max: round2(arr[arr.length - 1]),
    avg: round2(avg), median: round2(arr[Math.floor(arr.length / 2)]), std: round2(std),
    zscore: std ? round2((current - avg) / std) : 0, percentile: pct,
    bands: {
      '+3σ': round2(avg + 3 * std), '+2σ': round2(avg + 2 * std), '+1σ': round2(avg + std),
      '-1σ': round2(avg - std), '-2σ': round2(avg - 2 * std), '-3σ': round2(avg - 3 * std),
    },
  };
}

// คืน stats ตาม _valPeriod ที่เลือก — ใช้ของ backend (ตลอดกาล) เมื่อเลือก ALL,
// คำนวณสดฝั่ง client เมื่อเลือกช่วงสั้นกว่า เพื่อให้ avg/σ/percentile scope ตามช่วงจริง
function _periodStats(fullStats, filteredVals) {
  if (_valPeriod === 'ALL') return fullStats;
  return _calcStatsClient(filteredVals);
}

function _valZColor(z) {
  if (z == null) return 'var(--text2)';
  if (z >  2) return '#dc503c';
  if (z >  1) return '#dca032';
  if (z > -1) return '#c8d0dc';
  if (z > -2) return '#96c850';
  return '#3ab464';
}

// ตารางเปรียบเทียบ avg/z-score ทุกช่วงเวลา (Max→1Y) ของ P/E & P/BV ทั้ง SET และ mai
function renderValPeriodTable(seriesDefs) {
  const el = document.getElementById('val-period-table');
  if (!el) return;
  const periods = ['ALL', '30Y', '20Y', '10Y', '5Y', '3Y', '2Y', '1Y'];

  let html = `<thead><tr><th>ช่วงเวลา</th>` + seriesDefs.map(s =>
    `<th class="r" colspan="2">${s.label}<div style="font-weight:400;font-size:10px;color:var(--text2)">ปัจจุบัน ${s.current ?? '—'}x</div></th>`
  ).join('') + `</tr><tr><th style="font-size:10px;color:var(--text2)"></th>` + seriesDefs.map(() =>
    `<th class="r" style="font-size:10px;color:var(--text2)">avg</th><th class="r" style="font-size:10px;color:var(--text2)">z-score</th>`
  ).join('') + `</tr></thead><tbody>`;

  for (const p of periods) {
    html += `<tr><td style="font-weight:600">${p === 'ALL' ? 'Max' : p} <span style="color:var(--text2);font-weight:400;font-size:10px">(${VAL_PERIOD_LABEL[p]})</span></td>`;
    for (const s of seriesDefs) {
      const f = filterByPeriod(s.dates, s.vals, p);
      const st = p === 'ALL' ? (s.full || _calcStatsClient(f.vals)) : _calcStatsClient(f.vals);
      if (st.avg == null) { html += `<td class="r">—</td><td class="r">—</td>`; continue; }
      const z = st.zscore;
      html += `<td class="r">${st.avg}x</td>` +
        `<td class="r" style="color:${_valZColor(z)};font-weight:600" title="±1σ = ${st.std}x · ต่ำสุด ${st.min}x · สูงสุด ${st.max}x · percentile ${st.percentile}%">${z > 0 ? '+' : ''}${z}</td>`;
    }
    html += `</tr>`;
  }
  el.innerHTML = html + `</tbody>`;
}

function valZoneColor(v, thresholds) {
  const [t1, t2, t3] = thresholds;
  if (v === null) return 'rgba(0,0,0,0)';
  if (v < t1) return 'rgba(58,180,100,0.15)';
  if (v < t2) return 'rgba(150,200,80,0.12)';
  if (v < t3) return 'rgba(220,160,50,0.13)';
  return 'rgba(220,80,60,0.14)';
}

function valZoneLabel(v, thresholds, labels) {
  if (v === null) return '—';
  const [t1, t2, t3] = thresholds;
  if (v < t1) return labels[0];
  if (v < t2) return labels[1];
  if (v < t3) return labels[2];
  return labels[3];
}

function valZoneStyle(v, thresholds) {
  if (v === null) return '';
  const [t1, t2, t3] = thresholds;
  if (v < t1) return 'color:#3ab464;font-weight:700';
  if (v < t2) return 'color:#96c850;font-weight:700';
  if (v < t3) return 'color:#dca032;font-weight:700';
  return 'color:#dc503c;font-weight:700';
}

function drawValChart(canvasId, dates, vals, thresholds, stats) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth || canvas.offsetWidth || 700;
  const H = canvas.clientHeight || 260;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const pad = { t: 16, r: 72, b: 32, l: 44 };
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;

  const avg = stats.avg, std = stats.std;
  const bands = stats.bands || {};
  const current = stats.current;

  const validVals = vals.filter(v => v !== null);
  const sdMax = avg + 3.2 * std;
  const sdMin = Math.max(0, avg - 3.2 * std);
  const dataMax = Math.max(...validVals);
  const dataMin = Math.max(0, Math.min(...validVals));
  const minV = Math.min(dataMin * 0.9, sdMin);
  const maxV = Math.max(dataMax * 1.02, sdMax);
  const scaleY = v => pad.t + ch - ((v - minV) / (maxV - minV)) * ch;
  const scaleX = i => pad.l + (i / (dates.length - 1)) * cw;

  // SD band fills (between consecutive bands)
  const sdLevels = [
    avg + 3*std, avg + 2*std, avg + std,
    avg,
    avg - std, avg - 2*std, Math.max(0, avg - 3*std)
  ];
  const sdFills = [
    'rgba(200,60,40,0.07)',   // +2σ to +3σ
    'rgba(220,130,40,0.07)',  // +1σ to +2σ
    'rgba(200,200,80,0.05)',  // avg to +1σ
    'rgba(200,200,80,0.05)',  // -1σ to avg
    'rgba(80,180,100,0.07)',  // -2σ to -1σ
    'rgba(40,160,80,0.08)',   // -3σ to -2σ
  ];
  for (let i = 0; i < sdFills.length; i++) {
    const hi = Math.min(sdLevels[i], maxV);
    const lo = Math.max(sdLevels[i+1], minV);
    if (hi <= lo) continue;
    ctx.fillStyle = sdFills[i];
    ctx.fillRect(pad.l, scaleY(hi), cw, scaleY(lo) - scaleY(hi));
  }

  // SD lines
  const sdLines = [
    { v: avg + 3*std, label:'+3σ', color:'rgba(200,60,40,0.7)',   dash:[3,4] },
    { v: avg + 2*std, label:'+2σ', color:'rgba(220,130,40,0.8)',  dash:[5,3] },
    { v: avg + std,   label:'+1σ', color:'rgba(220,200,80,0.7)',  dash:[6,3] },
    { v: avg,         label:'avg', color:'rgba(255,200,80,0.85)', dash:[7,3] },
    { v: avg - std,   label:'-1σ', color:'rgba(130,210,100,0.7)', dash:[6,3] },
    { v: avg - 2*std, label:'-2σ', color:'rgba(60,180,80,0.7)',   dash:[5,3] },
    { v: avg - 3*std, label:'-3σ', color:'rgba(40,140,60,0.7)',   dash:[3,4] },
  ];
  ctx.font = '9px sans-serif';
  ctx.textAlign = 'left';
  for (const sd of sdLines) {
    if (sd.v < minV || sd.v > maxV) continue;
    const y = scaleY(sd.v);
    ctx.strokeStyle = sd.color;
    ctx.setLineDash(sd.dash);
    ctx.lineWidth = sd.label === 'avg' ? 1.5 : 1;
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(pad.l + cw, y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = sd.color;
    ctx.fillText(`${sd.label} ${sd.v.toFixed(1)}`, pad.l + cw + 4, y + 3.5);
  }

  // Price line
  ctx.strokeStyle = '#5ab4ff';
  ctx.lineWidth = 1.8;
  ctx.beginPath();
  let first = true;
  for (let i = 0; i < vals.length; i++) {
    if (vals[i] === null) { first = true; continue; }
    const x = scaleX(i), y = scaleY(vals[i]);
    if (first) { ctx.moveTo(x, y); first = false; }
    else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // X axis labels
  ctx.fillStyle = 'rgba(255,255,255,0.4)';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(dates.length / 8));
  for (let i = 0; i < dates.length; i += step) {
    ctx.fillText(dates[i].slice(0,4), scaleX(i), H - 8);
  }

  // Y axis labels
  ctx.textAlign = 'right';
  const yTicks = 5;
  for (let i = 0; i <= yTicks; i++) {
    const v = minV + (maxV - minV) * i / yTicks;
    ctx.fillStyle = 'rgba(255,255,255,0.35)';
    ctx.fillText(v.toFixed(1), pad.l - 4, scaleY(v) + 4);
  }

  // Current value dot + label
  const lastVal = vals[vals.length - 1];
  if (lastVal !== null) {
    const x = scaleX(vals.length - 1), y = scaleY(lastVal);
    ctx.fillStyle = '#fff';
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fill();
  }

  // Store for hover
  canvas._dates = dates; canvas._vals = vals;
  canvas._pad = pad; canvas._cw = cw; canvas._len = dates.length;
  canvas._W = W; canvas._H = H; canvas._minV = minV; canvas._maxV = maxV;
}

function _valZoneNameFull(v, thresholds, isPE) {
  if (v === null) return '';
  const [t1, t2, t3] = thresholds;
  if (isPE) {
    if (v < t1) return '<span style="color:#3ab464">Cheap</span> — ถูกมาก โอกาสซื้อ';
    if (v < t2) return '<span style="color:#96c850">Fair</span> — ราคายุติธรรม';
    if (v < t3) return '<span style="color:#dca032">Slightly High</span> — ค่อนข้างแพง';
    return '<span style="color:#dc503c">Expensive</span> — แพงมาก ระวัง';
  } else {
    if (v < t1) return '<span style="color:#3ab464">Cheap</span> — ถูกกว่า Book Value';
    if (v < t2) return '<span style="color:#96c850">Fair</span> — สมเหตุสมผล';
    if (v < t3) return '<span style="color:#dca032">Slightly High</span> — ค่อนข้างแพง';
    return '<span style="color:#dc503c">Expensive</span> — แพงมาก';
  }
}

function setupValHover(canvasId, thresholds, avg, isPE, std) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || canvas._hoverSet) return;
  canvas._hoverSet = true;
  canvas._thresholds = thresholds;
  canvas._avg = avg;
  canvas._std = std || 1;
  canvas._isPE = isPE;

  const tip = document.createElement('div');
  tip.style.cssText = [
    'position:absolute','background:#1a2030','border:1px solid #3a4a5a',
    'padding:8px 12px','border-radius:8px','font-size:12px','line-height:1.7',
    'pointer-events:none','display:none','z-index:99','min-width:180px',
    'box-shadow:0 4px 16px rgba(0,0,0,0.5)'
  ].join(';');
  canvas.parentElement.style.position = 'relative';
  canvas.parentElement.appendChild(tip);

  canvas.addEventListener('mousemove', e => {
    if (!canvas._dates) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const idx = Math.round((mx - canvas._pad.l) / canvas._cw * (canvas._len - 1));
    if (idx < 0 || idx >= canvas._len) { tip.style.display = 'none'; return; }
    const val = canvas._vals[idx];
    if (val === null) { tip.style.display = 'none'; return; }
    const diff = val - canvas._avg;
    const zscore = canvas._std ? (diff / canvas._std) : 0;
    const diffStr = (diff >= 0 ? '+' : '') + diff.toFixed(2) + 'x';
    const zStr = (zscore >= 0 ? '+' : '') + zscore.toFixed(2) + 'σ';
    const zColor = Math.abs(zscore) > 2 ? (zscore>0?'#dc503c':'#3ab464')
                 : Math.abs(zscore) > 1 ? (zscore>0?'#dca032':'#96c850')
                 : 'rgba(255,255,255,0.6)';
    tip.innerHTML =
      `<b style="color:#e8eef5">${canvas._dates[idx]}</b><br>` +
      `<span style="font-size:15px;font-weight:700">${val.toFixed(2)}x</span>` +
      `<span style="font-size:12px;color:${zColor};margin-left:8px">${zStr}</span><br>` +
      `${_valZoneNameFull(val, canvas._thresholds, canvas._isPE)}<br>` +
      `<span style="color:rgba(255,255,255,0.4);font-size:11px">vs ค่าเฉลี่ย ${canvas._avg}x: </span>` +
      `<span style="color:${zColor};font-size:11px">${diffStr}</span>`;
    tip.style.display = 'block';
    const tipW = 200;
    const tipX = mx + 14 + tipW > canvas._W ? mx - tipW - 4 : mx + 14;
    tip.style.left = tipX + 'px';
    tip.style.top  = Math.max(4, e.clientY - rect.top - 60) + 'px';
  });
  canvas.addEventListener('mouseleave', () => tip.style.display = 'none');
}

function renderValZoneFreq(containerId, vals, thresholds, labels, isPE) {
  const total = vals.filter(v => v !== null).length;
  if (!total) return;
  const counts = [0,0,0,0];
  for (const v of vals) {
    if (v === null) continue;
    if      (v < thresholds[0]) counts[0]++;
    else if (v < thresholds[1]) counts[1]++;
    else if (v < thresholds[2]) counts[2]++;
    else counts[3]++;
  }
  const colors = ['#3ab464','#96c850','#dca032','#dc503c'];
  const el = document.getElementById(containerId);
  if (!el) return;
  const thStr = isPE
    ? ['< 10x','10–15x','15–20x','> 20x']
    : ['< 1x', '1–1.5x','1.5–2x','> 2x'];
  const freqTips = counts.map((c, i) => {
    const pct = (c/total*100).toFixed(1);
    const rarity = c/total < 0.08 ? '⚡ หายากมาก — โอกาสพิเศษเมื่อเกิดขึ้น'
                 : c/total < 0.20 ? 'ค่อนข้างหายาก'
                 : c/total < 0.40 ? 'เกิดขึ้นพอสมควร'
                 : 'เกิดขึ้นบ่อย — เป็น zone ปกติของตลาด';
    return _zoneTip(labels[i], isPE) +
      `<hr><div class='tip-row'><span class='tip-label'>ความถี่</span>` +
      `<span class='tip-val'>${pct}% (${c}/${total} เดือน)</span></div>` +
      `<div style='margin-top:4px;font-size:11px;color:#96c850'>${rarity}</div>`;
  });
  el.innerHTML = counts.map((c, i) => `
    <div style="text-align:center;min-width:130px;cursor:help"
      data-vtip="${freqTips[i].replace(/"/g,'&quot;')}">
      <div style="font-size:22px;font-weight:700;color:${colors[i]}">${(c/total*100).toFixed(1)}%</div>
      <div style="font-size:12px;color:${colors[i]};font-weight:600">${labels[i]}</div>
      <div style="font-size:10px;color:var(--muted)">${thStr[i]} · ${c} เดือน</div>
      <div style="margin-top:4px;background:var(--bg-card2);border-radius:3px;height:4px">
        <div style="background:${colors[i]};opacity:0.6;height:100%;border-radius:3px;width:${Math.min(c/total*100,100)}%"></div>
      </div>
    </div>
  `).join('');
}

function _vtip(key, tips) {
  const t = tips[key] || '';
  return `<span class="val-tip" data-vtip="${t.replace(/"/g,'&quot;')}">ⓘ</span>`;
}

function _pctTip(pct) {
  let band, color, advice;
  if (pct < 30) { band='ถูกกว่าค่าเฉลี่ยมาก'; color='#3ab464'; advice='โอกาสซื้อระยะยาวที่หายาก'; }
  else if (pct < 60) { band='ปานกลาง สมเหตุสมผล'; color='#96c850'; advice='ราคายุติธรรมตามประวัติศาสตร์'; }
  else if (pct < 80) { band='แพงกว่าค่าเฉลี่ย'; color='#dca032'; advice='ควรระวัง ผลตอบแทนระยะยาวอาจต่ำกว่าปกติ'; }
  else { band='แพงมากในประวัติศาสตร์'; color='#dc503c'; advice='ความเสี่ยงสูง มักนำหน้าช่วงปรับฐาน'; }
  return `<div class='tip-head'>Percentile — ตำแหน่งในประวัติศาสตร์</div>` +
    `<b style='color:${color}'>${pct}%</b> — ${band}<hr>` +
    `<b>ความหมาย:</b> ใน 100 เดือนของประวัติศาสตร์<br>มี ${pct} เดือนที่ค่าต่ำกว่าปัจจุบัน<br>` +
    `→ ตลาดถูกกว่านี้มาแล้ว <b>${pct}%</b> ของเวลา<hr>` +
    `<div class='tip-row'><span class='tip-label'>0–30%</span><span class='tip-zone-cheap'>ถูกมาก (โอกาสซื้อ)</span></div>` +
    `<div class='tip-row'><span class='tip-label'>30–60%</span><span class='tip-zone-fair'>ปานกลาง</span></div>` +
    `<div class='tip-row'><span class='tip-label'>60–80%</span><span class='tip-zone-high'>แพงกว่าค่าเฉลี่ย</span></div>` +
    `<div class='tip-row'><span class='tip-label'>80–100%</span><span class='tip-zone-exp'>แพงมาก (เสี่ยงสูง)</span></div><hr>` +
    `<b>สรุป:</b> ${advice}`;
}

function _zoneTip(zone, isPE) {
  const tips = isPE ? {
    'Cheap':         { c:'#3ab464', desc:'P/E ต่ำกว่า 10x', detail:'ตลาดซื้อขายถูกมาก กำไรสูงเทียบราคา<br>เกิดเฉพาะช่วงวิกฤตหนัก (เช่น ปี 1997, 2008)<br><b>โอกาสทำกำไรระยะยาวสูงมาก</b>' },
    'Fair':          { c:'#96c850', desc:'P/E 10–15x',       detail:'ราคาสมเหตุสมผลตามมาตรฐาน SET<br>เป็น Zone ที่ตลาดอยู่บ่อยที่สุดในประวัติศาสตร์<br><b>เหมาะสำหรับลงทุนสะสมระยะยาว</b>' },
    'Slightly High': { c:'#dca032', desc:'P/E 15–20x',       detail:'ตลาดแพงกว่าค่าเฉลี่ยประวัติศาสตร์ (13.92x)<br>อาจอยู่ได้อีก 1-2 ปี หรือปรับฐานทุกเมื่อ<br><b>ลงทุนได้ แต่ควรเลือกหุ้นรายตัวมากกว่า DCA</b>' },
    'Expensive':     { c:'#dc503c', desc:'P/E > 20x',        detail:'ตลาดแพงมากในเชิงประวัติศาสตร์<br>มักเกิดช่วงฟองสบู่ (เช่น ปี 1993: P/E = 41x)<br><b>ความเสี่ยงสูง ควรระวังและลดการเปิดรับความเสี่ยง</b>' },
  } : {
    'Cheap':         { c:'#3ab464', desc:'P/BV ต่ำกว่า 1x',   detail:'ซื้อหุ้นได้ถูกกว่ามูลค่าทรัพย์สินสุทธิ<br>เกิดเฉพาะวิกฤตหนักมาก (เช่น ปี 1997–1998)<br><b>โอกาสทองที่หายากมากในประวัติศาสตร์</b>' },
    'Fair':          { c:'#96c850', desc:'P/BV 1–1.5x',       detail:'ราคาสมเหตุสมผล ตลาดซื้อขายใกล้มูลค่าทางบัญชี<br><b>เหมาะสำหรับลงทุนสะสมระยะยาว</b>' },
    'Slightly High': { c:'#dca032', desc:'P/BV 1.5–2x',       detail:'ตลาดมีพรีเมี่ยมเหนือมูลค่าทางบัญชีพอสมควร<br>ยังยอมรับได้ถ้ากำไรบริษัทยังเติบโต<br><b>เลือกหุ้น ROE สูงเพื่อ justify ราคา</b>' },
    'Expensive':     { c:'#dc503c', desc:'P/BV > 2x',         detail:'ตลาดซื้อขายแพงกว่ามูลค่าทางบัญชีมาก<br>ต้องการการเติบโตของกำไรสูงมากเพื่อ justify ราคา<br><b>ความเสี่ยงสูง ควรระวัง</b>' },
  };
  const t = tips[zone];
  if (!t) return '';
  return `<div class='tip-head' style='color:${t.c}'>${zone}</div>` +
    `<b>${t.desc}</b><hr>${t.detail}`;
}

function renderValuation() {
  if (!_valData) return;
  const pe  = _valData.pe;
  const pbv = _valData.pbv;

  const peF  = filterByPeriod(pe.dates,  pe.series['SET'],  _valPeriod);
  const pbvF = filterByPeriod(pbv.dates, pbv.series['SET'], _valPeriod);

  const peStats  = _periodStats(pe.stats['SET']   || {}, peF.vals);
  const pbvStats = _periodStats(pbv.stats['SET']  || {}, pbvF.vals);

  const peThresh  = [10, 15, 20];
  const pbvThresh = [1, 1.5, 2];
  const zoneLabels = ['Cheap','Fair','Slightly High','Expensive'];

  // ── SD zone helper ───────────────────────────
  function sdZoneInfo(zscore) {
    const z = zscore || 0;
    if (z >  2) return { label:`+${z.toFixed(2)}σ`, text:'แพงผิดปกติ',    color:'#dc503c', bg:'rgba(220,80,60,0.12)'  };
    if (z >  1) return { label:`+${z.toFixed(2)}σ`, text:'แพงเกินค่าเฉลี่ย', color:'#dca032', bg:'rgba(220,160,50,0.10)' };
    if (z > -1) return { label:(z>=0?'+':'')+z.toFixed(2)+'σ', text:z>=0?'ใกล้ค่าเฉลี่ย (แพงกว่านิด)':'ใกล้ค่าเฉลี่ย (ถูกกว่านิด)', color:'#c8d0dc', bg:'rgba(200,208,220,0.08)' };
    if (z > -2) return { label:`${z.toFixed(2)}σ`,  text:'ถูกกว่าค่าเฉลี่ย', color:'#96c850', bg:'rgba(150,200,80,0.10)'  };
    return              { label:`${z.toFixed(2)}σ`,  text:'ถูกผิดปกติ',    color:'#3ab464', bg:'rgba(58,180,100,0.12)' };
  }

  function sdTipContent(s, label) {
    const bands = s.bands || {};
    const z = s.zscore || 0;
    const zi = sdZoneInfo(z);
    return `<div class='tip-head'>Z-Score — ตำแหน่ง SD</div>` +
      `ปัจจุบัน <b style='color:${zi.color}'>${zi.label}</b> — ${zi.text}<hr>` +
      `<div class='tip-row'><span class='tip-label'>+3σ</span><span class='tip-zone-exp'>${bands['+3σ']}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>+2σ</span><span class='tip-zone-high'>${bands['+2σ']}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>+1σ</span><span style='color:#e0d060'>${bands['+1σ']}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>avg</span><span class='tip-val'>${s.avg}x (±${s.std}x)</span></div>` +
      `<div class='tip-row'><span class='tip-label'>-1σ</span><span style='color:#96c850'>${bands['-1σ']}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>-2σ</span><span class='tip-zone-cheap'>${bands['-2σ']}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>-3σ</span><span class='tip-zone-cheap'>${bands['-3σ']}x</span></div><hr>` +
      `ค่า <0 = ถูกกว่าค่าเฉลี่ย | ค่า >0 = แพงกว่าค่าเฉลี่ย<br>` +
      `|z| > 2 พบแค่ ~5% ของเวลา = ผิดปกติมาก`;
  }

  // ── Tooltip content ──────────────────────────
  const TIPS = {
    pe: `<div class='tip-head'>P/E Ratio (Price-to-Earnings)</div>` +
      `<b>สูตร:</b> ราคาหุ้น ÷ กำไรต่อหุ้น (EPS)<br>` +
      `บอกว่านักลงทุนยอมจ่ายกี่บาทต่อกำไร 1 บาท<hr>` +
      `<div class='tip-row'><span class='tip-label'>ปัจจุบัน</span><span class='tip-val'>${peStats.current}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>ค่าเฉลี่ย</span><span class='tip-val'>${peStats.avg}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>ต่ำสุดเคย</span><span class='tip-zone-cheap'>${peStats.min}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>สูงสุดเคย</span><span class='tip-zone-exp'>${peStats.max}x (1993 ฟองสบู่)</span></div><hr>` +
      `<div class='tip-row'><span class='tip-label'>&lt; 10x</span><span class='tip-zone-cheap'>Cheap — โอกาสซื้อ</span></div>` +
      `<div class='tip-row'><span class='tip-label'>10–15x</span><span class='tip-zone-fair'>Fair — ราคายุติธรรม</span></div>` +
      `<div class='tip-row'><span class='tip-label'>15–20x</span><span class='tip-zone-high'>Slightly High — ระวัง</span></div>` +
      `<div class='tip-row'><span class='tip-label'>&gt; 20x</span><span class='tip-zone-exp'>Expensive — เสี่ยงสูง</span></div>`,

    pbv: `<div class='tip-head'>P/BV Ratio (Price-to-Book Value)</div>` +
      `<b>สูตร:</b> ราคาหุ้น ÷ มูลค่าทางบัญชีต่อหุ้น<br>` +
      `บอกว่าตลาดให้ราคากี่เท่าของทรัพย์สินสุทธิ<hr>` +
      `<div class='tip-row'><span class='tip-label'>ปัจจุบัน</span><span class='tip-val'>${pbvStats.current}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>ค่าเฉลี่ย</span><span class='tip-val'>${pbvStats.avg}x</span></div>` +
      `<div class='tip-row'><span class='tip-label'>ต่ำสุดเคย</span><span class='tip-zone-cheap'>${pbvStats.min}x (วิกฤต 1997)</span></div>` +
      `<div class='tip-row'><span class='tip-label'>สูงสุดเคย</span><span class='tip-zone-exp'>${pbvStats.max}x</span></div><hr>` +
      `<div class='tip-row'><span class='tip-label'>&lt; 1x</span><span class='tip-zone-cheap'>ถูกกว่ามูลค่าทรัพย์สิน</span></div>` +
      `<div class='tip-row'><span class='tip-label'>1–1.5x</span><span class='tip-zone-fair'>Fair — สมเหตุสมผล</span></div>` +
      `<div class='tip-row'><span class='tip-label'>1.5–2x</span><span class='tip-zone-high'>Slightly High</span></div>` +
      `<div class='tip-row'><span class='tip-label'>&gt; 2x</span><span class='tip-zone-exp'>Expensive</span></div>` +
      `<hr><b>ข้อดีเทียบ P/E:</b> เสถียรกว่าในช่วงกำไรผันผวน<br>เช่น COVID ที่กำไรร่วงแต่ Book Value คงที่`,

    pePct: _pctTip(peStats.percentile),
    pbvPct: _pctTip(pbvStats.percentile),

    peChart: `<div class='tip-head'>วิธีอ่านกราฟ P/E</div>` +
      `<div class='tip-row'><span class='tip-label'>เส้นฟ้า</span><span class='tip-val'>P/E รายเดือน</span></div>` +
      `<div class='tip-row'><span class='tip-label'>เส้นเหลือง</span><span class='tip-val'>ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} (${peStats.avg}x)</span></div>` +
      `<div class='tip-row'><span class='tip-label'>จุดขาว</span><span class='tip-val'>ค่าปัจจุบัน</span></div><hr>` +
      `<b>เหตุการณ์สำคัญ:</b><br>` +
      `• <b>1993–1994:</b> P/E พุ่ง ~41x (ฟองสบู่ก่อนวิกฤต 2540)<br>` +
      `• <b>1997–1998:</b> วิกฤตต้มยำกุ้ง P/E ดิ่งหนัก<br>` +
      `• <b>2008–2009:</b> Subprime — P/E ต่ำมาก (โอกาสซื้อ)<br>` +
      `• <b>2020:</b> COVID — P/E สูงเพราะกำไรลด ไม่ใช่ราคาสูง<br>` +
      `• <b>hover</b> เหนือกราฟเพื่อดูค่ารายเดือน`,

    pbvChart: `<div class='tip-head'>วิธีอ่านกราฟ P/BV</div>` +
      `<div class='tip-row'><span class='tip-label'>เส้นฟ้า</span><span class='tip-val'>P/BV รายเดือน</span></div>` +
      `<div class='tip-row'><span class='tip-label'>เส้นเหลือง</span><span class='tip-val'>ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} (${pbvStats.avg}x)</span></div><hr>` +
      `<b>เหตุการณ์สำคัญ:</b><br>` +
      `• <b>1997–1998:</b> P/BV ต่ำกว่า 1x — ซื้อถูกกว่า Book Value<br>` +
      `• SET ปกติซื้อขายที่ 1.3–1.8x<br>` +
      `• P/BV &lt; 1x เกิดได้เฉพาะวิกฤตระดับ systemic<hr>` +
      `<b>ข้อจำกัด:</b> บางอุตสาหกรรม (Tech, บริการ)<br>มี Book Value ต่ำตามธรรมชาติ ทำให้ P/BV ดูสูง`,

    zoneFreq: `<div class='tip-head'>Zone Frequency — สัดส่วนเวลาในแต่ละ Zone</div>` +
      `คำนวณจากข้อมูลรายเดือนทั้งหมดในประวัติศาสตร์<hr>` +
      `<b>วิธีใช้:</b><br>` +
      `• Zone ที่เกิดน้อย = โอกาสหายาก ควรให้ความสำคัญ<br>` +
      `• ถ้า Cheap เกิดแค่ 5% → เวลาตลาดถูกจริงๆ หายากมาก<hr>` +
      `<b>ตัวอย่าง:</b> ถ้า Cheap zone เกิด 8% ของเวลา<br>แต่ตอนนี้ตลาดอยู่ Cheap → โอกาสพิเศษมาก<br>` +
      `ควรเพิ่ม position มากกว่าเวลาปกติ`,
  };

  const peZone  = valZoneLabel(peStats.current,  peThresh,  zoneLabels);
  const pbvZone = valZoneLabel(pbvStats.current, pbvThresh, zoneLabels);

  const peZI  = sdZoneInfo(peStats.zscore);
  const pbvZI = sdZoneInfo(pbvStats.zscore);

  // ── Summary cards ────────────────────────────
  function makeCard(title, s, thresholds, isPE, overrideTip) {
    if (!s || !s.current) return '';
    const zone     = valZoneLabel(s.current, thresholds, zoneLabels);
    const zStyle   = valZoneStyle(s.current, thresholds);
    const zi       = sdZoneInfo(s.zscore);
    const sdTip    = sdTipContent(s, title).replace(/"/g,'&quot;');
    const zoneTip  = _zoneTip(zone, isPE).replace(/"/g,'&quot;');
    const pctTip   = _pctTip(s.percentile).replace(/"/g,'&quot;');
    const mainTip  = (overrideTip || (isPE ? TIPS.pe : TIPS.pbv)).replace(/"/g,'&quot;');
    return `
    <div class="card" style="flex:1;min-width:240px;padding:16px">
      <div style="font-size:12px;color:var(--muted);margin-bottom:6px;display:flex;align-items:center">
        ${title}
        <span class="val-tip" data-vtip="${mainTip}">ⓘ</span>
      </div>
      <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:6px">
        <div style="font-size:28px;font-weight:700">${s.current}x</div>
        <div style="padding:2px 8px;border-radius:12px;font-size:12px;font-weight:700;
          background:${zi.bg};color:${zi.color};cursor:help;white-space:nowrap"
          data-vtip="${sdTip}">${zi.label} ${zi.text}</div>
      </div>
      <div style="margin:4px 0;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
        <span style="${zStyle};cursor:help" data-vtip="${zoneTip}">${zone}</span>
        <span style="font-size:11px;color:var(--muted)">·</span>
        <span style="font-size:11px;color:var(--muted);cursor:help" data-vtip="${pctTip}">
          Percentile <b style="color:#c8d0dc">${s.percentile}%</b>
        </span>
      </div>
      <div style="background:var(--bg-card2);border-radius:4px;height:5px;margin:8px 0;position:relative">
        <div style="background:#5ab4ff;height:100%;border-radius:4px;width:${Math.min(s.percentile,100)}%"></div>
      </div>
      <div style="font-size:11px;color:var(--muted)">
        เฉลี่ย ${s.avg}x &nbsp;·&nbsp; ±1σ = ${s.std}x &nbsp;·&nbsp; ต่ำสุด ${s.min}x &nbsp;·&nbsp; สูงสุด ${s.max}x
      </div>
    </div>`;
  }

  const maiPeRaw  = _trimNulls(pe.dates,  pe.series['mai']);
  const maiPbvRaw = _trimNulls(pbv.dates, pbv.series['mai']);
  const maiPeF  = filterByPeriod(maiPeRaw.dates,  maiPeRaw.vals,  _valPeriod);
  const maiPbvF = filterByPeriod(maiPbvRaw.dates, maiPbvRaw.vals, _valPeriod);
  const maiPeStats  = _periodStats(pe.stats['mai']  || {}, maiPeF.vals);
  const maiPbvStats = _periodStats(pbv.stats['mai'] || {}, maiPbvF.vals);
  const maiPeThresh  = [20, 40, 80];   // MAI PE สูงกว่า SET ตามธรรมชาติ
  const maiPbvThresh = [1, 1.5, 2];    // PBV เหมือนกัน

  renderValPeriodTable([
    { label: 'P/E SET',  dates: pe.dates,        vals: pe.series['SET'],  full: pe.stats['SET'],   current: (pe.stats['SET']   || {}).current },
    { label: 'P/BV SET', dates: pbv.dates,       vals: pbv.series['SET'], full: pbv.stats['SET'],  current: (pbv.stats['SET']  || {}).current },
    { label: 'P/E mai',  dates: maiPeRaw.dates,  vals: maiPeRaw.vals,     full: pe.stats['mai'],   current: (pe.stats['mai']   || {}).current },
    { label: 'P/BV mai', dates: maiPbvRaw.dates, vals: maiPbvRaw.vals,    full: pbv.stats['mai'],  current: (pbv.stats['mai']  || {}).current },
  ]);

  const TIPS_MAI_PE = `<div class='tip-head'>P/E Ratio (mai)</div>` +
    `<b>สูตร:</b> ราคา ÷ กำไรต่อหุ้น — ตลาด mai<hr>` +
    `<div class='tip-row'><span class='tip-label'>ปัจจุบัน</span><span class='tip-val'>${maiPeStats.current}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>ค่าเฉลี่ย</span><span class='tip-val'>${maiPeStats.avg}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>Median</span><span class='tip-val'>${maiPeStats.median}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>ต่ำสุดเคย</span><span class='tip-zone-cheap'>${maiPeStats.min}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>สูงสุดเคย</span><span class='tip-zone-exp'>${maiPeStats.max}x</span></div><hr>` +
    `<b>⚠ ข้อควรระวัง:</b> mai มีหุ้นขนาดเล็ก กำไรผันผวนสูง<br>P/E สูงมักเกิดจากกำไรหดชั่วคราว ไม่ใช่ราคาแพง<br>ควรดู <b>P/BV</b> และ <b>Median</b> มากกว่า avg<hr>` +
    `<div class='tip-row'><span class='tip-label'>&lt; 20x</span><span class='tip-zone-cheap'>Cheap</span></div>` +
    `<div class='tip-row'><span class='tip-label'>20–40x</span><span class='tip-zone-fair'>Fair</span></div>` +
    `<div class='tip-row'><span class='tip-label'>40–80x</span><span class='tip-zone-high'>Slightly High</span></div>` +
    `<div class='tip-row'><span class='tip-label'>&gt; 80x</span><span class='tip-zone-exp'>Expensive</span></div>`;

  const TIPS_MAI_PBV = `<div class='tip-head'>P/BV Ratio (mai)</div>` +
    `<b>สูตร:</b> ราคา ÷ มูลค่าทางบัญชี — ตลาด mai<hr>` +
    `<div class='tip-row'><span class='tip-label'>ปัจจุบัน</span><span class='tip-val'>${maiPbvStats.current}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>ค่าเฉลี่ย</span><span class='tip-val'>${maiPbvStats.avg}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>ต่ำสุดเคย</span><span class='tip-zone-cheap'>${maiPbvStats.min}x</span></div>` +
    `<div class='tip-row'><span class='tip-label'>สูงสุดเคย</span><span class='tip-zone-exp'>${maiPbvStats.max}x</span></div><hr>` +
    `P/BV เชื่อถือได้กว่า P/E สำหรับ mai<br>เพราะ Book Value เสถียรกว่ากำไร<hr>` +
    `<div class='tip-row'><span class='tip-label'>&lt; 1x</span><span class='tip-zone-cheap'>ถูกกว่า Book Value</span></div>` +
    `<div class='tip-row'><span class='tip-label'>1–1.5x</span><span class='tip-zone-fair'>Fair</span></div>` +
    `<div class='tip-row'><span class='tip-label'>1.5–2x</span><span class='tip-zone-high'>Slightly High</span></div>` +
    `<div class='tip-row'><span class='tip-label'>&gt; 2x</span><span class='tip-zone-exp'>Expensive</span></div>`;

  document.getElementById('val-summary').innerHTML =
    `<div style="width:100%;font-size:11px;color:var(--muted);margin-bottom:4px;font-weight:600;letter-spacing:.5px">SET INDEX</div>` +
    makeCard('P/E Ratio (SET)',  peStats,  peThresh,  true)  +
    makeCard('P/BV Ratio (SET)', pbvStats, pbvThresh, false) +
    `<div style="width:100%;font-size:11px;color:var(--muted);margin-top:10px;margin-bottom:4px;font-weight:600;letter-spacing:.5px">
       mai INDEX
       <span class="val-tip" data-vtip="&lt;div class='tip-head'&gt;mai vs SET&lt;/div&gt;mai (Market for Alternative Investment) คือตลาดสำหรับบริษัทขนาดกลาง-เล็ก&lt;hr&gt;&lt;b&gt;P/E mai&lt;/b&gt; มักสูงกว่า SET เพราะ&lt;br&gt;• หุ้นเล็กกำไรผันผวนสูง&lt;br&gt;• บางตัวขาดทุนชั่วคราวดึง avg ขึ้น&lt;br&gt;• ใช้ median แทน avg จะแม่นกว่า&lt;hr&gt;&lt;b&gt;P/BV mai&lt;/b&gt; เชื่อถือได้กว่า P/E&lt;br&gt;เพราะสะท้อน Book Value ที่เสถียรกว่า">ⓘ</span>
     </div>` +
    makeCard('P/E Ratio (mai)',  maiPeStats,  maiPeThresh,  true,  TIPS_MAI_PE)  +
    makeCard('P/BV Ratio (mai)', maiPbvStats, maiPbvThresh, false, TIPS_MAI_PBV);

  // ── Chart title tooltips (inject into DOM elements) ──
  setTimeout(() => {
    const peTitle = document.querySelector('#page-valuation .card:nth-child(3) div[style*="font-weight:600"]');
    const pbvTitle = document.querySelector('#page-valuation .card:nth-child(4) div[style*="font-weight:600"]');
    if (peTitle && !peTitle.querySelector('.val-tip')) {
      peTitle.insertAdjacentHTML('beforeend',
        `<span class="val-tip" data-vtip="${TIPS.peChart.replace(/"/g,'&quot;')}">ⓘ</span>`);
    }
    if (pbvTitle && !pbvTitle.querySelector('.val-tip')) {
      pbvTitle.insertAdjacentHTML('beforeend',
        `<span class="val-tip" data-vtip="${TIPS.pbvChart.replace(/"/g,'&quot;')}">ⓘ</span>`);
    }

    // Zone freq title
    const freqTitle = document.querySelector('#page-valuation .card:last-child div[style*="font-weight:600"]');
    if (freqTitle && !freqTitle.querySelector('.val-tip')) {
      freqTitle.insertAdjacentHTML('beforeend',
        `<span class="val-tip" data-vtip="${TIPS.zoneFreq.replace(/"/g,'&quot;')}">ⓘ</span>`);
    }

    // SET charts
    drawValChart('val-pe-canvas',  peF.dates,  peF.vals,  peThresh,  peStats);
    drawValChart('val-pbv-canvas', pbvF.dates, pbvF.vals, pbvThresh, pbvStats);
    setupValHover('val-pe-canvas',  peThresh,  peStats.avg,  true,  peStats.std);
    setupValHover('val-pbv-canvas', pbvThresh, pbvStats.avg, false, pbvStats.std);
    document.getElementById('val-pe-info').textContent =
      `${peF.dates[0]} – ${peF.dates[peF.dates.length-1]} | ${peF.dates.length} เดือน | เส้นสีเหลือง = ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} ${peStats.avg}x`;
    document.getElementById('val-pbv-info').textContent =
      `${pbvF.dates[0]} – ${pbvF.dates[pbvF.dates.length-1]} | ${pbvF.dates.length} เดือน | เส้นสีเหลือง = ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} ${pbvStats.avg}x`;

    // MAI charts — maiPeF/maiPbvF ถูกกรองตามช่วงเวลาแล้วด้านบน (ก่อน makeCard)
    if (maiPeStats.avg) {
      drawValChart('val-mai-pe-canvas',  maiPeF.dates,  maiPeF.vals,  maiPeThresh,  maiPeStats);
      setupValHover('val-mai-pe-canvas',  maiPeThresh,  maiPeStats.avg,  true,  maiPeStats.std);
      document.getElementById('val-mai-pe-info').textContent =
        `${maiPeF.dates[0]} – ${maiPeF.dates[maiPeF.dates.length-1]} | ${maiPeF.dates.length} เดือน | เส้นสีเหลือง = ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} ${maiPeStats.avg}x (median ${maiPeStats.median}x)`;
    }
    if (maiPbvStats.avg) {
      drawValChart('val-mai-pbv-canvas', maiPbvF.dates, maiPbvF.vals, maiPbvThresh, maiPbvStats);
      setupValHover('val-mai-pbv-canvas', maiPbvThresh, maiPbvStats.avg, false, maiPbvStats.std);
      document.getElementById('val-mai-pbv-info').textContent =
        `${maiPbvF.dates[0]} – ${maiPbvF.dates[maiPbvF.dates.length-1]} | ${maiPbvF.dates.length} เดือน | เส้นสีเหลือง = ค่าเฉลี่ย${VAL_PERIOD_LABEL[_valPeriod]} ${maiPbvStats.avg}x`;
    }
  }, 50);

  // ── Zone frequency ───────────────────────────
  const zoneFreqEl = document.getElementById('val-zone-freq');
  zoneFreqEl.innerHTML = `
    <div style="width:100%">
      <div style="font-size:12px;color:var(--muted);margin-bottom:10px">P/E — ${pe.dates.length} เดือน</div>
      <div id="val-zone-pe" style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:16px"></div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:10px">P/BV — ${pbv.dates.length} เดือน</div>
      <div id="val-zone-pbv" style="display:flex;gap:24px;flex-wrap:wrap"></div>
    </div>`;
  renderValZoneFreq('val-zone-pe',  pe.series['SET'],  peThresh,  zoneLabels, true);
  renderValZoneFreq('val-zone-pbv', pbv.series['SET'], pbvThresh, zoneLabels, false);
}

// ══════════════════════════════════════════════════════════
// NVDR DATA
// ══════════════════════════════════════════════════════════
let _nvdrData = null;

async function loadNvdrData() {
  if (_nvdrData) return _nvdrData;
  try {
    const r = await fetch('/api/nvdr?t=' + Date.now());
    if (!r.ok) return null;
    _nvdrData = await r.json();
    return _nvdrData;
  } catch { return null; }
}

function nvdrBadge(sym) {
  if (!_nvdrData) return '';
  const v = _nvdrData.stocks?.[sym];
  if (!v || v.nvdr_pct < 5) return '';
  const col = v.nvdr_pct >= 20 ? '#5ab4ff' : v.nvdr_pct >= 10 ? '#7090d0' : '#5070a0';
  const tip = `NVDR ${v.nvdr_pct.toFixed(2)}% — ต่างชาติถือผ่าน NVDR`;
  return `<span title="${tip}" style="display:inline-block;font-size:8px;font-weight:700;color:${col};border:1px solid ${col};border-radius:3px;padding:0 3px;margin-left:3px;vertical-align:middle;line-height:13px;cursor:help">N</span>`;
}

function renderNvdrPopup(sym) {
  if (!_nvdrData) return '<div style="color:var(--muted);font-size:12px">กำลังโหลด...</div>';
  const v = _nvdrData.stocks?.[sym];
  if (!v) return `<div style="color:var(--muted);font-size:12px">ไม่มีข้อมูล NVDR สำหรับ ${sym}</div>`;

  const row = (label, val, color='', tip='') =>
    `<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid rgba(255,255,255,0.05)"
          ${tip?`title="${tip}"`:''}><span style="font-size:11px;color:var(--muted);${tip?'cursor:help':''}">${label}</span>
     <span style="font-size:11px;font-weight:600;color:${color||'#c8d0dc'}">${val}</span></div>`;

  const pct = v.nvdr_pct;
  const pctCol = pct >= 20 ? '#5ab4ff' : pct >= 10 ? '#7090d0' : '#c8d0dc';
  const pctLevel = pct >= 20 ? 'สูงมาก — ต่างชาติ major holder'
                 : pct >= 10 ? 'สูง — ต่างชาติสนใจ'
                 : pct >= 5  ? 'ปานกลาง' : 'ต่ำ';

  const lastSnap = v.last_snap;
  const prevSnap = v.prev_snap;
  let trendHtml = '';
  if (lastSnap && prevSnap) {
    const chg = lastSnap.nvdr_pct - prevSnap.nvdr_pct;
    const chgStr = (chg >= 0 ? '+' : '') + chg.toFixed(3) + '%';
    const chgCol = chg > 0 ? '#3ab464' : '#e05252';
    const chgTip = chg > 0
      ? `NVDR เพิ่ม ${chgStr} จาก ${prevSnap.date} → ${lastSnap.date} — ต่างชาติซื้อเพิ่ม`
      : `NVDR ลด ${chgStr} จาก ${prevSnap.date} → ${lastSnap.date} — ต่างชาติขายออก`;
    trendHtml = row('เปลี่ยนแปลง (update ล่าสุด)', `<span style="color:${chgCol}">${chgStr}</span>`, '', chgTip);
  }

  return `
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px">อัพเดท ${_nvdrData.updated_at||'—'}</div>
    ${row('NVDR% ถือครอง', pct.toFixed(3) + '% · ' + pctLevel, pctCol,
      'NVDR% = % ของหุ้นชำระแล้วที่ต่างชาติถือผ่าน NVDR · >5%=มีนัยยะ · >10%=สูง · >20%=สูงมาก')}
    ${row('จำนวน NVDR', (v.nvdr_shares/1e6).toFixed(2) + 'M หุ้น', '',
      'จำนวนหุ้น NVDR ที่ต่างชาติถืออยู่ทั้งหมด ณ วันอัพเดทล่าสุด')}
    ${trendHtml}
    ${v.daily_count > 0
      ? `<div style="font-size:10px;color:var(--muted);margin-top:6px" title="เพิ่มทุกครั้งที่กด Quick Update">มี ${v.daily_count} snapshots · กด Quick Update เพื่อดู trend</div>`
      : `<div style="font-size:10px;color:var(--muted);margin-top:6px">กด Quick Update เพื่อเริ่มเก็บ daily trend</div>`}`;
}

// ══════════════════════════════════════════════════════════
// SHORT SALES DATA
// ══════════════════════════════════════════════════════════
let _shortData = null;  // {period_from, period_to, last_api_update, stocks:{sym:{...}}}

async function loadShortData() {
  if (_shortData) return _shortData;
  try {
    const r = await fetch('/api/short-sales?t=' + Date.now());
    if (!r.ok) return null;
    _shortData = await r.json();
    return _shortData;
  } catch { return null; }
}

function shortBadge(sym) {
  if (!_shortData) return '';
  const v = _shortData.stocks?.[sym];
  if (!v || !v.short_pos_pct) return '';
  const pct = v.short_pos_pct;
  // ไม่แสดงถ้า short position น้อยมาก
  if (pct < 0.5) return '';
  const col = pct >= 2 ? '#e05252' : pct >= 1 ? '#d07030' : '#b09030';
  const tip = `Short Position ${pct.toFixed(2)}% ของหุ้นชำระแล้ว · %Val 6M: ${v.period_pct_value?.toFixed(2)}%`;
  return `<span title="${tip}" style="display:inline-block;font-size:8px;font-weight:700;color:${col};border:1px solid ${col};border-radius:3px;padding:0 3px;margin-left:4px;vertical-align:middle;line-height:13px;cursor:help">S</span>`;
}

function renderShortPopup(sym) {
  if (!_shortData) return '<div style="color:var(--muted);font-size:12px">ไม่มีข้อมูล (รัน import_short_sales.py ก่อน)</div>';
  const v = _shortData.stocks?.[sym];
  if (!v) return '<div style="color:var(--muted);font-size:12px">ไม่มีข้อมูล short sales สำหรับ ' + sym + '</div>';

  const pos_m = (v.short_pos / 1e6).toFixed(2);
  const vol_m = (v.period_vol / 1e6).toFixed(1);
  const period = _shortData.period_from && _shortData.period_to
    ? `${_shortData.period_from} ถึง ${_shortData.period_to}` : '';
  const lastUpd = _shortData.last_api_update ? `อัพเดท ${_shortData.last_api_update}` : '';

  const row = (label, val, color='', tip='') =>
    `<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid rgba(255,255,255,0.05)"
          ${tip ? `title="${tip}"` : ''}>
      <span style="font-size:11px;color:var(--muted);${tip?'cursor:help':''}">${label}</span>
      <span style="font-size:11px;font-weight:600;color:${color||'#c8d0dc'}">${val}</span>
    </div>`;

  const posColor = v.short_pos_pct >= 2 ? '#e05252' : v.short_pos_pct >= 1 ? '#d07030' : '#c8d0dc';
  const posLevel = v.short_pos_pct >= 2 ? 'สูงมาก — squeeze potential สูง'
                 : v.short_pos_pct >= 1 ? 'สูง สำหรับตลาดไทย'
                 : v.short_pos_pct >= 0.5 ? 'เริ่มมีนัยยะ'
                 : 'ต่ำ';

  // daily trend — ใช้ last_snap + prev_snap จาก /api/short-sales (ไม่ได้ส่ง daily array ทั้งหมด)
  let dailyHtml = '';
  const lastSnap = v.last_snap;
  const prevSnap = v.prev_snap;
  const dailyCount = v.daily_count || 0;
  if (lastSnap && prevSnap) {
    const chg = lastSnap.short_pos - prevSnap.short_pos;
    const chgStr = (chg >= 0 ? '+' : '') + (chg / 1e6).toFixed(2) + 'M';
    const chgCol = chg > 0 ? '#e05252' : '#3ab464';
    const chgTip = chg > 0
      ? `Short เพิ่ม ${chgStr} จาก ${prevSnap.date} → ${lastSnap.date} — short seller เพิ่มตำแหน่ง`
      : `Short ลด ${chgStr} จาก ${prevSnap.date} → ${lastSnap.date} — short seller กำลัง cover อาจเป็นแรงหนุนราคา`;
    dailyHtml = row('เปลี่ยนแปลง (update ล่าสุด)', `<span style="color:${chgCol}">${chgStr}</span>`, '', chgTip);
  }

  return `
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px">${period} ${lastUpd ? '· ' + lastUpd : ''}</div>
    ${row('Short Position ค้าง', pos_m + 'M หุ้น', posColor,
      'จำนวนหุ้นที่ขาย short แล้วยังไม่ได้ซื้อคืน — ถ้าราคาขึ้น คนกลุ่มนี้ต้องรีบซื้อคืน (Short Squeeze)')}
    ${row('Short Position %', v.short_pos_pct.toFixed(3) + '% · ' + posLevel, posColor,
      'Short Position % ของหุ้นชำระแล้วทั้งหมด — ตลาดไทย: >0.5%=มีนัยยะ · >1%=สูง · >2%=สูงมาก')}
    ${row('ปริมาณ Short รวม 6 เดือน', vol_m + 'M หุ้น', '',
      'ปริมาณหุ้นที่ถูกขาย short สะสมตลอด 6 เดือน — บอกว่า short seller สนใจหุ้นนี้มากแค่ไหนในระยะยาว')}
    ${row('% Short ต่อการซื้อขาย 6 เดือน', v.period_pct_value.toFixed(2) + '%', '',
      '% ของมูลค่าซื้อขายรวมที่เป็น short sell ตลอด 6 เดือน — ยิ่งสูง = short seller มีบทบาทในหุ้นนี้มาก')}
    ${dailyHtml}
    ${dailyCount > 0
      ? `<div style="font-size:10px;color:var(--muted);margin-top:6px" title="อัพเดทเพิ่มทุกครั้งที่กด Quick Update">มี daily snapshot ${dailyCount} วัน · กด Quick Update เพื่อเพิ่ม</div>`
      : `<div style="font-size:10px;color:var(--muted);margin-top:6px">ยังไม่มี daily snapshot — กด Quick Update เพื่อเริ่มเก็บ trend</div>`}
  `;
}

// ══════════════════════════════════════════════════════════
// INSIDER — Accumulated Signal
// ══════════════════════════════════════════════════════════
let _insAccum = {};   // symbol → {buys, sells, net, people:Set, totalQty, lastBuy, lastSell}

function buildInsAccum() {
  if (!_insData) return;
  _insAccum = {};
  _insData.r59.forEach(rec => {
    const sym = rec.symbol;
    if (!_insAccum[sym]) _insAccum[sym] = {buys:0, sells:0, net:0, people:new Set(), totalQty:0, valuedQty:0, totalValue:0, lastBuy:null, lastSell:null};
    const a = _insAccum[sym];
    if (rec.action === 'buy') {
      a.buys++;
      a.people.add(rec.name || '?');
      a.totalQty += rec.qty || 0;
      // weighted avg: only count qty that has valid price
      if (rec.qty && rec.price && isFinite(rec.price)) {
        a.totalValue += rec.qty * rec.price;
        a.valuedQty  += rec.qty;
      }
      if (!a.lastBuy || rec.trade_date > a.lastBuy) a.lastBuy = rec.trade_date;
    } else if (rec.action === 'sell') {
      a.sells++;
      if (!a.lastSell || rec.trade_date > a.lastSell) a.lastSell = rec.trade_date;
    }
    a.net = a.buys - a.sells;
  });
}

function insiderBadge(sym) {
  const a = _insAccum[sym];
  if (!a || a.net === 0) return '';
  const isCluster = a.people.size >= 2;
  if (a.net > 0) {
    const col = isCluster ? '#3ab464' : '#7dd4a0';
    const tip = `Insider ซื้อสะสม ${a.buys}x (${a.people.size} คน) ใน ${_insDays} วัน`;
    return `<span title="${tip}" style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${col};margin-left:4px;vertical-align:middle;cursor:help"></span>`;
  } else {
    const col = '#dc503c';
    const tip = `Insider ขายสะสม ${a.sells}x ใน ${_insDays} วัน`;
    return `<span title="${tip}" style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${col};margin-left:4px;vertical-align:middle;cursor:help"></span>`;
  }
}

function renderInsAccumTable() {
  const el = document.getElementById('ins-accum-wrap');
  if (!el) return;

  const rows = Object.entries(_insAccum)
    .map(([sym, a]) => ({sym, ...a, peopleCount: a.people.size}))
    .filter(r => r.net !== 0)
    .sort((a,b) => b.net - a.net);

  if (!rows.length) { el.innerHTML = ''; return; }

  const netBuyers  = rows.filter(r => r.net > 0);
  const netSellers = rows.filter(r => r.net < 0);

  el.innerHTML = `
    <div class="card" style="padding:14px;margin-bottom:14px">
      <div style="font-size:13px;font-weight:600;color:#c8d0dc;margin-bottom:12px">
        สะสม Insider Activity ${_insDays} วัน
        <span style="font-size:11px;font-weight:400;color:var(--muted);margin-left:8px">
          🟢 net buy: ${netBuyers.length} หุ้น &nbsp;|&nbsp; 🔴 net sell: ${netSellers.length} หุ้น
        </span>
      </div>
      <div style="display:flex;gap:16px;flex-wrap:wrap">

        ${netBuyers.length ? `
        <div style="flex:1;min-width:280px">
          <div style="font-size:11px;color:#3ab464;font-weight:600;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid rgba(58,180,100,0.2)">
            🟢 Net Buy — สะสมซื้อ (${netBuyers.length} หุ้น)
          </div>
          ${netBuyers.map(r => _accumRow(r)).join('')}
        </div>` : ''}

        ${netSellers.length ? `
        <div style="flex:1;min-width:280px">
          <div style="font-size:11px;color:#dc503c;font-weight:600;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid rgba(220,80,60,0.2)">
            🔴 Net Sell — สะสมขาย (${netSellers.length} หุ้น)
          </div>
          ${netSellers.slice().reverse().map(r => _accumRow(r)).join('')}
        </div>` : ''}

      </div>
    </div>`;
}

function _accumRow(r) {
  const isCluster = r.peopleCount >= 2;
  const netCol = r.net > 0 ? '#3ab464' : '#dc503c';
  const netStr = (r.net > 0 ? '+' : '') + r.net;
  const clusterBadge = isCluster
    ? `<span style="font-size:9px;background:#1a3060;color:#5ab4ff;border-radius:3px;padding:1px 5px;margin-left:4px">Cluster ${r.peopleCount} คน</span>`
    : '';
  const qty = r.totalQty >= 1e6 ? (r.totalQty/1e6).toFixed(1)+'M' : r.totalQty >= 1e3 ? (r.totalQty/1e3).toFixed(0)+'K' : r.totalQty.toLocaleString();
  const lastDate = r.net > 0 ? r.lastBuy : r.lastSell;

  // find stock info from DATA
  const stock = window.DATA?.stocks?.find(s => s.symbol === r.sym);
  const sector = stock?.sector || '';
  const curPrice = stock?.price;

  // คำนวณ % ของ totalQty เทียบกับหุ้นทั้งหมด (mkt_cap / price)
  let unusualBadge = '';
  if (r.net > 0 && r.totalQty > 0 && stock?.mkt_cap > 0 && stock?.price > 0) {
    const totalShares = stock.mkt_cap / stock.price;  // mkt_cap in Baht
    const pctOfShares = r.totalQty / totalShares * 100;
    if (pctOfShares >= 1) {
      unusualBadge = `<span title="ซื้อ ${pctOfShares.toFixed(2)}% ของหุ้นทั้งหมด — ผิดปกติมาก" style="font-size:9px;background:#5c1a0a;color:#ff7070;border-radius:3px;padding:1px 5px;margin-left:4px;font-weight:700">🔴 ${pctOfShares.toFixed(2)}% ผิดปกติมาก</span>`;
    } else if (pctOfShares >= 0.2) {
      unusualBadge = `<span title="ซื้อ ${pctOfShares.toFixed(2)}% ของหุ้นทั้งหมด — ซื้อใหญ่" style="font-size:9px;background:#3d2000;color:#ff8c00;border-radius:3px;padding:1px 5px;margin-left:4px;font-weight:700">🟠 ${pctOfShares.toFixed(2)}% ซื้อใหญ่</span>`;
    } else if (pctOfShares >= 0.05) {
      unusualBadge = `<span title="ซื้อ ${pctOfShares.toFixed(2)}% ของหุ้นทั้งหมด — สังเกต" style="font-size:9px;background:#2d2a00;color:#e6cc00;border-radius:3px;padding:1px 5px;margin-left:4px;font-weight:700">🟡 ${pctOfShares.toFixed(2)}% สังเกต</span>`;
    }
  }

  // weighted avg buy price — ใช้ valuedQty (เฉพาะรายการที่มีราคา) ไม่ใช่ totalQty
  const avgPrice = (r.net > 0 && r.valuedQty > 0 && r.totalValue > 0)
    ? r.totalValue / r.valuedQty : null;
  let pricePart = '';
  if (avgPrice) {
    const avgStr = `฿${avgPrice.toFixed(2)}`;
    if (curPrice) {
      const diff = (curPrice - avgPrice) / avgPrice * 100;
      const diffCol = diff >= 0 ? '#3ab464' : '#dc503c';
      const diffStr = (diff >= 0 ? '+' : '') + diff.toFixed(1) + '%';
      pricePart = `<span style="font-size:10px;color:var(--muted)">avg&nbsp;<span style="color:#c8d0dc">${avgStr}</span>&nbsp;<span style="color:${diffCol}">${diffStr}</span></span>`;
    } else {
      pricePart = `<span style="font-size:10px;color:var(--muted)">avg <span style="color:#c8d0dc">${avgStr}</span></span>`;
    }
  }

  return `
    <div onclick="openChartModal('${r.sym}')"
         style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-radius:6px;
                cursor:pointer;margin-bottom:3px;transition:background .15s;flex-wrap:wrap"
         onmouseover="this.style.background='rgba(255,255,255,0.04)'"
         onmouseout="this.style.background=''">
      <span style="font-weight:700;font-size:13px;color:#5ab4ff;min-width:50px">${r.sym}${tvLink(r.sym)}</span>
      ${clusterBadge}
      ${unusualBadge}
      <span style="font-size:10px;color:var(--muted);flex:1;min-width:60px">${sector}</span>
      <span style="font-size:10px;color:var(--muted)">${r.buys}ซื้อ/${r.sells}ขาย · ${qty}หุ้น</span>
      ${pricePart}
      <span style="font-weight:700;font-size:13px;color:${netCol};min-width:28px;text-align:right">${netStr}</span>
      <span style="font-size:10px;color:var(--muted);min-width:70px;text-align:right">${lastDate||'—'}</span>
    </div>`;
}

// ══════════════════════════════════════════════════════════
// SHORT PAGE
// ══════════════════════════════════════════════════════════
let _shortSort = 'pos';   // column key
let _shortSortDir = -1;   // -1=desc, 1=asc
let _shortMinPct  = 0;    // filter: แสดงเฉพาะหุ้นที่ short_pos_pct >= ค่านี้

function setShortSortCol(col) {
  if (_shortSort === col) _shortSortDir *= -1;
  else { _shortSort = col; _shortSortDir = -1; }
  // sync top buttons
  document.getElementById('short-sort-pos').classList.toggle('active', col === 'pos');
  document.getElementById('short-sort-val').classList.toggle('active', col === 'val');
  renderShortTable();
}

function setShortSort(s, btn) {
  _shortSort = s; _shortSortDir = -1;
  document.querySelectorAll('[id^="short-sort-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderShortTable();
  renderShortSqueeze();
}

async function loadShortPage() {
  document.getElementById('short-table-wrap').innerHTML =
    '<div style="padding:20px;color:var(--muted);font-size:13px;text-align:center">กำลังโหลด...</div>';
  const data = await loadShortData();
  if (!data) {
    document.getElementById('short-table-wrap').innerHTML =
      '<div style="padding:20px;color:var(--red);font-size:13px">ไม่พบข้อมูล — กรุณารัน import_short_sales.py ก่อน</div>';
    return;
  }
  const upd  = data.last_api_update ? `อัพเดท API ${data.last_api_update}` : 'ยังไม่มี daily update';
  document.getElementById('short-status').textContent =
    `ข้อมูล ${data.period_from} ถึง ${data.period_to} · ${upd}`;
  renderShortSummary();
  renderShortSqueeze();
  renderShortTable();
}

function filterShortByPct(minPct) {
  _shortMinPct = (_shortMinPct === minPct) ? 0 : minPct;  // toggle
  renderShortSummary();
  renderShortTable();
  if (_shortMinPct > 0) {
    const tbl = document.getElementById('short-table-wrap');
    if (tbl) tbl.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function renderShortSummary() {
  const el = document.getElementById('short-summary');
  if (!el || !_shortData) return;
  const stocks = Object.values(_shortData.stocks);
  const hasPos = stocks.filter(v => v.short_pos > 0).length;
  const over05 = stocks.filter(v => v.short_pos_pct >= 0.5).length;
  const over1  = stocks.filter(v => v.short_pos_pct >= 1).length;
  const over2  = stocks.filter(v => v.short_pos_pct >= 2).length;

  const card = (label, val, sub, col, minPct, tip) => {
    const active = _shortMinPct === minPct && minPct > 0;
    const clickable = minPct > 0;
    return `
    <div class="card" onclick="${clickable ? `filterShortByPct(${minPct})` : ''}"
         title="${tip}"
         style="padding:12px 16px;min-width:140px;flex:1;
                ${clickable ? 'cursor:pointer;' : ''}
                ${active ? `border:1px solid ${col};background:rgba(255,255,255,0.07)` : ''}
                transition:background .15s"
         ${clickable ? `onmouseover="this.style.background='rgba(255,255,255,0.06)'"
                        onmouseout="this.style.background='${active ? 'rgba(255,255,255,0.07)' : ''}'"`  : ''}>
      <div style="font-size:11px;color:var(--muted);margin-bottom:4px">${label}</div>
      <div style="font-size:20px;font-weight:700;color:${col}">${val}</div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:3px">
        <div style="font-size:10px;color:var(--muted)">${sub}</div>
        ${clickable ? `<div style="font-size:10px;color:${active ? col : 'var(--muted)'}">
          ${active ? '✕ ล้าง filter' : '→ กรองตาราง'}</div>` : ''}
      </div>
    </div>`;
  };

  el.innerHTML = [
    card('หุ้นที่มี Short Position', hasPos + ' หุ้น',
         `จากทั้งหมด ${stocks.length} หุ้น`, '#c8d0dc', 0,
         'จำนวนหุ้นทั้งหมดที่มี Short Position > 0 ในช่วงที่โหลดข้อมูล'),
    card('Short Pos > 0.5%', over05 + ' หุ้น',
         'เริ่มมีนัยยะสำหรับตลาดไทย', '#b09030', 0.5,
         'Short Position > 0.5% ของ Paid-up Capital — เริ่มมีนัยยะสำคัญสำหรับตลาด SET\nกดเพื่อกรองตารางด้านล่าง'),
    card('Short Pos > 1%', over1 + ' หุ้น',
         'สูง — short seller มั่นใจ bearish', '#d07030', 1,
         'Short Position > 1% — ระดับสูง บ่งชี้ short seller มีความมั่นใจขาลง\nกดเพื่อกรองตารางด้านล่าง'),
    card('Short Pos > 2%', over2 + ' หุ้น',
         'สูงมาก — squeeze potential สูง', '#e05252', 2,
         'Short Position > 2% — ระดับสูงมาก มี Squeeze Potential สูง\nถ้าราคาขึ้น short seller ต้องรีบซื้อคืน → ดันราคาขึ้นต่อ\nกดเพื่อกรองตารางด้านล่าง'),
  ].join('');
}

function renderShortSqueeze() {
  const el = document.getElementById('short-squeeze-wrap');
  if (!el || !_shortData) return;

  // หุ้นที่ short สูง (>1%) + insider net buy
  const candidates = [];
  Object.entries(_shortData.stocks).forEach(([sym, v]) => {
    if (v.short_pos_pct < 1) return;
    const ins = _insAccum[sym];
    if (!ins || ins.net <= 0) return;
    const nvdr = _nvdrData?.stocks?.[sym];
    candidates.push({ sym, ...v, ins, nvdr });
  });
  candidates.sort((a, b) => b.short_pos_pct - a.short_pos_pct);

  if (!candidates.length) {
    el.innerHTML = `
      <div class="card" style="padding:12px 16px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:600;color:#c8d0dc;margin-bottom:6px">
          🎯 Squeeze Radar — Short สูง + Insider ซื้อ
        </div>
        <div style="font-size:12px;color:var(--muted)">
          ไม่พบ (ต้องโหลด Insider page ก่อน และมีหุ้นที่ short pos > 1% + insider ซื้อ)
        </div>
      </div>`;
    return;
  }

  el.innerHTML = `
    <div class="card" style="padding:14px;margin-bottom:14px">
      <div style="font-size:13px;font-weight:600;color:#c8d0dc;margin-bottom:12px">
        <span title="Squeeze = ราคาขึ้น ทำให้ short seller ต้องซื้อคืน (cover) → ดันราคาขึ้นต่อ · เงื่อนไข: Short Pos > 1% AND Insider ซื้อสะสม (net buy)">🎯 Squeeze Radar</span>
        <span style="font-size:11px;font-weight:400;color:var(--muted);margin-left:8px">
          Short Pos > 1% + Insider ซื้อสะสม · ${candidates.length} หุ้น
        </span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:8px">
        ${candidates.map(r => {
          const isCluster = r.ins.people.size >= 2;
          const stock = window.DATA?.stocks?.find(s => s.symbol === r.sym);
          const posCol = r.short_pos_pct >= 2 ? '#e05252' : '#d07030';
          return `
            <div onclick="openChartModal('${r.sym}')"
                 style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
                        border-radius:8px;padding:10px 14px;cursor:pointer;min-width:160px;flex:1;
                        transition:background .15s"
                 onmouseover="this.style.background='rgba(255,255,255,0.08)'"
                 onmouseout="this.style.background='rgba(255,255,255,0.04)'">
              <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px">
                <span style="font-weight:700;font-size:14px;color:#5ab4ff">${r.sym}${tvLink(r.sym)}</span>
                ${isCluster ? `<span title="Cluster = มี Insider ≥2 คนซื้อพร้อมกัน — สัญญาณแข็งแกร่งกว่า 1 คนซื้อ" style="font-size:9px;background:#1a3060;color:#5ab4ff;border-radius:3px;padding:1px 5px;cursor:help">Cluster</span>` : ''}
              </div>
              <div style="font-size:11px;color:var(--muted);margin-bottom:6px">${stock?.sector||''}</div>
              <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                <span style="font-size:11px;color:var(--muted)">Short Pos</span>
                <span style="font-size:11px;font-weight:700;color:${posCol}">${r.short_pos_pct.toFixed(2)}%</span>
              </div>
              <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                <span style="font-size:11px;color:var(--muted)">Insider ซื้อ</span>
                <span style="font-size:11px;font-weight:600;color:#3ab464">${r.ins.buys}x (${r.ins.people.size} คน)</span>
              </div>
              <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                <span style="font-size:11px;color:var(--muted)">ราคา</span>
                <span style="font-size:11px;color:#c8d0dc">${stock?.price?.toFixed(2)||'—'}</span>
              </div>
              ${r.nvdr ? `<div style="display:flex;justify-content:space-between" title="NVDR% = ต่างชาติถือผ่าน NVDR">
                <span style="font-size:11px;color:var(--muted)">NVDR%</span>
                <span style="font-size:11px;color:#5ab4ff">${r.nvdr.nvdr_pct.toFixed(2)}%</span>
              </div>` : ''}
            </div>`;
        }).join('')}
      </div>
    </div>`;
}

function renderShortTable() {
  const el = document.getElementById('short-table-wrap');
  if (!el || !_shortData) return;

  const sortFn = {
    pos:    (a,b) => (b.short_pos_pct   - a.short_pos_pct)   * _shortSortDir * -1,
    pos_m:  (a,b) => (b.short_pos       - a.short_pos)       * _shortSortDir * -1,
    val:    (a,b) => (b.period_pct_value - a.period_pct_value)* _shortSortDir * -1,
    vol:    (a,b) => (b.period_vol       - a.period_vol)      * _shortSortDir * -1,
    price:  (a,b) => {
      const sa = window.DATA?.stocks?.find(s=>s.symbol===a.sym);
      const sb = window.DATA?.stocks?.find(s=>s.symbol===b.sym);
      return ((sb?.price||0) - (sa?.price||0)) * _shortSortDir * -1;
    },
    ret1d:  (a,b) => {
      const sa = window.DATA?.stocks?.find(s=>s.symbol===a.sym);
      const sb = window.DATA?.stocks?.find(s=>s.symbol===b.sym);
      return ((sb?.ret_1d||0) - (sa?.ret_1d||0)) * _shortSortDir * -1;
    },
    sym:    (a,b) => a.sym.localeCompare(b.sym) * _shortSortDir,
    ins:    (a,b) => ((_insAccum[b.sym]?.net||0) - (_insAccum[a.sym]?.net||0)) * _shortSortDir * -1,
  };

  const _searchQ = (document.getElementById('short-search')?.value || '').trim().toUpperCase();
  const rows = Object.entries(_shortData.stocks)
    .filter(([sym, v]) => {
      if (_searchQ && !sym.toUpperCase().includes(_searchQ)) return false;
      return _shortMinPct > 0
        ? v.short_pos_pct >= _shortMinPct
        : (v.short_pos_pct > 0 || v.period_pct_value > 0);
    })
    .map(([sym, v]) => ({ sym, ...v }))
    .sort(sortFn[_shortSort] || sortFn.pos);

  const filterBadge = _shortMinPct > 0
    ? `<span onclick="filterShortByPct(${_shortMinPct})" title="กดเพื่อล้าง filter"
            style="font-size:11px;background:rgba(208,112,48,0.2);color:#d07030;border:1px solid #d07030;
                   border-radius:4px;padding:2px 8px;margin-left:8px;cursor:pointer">
         Short Pos ≥ ${_shortMinPct}% · ${rows.length} หุ้น &nbsp;✕
       </span>` : '';

  if (!rows.length) {
    el.innerHTML = `<div style="padding:20px;color:var(--muted)">ไม่มีข้อมูลที่ตรงกับเงื่อนไข ${filterBadge}</div>`;
    return;
  }

  const posBar = (pct) => {
    const w = Math.min(pct / 4 * 100, 100);  // 4% = 100%
    const col = pct >= 2 ? '#e05252' : pct >= 1 ? '#d07030' : pct >= 0.5 ? '#b09030' : '#607080';
    return `<div style="display:inline-block;width:${w.toFixed(0)}%;height:4px;background:${col};border-radius:2px;vertical-align:middle;margin-left:4px"></div>`;
  };

  const arr = (col) => _shortSort===col ? (_shortSortDir===-1?'▼':'▲') : '⇅';
  const th = (col, label, tip, right=true) =>
    `<th class="${right?'r':''}" style="cursor:pointer;user-select:none;white-space:nowrap"
         title="${tip}" onclick="setShortSortCol('${col}')">
       ${label} <span style="font-size:9px;color:${_shortSort===col?'#5ab4ff':'#506070'}">${arr(col)}</span>
     </th>`;

  el.innerHTML = `
    ${filterBadge ? `<div style="padding:6px 0 10px">${filterBadge}</div>` : ''}
    <table class="tbl" style="width:100%">
      <thead><tr>
        <th class="r" style="width:36px">#</th>
        ${th('sym','Symbol','เรียงตามชื่อหุ้น A→Z',false)}
        <th>ชื่อ</th>
        <th>Sector</th>
        ${th('pos','Pos%','Short Position ค้างอยู่ % ของหุ้นชำระแล้ว — ยิ่งสูงยิ่ง bearish')}
        ${th('pos_m','Position (M)','จำนวนหุ้น short ที่ยังค้างอยู่ (ยังไม่ได้ซื้อคืน)')}
        ${th('val','%Val 6M','% Short ต่อมูลค่าซื้อขายรวม ช่วง 6 เดือน — ยิ่งสูง = short seller สนใจมาก')}
        ${th('vol','Vol 6M (M)','ปริมาณหุ้น short รวม 6 เดือน')}
        ${th('price','ราคา','เรียงตามราคา')}
        ${th('ret1d','1D%','เรียงตาม % เปลี่ยนแปลงวันนี้')}
        ${th('ins','Insider','สถานะ Insider — net buy/sell ใน ' + _insDays + ' วัน (ต้องโหลด Insider page ก่อน)',false)}
      </tr></thead>
      <tbody>
        ${rows.map((r, i) => {
          const stock = window.DATA?.stocks?.find(s => s.symbol === r.sym);
          const ins   = _insAccum[r.sym];
          const insHtml = ins && ins.net !== 0
            ? ins.net > 0
              ? `<span style="color:#3ab464;font-size:11px">▲ ซื้อ ${ins.buys}x</span>`
              : `<span style="color:#e05252;font-size:11px">▼ ขาย ${ins.sells}x</span>`
            : '—';
          const posCol = r.short_pos_pct >= 2 ? '#e05252' : r.short_pos_pct >= 1 ? '#d07030' : r.short_pos_pct >= 0.5 ? '#b09030' : '#8090a0';
          return `<tr style="cursor:pointer" onclick="showShortDetail('${r.sym}',this)">
            <td class="r" style="color:var(--muted);font-size:11px">${i+1}</td>
            <td><strong class="sym-link" style="color:#5ab4ff" onclick="event.stopPropagation();openChartModal('${r.sym}')">${r.sym}</strong>${tvLink(r.sym)}${insiderBadge(r.sym)}</td>
            <td style="font-size:11px;color:var(--text2);max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${stock?.name||'—'}</td>
            <td style="font-size:11px;color:var(--muted)">${stock?.sector||'—'}</td>
            <td class="r" style="font-weight:700;color:${posCol}">
              ${r.short_pos_pct.toFixed(3)}%${posBar(r.short_pos_pct)}
            </td>
            <td class="r" style="font-size:11px">${(r.short_pos/1e6).toFixed(2)}M</td>
            <td class="r" style="font-size:11px;color:${r.period_pct_value>=5?'#e05252':r.period_pct_value>=3?'#d07030':'#8090a0'}">${r.period_pct_value.toFixed(2)}%</td>
            <td class="r" style="font-size:11px">${(r.period_vol/1e6).toFixed(1)}M</td>
            <td class="r" style="font-size:11px">${stock?.price?.toFixed(2)||'—'}</td>
            <td class="r">${stock ? pct(stock.ret_1d) : '—'}</td>
            <td style="font-size:11px">${insHtml}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>`;
}

let _shortDetailSym = null;

async function showShortDetail(sym, rowEl) {
  // highlight row
  document.querySelectorAll('#short-table-wrap tr.selected-row')
    .forEach(r => { r.classList.remove('selected-row'); r.style.background = ''; });
  if (rowEl) { rowEl.classList.add('selected-row'); rowEl.style.background = 'rgba(90,180,255,0.08)'; }

  const panel = document.getElementById('short-detail-panel');
  panel.style.display = 'block';
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  _shortDetailSym = sym;
  const stock = window.DATA?.stocks?.find(s => s.symbol === sym);
  document.getElementById('short-detail-title').textContent = sym + (stock ? '  ' + stock.name : '');

  // ดึง daily snapshots จาก API
  try {
    const r = await fetch(`/api/short-sales/${encodeURIComponent(sym)}`);
    const d = await r.json();
    const daily = d.daily || [];

    document.getElementById('short-detail-sub').textContent =
      daily.length > 0
        ? `${daily.length} snapshots (${daily[0].date} → ${daily[daily.length-1].date})`
        : 'ยังไม่มี daily snapshots — กด Quick Update เพื่อเริ่มเก็บข้อมูล';

    // stats
    const ins = _insAccum[sym];
    const insHtml = ins && ins.net !== 0
      ? ins.net > 0
        ? `<span style="color:#3ab464">▲ Insider ซื้อ ${ins.buys}x (${ins.people.size} คน)</span>`
        : `<span style="color:#e05252">▼ Insider ขาย ${ins.sells}x</span>`
      : '<span style="color:var(--muted)">ไม่มีข้อมูล Insider</span>';

    document.getElementById('short-detail-stats').innerHTML = [
      `<div title="Short Position ค้างอยู่ % ของหุ้นชำระแล้ว — ยิ่งสูงยิ่ง bearish · >1% = สูงสำหรับไทย · >2% = สูงมาก"><div style="font-size:10px;color:var(--muted);cursor:help">Short Pos %</div><div style="font-size:14px;font-weight:700;color:${d.short_pos_pct>=2?'#e05252':d.short_pos_pct>=1?'#d07030':'#c8d0dc'}">${d.short_pos_pct?.toFixed(3)}%</div></div>`,
      `<div title="จำนวนหุ้น short ที่ยังค้างอยู่ (ยังไม่ได้ซื้อคืน) — ถ้าราคาขึ้น คนเหล่านี้ต้องซื้อคืน = Short Squeeze"><div style="font-size:10px;color:var(--muted);cursor:help">Outstanding</div><div style="font-size:14px;font-weight:700;color:#c8d0dc">${(d.short_pos/1e6).toFixed(2)}M</div></div>`,
      `<div title="% Short ต่อมูลค่าซื้อขายรวม ช่วง 6 เดือน — ยิ่งสูง = short seller สนใจหุ้นนี้มาก ตลอด 6 เดือน"><div style="font-size:10px;color:var(--muted);cursor:help">%Val 6M</div><div style="font-size:14px;font-weight:700;color:#c8d0dc">${d.period_pct_value?.toFixed(2)}%</div></div>`,
      `<div><div style="font-size:10px;color:var(--muted)">ราคา</div><div style="font-size:14px;font-weight:700;color:#c8d0dc">${stock?.price?.toFixed(2)||'—'}</div></div>`,
      `<div title="สถานะ Insider ใน ${_insDays} วัน จากหน้า Insider (ต้องโหลดก่อน)"><div style="font-size:10px;color:var(--muted);cursor:help">Insider (${_insDays}ว)</div><div style="font-size:13px;font-weight:600">${insHtml}</div></div>`,
    ].join('');

    drawShortTrendChart(daily, d.short_pos);
  } catch(e) {
    document.getElementById('short-detail-sub').textContent = 'โหลดไม่สำเร็จ: ' + e.message;
  }
}

function drawShortTrendChart(daily, currentPos) {
  const canvas = document.getElementById('short-trend-canvas');
  if (!canvas) return;
  const W = canvas.offsetWidth || 600;
  const H = 140;
  canvas.width  = W;
  canvas.height = H;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  if (!daily || daily.length === 0) {
    // ยังไม่มีข้อมูล — แสดง placeholder
    ctx.fillStyle = 'rgba(255,255,255,0.06)';
    ctx.roundRect(0, 0, W, H, 8);
    ctx.fill();
    ctx.fillStyle = '#607080';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('ยังไม่มีข้อมูล trend — กด Quick Update เพื่อเริ่มเก็บ daily snapshots', W/2, H/2 - 8);
    ctx.font = '11px sans-serif';
    ctx.fillStyle = '#405060';
    ctx.fillText('Short Position จะถูกบันทึกทุกครั้งที่กด Quick Update', W/2, H/2 + 12);
    return;
  }

  // รวม daily + current point
  const points = [...daily.map(d => ({ date: d.date, val: d.short_pos }))];
  // ถ้า point สุดท้ายไม่ใช่วันนี้ให้เพิ่ม current
  if (currentPos && (points.length === 0 || points[points.length-1].val !== currentPos)) {
    const today = new Date().toISOString().slice(0,10);
    if (points.length === 0 || points[points.length-1].date !== today)
      points.push({ date: today, val: currentPos });
  }
  if (points.length < 2) {
    ctx.fillStyle = '#607080';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('ต้องการอย่างน้อย 2 snapshots เพื่อแสดง trend', W/2, H/2);
    return;
  }

  const PAD = { top: 16, bottom: 28, left: 52, right: 16 };
  const PW = W - PAD.left - PAD.right;
  const PH = H - PAD.top - PAD.bottom;

  const vals = points.map(p => p.val);
  const minV = Math.min(...vals);
  const maxV = Math.max(...vals);
  const range = maxV - minV || 1;
  const toX = i => PAD.left + (i / (points.length - 1)) * PW;
  const toY = v => PAD.top + PH - ((v - minV) / range) * PH;

  // grid lines
  ctx.strokeStyle = 'rgba(255,255,255,0.06)';
  ctx.lineWidth = 1;
  [0, 0.25, 0.5, 0.75, 1].forEach(f => {
    const y = PAD.top + PH * (1 - f);
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + PW, y); ctx.stroke();
    const label = ((minV + range * f) / 1e6).toFixed(1) + 'M';
    ctx.fillStyle = '#506070';
    ctx.font = '9px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(label, PAD.left - 4, y + 3);
  });

  // gradient fill
  const grad = ctx.createLinearGradient(0, PAD.top, 0, PAD.top + PH);
  grad.addColorStop(0, 'rgba(224,82,82,0.25)');
  grad.addColorStop(1, 'rgba(224,82,82,0.02)');
  ctx.beginPath();
  ctx.moveTo(toX(0), toY(points[0].val));
  points.forEach((p, i) => ctx.lineTo(toX(i), toY(p.val)));
  ctx.lineTo(toX(points.length-1), PAD.top + PH);
  ctx.lineTo(toX(0), PAD.top + PH);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  // line
  ctx.beginPath();
  ctx.moveTo(toX(0), toY(points[0].val));
  points.forEach((p, i) => ctx.lineTo(toX(i), toY(p.val)));
  ctx.strokeStyle = '#e05252';
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.stroke();

  // dots + date labels (แสดงทุก N จุด)
  const step = Math.max(1, Math.floor(points.length / 8));
  points.forEach((p, i) => {
    const x = toX(i), y = toY(p.val);
    ctx.beginPath();
    ctx.arc(x, y, i === points.length-1 ? 4 : 2.5, 0, Math.PI*2);
    ctx.fillStyle = i === points.length-1 ? '#ff8080' : '#e05252';
    ctx.fill();
    if (i % step === 0 || i === points.length-1) {
      ctx.fillStyle = '#607080';
      ctx.font = '9px sans-serif';
      ctx.textAlign = i === 0 ? 'left' : i === points.length-1 ? 'right' : 'center';
      ctx.fillText(p.date.slice(5), x, H - 6); // MM-DD
    }
  });

  // trend arrow (เพิ่ม/ลด) — ลด = ดี (short cover), เพิ่ม = ระวัง
  const first = points[0].val, last = points[points.length-1].val;
  const chgPct = ((last - first) / first * 100).toFixed(1);
  const chgCol = last < first ? '#3ab464' : '#e05252';
  const arrow  = last < first ? '▼' : '▲';
  const chgLabel = last < first ? `▼ ${chgPct}% short ลด (short cover)` : `▲ +${chgPct}% short เพิ่ม`;
  ctx.font = 'bold 11px sans-serif';
  ctx.fillStyle = chgCol;
  ctx.textAlign = 'right';
  ctx.fillText(`${arrow} ${Math.abs(parseFloat(chgPct))}%`, W - PAD.right, PAD.top - 3);
}

// ══════════════════════════════════════════════════════════
// INSIDER PAGE
// ══════════════════════════════════════════════════════════
let _insDays   = 30;
let _insFilter = 'all';   // 'all' | 'buy' | 'sell'
let _insSrc    = {r59: true, r246: true};
let _insData   = null;   // {r59:[...], r246:[...]}

async function loadInsiderPage() {
  if (_insData) {
    renderInsAccumTable();
    renderInsiderSummary();
    renderInsiderTable();
    return;
  }
  document.getElementById('ins-status').textContent = 'กำลังโหลด...';
  await fetchInsiderData();
}

async function fetchInsiderData() {
  document.getElementById('ins-table-wrap').innerHTML =
    '<div style="padding:20px;color:var(--muted);font-size:13px;text-align:center">กำลังโหลดข้อมูลจาก SEC...</div>';
  try {
    const [r59res, r246res] = await Promise.all([
      fetch(`/api/insider-trades?days=${_insDays}`).then(r => r.json()),
      fetch(`/api/major-changes?days=${_insDays}`).then(r => r.json()),
    ]);
    _insData = {
      r59:   r59res.records  || [],
      r246:  r246res.records || [],
      fetched_at: r59res.fetched_at || '',
    };
    document.getElementById('ins-status').textContent =
      `อัพเดท ${_insData.fetched_at} · SEC ย้อนหลัง ${_insDays} วัน`;
    buildInsAccum();
    renderInsAccumTable();
    renderInsiderSummary();
    renderInsiderTable();
    // refresh badges in main stocks table only if currently visible
    if (window._currentStockList && document.getElementById('page-stocks')?.classList.contains('active')) {
      renderStocksTable();
    }
  } catch(e) {
    document.getElementById('ins-table-wrap').innerHTML =
      `<div style="padding:20px;color:var(--red);font-size:13px">เกิดข้อผิดพลาด: ${e.message}</div>`;
  }
}

function setInsDays(d, btn) {
  _insDays = d; _insData = null;
  document.querySelectorAll('[id^="ins-days-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  fetchInsiderData();
}

function setInsFilter(f, btn) {
  _insFilter = f;
  document.querySelectorAll('[id^="ins-filter-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderInsiderTable();
}

function setInsSrc(s, btn) {
  const other = s === 'r59' ? 'r246' : 'r59';
  if (_insSrc[s] && !_insSrc[other]) return; // ต้องมีอย่างน้อย 1 แหล่งข้อมูล
  _insSrc[s] = !_insSrc[s];
  btn.classList.toggle('active', _insSrc[s]);
  renderInsiderTable();
}

function renderInsiderSummary() {
  if (!_insData) return;
  const r59  = _insData.r59;
  const r246 = _insData.r246;

  const r59buy  = r59.filter(x => x.action === 'buy');
  const r59sell = r59.filter(x => x.action === 'sell');
  const r246buy  = r246.filter(x => x.action === 'buy');
  const r246sell = r246.filter(x => x.action === 'sell');

  const totBuyQty = r59buy.reduce((s,x) => s + (x.qty||0), 0);
  const totSellQty = r59sell.reduce((s,x) => s + (x.qty||0), 0);
  const uniqBuyStocks  = new Set(r59buy.map(x=>x.symbol)).size;
  const uniqSellStocks = new Set(r59sell.map(x=>x.symbol)).size;

  const cards = [
    { label:'ผู้บริหารซื้อ', value: r59buy.length + ' รายการ', sub: `${uniqBuyStocks} หุ้น · ${(totBuyQty/1e6).toFixed(1)}M หุ้น`, color:'#3ab464' },
    { label:'ผู้บริหารขาย', value: r59sell.length + ' รายการ', sub: `${uniqSellStocks} หุ้น`, color:'#dc503c' },
    { label:'ผู้ถือหุ้นใหญ่เพิ่ม', value: r246buy.length + ' รายการ', sub: r246buy.map(x=>x.symbol).join(', ').slice(0,40)||'-', color:'#3ab464' },
    { label:'ผู้ถือหุ้นใหญ่ลด', value: r246sell.length + ' รายการ', sub: r246sell.map(x=>x.symbol).join(', ').slice(0,40)||'-', color:'#dc503c' },
  ];
  document.getElementById('ins-summary').innerHTML = cards.map(c => `
    <div class="card" style="padding:12px 16px;min-width:160px;flex:1">
      <div style="font-size:11px;color:var(--muted);margin-bottom:4px">${c.label}</div>
      <div style="font-size:18px;font-weight:700;color:${c.color}">${c.value}</div>
      <div style="font-size:10px;color:var(--muted);margin-top:2px">${c.sub}</div>
    </div>`).join('');
}

function renderInsiderTable() {
  if (!_insData) return;
  const q = (document.getElementById('ins-search')?.value || '').toUpperCase();

  // merge r59 + r246 into unified rows
  let rows = [];

  if (_insSrc.r59) {
    _insData.r59.forEach(x => {
      if (_insFilter !== 'all' && x.action !== _insFilter) return;
      if (q && !x.symbol.includes(q) && !x.name.includes(q)) return;
      rows.push({ ...x, src: 'r59' });
    });
  }
  if (_insSrc.r246) {
    _insData.r246.forEach(x => {
      if (_insFilter !== 'all' && x.action !== _insFilter) return;
      if (q && !x.symbol.includes(q)) return;
      rows.push({ ...x, src: 'r246' });
    });
  }

  // sort by date desc
  rows.sort((a,b) => (b.trade_date||'').localeCompare(a.trade_date||''));

  if (!rows.length) {
    document.getElementById('ins-table-wrap').innerHTML =
      '<div style="padding:20px;color:var(--muted);font-size:13px;text-align:center">ไม่พบรายการ</div>';
    return;
  }

  const actColor = a => a==='buy' ? '#3ab464' : a==='sell' ? '#dc503c' : '#8090a0';
  const actLabel = a => a==='buy' ? '🟢 ซื้อ' : a==='sell' ? '🔴 ขาย' : '— อื่นๆ';

  document.getElementById('ins-table-wrap').innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead>
        <tr style="background:var(--bg-card2);position:sticky;top:0;z-index:1">
          <th style="padding:8px 10px;text-align:left;color:var(--muted);font-weight:600">วันที่</th>
          <th style="padding:8px 10px;text-align:left;color:var(--muted)">หุ้น</th>
          <th style="padding:8px 10px;text-align:left;color:var(--muted)">ประเภท</th>
          <th style="padding:8px 10px;text-align:left;color:var(--muted)">ชื่อ</th>
          <th style="padding:8px 10px;text-align:right;color:var(--muted)">ซื้อ/ขาย</th>
          <th style="padding:8px 10px;text-align:right;color:var(--muted)">จำนวน / %</th>
          <th style="padding:8px 10px;text-align:right;color:var(--muted)">ราคา</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(row => {
          const isR59  = row.src === 'r59';
          const detail = isR59
            ? `${(row.qty||0).toLocaleString()} หุ้น`
            : row.pct_change != null ? `${row.pct_change.toFixed(2)}% (${row.pct_before?.toFixed(2)}→${row.pct_after?.toFixed(2)}%)` : '—';
          const price = isR59 && row.price ? `฿${row.price.toFixed(2)}` : '—';
          const srcBadge = isR59
            ? '<span style="font-size:9px;background:#1a3060;color:#5ab4ff;border-radius:3px;padding:1px 4px">ผู้บริหาร</span>'
            : '<span style="font-size:9px;background:#2a1a40;color:#c06bff;border-radius:3px;padding:1px 4px">ผู้ถือหุ้นใหญ่</span>';
          const who = isR59 ? (row.name||'') : (row.holder||'');
          return `<tr style="border-bottom:1px solid rgba(255,255,255,0.04);cursor:pointer"
                      onclick="openChartModal('${row.symbol}')"
                      onmouseover="this.style.background='rgba(255,255,255,0.03)'"
                      onmouseout="this.style.background=''">
            <td style="padding:7px 10px;color:var(--muted)">${row.trade_date||'—'}</td>
            <td style="padding:7px 10px">
              <strong style="color:#5ab4ff">${row.symbol}</strong>${tvLink(row.symbol)}
            </td>
            <td style="padding:7px 10px">${srcBadge}</td>
            <td style="padding:7px 10px;color:#c8d0dc;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
                title="${who}">${who}</td>
            <td style="padding:7px 10px;text-align:right;color:${actColor(row.action)};font-weight:600">${actLabel(row.action)}</td>
            <td style="padding:7px 10px;text-align:right;color:#c8d0dc">${detail}</td>
            <td style="padding:7px 10px;text-align:right;color:#c8d0dc">${price}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>
    <div style="padding:8px 12px;color:var(--muted);font-size:10px;border-top:1px solid var(--border)">
      แหล่งข้อมูล: SEC Thailand (r59 + r246) · ${rows.length} รายการ
    </div>`;
}

// ── Section ใน stock popup ──────────────────────────────────
async function loadInsiderForStock(symbol) {
  const el = document.getElementById('popup-insider-section');
  if (!el) return;
  el.innerHTML = '<div style="color:var(--muted);font-size:11px;padding:4px 0">กำลังโหลด insider...</div>';

  try {
    const [r59res, r246res] = await Promise.all([
      fetch('/api/insider-trades?days=180').then(r => r.json()),
      fetch('/api/major-changes?days=180').then(r => r.json()),
    ]);
    const sym = symbol.toUpperCase();
    const r59  = (r59res.records  || []).filter(x => x.symbol === sym);
    const r246 = (r246res.records || []).filter(x => x.symbol === sym);
    const all  = [...r59.map(x=>({...x,src:'r59'})), ...r246.map(x=>({...x,src:'r246'}))]
                   .sort((a,b) => (b.trade_date||'').localeCompare(a.trade_date||''));

    if (!all.length) {
      el.innerHTML = '<div style="color:var(--muted);font-size:11px;padding:4px 0">ไม่มีรายการ insider 180 วัน</div>';
      return;
    }

    const actColor = a => a==='buy' ? '#3ab464' : a==='sell' ? '#dc503c' : '#8090a0';
    el.innerHTML = `
      <div style="font-size:11px;font-weight:600;color:#c8d0dc;margin-bottom:6px">
        Insider Activity 180 วัน (${all.length} รายการ)
      </div>
      ${all.map(row => {
        const isR59 = row.src === 'r59';
        const detail = isR59
          ? `${(row.qty||0).toLocaleString()} หุ้น${row.price ? ' @ ฿'+row.price.toFixed(2) : ''}`
          : row.pct_change != null ? `${row.pct_change.toFixed(2)}% (${row.pct_before?.toFixed(2)}%→${row.pct_after?.toFixed(2)}%)` : '';
        const who = isR59 ? (row.name||'') : (row.holder||'');
        const badge = isR59
          ? '<span style="font-size:9px;background:#1a3060;color:#5ab4ff;border-radius:3px;padding:1px 4px">บริหาร</span>'
          : '<span style="font-size:9px;background:#2a1a40;color:#c06bff;border-radius:3px;padding:1px 4px">ถือหุ้นใหญ่</span>';
        return `<div style="display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.04)">
          <span style="color:var(--muted);font-size:10px;min-width:75px">${row.trade_date||'—'}</span>
          ${badge}
          <span style="color:${actColor(row.action)};font-weight:600;font-size:11px">${row.action==='buy'?'ซื้อ':row.action==='sell'?'ขาย':'อื่น'}</span>
          <span style="color:#c8d0dc;font-size:11px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${who}">${who}</span>
          <span style="color:var(--muted);font-size:10px;white-space:nowrap">${detail}</span>
        </div>`;
      }).join('')}`;
  } catch(e) {
    el.innerHTML = `<div style="color:var(--muted);font-size:11px">ไม่สามารถโหลด insider: ${e.message}</div>`;
  }
}

// โหลด background data — อยู่ใน block เดียวกับที่ define functions เหล่านี้
loadShortData().then(() => { if (window._reRenderStockBadges) window._reRenderStockBadges(); });
loadNvdrData().then(()  => { if (window._reRenderStockBadges) window._reRenderStockBadges(); });


/* ═════════ inline block boundary ═════════ */


/* ============================================================
   WATCHLIST ALERT MODAL
   ============================================================ */
let _wlAlertSym = null;

function openWlAlertModal(sym) {
  _wlAlertSym = sym;
  const isDR = _isDRSym(sym);
  const label = isDR ? `DR: ${_drUnder(sym)} (USD)` : sym;
  document.getElementById("wl-alert-modal-title").textContent = `🔔 แจ้งเตือนราคา — ${label}`;
  const priceLabel = _currentPriceLabel(sym);
  document.getElementById("wl-al-price").placeholder = priceLabel || "0.00";
  document.getElementById("wl-al-price").value = "";
  document.getElementById("wl-al-note").value = "";
  _renderWlExistingAlerts(sym);
  document.getElementById("wl-alert-modal").classList.add("open");
  document.getElementById("wl-al-price").focus();
}

function closeWlAlertModal() {
  document.getElementById("wl-alert-modal").classList.remove("open");
  _wlAlertSym = null;
}

function saveWlAlert() {
  if (!_wlAlertSym) return;
  const cond = document.getElementById("wl-al-cond").value;
  const price = parseFloat(document.getElementById("wl-al-price").value);
  const note = document.getElementById("wl-al-note").value.trim();
  if (!price || price <= 0) { document.getElementById("wl-al-price").focus(); return; }

  const alerts = _loadAlerts();
  alerts.unshift({
    id: Date.now().toString(),
    symbol: _wlAlertSym,
    condition: cond,
    targetPrice: price,
    note: note || "",
    createdAt: new Date().toISOString(),
    triggered: false,
    triggeredAt: null,
    triggeredPrice: null,
  });
  _saveAlerts(alerts);
  _updateBellBadge();
  _renderWlExistingAlerts(_wlAlertSym);
  document.getElementById("wl-al-price").value = "";
  document.getElementById("wl-al-note").value = "";
  renderWatchlist();
}

function deleteWlAlert(id) {
  _saveAlerts(_loadAlerts().filter(a => a.id !== id));
  _updateBellBadge();
  if (_wlAlertSym) _renderWlExistingAlerts(_wlAlertSym);
  renderWatchlist();
}

function _renderWlExistingAlerts(sym) {
  const alerts = _loadAlerts().filter(a => a.symbol === sym);
  const sec = document.getElementById("wl-alert-existing");
  const list = document.getElementById("wl-alert-existing-list");
  if (alerts.length === 0) { sec.style.display = "none"; return; }
  sec.style.display = "";
  list.innerHTML = alerts.map(a => {
    const condTh = a.condition === "above" ? "≥" : "≤";
    const statusBadge = a.triggered
      ? `<span style="color:var(--yellow);font-size:10px">✓ triggered @ ${a.triggeredPrice?.toFixed(2)??""}</span>`
      : `<span style="color:var(--green);font-size:10px">● active</span>`;
    return `<div class="wl-alert-ex-item">
      <div style="flex:1">
        <span style="font-weight:600">ราคา ${condTh} ${a.targetPrice.toFixed(2)} บาท</span>
        ${a.note ? `<span style="color:var(--text2)"> · ${a.note}</span>` : ""}
        <br>${statusBadge}
      </div>
      <button class="wl-alert-ex-del" onclick="deleteWlAlert('${a.id}')">×</button>
    </div>`;
  }).join("");
}

/* ============================================================
   PRICE ALERT SYSTEM
   - localStorage key: "set_price_alerts"
   - alert obj: { id, symbol, condition:'above'|'below', targetPrice, note, createdAt, triggered, triggeredAt, triggeredPrice }
   ============================================================ */

const ALERT_STORAGE_KEY = "set_price_alerts";
let _alertPanelOpen = false;
let _alertCheckTimer = null;

function _loadAlerts() {
  try { return JSON.parse(localStorage.getItem(ALERT_STORAGE_KEY) || "[]"); }
  catch { return []; }
}
function _saveAlerts(arr) {
  localStorage.setItem(ALERT_STORAGE_KEY, JSON.stringify(arr));
}

function toggleAlertPanel() {
  _alertPanelOpen = !_alertPanelOpen;
  document.getElementById("alert-panel").classList.toggle("open", _alertPanelOpen);
  if (_alertPanelOpen) {
    _populateAlertSymList();
    renderAlertPanel();
  }
}

function _populateAlertSymList() {
  const dl = document.getElementById("al-sym-list");
  if (!dl) return;
  const setSyms = (window.DATA?.stocks || []).map(s => s.symbol).filter(Boolean).sort();
  const drSyms  = (_drData || []).map(s => "DR:" + s.sym).sort();
  // rebuild เสมอ เพราะ SET และ DR load คนละเวลา
  dl.innerHTML = [...setSyms, ...drSyms].map(s => `<option value="${s}">`).join("");
}

function _isDRSym(sym) { return sym.startsWith("DR:"); }
function _drUnder(sym) { return sym.slice(3); }

function _currentPriceLabel(sym) {
  if (_isDRSym(sym)) {
    const under = _drUnder(sym);
    const dr = (_drData || []).find(x => x.sym === under);
    const curr = dr?.region === "TH" ? "THB" : (under.includes(".") ? "" : "USD");
    return dr ? `ราคาปัจจุบัน: ${dr.price?.toFixed(2)} ${curr}` : null;
  }
  const s = window.DATA?.stocks?.find(x => x.symbol === sym);
  return s ? `ราคาปัจจุบัน: ${s.price?.toFixed(2)} บาท` : null;
}

function addAlert() {
  let sym = document.getElementById("al-sym").value.trim().toUpperCase();
  const cond = document.getElementById("al-cond").value;
  const price = parseFloat(document.getElementById("al-price").value);
  const note = document.getElementById("al-note").value.trim();

  if (!sym) { alert("กรุณากรอกชื่อหุ้น"); return; }
  if (!price || price <= 0) { alert("กรุณากรอกราคาเป้าหมาย"); return; }

  // auto-detect DR เหมือน Watchlist
  if (!sym.startsWith("DR:")) {
    const matchesDR  = (_drData || []).some(s => s.sym === sym);
    const matchesSET = (DATA?.stocks || []).some(s => s.symbol === sym);
    if (matchesDR && !matchesSET) sym = "DR:" + sym;
  }

  const alerts = _loadAlerts();
  alerts.unshift({
    id: Date.now().toString(),
    symbol: sym,
    condition: cond,
    targetPrice: price,
    note: note || "",
    createdAt: new Date().toISOString(),
    triggered: false,
    triggeredAt: null,
    triggeredPrice: null,
  });
  _saveAlerts(alerts);

  document.getElementById("al-sym").value = "";
  document.getElementById("al-price").value = "";
  document.getElementById("al-note").value = "";
  renderAlertPanel();
  _updateBellBadge();
}

function deleteAlert(id) {
  _saveAlerts(_loadAlerts().filter(a => a.id !== id));
  renderAlertPanel();
  _updateBellBadge();
}

function clearTriggeredAlerts() {
  _saveAlerts(_loadAlerts().filter(a => !a.triggered));
  renderAlertPanel();
  _updateBellBadge();
}

function renderAlertPanel() {
  const alerts = _loadAlerts();
  const active = alerts.filter(a => !a.triggered);
  const triggered = alerts.filter(a => a.triggered);

  const activeEl = document.getElementById("al-active-list");
  activeEl.innerHTML = active.length === 0
    ? `<div class="alert-empty">ยังไม่มีแจ้งเตือน — เพิ่มได้ด้านบน</div>`
    : active.map(a => {
        const condTh = a.condition === "above" ? "≥" : "≤";
        return `<div class="alert-item">
          <div class="alert-item-body">
            <div class="alert-sym">${a.symbol}</div>
            <div class="alert-cond">ราคา ${condTh} <strong>${a.targetPrice.toFixed(2)}</strong> บาท</div>
            ${a.note ? `<div class="alert-note">${a.note}</div>` : ""}
          </div>
          <button class="alert-del-btn" onclick="deleteAlert('${a.id}')" title="ลบ">×</button>
        </div>`;
      }).join("");

  const triggeredSec = document.getElementById("al-triggered-section");
  const triggeredEl = document.getElementById("al-triggered-list");
  triggeredSec.style.display = triggered.length > 0 ? "" : "none";
  triggeredEl.innerHTML = triggered.map(a => {
    const condTh = a.condition === "above" ? "≥" : "≤";
    const when = a.triggeredAt ? new Date(a.triggeredAt).toLocaleString("th-TH", { hour12: false }) : "";
    return `<div class="alert-item triggered">
      <div class="alert-item-body">
        <div class="alert-sym">${a.symbol} <span style="font-size:10px;color:var(--yellow)">✓ triggered</span></div>
        <div class="alert-cond">ราคา ${condTh} ${a.targetPrice.toFixed(2)} → ราคาจริง <strong>${a.triggeredPrice?.toFixed(2) ?? "—"}</strong></div>
        ${when ? `<div class="alert-note">${when}</div>` : ""}
        ${a.note ? `<div class="alert-note">${a.note}</div>` : ""}
      </div>
      <button class="alert-del-btn" onclick="deleteAlert('${a.id}')" title="ลบ">×</button>
    </div>`;
  }).join("") + (triggered.length > 0 ? `<div style="padding:0 12px 12px"><button onclick="clearTriggeredAlerts()" style="width:100%;padding:5px;background:transparent;border:1px solid var(--border);border-radius:5px;color:var(--text2);cursor:pointer;font-size:11px">ล้างทั้งหมด</button></div>` : "");
}

function _updateBellBadge() {
  const alerts = _loadAlerts();
  const activeCount = alerts.filter(a => !a.triggered).length;
  const badge = document.getElementById("alert-badge");
  if (activeCount > 0) {
    badge.textContent = activeCount > 9 ? "9+" : activeCount;
    badge.style.display = "block";
  } else {
    badge.style.display = "none";
  }
}

/* ---------- PRICE CHECK ---------- */
async function checkAlerts() {
  const alerts = _loadAlerts();
  const active = alerts.filter(a => !a.triggered);
  if (active.length === 0) return;

  try {
    const r = await fetch("/api/prices?t=" + Date.now());
    const d = await r.json();
    if (d.error) return;
    const prices = d.prices; // { symbol: price }

    let changed = false;
    const now = new Date().toISOString();

    for (const a of alerts) {
      if (a.triggered) continue;
      const price = prices[a.symbol];
      if (price == null) continue;
      const hit = (a.condition === "above" && price >= a.targetPrice)
               || (a.condition === "below" && price <= a.targetPrice);
      if (hit) {
        a.triggered = true;
        a.triggeredAt = now;
        a.triggeredPrice = price;
        changed = true;
        _showAlertToast(a, price);
        _showBrowserNotification(a, price);
      }
    }

    if (changed) {
      _saveAlerts(alerts);
      _updateBellBadge();
      if (_alertPanelOpen) renderAlertPanel();
    }

    const checkEl = document.getElementById("al-last-check");
    if (checkEl) {
      const t = new Date().toLocaleTimeString("th-TH", { hour12: false });
      checkEl.textContent = `เช็คล่าสุด: ${t}`;
    }
  } catch(e) {
    console.warn("alert check failed:", e);
  }
}

let _alertPopupQueue = [];
let _alertPopupShowing = false;

function _showAlertToast(alert, price) {
  _alertPopupQueue.push({ alert, price });
  if (!_alertPopupShowing) _showNextAlertPopup();
}

function _showNextAlertPopup() {
  if (_alertPopupQueue.length === 0) { _alertPopupShowing = false; return; }
  _alertPopupShowing = true;
  const { alert, price } = _alertPopupQueue[0];
  const condTh = alert.condition === "above" ? "≥" : "≤";
  const isUp = alert.condition === "above";
  const box = document.getElementById("alert-popup-box");
  box.className = isUp ? "up" : "down";
  document.getElementById("alert-popup-icon").textContent = isUp ? "🔼" : "🔽";
  const isDRAlert = _isDRSym(alert.symbol);
  const curr = isDRAlert ? "USD" : "บาท";
  const symLabel = isDRAlert ? `DR: ${_drUnder(alert.symbol)}` : alert.symbol;
  document.getElementById("alert-popup-sym").textContent = symLabel;
  document.getElementById("alert-popup-msg").innerHTML =
    `ราคา <strong style="font-size:18px;color:${isUp ? "var(--green)" : "var(--red)"}">${price.toFixed(isDRAlert ? 4 : 2)}</strong> ${curr}<br>
     <span style="font-size:12px;color:var(--text2)">เป้าหมาย ${condTh} ${alert.targetPrice.toFixed(isDRAlert ? 4 : 2)} ${curr}</span>`;
  document.getElementById("alert-popup-note").textContent = alert.note || "";
  const remaining = _alertPopupQueue.length - 1;
  document.getElementById("alert-popup-queue").textContent = remaining > 0 ? `และอีก ${remaining} รายการ` : "";
  document.getElementById("alert-popup").classList.add("open");
  // force re-animation
  box.style.animation = "none";
  requestAnimationFrame(() => { box.style.animation = ""; });
}

function dismissAlertPopup() {
  _alertPopupQueue.shift();
  document.getElementById("alert-popup").classList.remove("open");
  if (_alertPopupQueue.length > 0) {
    setTimeout(_showNextAlertPopup, 200);
  } else {
    _alertPopupShowing = false;
    renderWatchlist();
  }
}

function _showBrowserNotification(alert, price) {
  if (!("Notification" in window)) return;
  if (Notification.permission !== "granted") return;
  const condTh = alert.condition === "above" ? "≥" : "≤";
  const isDR = _isDRSym(alert.symbol);
  const curr = isDR ? "USD" : "บาท";
  const dp = isDR ? 4 : 2;
  const symLabel = isDR ? `DR: ${_drUnder(alert.symbol)}` : alert.symbol;
  new Notification(`🔔 ${symLabel} ถึงราคาเป้า!`, {
    body: `ราคา ${price.toFixed(dp)} ${curr} (เป้า ${condTh} ${alert.targetPrice.toFixed(dp)})${alert.note ? "\n" + alert.note : ""}`,
    icon: "/favicon.ico",
  });
}

function _requestNotificationPermission() {
  if ("Notification" in window && Notification.permission === "default") {
    Notification.requestPermission();
  }
}

/* ---------- INIT & POLLING ---------- */
let _alertSystemInited = false;
function initAlertSystem() {
  _updateBellBadge();
  _requestNotificationPermission();
  // เช็คทันทีหลังโหลดข้อมูล (ทุกครั้งที่ data refresh)
  setTimeout(checkAlerts, 2000);
  // สร้าง interval เพียงครั้งเดียว
  if (!_alertSystemInited) {
    _alertSystemInited = true;
    setInterval(checkAlerts, 5 * 60 * 1000);
  }
}

// เริ่ม badge ทันที (ก่อน data load)
document.addEventListener("DOMContentLoaded", () => {
  _updateBellBadge();
  renderAlertPanel();
});

// ============================================================
// CAPITAL FLOW
// ============================================================
let _flowData   = null;
let _flowPeriod = 3;    // months
let _flowView   = 'cum';

// ============================================================
// NVDR RANKING (section ในหน้า Capital Flow)
// ============================================================
let _nvdrDeltaMode = 1;   // จำนวน snapshots ย้อนหลังที่ใช้คิด Δ (1 = ล่าสุด, 5, 20)

function _nvdrDeltaOf(v, n) {
  // คืน {dPct, dShr} จาก daily_tail โดยเทียบ snapshot ล่าสุดกับ n ครั้งก่อน
  // ต้องมีครบ n+1 snapshots — ไม่ครบคืน null (ไม่เดา ไม่ใช้ช่วงสั้นกว่าแทน)
  const t = v.daily_tail;
  if (!t || t.length < n + 1) return { dPct: null, dShr: null };
  const last = t[t.length - 1], base = t[t.length - 1 - n];
  return { dPct: (last[1] ?? 0) - (base[1] ?? 0),
           dShr: (last[2] ?? 0) - (base[2] ?? 0) };
}

function setNvdrDeltaMode(n, btn) {
  _nvdrDeltaMode = n;
  document.querySelectorAll('[id^="nvdr-d-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderNvdrRanking();
}

async function renderNvdrRanking() {
  const elHold = document.getElementById('nvdr-top-hold');
  const elIn   = document.getElementById('nvdr-top-in');
  const elOut  = document.getElementById('nvdr-top-out');
  if (!elHold) return;
  const d = await loadNvdrData();
  if (!d || !d.stocks) {
    elHold.innerHTML = '<tr><td style="color:var(--muted);padding:10px">ไม่มีข้อมูล NVDR</td></tr>';
    elIn.innerHTML = elOut.innerHTML = '';
    return;
  }

  const n = _nvdrDeltaMode;
  const rows = Object.entries(d.stocks).map(([sym, v]) => {
    const { dPct, dShr } = _nvdrDeltaOf(v, n);
    return { sym, pct: v.nvdr_pct ?? 0, shares: v.nvdr_shares ?? 0, dPct, dShr,
             snaps: v.daily_count ?? 0 };
  });

  const symTd = r =>
    `<td><strong class="sym-link" onclick="openChartModal('${r.sym}')">${r.sym}</strong>${tvLink(r.sym)}</td>`;
  const dHtml = r => {
    if (r.dPct == null) return '<span class="text2">—</span>';
    const c = r.dPct > 0 ? 'green' : r.dPct < 0 ? 'red' : 'text2';
    return `<span class="${c}">${r.dPct >= 0 ? '+' : ''}${r.dPct.toFixed(3)}%</span>`;
  };
  const shrHtml = v => v == null ? '' :
    `<span style="font-size:10px;color:var(--text2)">${v >= 0 ? '+' : ''}${(v/1e6).toFixed(1)}M</span>`;

  // 1) ถือครองสูงสุด
  elHold.innerHTML = [...rows].sort((a, b) => b.pct - a.pct).slice(0, 30).map((r, i) => `
    <tr><td class="r" style="color:var(--text2);width:22px">${i+1}</td>${symTd(r)}
        <td class="r" style="font-weight:700;color:${r.pct >= 20 ? '#5ab4ff' : r.pct >= 10 ? '#7090d0' : 'var(--text)'}">${r.pct.toFixed(2)}%</td>
        <td class="r">${dHtml(r)}</td></tr>`).join('');

  // 2)/3) เข้าเพิ่ม / ขายออก — เฉพาะตัวที่มี delta และขยับจริง (>0.005%)
  const withD = rows.filter(r => r.dPct != null && Math.abs(r.dPct) > 0.005);
  const mk = list => list.map((r, i) => `
    <tr><td class="r" style="color:var(--text2);width:22px">${i+1}</td>${symTd(r)}
        <td class="r">${dHtml(r)} ${shrHtml(r.dShr)}</td>
        <td class="r" style="color:var(--text2)">${r.pct.toFixed(2)}%</td></tr>`).join('');
  const inn = withD.filter(r => r.dPct > 0).sort((a, b) => b.dPct - a.dPct).slice(0, 30);
  const out = withD.filter(r => r.dPct < 0).sort((a, b) => a.dPct - b.dPct).slice(0, 30);
  const empty = `<tr><td style="color:var(--muted);padding:10px;font-size:11px">ยังไม่มีข้อมูลพอสำหรับ Δ ${n === 1 ? 'ล่าสุด' : n + ' วัน'} — ต้องมี snapshot อย่างน้อย ${n + 1} ครั้ง (เก็บเพิ่มทุกครั้งที่กด Quick Update วันทำการใหม่)</td></tr>`;
  elIn.innerHTML  = inn.length ? mk(inn) : empty;
  elOut.innerHTML = out.length ? mk(out) : empty;

  const st = document.getElementById('nvdr-rank-status');
  if (st) {
    const modeLbl = n === 1 ? 'Δ ล่าสุด' : `Δ สะสม ${n} snapshots`;
    st.textContent = `อัพเดท: ${d.updated_at || '—'} · ${modeLbl} · ${withD.length} หุ้นมีการเปลี่ยนแปลง`;
  }
}

function exportNvdrCSV() {
  // ข้อ 4: ดูครบทุกตัวไม่จำกัดอันดับ — export ทุกหุ้นพร้อม Δ ทุกช่วง
  if (!_nvdrData || !_nvdrData.stocks) { alert('ยังไม่ได้โหลดข้อมูล NVDR'); return; }
  const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const header = ['Symbol', 'NVDR %', 'NVDR Shares (M)',
                  'D1 %', 'D1 Shares (M)', 'D5 %', 'D5 Shares (M)',
                  'D20 %', 'D20 Shares (M)', 'Snapshots', 'Last Date'].map(esc).join(',');
  const lines = Object.entries(_nvdrData.stocks).map(([sym, v]) => {
    const d1 = _nvdrDeltaOf(v, 1), d5 = _nvdrDeltaOf(v, 5), d20 = _nvdrDeltaOf(v, 20);
    const t = v.daily_tail;
    const f = (x, dg = 4) => x == null ? '' : x.toFixed(dg);
    const m = x => x == null ? '' : (x / 1e6).toFixed(2);
    return [sym, (v.nvdr_pct ?? 0).toFixed(4), ((v.nvdr_shares ?? 0) / 1e6).toFixed(2),
            f(d1.dPct), m(d1.dShr), f(d5.dPct), m(d5.dShr), f(d20.dPct), m(d20.dShr),
            v.daily_count ?? 0, t && t.length ? t[t.length - 1][0] : ''].map(esc).join(',');
  });
  const csv = '﻿' + header + '\n' + lines.join('\n');
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' })),
    download: `NVDR_${new Date().toISOString().slice(0, 10)}.csv`,
  });
  a.click(); URL.revokeObjectURL(a.href);
}

async function loadFlowPage() {
  renderNvdrRanking();   // section NVDR โหลดคู่ขนาน ไม่ block ตาราง flow
  if (_flowData) { renderFlowPage(); return; }
  document.getElementById('flow-tbody').innerHTML =
    '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--muted)">กำลังดึงข้อมูลจาก siamchart.com...</td></tr>';
  try {
    const res = await fetch('/api/market-flow?t=' + Date.now());
    const d   = await res.json();
    if (d.error) throw new Error(d.error);
    _flowData = d;
    document.getElementById('flow-status').textContent = 'อัพเดท: ' + (d.fetched_at || '');
    renderFlowPage();
  } catch(e) {
    document.getElementById('flow-tbody').innerHTML =
      `<tr><td colspan="6" style="padding:20px;color:var(--red)">โหลดไม่ได้: ${e.message}</td></tr>`;
  }
}

function setFlowPeriod(m, btn) {
  _flowPeriod = m;
  document.querySelectorAll('[id^="flow-p-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderFlowPage();
}

function setFlowView(v, btn) {
  _flowView = v;
  document.querySelectorAll('[id^="flow-v-"]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderFlowPage();
}

function renderFlowPage() {
  if (!_flowData) return;
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - _flowPeriod);
  const cutStr = cutoff.toISOString().slice(0, 10);
  // sort ตามวันที่ตรงๆ ไม่พึ่งลำดับจาก API (เดิม .reverse() เดาว่า API ส่ง
  // oldest-first แต่จริงๆ ส่ง newest-first มา ทำให้ตาราง/กราฟกลับหัว)
  const rows = _flowData.rows.filter(r => r.date >= cutStr)
    .slice().sort((a, b) => b.date.localeCompare(a.date));   // newest first สำหรับตาราง

  _renderFlowTable(rows);
  _renderFlowChart(rows.slice().reverse()); // oldest first สำหรับกราฟ
}

function _renderFlowTable(rows) {
  const fmt = v => {
    if (v == null) return '<span style="color:var(--muted)">—</span>';
    const col = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--text2)';
    return `<span style="color:${col}">${v > 0 ? '+' : ''}${v.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2})}</span>`;
  };
  const fmtSet = v => v != null ? v.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '—';

  // Total row
  const totFund    = rows.reduce((s, r) => s + (r.fund    || 0), 0);
  const totForeign = rows.reduce((s, r) => s + (r.foreign || 0), 0);
  const totRetail  = rows.reduce((s, r) => s + (r.retail  || 0), 0);

  const totalRow = `<tr style="background:var(--bg3);font-weight:700;position:sticky;top:41px;z-index:1">
    <td style="padding:6px 10px">ยอดรวม</td>
    <td style="padding:6px 10px;text-align:right">${fmt(Math.round(totFund))}</td>
    <td style="padding:6px 10px;text-align:right">${fmt(Math.round(totForeign))}</td>
    <td style="padding:6px 10px;text-align:right">${fmt(Math.round(totRetail))}</td>
    <td style="padding:6px 10px;text-align:right"></td>
    <td style="padding:6px 10px;text-align:right"></td>
  </tr>`;

  const dataRows = rows.map(r => {
    const chgHtml = r.chg != null
      ? `<span style="color:${r.chg > 0 ? 'var(--green)' : r.chg < 0 ? 'var(--red)' : 'var(--text2)'}">
           ${r.chg > 0 ? '+' : ''}${r.chg.toFixed(2)}</span>`
      : '—';
    return `<tr style="border-bottom:1px solid var(--border)">
      <td style="padding:5px 10px;white-space:nowrap;color:var(--text2)">${r.date.slice(2)}</td>
      <td style="padding:5px 10px;text-align:right">${fmt(r.fund)}</td>
      <td style="padding:5px 10px;text-align:right">${fmt(r.foreign)}</td>
      <td style="padding:5px 10px;text-align:right">${fmt(r.retail)}</td>
      <td style="padding:5px 10px;text-align:right;color:var(--fg)">${fmtSet(r.set)}</td>
      <td style="padding:5px 10px;text-align:right">${chgHtml}</td>
    </tr>`;
  }).join('');

  document.getElementById('flow-tbody').innerHTML = totalRow + dataRows;
}

function _renderFlowChart(rows) {  // rows = oldest first
  const cv = document.getElementById('flow-chart');
  if (!cv || !rows.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W = cv.offsetWidth || 800, H = 180;
  cv.width = W * dpr; cv.height = H * dpr;
  cv.style.width = W + 'px'; cv.style.height = H + 'px';
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);

  const PAD = { t: 10, b: 30, l: 50, r: 10 };
  const PW = W - PAD.l - PAD.r;
  const PH = H - PAD.t - PAD.b;

  // Compute series
  let cumFund = 0, cumForeign = 0, cumRetail = 0;
  const series = rows.map(r => {
    if (_flowView === 'cum') {
      cumFund    += (r.fund    || 0);
      cumForeign += (r.foreign || 0);
      cumRetail  += (r.retail  || 0);
      return { date: r.date, fund: cumFund, foreign: cumForeign, retail: cumRetail, set: r.set };
    }
    return { date: r.date, fund: r.fund || 0, foreign: r.foreign || 0, retail: r.retail || 0, set: r.set };
  });

  const allVals = series.flatMap(r => [r.fund, r.foreign, r.retail]);
  const minV = Math.min(...allVals, 0);
  const maxV = Math.max(...allVals, 0);
  const range = maxV - minV || 1;

  const toX = i => PAD.l + (i / (series.length - 1 || 1)) * PW;
  const toY = v => PAD.t + PH - ((v - minV) / range) * PH;

  // Background
  ctx.fillStyle = 'transparent';
  ctx.clearRect(0, 0, W, H);

  // Zero line
  const zeroY = toY(0);
  ctx.strokeStyle = 'rgba(255,255,255,0.15)';
  ctx.lineWidth = 1; ctx.setLineDash([4, 4]);
  ctx.beginPath(); ctx.moveTo(PAD.l, zeroY); ctx.lineTo(W - PAD.r, zeroY); ctx.stroke();
  ctx.setLineDash([]);

  // Draw lines
  const lines = [
    { key: 'fund',    color: '#FCD202' },
    { key: 'foreign', color: '#04D215' },
    { key: 'retail',  color: '#f85149' },
  ];
  lines.forEach(({ key, color }) => {
    ctx.beginPath();
    ctx.strokeStyle = color; ctx.lineWidth = 1.5;
    series.forEach((r, i) => {
      const x = toX(i), y = toY(r[key]);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();
  });

  // SET index on right axis
  const setVals = series.map(r => r.set).filter(v => v != null);
  if (setVals.length > 1) {
    const minS = Math.min(...setVals), maxS = Math.max(...setVals);
    const rangeS = maxS - minS || 1;
    const toYS = v => PAD.t + PH - ((v - minS) / rangeS) * PH;
    ctx.beginPath();
    ctx.strokeStyle = 'rgba(200,200,220,0.6)'; ctx.lineWidth = 1; ctx.setLineDash([2, 3]);
    series.forEach((r, i) => {
      if (r.set == null) return;
      const x = toX(i), y = toYS(r.set);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke(); ctx.setLineDash([]);
  }

  // X-axis labels (show ~6 dates)
  ctx.fillStyle = 'var(--text2)'; ctx.font = '10px sans-serif'; ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(series.length / 6));
  for (let i = 0; i < series.length; i += step) {
    ctx.fillStyle = '#8b949e';
    ctx.fillText(series[i].date.slice(2), toX(i), H - PAD.b + 14);
  }

  // Y-axis label
  ctx.textAlign = 'right'; ctx.fillStyle = '#8b949e';
  ctx.fillText((maxV / 1000).toFixed(0) + 'K', PAD.l - 4, PAD.t + 10);
  ctx.fillText((minV / 1000).toFixed(0) + 'K', PAD.l - 4, PAD.t + PH);

  // Legend
  const leg = [
    { label: 'กองทุน+โบรก', color: '#FCD202' },
    { label: 'ต่างชาติ',    color: '#04D215' },
    { label: 'รายย่อย',     color: '#f85149' },
    { label: 'SET',         color: 'rgba(200,200,220,0.7)' },
  ];
  ctx.textAlign = 'left'; ctx.font = '10px sans-serif';
  let lx = PAD.l;
  leg.forEach(({ label, color }) => {
    ctx.fillStyle = color;
    ctx.fillRect(lx, PAD.t - 2, 10, 8);
    ctx.fillStyle = '#8b949e';
    ctx.fillText(label, lx + 13, PAD.t + 6);
    lx += ctx.measureText(label).width + 30;
  });
}
