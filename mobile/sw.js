/* Service worker — เวอร์ชันมือถือ SET Dashboard (โฟลเดอร์ mobile/)
   namespace แยกจาก sw.js เดิมที่ root (set-dash-v*) — ไม่ชนกัน

   กลยุทธ์:
   - ../data/*.json  : network-first (เอาของสดก่อน, ล่ม/ช้าเกินค่อยใช้ cache ล่าสุด)
   - หน้า/asset อื่น  : stale-while-revalidate (เปิดเร็ว + อัปเดตตัวเองเงียบ ๆ)
   bump เลข CACHE ทุกครั้งที่ปล่อยของใหม่แล้วอยากล้าง cache เก่าทั้งชุด */
const CACHE = 'set-mobile-v23';
const PRECACHE = [
  './',
  'index.html',
  'app.css?v=23',
  'app.js?v=23',
  'manifest.json',
  '../static/icons/icon-192.png',
  '../static/icons/icon-512.png',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(PRECACHE).catch(() => {})).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    // ลบเฉพาะ cache ของแอปมือถือรุ่นเก่า (prefix set-mobile-) — ห้ามแตะ set-dash-*
    // ของ SW เว็บเต็มที่ scope / ครอบอยู่ (คนที่เคยติดตั้ง PWA เก่าแล้วถูก redirect
    // มา mobile/ จะมี SW สองตัว — ต่างคนต่างล้าง cache กันเองถ้าไม่กรอง prefix)
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k.startsWith('set-mobile-') && k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;

  // ข้อมูลตลาด: network-first
  if (url.pathname.includes('/data/') && url.pathname.endsWith('.json')) {
    e.respondWith(
      fetch(req).then(res => {
        if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
        return res;
      }).catch(() => caches.match(req).then(r => r || Response.error()))
    );
    return;
  }

  // navigation: network-first เพื่อได้ HTML ใหม่เร็วสุด, ออฟไลน์ใช้ cache
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req).then(res => {
        if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
        return res;
      }).catch(() => caches.match(req).then(r => r || caches.match('index.html')))
    );
    return;
  }

  // asset อื่น: stale-while-revalidate
  e.respondWith(
    caches.match(req).then(cached => {
      const fresh = fetch(req).then(res => {
        if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(req, copy)); }
        return res;
      }).catch(() => cached);
      return cached || fresh;
    })
  );
});
