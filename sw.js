/* Service Worker ของเวอร์ชันเว็บ (GitHub Pages) — ทำให้ติดตั้งเป็นแอปได้ + เปิดซ้ำเร็ว
   + เปิดดูข้อมูลรอบล่าสุดที่เคยโหลดได้แม้ออฟไลน์

   กลยุทธ์ cache:
   - หน้า/ไฟล์ static (html/js/css/png): stale-while-revalidate — โชว์จาก cache ทันที
     แล้วดึงเวอร์ชันใหม่มาแทนเงียบๆ (เปิดครั้งถัดไปได้ของใหม่)
   - ไฟล์ข้อมูล data/*.json: network-first — เอาของสดก่อนเสมอ ถ้าเน็ตล่ม/ช้าเกิน
     ค่อย fallback ไป cache ล่าสุด ( off-line ยังเปิดดูชุดล่าสุดได้)
   หมายเหตุ: ลงทะเบียนเฉพาะบน github.io (ดู dashboard.js) — เวอร์ชัน local Flask ไม่ใช้ */
// bump เลขนี้ทุกครั้งที่ปล่อยของใหม่แล้วอยากล้าง cache เก่าทิ้งทั้งหมด —
// activate() ด้านล่างลบ cache ที่ชื่อไม่ตรงกับค่านี้ ทำให้เครื่องที่ค้างของเก่า
// (โดยเฉพาะแอปที่ติดตั้งเป็น PWA) ดึงไฟล์ใหม่ทั้งชุดในการเปิดครั้งถัดไป
const CACHE = 'set-dash-v3';
const PRECACHE = [
  './',
  'set_dashboard.html',
  'manifest.json',
  'static/dashboard.css',
  'static/icons/icon-192.png',
  'static/icons/icon-512.png',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(PRECACHE)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;   // ไม่ยุ่งกับ request ข้ามโดเมน

  // ข้อมูลตลาด: ของสดสำคัญกว่า — network-first, ล่มค่อยใช้ cache ล่าสุด
  if (url.pathname.includes('/data/') && url.pathname.endsWith('.json')) {
    e.respondWith(
      fetch(req).then(res => {
        if (res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy));
        }
        return res;
      }).catch(() => caches.match(req).then(r => r || Response.error()))
    );
    return;
  }

  // หน้า (navigation): network-first เพื่อให้ได้ HTML เวอร์ชันใหม่เร็วสุด, ออฟไลน์ใช้ cache
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req).then(res => {
        if (res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy));
        }
        return res;
      }).catch(() => caches.match(req).then(r => r || caches.match('set_dashboard.html')))
    );
    return;
  }

  // ไฟล์ static อื่น (js/css/png): stale-while-revalidate — เร็ว + อัพเดตตัวเองเงียบๆ
  e.respondWith(
    caches.match(req).then(cached => {
      const fresh = fetch(req).then(res => {
        if (res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy));
        }
        return res;
      }).catch(() => cached);
      return cached || fresh;
    })
  );
});
