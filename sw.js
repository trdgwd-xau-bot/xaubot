const CACHE = "xaubot-xtb-v1";
const ASSETS = ["./", "./index.html"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener("activate", e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

self.addEventListener("fetch", e => {
  // Let WebSocket connections pass through (ws://, wss://)
  if (e.request.url.startsWith("ws")) return;

  // XTB API calls — network only, no cache
  if (e.request.url.includes("xtb.com")) {
    e.respondWith(
      fetch(e.request).catch(() =>
        new Response(JSON.stringify({ error: "offline" }), {
          headers: { "Content-Type": "application/json" }
        })
      )
    );
    return;
  }

  // App shell — cache first
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});
