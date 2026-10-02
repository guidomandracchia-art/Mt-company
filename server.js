// MT COMPANY 1.1BETA - server stanze (MT-1 ... MT-100) + hosting del gioco
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PER_ROOM = 30;
const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.ico': 'image/x-icon' };

// il server serve anche il gioco: così APK, YouTube e WebSocket hanno lo stesso indirizzo https
const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/healthz') { res.end('ok'); return; }
  const file = path.normalize(path.join(PUB, url === '/' ? 'index.html' : url));
  if (!file.startsWith(PUB)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8192 });
const rooms = new Map();
let nextId = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (set, o, except) => { const s = JSON.stringify(o); set.forEach(c => { if (c !== except && c.readyState === 1) c.send(s); }); };

const fin = v => typeof v === 'number' && Number.isFinite(v);
const num = (v, l) => fin(v) ? Math.max(-l, Math.min(l, v)) : 0;
const YT = /^[\w-]{11}$/;

function cleanPres(p) {
  const o = {};
  for (const k of ['x', 'y', 'z']) if (k in p) o[k] = num(p[k], 500);
  if ('w' in p) o.w = num(p.w, 1e5);
  if (Array.isArray(p.r) && p.r.length === 4) o.r = p.r.map(v => num(v, 10));
  for (const k of ['j', 'h']) if (k in p) o[k] = p[k] ? 1 : 0;
  if (typeof p.v === 'string' && YT.test(p.v)) o.v = p.v;
  if (fin(p.vt)) o.vt = p.vt;
  return o;
}
function cleanEmit(t, d) {
  d = d && typeof d === 'object' ? d : {};
  if (t === 'chat') { const tx = String(d.t || '').trim().slice(0, 200); return tx ? { n: String(d.n || '?').slice(0, 20), t: tx } : null; }
  if (t === 'video') return typeof d.id === 'string' && YT.test(d.id) ? { id: d.id, t: fin(d.t) ? d.t : Date.now() } : null;
  if (t === 'fx') return { k: 'b' };
  return null;
}

wss.on('connection', ws => {
  ws.id = 'p' + (nextId++); ws.room = null; ws.pres = {}; ws.tok = 60; ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    if (--ws.tok < 0) return; // anti-spam
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'join' && !ws.room) {
      const mt = /^mt-(\d{1,3})$/.exec(String(m.room)); const n = mt ? +mt[1] : 0;
      if (n < 1 || n > 100) return send(ws, { type: 'error', code: 'bad_room' });
      const key = 'mt-' + n, set = rooms.get(key) || new Set();
      if (set.size >= MAX_PER_ROOM) return send(ws, { type: 'error', code: 'full' });
      ws.room = key; set.add(ws); rooms.set(key, set);
      send(ws, { type: 'welcome', id: ws.id, peers: [...set].map(c => ({ id: c.id, presence: c.pres })) });
      bcast(set, { type: 'peer', id: ws.id, presence: ws.pres }, ws);
      return;
    }
    const set = ws.room && rooms.get(ws.room);
    if (!set) return;
    if (m.type === 'presence' && m.presence && typeof m.presence === 'object') {
      Object.assign(ws.pres, cleanPres(m.presence));
      bcast(set, { type: 'peer', id: ws.id, presence: ws.pres }, ws);
    } else if (m.type === 'emit') {
      const d = cleanEmit(m.topic, m.data);
      if (d) bcast(set, { type: 'emit', topic: m.topic, from: ws.id, data: d }); // torna anche al mittente
    }
  });

  ws.on('close', () => {
    const set = ws.room && rooms.get(ws.room);
    if (!set) return;
    set.delete(ws);
    if (!set.size) rooms.delete(ws.room); else bcast(set, { type: 'left', id: ws.id });
  });
});

setInterval(() => wss.clients.forEach(c => { c.tok = Math.min(60, c.tok + 30); }), 1000); // anti-spam
setInterval(() => wss.clients.forEach(c => { // elimina connessioni morte
  if (!c.alive) return c.terminate();
  c.alive = false; c.ping();
}), 30000);

server.listen(PORT, () => console.
