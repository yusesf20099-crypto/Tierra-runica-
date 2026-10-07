// Servidor de Tierras Rúnicas: multijugador + cuentas con progreso guardado.
// Uso:  npm install   y luego   ADMIN_KEY=tu_clave node servidor.js
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const KEY = process.env.ADMIN_KEY || '';               // misma clave que tu versión admin; sin ella nadie es GM
const FILE = process.env.DATA_FILE || path.join(__dirname, 'cuentas.json');

// ---------- cuentas (se guardan en un archivo JSON) ----------
let DB = { users: {} };
try { DB = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (e) {}
let dirty = false;
const markDirty = () => { dirty = true; };
function flush() {
  if (!dirty) return; dirty = false;
  try { fs.writeFileSync(FILE + '.tmp', JSON.stringify(DB)); fs.renameSync(FILE + '.tmp', FILE); }
  catch (e) { console.error('No se pudo guardar cuentas:', e.message); dirty = true; }
}
setInterval(flush, 5000);
for (const sg of ['SIGINT', 'SIGTERM']) process.on(sg, () => { flush(); process.exit(0); });

const hash = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const same = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const clamp = (v, a, b) => Math.min(b, Math.max(a, +v || 0));

// ---------- conexiones ----------
const PUB = [path.join(__dirname, 'index.html'), path.join(__dirname, 'public', 'index.html')].find(p => fs.existsSync(p)) || '';   // el juego para jugadores (SIN admin)
const server = http.createServer((q, r) => {
  if (q.url.split('?')[0] === '/') {
    try {
      const host = String(q.headers['x-forwarded-host'] || q.headers.host || 'localhost');
      if (!/^[a-z0-9.\-:]+$/i.test(host)) throw 0;
      const url = (String(q.headers['x-forwarded-proto'] || '').includes('https') ? 'wss://' : 'ws://') + host;
      const html = fs.readFileSync(PUB, 'utf8').replace("const SERVER_URL='';", "const SERVER_URL='" + url + "';");
      r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return r.end(html);
    } catch (e) {}
  }
  r.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); r.end('Tierras Rúnicas: servidor activo');
});
const wss = new WebSocketServer({ server, maxPayload: 40000 });
const send = (w, o) => { if (w.readyState === 1) w.send(JSON.stringify(o)); };
const bcast = (o, except) => { const s = JSON.stringify(o); for (const c of wss.clients) if (c !== except && c.readyState === 1) c.send(s); };
const pub = c => ({ id: c.id, n: c.n, c: c.c, lv: c.lv, x: c.x, y: c.y, dr: c.dr, f: c.f, gm: c.gm ? 1 : 0 });
let NID = 1;

wss.on('connection', ws => {
  ws.id = NID++; ws.alive = true; ws.fails = 0; ws.lastChat = 0; ws.gm = false; ws.user = '';
  ws.n = 'Jugador'; ws.c = 'm'; ws.lv = 1; ws.x = 0; ws.y = 0; ws.dr = 0; ws.f = 1; ws.seen = false;
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'w', players: [...wss.clients].filter(c => c !== ws && c.seen).map(pub) });

  const fail = msg => { ws.fails++; if (ws.fails > 8) ws.close(); return msg; };

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'p') {                                   // posición
      ws.n = String(m.n || 'Jugador').replace(/\[\s*gm\s*\]/ig, '').trim().slice(0, 16) || 'Jugador';
      ws.c = ['m', 'd', 'g'].includes(m.c) ? m.c : 'm';
      ws.lv = clamp(m.lv, 1, 999); ws.x = clamp(m.x, -2000, 2000); ws.y = clamp(m.y, -2000, 2000);
      ws.dr = clamp(m.dr, 0, 7) | 0; ws.f = m.f === -1 ? -1 : 1; ws.seen = true;
      bcast({ t: 'p', ...pub(ws), a: m.a ? 1 : 0 }, ws);
    } else if (m.t === 'm') {                            // chat
      const now = Date.now(); if (now - ws.lastChat < 700) return; ws.lastChat = now;
      const txt = String(m.txt || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 80); if (!txt) return;
      bcast({ t: 'm', id: ws.id, n: ws.n, txt }, ws);
    } else if (m.t === 'gm') {                           // verificar GM en el servidor
      if (KEY && same(m.k || '', KEY)) ws.gm = true; else fail();
    } else if (m.t === 'login') {                        // entrar o crear cuenta
      const u = String(m.u || '').trim().toLowerCase(), pw = String(m.pw || '');
      if (!/^[a-z0-9_]{3,16}$/.test(u)) return send(ws, { t: 'err', msg: 'Usuario: 3-16 letras, números o _' });
      if (pw.length < 4 || pw.length > 64) return send(ws, { t: 'err', msg: 'La clave debe tener 4 o más caracteres' });
      let a = DB.users[u], created = false;
      if (!a) {
        if (Object.keys(DB.users).length >= 5000) return send(ws, { t: 'err', msg: 'Servidor lleno' });
        const salt = crypto.randomBytes(16).toString('hex');
        a = DB.users[u] = { salt, h: hash(pw, salt), tks: [], save: null, bak: null, bakt: 0 }; created = true;
      } else if (!same(hash(pw, a.salt), a.h)) return send(ws, { t: 'err', msg: fail('Clave incorrecta') });
      const tk = crypto.randomBytes(24).toString('hex'); a.tks.push(tk); if (a.tks.length > 5) a.tks.shift();
      ws.user = u; markDirty();
      send(ws, { t: 'ok', u, tk, save: a.save, created });
    } else if (m.t === 'tok') {                          // volver a entrar sin clave
      const u = String(m.u || '').toLowerCase(), a = DB.users[u];
      if (!a || !a.tks.some(t => same(t, m.tk || ''))) return send(ws, { t: 'err', msg: fail('Sesión caducada, vuelve a entrar'), bad: 1 });
      ws.user = u; send(ws, { t: 'ok', u, save: a.save });
    } else if (m.t === 'save') {                         // guardar progreso
      const a = ws.user && DB.users[ws.user], d = m.d;
      if (!a || !d || typeof d !== 'object' || !['m', 'd', 'g'].includes(d.c) || !(d.lv >= 1 && d.lv <= 999)) return;
      const s = JSON.stringify(d); if (s.length > 30000) return;
      const now = Date.now();
      if (a.save && now - a.bakt > 600000) { a.bak = a.save; a.bakt = now; }   // copia de respaldo cada 10 min
      a.save = d; markDirty();
    }
  });

  ws.on('close', () => { if (ws.seen) bcast({ t: 'l', id: ws.id }); });
  ws.on('error', () => {});
});

setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); } }, 30000);
server.listen(PORT, () => console.log('Servidor de Tierras Rúnicas en el puerto ' + PORT + (KEY ? '' : '  (sin ADMIN_KEY: nadie será GM)')));
