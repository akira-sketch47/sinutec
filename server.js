// SINUTEC - servidor repassador de mensagens (salas via WebSocket)
// Não guarda estado do jogo: só liga o dono da sala aos convidados.
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;
const rooms = new Map(); // codigo -> { host: ws, guests: Map(id -> ws), latestState }

const srv = http.createServer((q, r) => { r.writeHead(200, { 'Content-Type': 'text/plain' }); r.end('sinutec ok'); });
const wss = new WebSocketServer({ server: srv, maxPayload: 1 << 20 });
const tx = (w, o) => { if (w && w.readyState === 1) w.send(JSON.stringify(o)); };
setInterval(() => {
  for (const r of rooms.values()) {
    const state = r.latestState;
    if (!state) continue;
    r.latestState = null;
    r.guests.forEach(g => {
      if (g.readyState === 1 && g.bufferedAmount < 32768) tx(g, state);
    });
  }
}, 16);

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => (ws.alive = true));
  ws.on('message', raw => {
    let o; try { o = JSON.parse(raw); } catch { return; }
    const c = String(o.code || '');
    if (o.k === 'host') {
      if (ws.room) return;
      if (!/^[A-Z0-9]{5}$/.test(c)) return tx(ws, { k: 'err', e: 'bad-code' });
      if (rooms.has(c) || rooms.size >= 300) return tx(ws, { k: 'err', e: 'unavailable-id' });
      ws.room = { c, host: true };
      rooms.set(c, { host: ws, guests: new Map(), latestState: null });
      tx(ws, { k: 'ok' });
    } else if (o.k === 'join') {
      if (ws.room) return;
      const r = rooms.get(c);
      if (!r) return tx(ws, { k: 'err', e: 'peer-unavailable' });
      if (r.guests.size >= 8) return tx(ws, { k: 'err', e: 'full' });
      const id = 'p' + Math.random().toString(36).slice(2, 8);
      ws.room = { c, id };
      r.guests.set(id, ws);
      tx(ws, { k: 'joined', id });
      tx(r.host, { k: 'g+', id });
    } else if (o.k === 'd' && ws.room) {
      const r = rooms.get(ws.room.c); if (!r) return;
      if (ws.room.host) {
        const out = { k: 'd', m: o.m };
        if (o.m && o.m.t === 'st' && o.m.mv) r.latestState = out;
        else {
          if (o.m && o.m.t === 'st') r.latestState = null;
          if (o.to) tx(r.guests.get(o.to), out); else r.guests.forEach(g => tx(g, out));
        }
      } else tx(r.host, { k: 'd', from: ws.room.id, m: o.m });
    } else if (o.k === 'kick' && ws.room && ws.room.host) {
      const r = rooms.get(ws.room.c), g = r && r.guests.get(o.id); if (g) g.close();
    }
  });
  ws.on('close', () => {
    const x = ws.room; if (!x) return;
    const r = rooms.get(x.c); if (!r) return;
    if (x.host) { rooms.delete(x.c); r.guests.forEach(g => g.close()); }
    else if (r.guests.delete(x.id)) tx(r.host, { k: 'g-', id: x.id });
  });
});
// ping a cada 25s: mantém a conexão viva atrás do proxy e derruba conexões mortas
setInterval(() => wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); }), 25000);
srv.listen(PORT, () => console.log('SINUTEC relay na porta', PORT));
