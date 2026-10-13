'use strict';
/*
  Smiley & Stars room server.
  Players join a room by name. Each player has a small "presence" object
  (who they are, what they picked, the host's game snapshot, a friend's
  direction). The server keeps it and relays every change to the rest of
  the room. All game logic runs in the host's browser.
*/
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_ROOM = 8;            // players per room
const MAX_ROOMS = 500;
const MAX_MSG = 48 * 1024;     // bytes per message
const MAX_RATE = 80;           // messages per second per player

const rooms = new Map();       // room name -> Map(peer id -> client)
let nextId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(`Smiley & Stars server is up. Rooms: ${rooms.size}`);
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}
function broadcast(room, msg, except) {
  const data = JSON.stringify(msg);
  for (const c of room.values()) if (c !== except && c.ws.readyState === 1) c.ws.send(data);
}
function leaveRoom(c) {
  if (!c.roomName) return;
  const room = rooms.get(c.roomName);
  c.roomName = null;
  if (!room) return;
  room.delete(c.id);
  if (room.size === 0) rooms.delete(c.roomNameLast);
  else broadcast(room, { t: 'leave', peer: c.id });
}

wss.on('connection', ws => {
  const c = { id: 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 7), ws, roomName: null, roomNameLast: null, presence: {}, alive: true, count: 0, windowStart: Date.now() };
  ws.client = c;
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => {
    const t = Date.now();
    if (t - c.windowStart > 1000) { c.windowStart = t; c.count = 0; }
    if (++c.count > MAX_RATE) return;
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join') {
      const name = typeof m.room === 'string' ? m.room.slice(0, 40) : '';
      if (!name || c.roomName) return;
      let room = rooms.get(name);
      if (!room) {
        if (rooms.size >= MAX_ROOMS) { send(ws, { t: 'error', msg: 'busy' }); return; }
        room = new Map();
        rooms.set(name, room);
      }
      if (room.size >= MAX_ROOM) { send(ws, { t: 'error', msg: 'full' }); return; }
      room.set(c.id, c);
      c.roomName = name; c.roomNameLast = name;
      send(ws, { t: 'joined', peer: c.id, peers: [...room.values()].map(p => ({ peer: p.id, presence: p.presence })) });
      broadcast(room, { t: 'peer', peer: c.id, presence: c.presence }, c);
    } else if (m.t === 'presence') {
      const room = c.roomName && rooms.get(c.roomName);
      if (!room || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
      Object.assign(c.presence, m.patch);
      broadcast(room, { t: 'presence', peer: c.id, patch: m.patch }, c);
    } else if (m.t === 'leave') {
      leaveRoom(c);
    }
  });
  ws.on('close', () => leaveRoom(c));
  ws.on('error', () => {});
});

// Drop connections that stopped answering, and keep idle ones alive through proxies
setInterval(() => {
  for (const ws of wss.clients) {
    const c = ws.client;
    if (!c.alive) { ws.terminate(); continue; }
    c.alive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log(`Smiley & Stars server listening on ${PORT}`));
