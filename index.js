const express = require('express');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static client files
app.use(express.static(path.join(__dirname, '../client/dist')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../client/dist/index.html'));
});

// rooms: { roomId: { host: ws, guests: Set<ws> } }
const rooms = new Map();

function send(ws, message) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(message));
  }
}

function broadcast(room, message, excludeWs = null) {
  const data = JSON.stringify(message);
  if (room.host && room.host !== excludeWs && room.host.readyState === 1) {
    room.host.send(data);
  }
  for (const guest of room.guests) {
    if (guest !== excludeWs && guest.readyState === 1) {
      guest.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  ws.roomId = null;
  ws.isHost = false;
  ws.peerId = uuidv4();

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'create_room': {
        const roomId = uuidv4().slice(0, 6).toUpperCase();
        rooms.set(roomId, { host: ws, guests: new Set() });
        ws.roomId = roomId;
        ws.isHost = true;
        send(ws, { type: 'room_created', roomId, peerId: ws.peerId });
        console.log(`Room created: ${roomId}`);
        break;
      }

      case 'join_room': {
        const { roomId } = msg;
        const room = rooms.get(roomId);
        if (!room) {
          send(ws, { type: 'error', message: 'Комната не найдена' });
          return;
        }
        room.guests.add(ws);
        ws.roomId = roomId;
        ws.isHost = false;
        send(ws, { type: 'joined', roomId, peerId: ws.peerId });
        // Tell host a new guest joined with their peerId
        send(room.host, { type: 'guest_joined', peerId: ws.peerId });
        console.log(`Guest joined room: ${roomId}`);
        break;
      }

      // WebRTC signaling - relay offer/answer/ice between peers
      case 'offer': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        // Host sends offer to a specific guest, or broadcast to all guests
        if (msg.targetPeerId) {
          for (const guest of room.guests) {
            if (guest.peerId === msg.targetPeerId) {
              send(guest, { type: 'offer', sdp: msg.sdp, fromPeerId: ws.peerId });
              break;
            }
          }
        } else {
          for (const guest of room.guests) {
            send(guest, { type: 'offer', sdp: msg.sdp, fromPeerId: ws.peerId });
          }
        }
        break;
      }

      case 'answer': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        // Guest sends answer back to host
        send(room.host, { type: 'answer', sdp: msg.sdp, fromPeerId: ws.peerId });
        break;
      }

      case 'ice': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        if (msg.targetPeerId) {
          // Send to specific peer
          const allPeers = [room.host, ...room.guests];
          for (const peer of allPeers) {
            if (peer.peerId === msg.targetPeerId) {
              send(peer, { type: 'ice', candidate: msg.candidate, fromPeerId: ws.peerId });
              break;
            }
          }
        } else {
          broadcast(room, { type: 'ice', candidate: msg.candidate, fromPeerId: ws.peerId }, ws);
        }
        break;
      }

      case 'ping':
        send(ws, { type: 'pong' });
        break;

      case 'chat': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        const name = ws.isHost ? 'Хост' : 'Гость';
        broadcast(room, { type: 'chat', name, text: msg.text });
        break;
      }

      case 'host_status': {
        // Host broadcasts sharing status to guests
        const room = rooms.get(ws.roomId);
        if (!room || !ws.isHost) return;
        broadcast(room, { type: 'host_status', sharing: msg.sharing }, ws);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.isHost) {
      broadcast(room, { type: 'host_left' });
      rooms.delete(ws.roomId);
      console.log(`Room closed: ${ws.roomId}`);
    } else {
      room.guests.delete(ws);
      send(room.host, { type: 'guest_left', peerId: ws.peerId });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`WatchTogether server running on http://localhost:${PORT}`);
});

// Server-side keepalive — ping all clients every 25 seconds
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) {
      ws.ping();
    }
  });
}, 25000);
