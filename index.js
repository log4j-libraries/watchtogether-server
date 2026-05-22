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

// rooms: { roomId: { host: ws, guests: Set<ws>, state: { url, playing, currentTime } } }
const rooms = new Map();

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

function send(ws, message) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(message));
  }
}

wss.on('connection', (ws) => {
  ws.roomId = null;
  ws.isHost = false;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // Host creates a room
      case 'create_room': {
        const roomId = uuidv4().slice(0, 6).toUpperCase();
        const room = {
          host: ws,
          guests: new Set(),
          state: { url: '', playing: false, currentTime: 0 }
        };
        rooms.set(roomId, room);
        ws.roomId = roomId;
        ws.isHost = true;
        send(ws, { type: 'room_created', roomId });
        console.log(`Room created: ${roomId}`);
        break;
      }

      // Guest joins a room
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
        // Send current state to new guest
        send(ws, { type: 'joined', roomId, state: room.state });
        // Notify host
        send(room.host, { type: 'guest_joined' });
        console.log(`Guest joined room: ${roomId}`);
        break;
      }

      // Host sets video URL
      case 'set_url': {
        const room = rooms.get(ws.roomId);
        if (!room || !ws.isHost) return;
        room.state.url = msg.url;
        room.state.playing = false;
        room.state.currentTime = 0;
        broadcast(room, { type: 'set_url', url: msg.url }, ws);
        break;
      }

      // Play/pause sync
      case 'play':
      case 'pause': {
        const room = rooms.get(ws.roomId);
        if (!room || !ws.isHost) return;
        room.state.playing = msg.type === 'play';
        room.state.currentTime = msg.currentTime || 0;
        broadcast(room, { type: msg.type, currentTime: room.state.currentTime }, ws);
        break;
      }

      // Seek sync
      case 'seek': {
        const room = rooms.get(ws.roomId);
        if (!room || !ws.isHost) return;
        room.state.currentTime = msg.currentTime;
        broadcast(room, { type: 'seek', currentTime: msg.currentTime }, ws);
        break;
      }

      // Chat message
      case 'chat': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        const name = ws.isHost ? 'Хост' : 'Гость';
        broadcast(room, { type: 'chat', name, text: msg.text });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.isHost) {
      // Notify guests and close room
      broadcast(room, { type: 'host_left' });
      rooms.delete(ws.roomId);
      console.log(`Room closed: ${ws.roomId}`);
    } else {
      room.guests.delete(ws);
      send(room.host, { type: 'guest_left' });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Rave server running on http://localhost:${PORT}`);
});
