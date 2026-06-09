const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// Хранилище: комнаты -> { messages, users }
const rooms = new Map();

io.on('connection', (socket) => {
  let currentRoom = null;
  let nickname = null;

  socket.on('join', ({ nickname: name, room }) => {
    if (currentRoom) socket.leave(currentRoom);
    socket.join(room);
    currentRoom = room;
    nickname = name;

    // Инициализируем комнату, если её нет
    if (!rooms.has(room)) {
      rooms.set(room, { messages: [], users: new Set() });
    }
    const roomData = rooms.get(room);
    roomData.users.add(nickname);

    // Отправить историю новому пользователю
    socket.emit('history', roomData.messages.slice(-50));
    // Отправить текущий список пользователей
    io.to(room).emit('user_list', Array.from(roomData.users));
    // Оповестить остальных о новом пользователе
    socket.to(room).emit('message', {
      nickname: 'system',
      text: `${nickname} присоединился к комнате`,
      timestamp: Date.now()
    });
  });

  socket.on('message', (text) => {
    if (!currentRoom || !nickname) return;
    const roomData = rooms.get(currentRoom);
    const msg = {
      nickname,
      text,
      timestamp: Date.now()
    };
    roomData.messages.push(msg);
    // Ограничим историю 50 сообщениями
    while (roomData.messages.length > 50) roomData.messages.shift();
    io.to(currentRoom).emit('message', msg);
  });

  socket.on('disconnect', () => {
    if (currentRoom && nickname) {
      const roomData = rooms.get(currentRoom);
      if (roomData) {
        roomData.users.delete(nickname);
        io.to(currentRoom).emit('user_list', Array.from(roomData.users));
        socket.to(currentRoom).emit('message', {
          nickname: 'system',
          text: `${nickname} покинул комнату`,
          timestamp: Date.now()
        });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Node.js чат на http://localhost:${PORT}`));
