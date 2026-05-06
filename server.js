const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

app.use(express.static("public"));

/*
STATE
----------------------------------
waitingQueue → users waiting for match
pairs → socket.id -> partner socket.id
*/

const waitingQueue = [];
const pairs = new Map();

/*
UTILS
----------------------------------
*/

function pairUsers(a, b) {
  pairs.set(a.id, b.id);
  pairs.set(b.id, a.id);

  a.emit("connected");
  b.emit("connected");
}

function unpair(socket) {
  const partnerId = pairs.get(socket.id);

  if (partnerId) {
    const partner = io.sockets.sockets.get(partnerId);

    if (partner) {
      partner.emit("disconnected");
      pairs.delete(partner.id);
    }

    pairs.delete(socket.id);
  }
}

function removeFromQueue(socket) {
  const index = waitingQueue.indexOf(socket);
  if (index !== -1) waitingQueue.splice(index, 1);
}

/*
MATCHMAKING
----------------------------------
*/

function findPartner(socket) {
  // remove if already waiting
  removeFromQueue(socket);

  // try match
  if (waitingQueue.length > 0) {
    const partner = waitingQueue.shift();
    pairUsers(socket, partner);
  } else {
    waitingQueue.push(socket);
    socket.emit("finding");
  }
}

/*
SOCKET
----------------------------------
*/

io.on("connection", (socket) => {
  console.log("connect:", socket.id);

  socket.on("find", () => {
    unpair(socket);
    findPartner(socket);
  });

  socket.on("message", (msg) => {
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;

    const partner = io.sockets.sockets.get(partnerId);
    if (partner) {
      partner.emit("message", msg);
    }
  });

  socket.on("next", () => {
    unpair(socket);
    findPartner(socket);
  });

  socket.on("disconnect", () => {
    unpair(socket);
    removeFromQueue(socket);
    console.log("disconnect:", socket.id);
  });
});

/*
START
----------------------------------
*/

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("Running on port", PORT);
});