const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  pingTimeout: 30000,
  pingInterval: 10000,
});

app.use(express.json());

// Serve the frontend
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});
app.use(express.static(path.join(__dirname, "public")));

// ─── In-memory state ────────────────────────────────────────────────────────

// socketId → { userId, partnerId|null, inQueue }
const users = new Map();

// waiting queue: array of socketIds
const queue = [];

// roomId → { userA, userB } (socketIds)
const rooms = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────

function getOnlineCount() {
  return users.size;
}

function broadcastOnlineCount() {
  io.emit("online_count", { count: getOnlineCount() });
}

function removeFromQueue(socketId) {
  const idx = queue.indexOf(socketId);
  if (idx !== -1) queue.splice(idx, 1);
}

function matchUsers(socketIdA, socketIdB) {
  const roomId = uuidv4();
  rooms.set(roomId, { userA: socketIdA, userB: socketIdB });

  const userA = users.get(socketIdA);
  const userB = users.get(socketIdB);
  if (userA) { userA.partnerId = socketIdB; userA.roomId = roomId; userA.inQueue = false; }
  if (userB) { userB.partnerId = socketIdA; userB.roomId = roomId; userB.inQueue = false; }

  io.to(socketIdA).emit("matched", { roomId });
  io.to(socketIdB).emit("matched", { roomId });
}

function disconnectFromPartner(socketId) {
  const user = users.get(socketId);
  if (!user || !user.partnerId) return;

  const partnerId = user.partnerId;
  const partner = users.get(partnerId);

  // Clean up room
  if (user.roomId) rooms.delete(user.roomId);

  // Notify partner
  if (partner) {
    io.to(partnerId).emit("stranger_left");
    partner.partnerId = null;
    partner.roomId = null;
    partner.inQueue = false;
  }

  user.partnerId = null;
  user.roomId = null;
  user.inQueue = false;
}

// ─── REST: status endpoint (for health checks) ───────────────────────────────

app.get("/api/status", (req, res) => {
  res.json({ online: getOnlineCount(), queued: queue.length });
});

// ─── Socket.io ───────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  // Register user
  users.set(socket.id, {
    userId: uuidv4(),
    partnerId: null,
    roomId: null,
    inQueue: false,
  });

  broadcastOnlineCount();

  // ── Find a stranger ──────────────────────────────────────────────────────
  socket.on("find", () => {
    const user = users.get(socket.id);
    if (!user) return;

    // Already in a chat — disconnect first
    if (user.partnerId) disconnectFromPartner(socket.id);

    // Already in queue — ignore
    if (user.inQueue) return;

    if (queue.length > 0) {
      const partnerId = queue.shift();
      const partner = users.get(partnerId);
      if (!partner) {
        // Stale entry — try again next tick
        user.inQueue = true;
        queue.push(socket.id);
        return;
      }
      matchUsers(socket.id, partnerId);
    } else {
      user.inQueue = true;
      queue.push(socket.id);
      socket.emit("waiting");
    }
  });

  // ── Send message ─────────────────────────────────────────────────────────
  socket.on("message", (data) => {
    const user = users.get(socket.id);
    if (!user || !user.partnerId) return;

    const text = (data.text || "").toString().trim().slice(0, 2000);
    if (!text) return;

    io.to(user.partnerId).emit("message", {
      from: "stranger",
      text,
      ts: Date.now(),
    });
  });

  // ── Typing indicator ─────────────────────────────────────────────────────
  socket.on("typing", (data) => {
    const user = users.get(socket.id);
    if (!user || !user.partnerId) return;
    io.to(user.partnerId).emit("typing", { typing: !!data.typing });
  });

  // ── Skip / leave ──────────────────────────────────────────────────────────
  socket.on("leave", () => {
    const user = users.get(socket.id);
    if (!user) return;
    removeFromQueue(socket.id);
    disconnectFromPartner(socket.id);
    socket.emit("left");
  });

  // ── Disconnect ────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    removeFromQueue(socket.id);
    disconnectFromPartner(socket.id);
    users.delete(socket.id);
    broadcastOnlineCount();
  });
});

// ─── Start ───────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`yooo Void running on http://localhost:${PORT}`);
});
