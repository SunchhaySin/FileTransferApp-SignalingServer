import http from "http";
import { WebSocketServer } from "ws";
import { randomUUID } from "crypto";

const HEARTBEAT_MS = 15_000;
const SESSION_TTL_MS = 24 * 60 * 60 * 1000; // forget unused reconnect tokens after 24h
const RELAYED_TYPES = ["offer", "answer", "ice", "reject"];

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("OK");
    return;
  }

  // Fallback for non-existent HTTP routes
  res.writeHead(404);
  res.end();
});

// Attach WebSocketServer to the HTTP server
const wss = new WebSocketServer({
  server,
  maxPayload: 64 * 1024, // SDP fits nicely in 64 KB
  verifyClient: (info, callback) => {
    const origin = info.origin;
    const allowedOrigins = [
      "https://file-transfer-web-pi.vercel.app",
      "http://localhost:5173", // Dev mode
    ];

    // Allow connections if origin matches or during server-to-server tests
    if (!origin || allowedOrigins.some((o) => origin.startsWith(o))) {
      callback(true);
    } else {
      callback(false, 403, "Forbidden Origin");
    }
  },
}); 

const peers = new Map(); // id    -> { ws, name }            (online devices)
const sessions = new Map(); // token -> { id, lastSeen }        (private reconnect tokens)

const send = (ws, obj) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj)); // 1 = OPEN
};

const broadcastPeers = () => {
  const list = [...peers].map(([id, p]) => ({ id, name: p.name }));
  peers.forEach((p) => send(p.ws, { type: "peers", list }));
};

wss.on("connection", (ws) => {
  let id = null; // set once the client registers
  ws.isAlive = true;

  // React to the ping request from heartbeat reconnection
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw, isBinary) => {
    if (isBinary) return; // signaling is JSON text only

    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed messages instead of crashing
    }

    if (!msg || typeof msg !== "object") return;

    if (msg.type === "register") {
      // Returning device: reuse its ID if the token is valid. Otherwise it is a new device.
      let token = typeof msg.token === "string" ? msg.token : null;
      const session = token ? sessions.get(token) : null;

      const holder = session ? peers.get(session.id) : null;
      const inUse = holder && holder.ws !== ws && holder.ws.readyState === 1;

      if (session && !inUse) {
        id = session.id; // returning device
      } else {
        id = randomUUID(); // new device, or a copied token whose socket is still live
        token = randomUUID();
        sessions.set(token, { id, lastSeen: Date.now() });
      }
      sessions.get(token).lastSeen = Date.now();

      // Same device reconnecting: drop its old socket
      const old = peers.get(id);
      if (old && old.ws !== ws) old.ws.terminate();

      peers.set(id, { ws, name: String(msg.name || "Unknown").slice(0, 50) });
      console.log("registered:", id, `(${peers.size} online)`);

      send(ws, { type: "registered", id, token }); // send token back to the connected device under this specified id
      broadcastPeers();
      return;
    }

    if (msg.type === "rename") {
      const p = id ? peers.get(id) : null;
      if (p && p.ws === ws) {
        p.name = String(msg.name || "Unknown").slice(0, 50);
        broadcastPeers();
      }
      return;
    }

    // Everything else requires being registered first
    if (!id) return;

    if (
      RELAYED_TYPES.includes(msg.type) &&
      typeof msg.to === "string" &&
      msg.to !== id
    ) {
      const target = peers.get(msg.to);
      if (target) send(target.ws, { ...msg, from: id }); // `from` is set by the server, never trusted from the client
    }
  });

  ws.on("close", () => {
    // Only remove the peer if this socket is still the current one for that ID
    if (id && peers.get(id)?.ws === ws) {
      console.log("client disconnected:", id);
      peers.delete(id);
      broadcastPeers();
    }
  });

  ws.on("error", (err) => console.error("socket error:", err.message));
});

// Heartbeat: drop clients that vanished without a close frame (Wi-Fi drop, phone sleep)
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate(); // triggers 'close'
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_MS);

// Forget reconnect tokens for devices that have been offline for a long time
setInterval(
  () => {
    const now = Date.now();
    for (const [token, s] of sessions) {
      if (!peers.has(s.id) && now - s.lastSeen > SESSION_TTL_MS)
        sessions.delete(token);
    }
  },
  60 * 60 * 1000,
);
const PORT = process.env.PORT || 8080;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Signaling server running on ${PORT}`);
});
