// Shared test harness: HTTP helpers plus a minimal Socket.IO v4 client built on
// `ws` (a transitive dep of socket.io — no new packages). Just enough of the
// protocol to connect, emit events with acknowledgement callbacks, and listen
// for server-pushed events. Extracted so integration tests can drive the real
// server without each file re-implementing the client.
const http = require("node:http");
const WebSocket = require("ws");

function get(port, reqPath) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path: reqPath }, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => resolve({ status: res.statusCode, body }));
    });
    req.on("error", reject);
    req.setTimeout(2000, () => req.destroy(new Error("request timeout")));
  });
}

async function waitForHealth(port, timeoutMs = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await get(port, "/health");
      if (r.status === 200) return;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 150));
  }
  throw new Error(`server on port ${port} did not become healthy in time`);
}

function postJson(port, reqPath, obj) {
  return new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(obj));
    const req = http.request(
      { host: "127.0.0.1", port, path: reqPath, method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": data.length } },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => resolve({ status: res.statusCode, body: body ? JSON.parse(body) : null }));
      }
    );
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

class IOClient {
  constructor(port, token) {
    this.port = port;
    this.authToken = token || null; // session token → carried in the SIO CONNECT packet
    this.ackId = 0;
    this.acks = new Map();
    this.handlers = new Map();
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`ws://127.0.0.1:${this.port}/socket.io/?EIO=4&transport=websocket`);
      this.ws.on("error", reject);
      this.ws.on("message", (data) => this._onMessage(data.toString(), resolve));
    });
  }

  _onMessage(msg, onConnect) {
    const eio = msg[0];
    // engine open → SIO connect, attaching auth so the server populates handshake.auth
    if (eio === "0") { this.ws.send("40" + (this.authToken ? JSON.stringify({ token: this.authToken }) : "")); return; }
    if (eio === "2") { this.ws.send("3"); return; }  // ping → pong
    if (eio !== "4") return;                          // only message packets matter

    const sio = msg[1];
    const rest = msg.slice(2);
    if (sio === "0") { onConnect?.(this); return; }   // SIO CONNECT acknowledged
    if (sio === "2") {                                 // EVENT
      const payload = JSON.parse(rest.slice(rest.indexOf("[")));
      const [event, ...args] = payload;
      (this.handlers.get(event) || []).slice().forEach((h) => h(...args));
    } else if (sio === "3") {                          // ACK (server callback reply)
      const br = rest.indexOf("[");
      const id = Number(rest.slice(0, br));
      const payload = JSON.parse(rest.slice(br));
      const cb = this.acks.get(id);
      if (cb) { this.acks.delete(id); cb(...payload); }
    }
  }

  emit(event, arg) {
    return new Promise((resolve) => {
      const id = this.ackId++;
      this.acks.set(id, resolve);
      const body = JSON.stringify(arg === undefined ? [event] : [event, arg]);
      this.ws.send(`42${id}${body}`);
    });
  }

  on(event, handler) {
    if (!this.handlers.has(event)) this.handlers.set(event, []);
    this.handlers.get(event).push(handler);
  }

  once(event, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}"`)), timeoutMs);
      const h = (...args) => {
        clearTimeout(timer);
        const arr = this.handlers.get(event);
        arr.splice(arr.indexOf(h), 1);
        resolve(args.length <= 1 ? args[0] : args);
      };
      this.on(event, h);
    });
  }

  close() { try { this.ws?.close(); } catch { /* ignore */ } }
}

// Sign in (dev-login, non-prod only) and return a connected, AUTHENTICATED
// client — required for hosting a room. Each call uses a distinct account.
let devUserSeq = 0;
async function authedClient(port, email) {
  const who = email || `host${++devUserSeq}@test.dev`;
  const res = await postJson(port, "/auth/dev-login", { email: who });
  if (!res.body?.ok) throw new Error("dev-login failed: " + JSON.stringify(res.body));
  const c = new IOClient(port, res.body.token);
  await c.connect();
  return c;
}

module.exports = { get, postJson, waitForHealth, IOClient, authedClient };
