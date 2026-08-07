const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");
const { v4: uuidv4 } = require("uuid");
const Redis = require("ioredis");

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || "development";
const IS_PROD = NODE_ENV === "production";

// ── Structured logging ──────────────────────────────────────────────────────
// One JSON object per line so Railway's log tail stays grep/parse-friendly.
function logEvent(level, event, fields = {}) {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields });
  if (level === "error" || level === "fatal") console.error(line);
  else console.log(line);
}

// Process-level safety nets — capture crashes instead of dying silently.
process.on("uncaughtException", (err) => {
  logEvent("error", "uncaught_exception", { message: err && err.message, stack: err && err.stack });
});
process.on("unhandledRejection", (reason) => {
  logEvent("error", "unhandled_rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
    stack: reason && reason.stack
  });
});

const ROOM_EXPIRY_MS = 2 * 60 * 60 * 1000;
const MAX_PLAYERS = 20;
const MAX_GUESSES = 6;
const WORD_LENGTH = 5;
// Hint cost (server-authoritative, never sent to the client). Revealing the
// chooser's hint subtracts from the guesser's round score. The cost is locked
// in at reveal time and decays as the player keeps battling on their own.
const HINT_PENALTY_BASE = 250; // cost if revealed before any guess
const HINT_PENALTY_STEP = 20;  // cost drops by this for each guess already made
// How long the FINAL round's appreciation + leaderboard stays up before the
// final scoreboard replaces it. Long enough to read the meaning and standings.
const ROUND_END_DELAY = 8000;
// For non-final rounds we don't force everyone off the leaderboard. Instead, once
// the appreciation has played we quietly announce the next chooser so they can
// start picking. Each player leaves the leaderboard on their own — by tapping to
// continue, or automatically the moment the next round actually starts.
const ROUND_REVEAL_DELAY = 4000;
// A chooser who hasn't locked in a word within this window is automatically
// passed over so one slow picker can't stall the whole room. The picker can also
// hand off early (game:passTurn). A skipped player keeps their place in the
// rotation and is asked again on the next full cycle. Overridable for tuning/tests.
const CHOOSER_PICK_SECONDS = Math.max(10, Number(process.env.CHOOSER_PICK_SECONDS) || 180);

const MW_API_KEY = process.env.MW_API_KEY;
if (!MW_API_KEY) {
  if (IS_PROD) {
    // In production the dictionary key is mandatory — fail fast and loud.
    logEvent("fatal", "missing_mw_api_key", {
      message: "MW_API_KEY is not set. Get a free key at https://dictionaryapi.com/register/index"
    });
    process.exit(1);
  }
  // In development we fail-open: words are accepted unverified and definitions skipped.
  logEvent("warn", "missing_mw_api_key", {
    message: "MW_API_KEY not set — word validation falls back to fail-open. Get a free key at https://dictionaryapi.com/register/index"
  });
}
const wordCache = new Map();
const CACHE_MAX_SIZE = 2000;

// ── Persistence (Redis snapshot/restore) ─────────────────────────────────────
// The in-memory `rooms` Map is the runtime source of truth. Redis is a side
// channel: we snapshot serialized room state on an interval (and on SIGTERM),
// and restore it on boot, so active games survive a Railway redeploy/restart.
// Entirely optional — if REDIS_URL is unset, the server runs in-memory exactly
// as before (and every Redis call is a no-op). Failures never break gameplay.
const REDIS_URL = process.env.REDIS_URL;
const REDIS_STATE_KEY = "wordwars:state";
const SNAPSHOT_INTERVAL_MS = 5000;
let redis = null;
if (REDIS_URL) {
  redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 2,
    retryStrategy: (times) => Math.min(times * 200, 2000)
  });
  // Handle errors so a Redis outage logs but never crashes the game server.
  redis.on("error", (err) => logEvent("error", "redis_error", { message: err && err.message }));
  redis.on("connect", () => logEvent("info", "redis_connected", {}));
  logEvent("info", "persistence_enabled", { store: "redis" });
} else {
  logEvent("warn", "persistence_disabled", {
    message: "REDIS_URL not set — room state is in-memory only and will not survive a restart/redeploy."
  });
}

// CORS origin policy. In production an explicit ALLOWED_ORIGIN is required; if it
// is missing we fall back to the Railway-provided public domain rather than a
// wildcard, and deny cross-origin entirely if neither is set. In development we
// default to "*" for convenience.
const ALLOWED_ORIGIN = (() => {
  if (!IS_PROD) return process.env.ALLOWED_ORIGIN || "*";
  if (process.env.ALLOWED_ORIGIN) return process.env.ALLOWED_ORIGIN;
  if (process.env.RAILWAY_PUBLIC_DOMAIN) {
    logEvent("warn", "allowed_origin_unset", {
      message: `ALLOWED_ORIGIN not set in production — restricting to RAILWAY_PUBLIC_DOMAIN (${process.env.RAILWAY_PUBLIC_DOMAIN}).`
    });
    return `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`;
  }
  logEvent("warn", "allowed_origin_unset", {
    message: "ALLOWED_ORIGIN and RAILWAY_PUBLIC_DOMAIN both unset in production — denying cross-origin requests."
  });
  return false;
})();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGIN,
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"],
  allowEIO3: true
});
// Runtime source of truth for all room state. Snapshotted to Redis (when
// REDIS_URL is set) so it survives a redeploy/restart; without Redis it is
// in-process only and a redeploy clears it. Reconnects within a live process —
// and after a restore — are handled via the room:rejoin flow.
const rooms = new Map();

// Honour X-Forwarded-Proto/Host behind Railway's proxy so magic-link URLs and
// the Secure cookie flag resolve to https in production.
app.set("trust proxy", 1);

// ── Email delivery for magic-link sign-in ────────────────────────────────────
// Two providers, in priority order:
//   1. Resend HTTP API   — set RESEND_API_KEY (preferred; no SMTP, no extra deps).
//   2. SMTP (nodemailer) — set SMTP_* (works with any provider, incl. Resend SMTP).
// If neither is set, sendMail returns false and the auth layer falls back to
// logging the link (dev) so local sign-in still works without an email provider.
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const MAIL_FROM = process.env.MAIL_FROM || process.env.SMTP_FROM || "WordWars <onboarding@resend.dev>";

let smtpMailer = null;
if (process.env.SMTP_HOST) {
  try {
    const nodemailer = require("nodemailer");
    smtpMailer = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: process.env.SMTP_SECURE === "true",
      auth: process.env.SMTP_USER ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } : undefined
    });
  } catch (err) {
    logEvent("error", "nodemailer_unavailable", { message: err && err.message });
  }
}

if (RESEND_API_KEY) logEvent("info", "mail_provider", { provider: "resend", from: MAIL_FROM });
else if (smtpMailer) logEvent("info", "mail_provider", { provider: "smtp", host: process.env.SMTP_HOST, from: MAIL_FROM });
else logEvent(IS_PROD ? "error" : "warn", "mail_not_configured", {
  message: "No RESEND_API_KEY or SMTP_HOST set — magic-link emails cannot be sent. Set one to enable email sign-in."
});

async function sendViaResend(to, subject, html) {
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from: MAIL_FROM, to, subject, html })
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`Resend API ${res.status}: ${detail.slice(0, 300)}`);
  }
  return true;
}

// Returns true if the message was accepted by a provider, false if no provider is
// configured. Throws only when a configured provider hard-fails with no fallback —
// the auth layer treats that as "not sent" too.
async function sendMail(to, subject, html) {
  if (RESEND_API_KEY) {
    try {
      return await sendViaResend(to, subject, html);
    } catch (err) {
      logEvent("error", "resend_send_failed", { message: err && err.message });
      if (!smtpMailer) return false; // surface honest "couldn't send" to the user
      // else fall through and try SMTP
    }
  }
  if (smtpMailer) {
    await smtpMailer.sendMail({ from: MAIL_FROM, to, subject, html });
    return true;
  }
  return false;
}

// ── Host-auth policy ─────────────────────────────────────────────────────────
// Hosting a room can require a signed-in account. Default: require it ONLY once
// we can actually email sign-in links (a provider is configured) — so while email
// is still being set up, hosting stays open and the rest of the app is fully
// usable. Override explicitly with REQUIRE_HOST_AUTH=true|false.
const mailConfigured = Boolean(RESEND_API_KEY || smtpMailer);
const REQUIRE_HOST_AUTH = (() => {
  const v = process.env.REQUIRE_HOST_AUTH;
  if (v === "true") return true;
  if (v === "false") return false;
  return mailConfigured;
})();
logEvent("info", "host_auth_policy", { requireHostAuth: REQUIRE_HOST_AUTH, mailConfigured });

const auth = require("./auth")({
  requireHostAuth: REQUIRE_HOST_AUTH,
  app, express, redis, logEvent, isProd: IS_PROD, sendMail
});

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use(express.static(__dirname));

app.get("/health", (_req, res) => {
  res.json({ status: "ok", uptime: process.uptime() });
});

app.get("/api/validate-word/:word", async (req, res) => {
  const word = String(req.params.word || "").trim().toUpperCase();
  if (!isFiveLetterWord(word)) return res.json({ valid: false, reason: "not-five-letters" });
  try {
    const valid = await isValidWord(word);
    if (valid === true) return res.json({ valid: true });
    if (valid === false) return res.json({ valid: false, reason: "not-a-common-word" });
    // null → couldn't verify (no key / MW outage). Don't block the user: allow with a notice.
    return res.json({ valid: true, unverified: true, reason: "unverified" });
  } catch {
    res.json({ valid: true, unverified: true, reason: "unverified" });
  }
});

function normalizeWord(word) {
  return String(word || "").trim().toUpperCase();
}

function isFiveLetterWord(word) {
  return /^[A-Z]{5}$/.test(normalizeWord(word));
}

// Single Merriam-Webster lookup that returns BOTH the validity verdict and a
// short definition from ONE request. Doing both in one call (instead of a
// separate validate + define round-trip) means the round-end "meaning" is
// guaranteed to be available to EVERY player whenever the word was accepted —
// and it halves our MW traffic so rate-limits never silently drop the meaning.
//
// Returns { valid, definition } where valid is tri-state:
//   true  → definitely a valid common word
//   false → definitely NOT a word (MW responded, nothing matched) — safe to cache
//   null  → could not verify (no key / HTTP error / network / timeout) — NEVER cached,
//           callers should fail-open so a transient MW outage never blocks play.
// definition is a concise sense string, or "" if none is available.
async function lookupWord(word) {
  if (!MW_API_KEY) return { valid: null, definition: "" };
  const normalized = normalizeWord(word).toLowerCase();
  if (wordCache.has(normalized)) return wordCache.get(normalized);

  try {
    const url = `https://www.dictionaryapi.com/api/v3/references/collegiate/json/${encodeURIComponent(normalized)}?key=${MW_API_KEY}`;
    const response = await fetch(url, { signal: AbortSignal.timeout(5000) });
    // HTTP error (5xx/429/etc.) is an UNKNOWN result — do not cache, fail-open.
    if (!response.ok) { logEvent("error", "mw_api_http_error", { statusCode: response.status, word: normalized }); return { valid: null, definition: "" }; }

    const data = await response.json();
    if (!Array.isArray(data) || data.length === 0 || typeof data[0] === "string") {
      // MW answered definitively: not a known word (data[0] strings are spelling suggestions).
      const result = { valid: false, definition: "" };
      cacheWord(normalized, result);
      return result;
    }

    const REJECTED_LABELS = new Set([
      "biographical name", "geographical name", "trademark",
      "abbreviation", "symbol"
    ]);

    const valid = data.some(entry => {
      if (typeof entry !== "object" || !entry.fl) return false;
      if (REJECTED_LABELS.has(entry.fl.toLowerCase())) return false;
      const hw = entry?.hwi?.hw || "";
      if (hw && hw[0] === hw[0].toUpperCase() && hw[0] !== hw[0].toLowerCase()) return false;
      return true;
    });

    // Pull a concise definition from the same payload so the round-end reveal
    // never needs a second API call.
    const defEntry = data.find(e => typeof e === "object" && Array.isArray(e.shortdef) && e.shortdef.length);
    let definition = defEntry ? String(defEntry.shortdef[0] || "").trim() : "";
    if (definition.length > 160) definition = definition.slice(0, 157).trimEnd() + "…";

    const result = { valid, definition };
    cacheWord(normalized, result);
    return result;

  } catch (err) {
    // Network failure / 5s timeout abort — UNKNOWN, do not cache, fail-open.
    logEvent("error", "mw_api_error", { word: normalized, message: err.message });
    return { valid: null, definition: "" };
  }
}

// Thin wrapper used by the guess path and the /api/validate-word route, which
// only care about the tri-state verdict.
async function isValidWord(word) {
  return (await lookupWord(word)).valid;
}

function cacheWord(key, value) {
  if (wordCache.size > CACHE_MAX_SIZE) wordCache.delete(wordCache.keys().next().value);
  wordCache.set(key, value);
}

// Display names accept what real names actually contain: any Unicode letter
// (so accents and non-Latin scripts work), digits, and the separators that show
// up in names — space, hyphen, apostrophe, period. Deliberately still excludes
// emoji, underscores and symbols, which read as impersonation/spoofing vectors
// more than names. Length is counted AFTER trimming, so "  Al  " is still short.
// Names are escaped at every render site, so this is a display rule, not a
// safety boundary.
const DISPLAY_NAME_RE = /^[\p{L}\p{M}0-9][\p{L}\p{M}0-9 '.-]{1,14}[\p{L}\p{M}0-9.]$/u;

function isDisplayName(name) {
  const trimmed = String(name || "").trim();
  // Reject a repeated separator ("A  B", "A--B") so a name can't be padded into
  // something visually misleading. Different separators in sequence are fine —
  // "J. Smith" is a real name.
  if (/([ '.-])\1/.test(trimmed)) return false;
  return DISPLAY_NAME_RE.test(trimmed);
}

function makeRoomCode() {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  do {
    code = Array.from({ length: 6 }, () => alphabet[Math.floor(Math.random() * alphabet.length)]).join("");
  } while (rooms.has(code));
  return code;
}

// Normalize a host-chosen room code: uppercase, strip anything that isn't
// a letter or digit (spaces, punctuation, etc.).
function normalizeRoomCode(raw) {
  return String(raw || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
}

// A valid custom code is 3–6 letters/digits, so it fits the join field
// (maxlength 6) and is short enough to share verbally.
function isValidCustomCode(code) {
  return /^[A-Z0-9]{3,6}$/.test(code);
}

function nowSeconds() {
  return Math.floor(Date.now() / 1000);
}

function touchRoom(room) {
  room.lastActive = Date.now();
}

function getRoomForSocket(socket) {
  const code = socket.data.roomCode;
  return code ? rooms.get(code) : null;
}

function getPublicPlayers(room) {
  return Array.from(room.players.values()).map((player) => ({
    id: player.id,
    name: player.name,
    isHost: player.id === room.hostId,
    isChooser: player.id === room.currentChooserId,
    active: player.active,
    connected: player.connected,
    cumulativeScore: player.cumulativeScore,
    roundScore: player.roundScore,
    solved: player.solved,
    guessesUsed: player.guesses.length
  }));
}

function getRoomSummary(room) {
  return {
    code: room.code,
    hostId: room.hostId,
    status: room.status,
    roundTime: room.config.roundTime,
    totalRounds: room.config.totalRounds,
    currentRound: room.currentRound,
    hintAvailable: Boolean(room.currentRoundConfig?.hint),
    currentChooserId: room.currentChooserId,
    waitingForWord: room.waitingForWord,
    playerOrder: room.playerOrder || [],
    players: getPublicPlayers(room)
  };
}

function emitPlayers(room, eventName = "room:playerJoined") {
  io.to(room.code).emit(eventName, getRoomSummary(room));
}

function findPlayer(room, socketId) {
  return room.players.get(socketId);
}

function assertHost(socket, room) {
  return room && room.hostId === socket.id;
}

function makePlayer(socket, name, isHost = false) {
  return {
    id: socket.id,
    sessionId: uuidv4(),
    name,
    connected: true,
    active: true,
    isHost,
    cumulativeScore: 0,
    roundScore: 0,
    solved: false,
    solvedAt: null,
    guesses: [],
    breakdown: null,
    usedHint: false,
    hintPenalty: 0
  };
}

function resetRoundPlayerState(room) {
  for (const player of room.players.values()) {
    player.roundScore = 0;
    player.solved = false;
    player.solvedAt = null;
    player.guesses = [];
    player.breakdown = null;
    player.usedHint = false;
    player.hintPenalty = 0;
  }
}

function makeFeedback(guess, answer) {
  const result = Array(WORD_LENGTH).fill("gray");
  const counts = {};

  for (let i = 0; i < WORD_LENGTH; i += 1) {
    if (guess[i] === answer[i]) {
      result[i] = "green";
    } else {
      counts[answer[i]] = (counts[answer[i]] || 0) + 1;
    }
  }

  for (let i = 0; i < WORD_LENGTH; i += 1) {
    if (result[i] === "green") continue;
    if (counts[guess[i]] > 0) {
      result[i] = "yellow";
      counts[guess[i]] -= 1;
    }
  }

  return result;
}

function calculateScore(room, player) {
  const elapsed = Math.max(0, nowSeconds() - room.roundStartedAt);
  const timeRemaining = Math.max(0, room.config.roundTime - elapsed);
  const unusedGuesses = Math.max(0, MAX_GUESSES - player.guesses.length);
  const base = 1000;
  const timeBonus = timeRemaining * 10;
  const guessBonus = unusedGuesses * 50;
  const penalty = player.usedHint ? player.hintPenalty : 0;
  return {
    base,
    timeRemaining,
    timeBonus,
    unusedGuesses,
    guessBonus,
    hintPenalty: penalty,
    score: Math.max(0, base + timeBonus + guessBonus - penalty)
  };
}

/*
 * CHOOSER SCORING — points per guesser per round
 * ─────────────────────────────────────────────
 * Guesser solved in 1 guess   →   0 pts  (too easy)
 * Guesser solved in 2 guesses →  100 pts
 * Guesser solved in 3 guesses →  200 pts
 * Guesser solved in 4 guesses →  300 pts
 * Guesser solved in 5 guesses →  400 pts
 * Guesser solved in 6 guesses →  500 pts
 * Guesser failed entirely     →  500 pts
 * Stump bonus (nobody solved,
 *   2+ guessers)              → +300 pts flat
 * Round cap                   → 1500 pts max
 */
function calculateChooserScore(room) {
  const guessers = Array.from(room.players.values()).filter(
    p => p.id !== room.currentChooserId && p.active
  );
  if (guessers.length === 0) return 0;

  let total = 0;
  for (const g of guessers) {
    total += g.solved
      ? Math.max(0, (g.guesses.length - 1) * 100)
      : 500;
  }

  const nonesolve = guessers.every(g => !g.solved);
  if (nonesolve && guessers.length >= 2) total += 300;

  return Math.min(total, 1500);
}

function getLiveLeaderboard(room) {
  return Array.from(room.players.values())
    .filter((player) => player.solved)
    .sort((a, b) => {
      if (b.roundScore !== a.roundScore) return b.roundScore - a.roundScore;
      return (a.solvedAt || Infinity) - (b.solvedAt || Infinity);
    })
    .map((player) => ({
      id: player.id,
      name: player.name,
      time: player.solvedAt,
      score: player.roundScore,
      guessesUsed: player.guesses.length
    }));
}

function getRoundLeaderboard(room) {
  return Array.from(room.players.values())
    // Rank by CUMULATIVE total (skribbl-style running scoreboard between rounds),
    // tie-broken by this round's score, then solve time, then name.
    .sort((a, b) => {
      if (b.cumulativeScore !== a.cumulativeScore) return b.cumulativeScore - a.cumulativeScore;
      if (b.roundScore !== a.roundScore) return b.roundScore - a.roundScore;
      if ((a.solvedAt || Infinity) !== (b.solvedAt || Infinity)) return (a.solvedAt || Infinity) - (b.solvedAt || Infinity);
      return a.name.localeCompare(b.name);
    })
    .map((player) => ({
      id: player.id,
      name: player.name,
      active: player.active,
      isChooser: player.id === room.currentChooserId,
      solved: player.solved,
      time: player.solvedAt,
      guessesUsed: player.guesses.length,
      score: player.roundScore,
      cumulativeScore: player.cumulativeScore,
      breakdown: player.breakdown || {
        base: 0,
        timeRemaining: 0,
        timeBonus: 0,
        unusedGuesses: 0,
        guessBonus: 0,
        score: 0
      }
    }));
}

function getFinalLeaderboard(room) {
  return Array.from(room.players.values())
    .sort((a, b) => b.cumulativeScore - a.cumulativeScore || a.name.localeCompare(b.name))
    .map((player) => ({
      id: player.id,
      name: player.name,
      active: player.active,
      cumulativeScore: player.cumulativeScore
    }));
}

// Cumulative standings shown on the "waiting while X chooses" screen so idle
// players watch the scoreboard instead of a spinner. Ranked by running total.
function getChoosingStandings(room) {
  return Array.from(room.players.values())
    .filter((p) => p.active)
    .map((p) => ({
      id: p.id,
      name: p.name,
      cumulativeScore: p.cumulativeScore,
      isChooser: p.id === room.currentChooserId,
      connected: p.connected
    }))
    .sort((a, b) => b.cumulativeScore - a.cumulativeScore || a.name.localeCompare(b.name));
}

// Live per-player progress for the CURRENT round (attempt counts + solves), so
// the word-picker (and anyone already done) can watch the race. Letters are
// never included — only how many guesses each player has used. Solved players
// rank first by solve time, then the rest by guesses used.
function getRoundProgress(room) {
  return Array.from(room.players.values())
    .filter((p) => p.active)
    .map((p) => ({
      id: p.id,
      name: p.name,
      isChooser: p.id === room.currentChooserId,
      solved: p.solved,
      guessesUsed: p.guesses.length,
      score: p.roundScore,
      time: p.solvedAt,
      // Who revealed the chooser's hint — visible to the word-picker and to any
      // player already done. Just the boolean; the hint TEXT is never leaked
      // here (only the player who paid for it ever receives the words).
      usedHint: p.usedHint
    }))
    .sort((a, b) => {
      if (a.isChooser !== b.isChooser) return a.isChooser ? 1 : -1; // picker last
      if (a.solved !== b.solved) return a.solved ? -1 : 1;          // solved first
      if (a.solved && b.solved) return (a.time ?? Infinity) - (b.time ?? Infinity);
      if (a.guessesUsed !== b.guessesUsed) return b.guessesUsed - a.guessesUsed;
      return a.name.localeCompare(b.name);
    });
}

// PICKER-ONLY spectator feed. The word-picker can't guess, so they get to watch
// the round they created unfold tile by tile — every guesser's letters and
// colours, live. Safe to hand over the letters here ONLY because the recipient
// already knows the answer; this payload must never reach anyone else (see
// emitProgress), or a solved player could read a struggling player's board.
function getSpectatorBoards(room) {
  return Array.from(room.players.values())
    .filter((p) => p.active && p.id !== room.currentChooserId)
    .map((p) => ({
      id: p.id,
      name: p.name,
      connected: p.connected,
      solved: p.solved,
      score: p.roundScore,
      time: p.solvedAt,
      usedHint: p.usedHint,
      rows: p.guesses.map((g) => ({ guess: g.guess, feedback: g.feedback }))
    }))
    .sort((a, b) => {
      if (a.solved !== b.solved) return a.solved ? -1 : 1;            // done first
      if (a.solved && b.solved) return (a.time ?? Infinity) - (b.time ?? Infinity);
      if (a.rows.length !== b.rows.length) return b.rows.length - a.rows.length; // furthest along first
      return a.name.localeCompare(b.name);
    });
}

// The whole live-spectator broadcast in one call: letter-free progress to the
// room, and the full tile-by-tile boards to the picker alone.
function emitProgress(room) {
  io.to(room.code).emit("game:guessProgress", { progress: getRoundProgress(room) });
  const pickerSocket = io.sockets.sockets.get(room.currentChooserId);
  if (pickerSocket) {
    pickerSocket.emit("game:spectatorBoards", { boards: getSpectatorBoards(room) });
  }
}

// Snapshot of the CURRENT round from one player's perspective, used to rebuild
// their game screen after a hard refresh mid-round. Includes THIS player's own
// guesses + feedback (so the board can be redrawn) plus their solved/hint state.
// Only the player's own letters are ever sent — never the secret word, and never
// another player's guesses. The hint text rides along only if this player has
// already revealed it, so a refresh neither re-charges the penalty nor leaks the
// hint for free.
function getActiveRoundSnapshot(room, player) {
  const chooser = room.players.get(room.currentChooserId);
  return {
    wordLength: WORD_LENGTH,
    maxGuesses: MAX_GUESSES,
    round: room.currentRound,
    totalRounds: room.config.totalRounds,
    roundTime: room.config.roundTime,
    elapsed: Math.max(0, nowSeconds() - room.roundStartedAt),
    chooserId: room.currentChooserId,
    chooserName: chooser ? chooser.name : "",
    hintAvailable: Boolean(room.currentRoundConfig.hint),
    progress: getRoundProgress(room),
    // The picker's spectator boards survive a refresh too — gated on them
    // actually being the picker, so a rejoining guesser never receives letters.
    boards: player.id === room.currentChooserId ? getSpectatorBoards(room) : null,
    // Player-specific replay data:
    guesses: player.guesses.map((g) => ({ guess: g.guess, feedback: g.feedback })),
    solved: player.solved,
    solvedAt: player.solvedAt,
    usedHint: player.usedHint,
    hint: player.usedHint ? (room.currentRoundConfig.hint || "") : ""
  };
}

function clearRoomTimers(room) {
  if (room.timerInterval) clearInterval(room.timerInterval);
  if (room.countdownInterval) clearInterval(room.countdownInterval);
  if (room.chooserInterval) clearInterval(room.chooserInterval);
  room.timerInterval = null;
  room.countdownInterval = null;
  room.chooserInterval = null;
}

function clearChooserTimer(room) {
  if (room.chooserInterval) clearInterval(room.chooserInterval);
  room.chooserInterval = null;
}

// SERVER-AUTHORITATIVE pick clock. While a room waits on the chooser, this
// broadcasts game:chooserTick every second so the picker (and the players
// watching) see the countdown; when it reaches zero the turn is automatically
// handed to the next player in the queue. Only armed when there's actually
// someone else to pass to.
function startChooserTimer(room) {
  clearChooserTimer(room);
  let remaining = CHOOSER_PICK_SECONDS;
  io.to(room.code).emit("game:chooserTick", { remaining });
  room.chooserInterval = setInterval(() => {
    remaining -= 1;
    io.to(room.code).emit("game:chooserTick", { remaining });
    if (remaining <= 0) {
      clearChooserTimer(room);
      const slow = room.players.get(room.currentChooserId);
      room.chooserSkips = (room.chooserSkips || 0) + 1; // only timeouts count toward the all-idle guard
      passChooserTurn(room, slow ? slow.name : "The picker", "timeout");
    }
  }, 1000);
}

function finishRound(room, reason = "complete") {
  if (!room || room.status !== "playing") return;
  clearRoomTimers(room);
  room.status = "round-ended";
  touchRoom(room);

  // Award chooser score BEFORE building leaderboard
  const chooserPlayer = room.players.get(room.currentChooserId);
  if (chooserPlayer) {
    const pts = calculateChooserScore(room);
    chooserPlayer.roundScore = pts;
    chooserPlayer.cumulativeScore += pts;
    chooserPlayer.breakdown = {
      base: 0, timeRemaining: 0, timeBonus: 0,
      unusedGuesses: 0, guessBonus: 0,
      chooserBonus: pts, score: pts
    };
  }

  const leaderboard = getRoundLeaderboard(room);
  io.to(room.code).emit("game:roundEnd", {
    reason,
    word: room.currentRoundConfig.word,
    definition: room.currentRoundConfig.definition || "",
    // The round is over and the answer itself is in this payload, so the hint
    // has nothing left to protect — everyone finally gets to see what the picker
    // wrote, including the players who never paid to reveal it.
    hint: room.currentRoundConfig.hint || "",
    round: room.currentRound,
    totalRounds: room.config.totalRounds,
    chooserId: room.currentChooserId,
    chooserScore: chooserPlayer ? chooserPlayer.roundScore : 0,
    leaderboard
  });

  if (room.currentRound >= room.config.totalRounds) {
    // Final round — hold on the appreciation + round leaderboard before the
    // final scoreboard replaces them, so the last round's standings are seen too.
    setTimeout(() => {
      if (!rooms.has(room.code) || room.status !== "round-ended") return;
      room.status = "session-ended";
      io.to(room.code).emit("game:sessionEnd", {
        leaderboard: getFinalLeaderboard(room)
      });
    }, ROUND_END_DELAY);
  } else {
    // Announce the next chooser once the appreciation has played. Players keep
    // studying the leaderboard until they tap to continue (→ choosing screen) or
    // until the next round starts (→ straight to the game). The next chooser can
    // begin picking right away without waiting on everyone else.
    setTimeout(() => {
      if (rooms.has(room.code) && room.status === "round-ended") {
        startRoundRobin(room);
      }
    }, ROUND_REVEAL_DELAY);
  }
}

function activeGuessers(room) {
  return Array.from(room.players.values()).filter(
    p => p.active && p.id !== room.currentChooserId
  );
}

function allActivePlayersSolved(room) {
  const activePlayers = activeGuessers(room);
  return activePlayers.length > 0 && activePlayers.every(p => p.solved);
}

// A guesser is "finished" once they've either solved the word OR used up all of
// their guesses. The round should end as soon as EVERY guesser is finished — we
// don't keep the survivors (or the picker) waiting on the round timer just
// because someone failed to solve. With a single guesser this means the round
// ends the instant they solve or exhaust their attempts.
function allActivePlayersFinished(room) {
  const activePlayers = activeGuessers(room);
  return activePlayers.length > 0 &&
    activePlayers.every(p => p.solved || p.guesses.length >= MAX_GUESSES);
}

// Reason for an all-finished round: "all-solved" only when everyone actually
// cracked it, otherwise the more accurate "all-finished".
function finishedReason(room) {
  return allActivePlayersSolved(room) ? "all-solved" : "all-finished";
}

// The round timer is SERVER-AUTHORITATIVE. This interval broadcasts the canonical
// countdown (game:timerTick) every second and, when it reaches zero, advances the
// game itself via finishRound — independent of any client action. The client's
// timer display only mirrors these ticks; it never drives round expiry.
function startRoundTimer(room) {
  room.timerInterval = setInterval(() => {
    const remaining = Math.max(0, room.config.roundTime - (nowSeconds() - room.roundStartedAt));
    io.to(room.code).emit("game:timerTick", { remaining });
    if (remaining <= 0) {
      finishRound(room, "timer");
    }
  }, 1000);
}

function startGameRound(room, roundConfig = null) {
  clearRoomTimers(room);
  room.chooserSkips = 0; // a word was locked in — clear the all-idle guard
  resetRoundPlayerState(room);

  if (roundConfig) {
    room.currentRoundConfig = roundConfig;
  }

  room.currentRound += 1;
  room.status = "playing";
  room.roundStartedAt = nowSeconds();
  touchRoom(room);

  io.to(room.code).emit("game:started", {
    wordLength: WORD_LENGTH,
    maxGuesses: MAX_GUESSES,
    hintAvailable: Boolean(room.currentRoundConfig.hint),
    round: room.currentRound,
    totalRounds: room.config.totalRounds,
    roundTime: room.config.roundTime,
    timerStart: room.roundStartedAt,
    chooserId: room.currentChooserId,
    chooserName: room.players.get(room.currentChooserId)?.name || "",
    players: getPublicPlayers(room),
    progress: getRoundProgress(room)
  });

  // Seed the picker's spectator view with the (still empty) boards so the grid
  // frame is on screen before the first guess lands.
  const pickerSocket = io.sockets.sockets.get(room.currentChooserId);
  if (pickerSocket) {
    pickerSocket.emit("game:spectatorBoards", { boards: getSpectatorBoards(room) });
  }

  io.to(room.code).emit("game:timerTick", { remaining: room.config.roundTime });
  startRoundTimer(room);
}

function startRoundRobin(room) {
  if (room.currentRound === 0) {
    room.playerOrder = Array.from(room.players.values())
      .filter(p => p.active && p.connected)
      .map(p => p.id);
    room.chooserIndex = 0;
  } else {
    room.chooserIndex = (room.chooserIndex + 1) % Math.max(room.playerOrder.length, 1);
  }
  room.chooserSkips = 0; // fresh cycle — reset the all-idle safeguard
  assignChooser(room);
}

// Hand the pick to the NEXT player in the queue without rebuilding the order or
// resetting to the top — safe to call mid-cycle (including round 0). Triggered
// when the chooser runs out of pick time, hands off voluntarily, or is skipped
// by the host. The skipped player keeps their slot and is asked again on the
// next full rotation.
function passChooserTurn(room, skippedName, reason = "timeout") {
  if (room.status !== "choosing" || !room.waitingForWord) return;
  clearChooserTimer(room);
  room.waitingForWord = false;

  io.to(room.code).emit("game:chooserPassed", {
    skippedName: skippedName || "The picker",
    reason
  });

  // A host skip is a deliberate call, not the room going idle — it clears the
  // all-idle guard so the next picker still gets a live pick clock.
  if (reason === "host") room.chooserSkips = 0;

  const queueLen = room.playerOrder.filter(id => room.players.has(id)).length;
  // All-idle safeguard: a full cycle of consecutive timeouts with nobody
  // picking → re-ask the current player but stop the auto-timer so we don't
  // loop forever. Any successful pick resets the counter (startGameRound).
  const stopTimer = queueLen > 0 && (room.chooserSkips || 0) >= queueLen;

  if (queueLen > 1 && !stopTimer) {
    room.chooserIndex = (room.chooserIndex + 1) % room.playerOrder.length;
  }
  assignChooser(room, { noTimer: stopTimer });
}

// Put the room into the "choosing" state for whoever sits at chooserIndex and
// (unless suppressed) arm the auto-pass clock. Index selection happens in the
// callers; this is the shared "ask this player to pick" step.
function assignChooser(room, opts = {}) {
  clearChooserTimer(room);

  // Remove players who left the room entirely (disconnected-but-present players
  // are kept so they can reclaim their turn on reconnect).
  room.playerOrder = room.playerOrder.filter(id => room.players.has(id));

  if (room.playerOrder.length === 0) {
    room.status = "session-ended";
    io.to(room.code).emit("game:sessionEnd", {
      leaderboard: getFinalLeaderboard(room)
    });
    return;
  }

  room.chooserIndex = room.chooserIndex % room.playerOrder.length;
  const chooserId = room.playerOrder[room.chooserIndex];
  room.currentChooserId = chooserId;
  room.waitingForWord = true;
  room.status = "choosing";

  const chooser = room.players.get(chooserId);
  const nextRoundNumber = room.currentRound + 1;

  io.to(room.code).emit("game:choosingWord", {
    chooserId,
    chooserName: chooser ? chooser.name : "Unknown",
    round: nextRoundNumber,
    totalRounds: room.config.totalRounds,
    standings: getChoosingStandings(room),
    pickSeconds: CHOOSER_PICK_SECONDS
  });

  const chooserSocket = io.sockets.sockets.get(chooserId);
  if (chooserSocket) {
    chooserSocket.emit("game:requestWord", {
      round: nextRoundNumber,
      totalRounds: room.config.totalRounds
    });
    // Only arm the auto-pass clock when there's another player to hand off to,
    // and not while the all-idle safeguard is holding the timer off.
    if (!opts.noTimer && room.playerOrder.length > 1) {
      startChooserTimer(room);
    }
  } else {
    // Chooser disconnected — skip them (kept in the order for reconnect).
    room.waitingForWord = false;
    room.chooserIndex = (room.chooserIndex + 1) % room.playerOrder.length;
    assignChooser(room, opts);
  }
}

function promoteOrCloseRoom(room) {
  const connectedPlayers = Array.from(room.players.values()).filter((player) => player.connected);
  const nextHost = connectedPlayers[0];

  if (!nextHost) {
    clearRoomTimers(room);
    rooms.delete(room.code);
    return;
  }

  room.hostId = nextHost.id;
  io.to(room.code).emit("room:hostChanged", {
    hostId: nextHost.id,
    hostName: nextHost.name,
    room: getRoomSummary(room)
  });
}

// Fix 15: when the host drops, give them a grace window to reconnect (this pairs
// with the session-reconnect flow — a brief blip no longer disrupts the room).
// If the host is still absent after the window, end the game for everyone and
// clean up the room. The grace timer is cancelled if the host reconnects.
const HOST_GRACE_MS = 10000;
function startHostGrace(room) {
  if (room.hostGraceTimer) return; // already counting down
  room.hostGraceTimer = setTimeout(() => {
    room.hostGraceTimer = null;
    if (!rooms.has(room.code)) return;
    const host = room.players.get(room.hostId);
    if (host && host.connected) return; // host reconnected within the window
    clearRoomTimers(room);
    io.to(room.code).emit("room:hostLeft", { message: "Host disconnected. Game ended." });
    rooms.delete(room.code);
  }, HOST_GRACE_MS);
}

function removeExpiredRooms() {
  const cutoff = Date.now() - ROOM_EXPIRY_MS;
  for (const [code, room] of rooms.entries()) {
    if (room.lastActive < cutoff) {
      clearRoomTimers(room);
      io.to(code).emit("room:expired");
      rooms.delete(code);
    }
  }
}

setInterval(removeExpiredRooms, 60 * 1000).unref();

// Cleanly remove a socket's player from whatever room it's currently in.
// Shared by room:leave and by room:join/room:create, so a user is never left
// "stuck" already-in-a-room after navigating home without an explicit leave.
// Returns true if the socket was actually in a (still-existing) room.
// A game needs at least two participants (a word-picker plus someone to guess).
// If the session is in progress and only one (or zero) active player remains,
// end the session and show the final leaderboard to whoever's left.
function endSessionIfAbandoned(room) {
  if (!room || !rooms.has(room.code)) return false;
  const inProgress = ["countdown", "choosing", "playing", "round-ended"].includes(room.status);
  if (!inProgress) return false;
  const activeCount = Array.from(room.players.values()).filter(p => p.active && p.connected).length;
  if (activeCount > 1) return false;

  clearRoomTimers(room);
  room.status = "session-ended";
  room.waitingForWord = false;
  io.to(room.code).emit("game:sessionEnd", {
    leaderboard: getFinalLeaderboard(room),
    reason: "not-enough-players"
  });
  return true;
}

function detachFromRoom(socket) {
  const code = socket.data.roomCode;
  socket.data.roomCode = null;
  if (!code) return false;
  socket.leave(code);
  const room = rooms.get(code);
  if (!room) return false; // stale pointer — room already gone

  const wasChooser = socket.id === room.currentChooserId;
  const wasHost = socket.id === room.hostId;

  room.players.delete(socket.id);
  room.playerOrder = room.playerOrder.filter(id => id !== socket.id);
  touchRoom(room);

  if (room.players.size === 0) {
    clearRoomTimers(room);
    rooms.delete(room.code);
    return true;
  }

  if (wasHost) promoteOrCloseRoom(room);
  emitPlayers(room, "room:playerLeft");

  // Only one player left in a live game → close it and show the leaderboard.
  if (endSessionIfAbandoned(room)) return true;

  if (wasChooser && room.waitingForWord && room.status === "choosing") {
    room.waitingForWord = false;
    if (room.playerOrder.length > 0) startRoundRobin(room);
  }
  if (wasChooser && room.status === "playing") {
    finishRound(room, "chooser-left");
  }
  if (room.status === "playing" && allActivePlayersFinished(room)) {
    finishRound(room, finishedReason(room));
  }
  return true;
}

// Hand a disconnected player's slot to a new socket — restoring their score,
// board, host authority, and chooser turn. Shared by room:rejoin (matched by
// session token) and room:join (a returning player matched by name). Returns the
// callback reply, including a roundState snapshot when a round is in progress.
function reclaimPlayerSlot(room, existing, socket) {
  const wasHost = existing.id === room.hostId;
  const wasChooser = existing.id === room.currentChooserId;
  const oldId = existing.id;

  // Swap old socket ID → new socket ID, reactivating the slot.
  room.players.delete(oldId);
  existing.id = socket.id;
  existing.connected = true;
  existing.active = true;
  room.players.set(socket.id, existing);

  if (wasHost) {
    room.hostId = socket.id;
    // Host made it back within the grace window — cancel the end-game timer.
    if (room.hostGraceTimer) { clearTimeout(room.hostGraceTimer); room.hostGraceTimer = null; }
  }
  if (wasChooser) room.currentChooserId = socket.id;
  room.playerOrder = room.playerOrder.map(id => id === oldId ? socket.id : id);

  // Leave any other room this socket was bound to before binding to this one.
  if (socket.data.roomCode && socket.data.roomCode !== room.code) detachFromRoom(socket);
  socket.data.roomCode = room.code;
  socket.join(room.code);
  touchRoom(room);

  emitPlayers(room, "room:playerJoined");
  const reply = {
    ok: true,
    isHost: wasHost,
    room: getRoomSummary(room),
    playerId: socket.id,
    sessionId: existing.sessionId
  };
  // Mid-round: hand back enough to rebuild this player's board on a cold load.
  if (room.status === "playing") reply.roundState = getActiveRoundSnapshot(room, existing);
  return reply;
}

// ── Snapshot / restore ───────────────────────────────────────────────────────
// A room carries live, non-serializable handles (timers) and a Map of players.
// serializeRoom strips the timers and flattens the Map; deserializeRoom rebuilds
// the Map and nulls the timers. Players are marked disconnected on restore — the
// sockets from the previous process are gone; clients re-attach via room:rejoin.
function serializeRoom(room) {
  return {
    code: room.code,
    hostId: room.hostId,
    createdAt: room.createdAt,
    lastActive: room.lastActive,
    status: room.status,
    currentRound: room.currentRound,
    roundStartedAt: room.roundStartedAt,
    playerOrder: room.playerOrder || [],
    chooserIndex: room.chooserIndex,
    currentChooserId: room.currentChooserId,
    waitingForWord: room.waitingForWord,
    config: room.config,
    currentRoundConfig: room.currentRoundConfig,
    players: Array.from(room.players.values()).map((p) => ({
      id: p.id, sessionId: p.sessionId, name: p.name, connected: p.connected, active: p.active,
      isHost: p.isHost, cumulativeScore: p.cumulativeScore, roundScore: p.roundScore,
      solved: p.solved, solvedAt: p.solvedAt, guesses: p.guesses, breakdown: p.breakdown,
      usedHint: p.usedHint, hintPenalty: p.hintPenalty
    }))
  };
}

function deserializeRoom(obj) {
  const room = {
    code: obj.code,
    hostId: obj.hostId,
    createdAt: obj.createdAt,
    lastActive: obj.lastActive,
    status: obj.status,
    currentRound: obj.currentRound,
    roundStartedAt: obj.roundStartedAt,
    timerInterval: null,
    countdownInterval: null,
    chooserInterval: null,
    hostGraceTimer: null,
    players: new Map(),
    playerOrder: obj.playerOrder || [],
    chooserIndex: obj.chooserIndex || 0,
    chooserSkips: 0,
    currentChooserId: obj.currentChooserId || null,
    waitingForWord: Boolean(obj.waitingForWord),
    config: obj.config || { roundTime: 180, totalRounds: 0 },
    currentRoundConfig: obj.currentRoundConfig || { word: null, hint: "" }
  };
  for (const p of (obj.players || [])) {
    p.connected = false;   // previous-process sockets are dead
    p.guessing = false;    // clear any in-flight guard
    room.players.set(p.id, p);
  }
  return room;
}

// Re-establish the live timers/transitions a restored room needs so a round in
// progress keeps moving even though setInterval/setTimeout handles didn't survive.
function resumeRoomAfterRestore(room) {
  if (room.status === "playing") {
    const elapsed = Math.max(0, nowSeconds() - room.roundStartedAt);
    if (elapsed >= room.config.roundTime) {
      finishRound(room, "timer");          // round already expired during downtime
    } else {
      startRoundTimer(room);               // resume the authoritative countdown
    }
  } else if (room.status === "countdown") {
    room.status = "lobby";                  // brief pre-round countdown can't resume; host re-starts
  } else if (room.status === "round-ended") {
    // The auto-advance timer was lost — re-arm it so the session doesn't stall.
    if (room.currentRound >= room.config.totalRounds) {
      room.status = "session-ended";
    } else {
      setTimeout(() => {
        if (rooms.has(room.code) && room.status === "round-ended") startRoundRobin(room);
      }, ROUND_REVEAL_DELAY);
    }
  } else if (room.status === "choosing" && room.waitingForWord) {
    // The auto-pass clock was lost in the restart — re-arm it (with a fresh full
    // window) so a mid-pick room can't stall after a restore.
    room.chooserSkips = 0;
    const queueLen = room.playerOrder.filter(id => room.players.has(id)).length;
    if (queueLen > 1) startChooserTimer(room);
  }
  // "lobby" / "session-ended" need no re-arming — they're static until a player/host acts.
}

async function persistState() {
  if (!redis) return;
  try {
    const data = JSON.stringify(Array.from(rooms.values()).map(serializeRoom));
    await redis.set(REDIS_STATE_KEY, data, "EX", Math.ceil(ROOM_EXPIRY_MS / 1000));
  } catch (err) {
    logEvent("error", "persist_failed", { message: err && err.message });
  }
}

async function restoreRooms() {
  if (!redis) return;
  try {
    const raw = await redis.get(REDIS_STATE_KEY);
    if (!raw) return;
    let restored = 0;
    for (const obj of JSON.parse(raw)) {
      if (!obj || !obj.code) continue;
      const room = deserializeRoom(obj);
      rooms.set(room.code, room);
      resumeRoomAfterRestore(room);
      restored += 1;
    }
    logEvent("info", "rooms_restored", { count: restored });
  } catch (err) {
    logEvent("error", "restore_failed", { message: err && err.message });
  }
}

// Attach the signed-in user (if any) to every socket. The session token rides in
// via the httpOnly cookie for browsers, or socket.handshake.auth.token for
// programmatic clients (tests). Never rejects — guests connect fine; only hosting
// a room requires socket.data.user.
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.token
      || auth.parseCookies(socket.handshake.headers.cookie)[auth.cookieName]
      || null;
    socket.data.user = await auth.userFromToken(token);
  } catch (err) {
    socket.data.user = null;
    logEvent("error", "socket_auth_error", { message: err && err.message });
  }
  next();
});

io.on("connection", (socket) => {
  // Capture transport-level socket errors so they surface in the Railway logs.
  socket.on("error", (err) => {
    logEvent("error", "socket_error", { socketId: socket.id, message: err && err.message });
  });

  // Host creates a room. Hosting requires a signed-in account (the socket carries
  // the authenticated user); joining a room never does. Word and hint are set by
  // each round's chooser. Total rounds = 2x player count, set when the session starts.
  socket.on("room:create", (payload = {}, callback) => {
    if (REQUIRE_HOST_AUTH && !socket.data.user) {
      return callback?.({ ok: false, needAuth: true, error: "Please sign in to create a room." });
    }
    // Reject a bad host name instead of silently renaming them to "Host" — the
    // old fallback meant any name with a space, accent or symbol vanished with
    // no feedback at all. Same rule and wording as room:join.
    const name = String(payload.name || "").trim();
    if (!isDisplayName(name)) {
      return callback?.({ ok: false, error: "Name must be 3-16 characters — letters, numbers, spaces, hyphens or apostrophes." });
    }
    const roundTime = Number(payload.roundTime);

    if (![60, 120, 180, 300].includes(roundTime)) return callback?.({ ok: false, error: "Invalid round timer." });

    // Host can pick their own room code (e.g. a word). If they leave it blank,
    // fall back to a random code.
    let code;
    const hasCustomCode = payload.customCode != null && String(payload.customCode).trim() !== "";
    if (hasCustomCode) {
      code = normalizeRoomCode(payload.customCode);
      if (!isValidCustomCode(code)) {
        return callback?.({ ok: false, error: "Room code must be 3–6 letters or numbers." });
      }
      if (rooms.has(code)) {
        return callback?.({ ok: false, error: "That room code is already taken — try another." });
      }
    } else {
      code = makeRoomCode();
    }
    const room = {
      code,
      hostId: socket.id,
      createdBy: socket.data.user ? socket.data.user.email : null,
      createdAt: Date.now(),
      lastActive: Date.now(),
      status: "lobby",
      currentRound: 0,
      roundStartedAt: null,
      timerInterval: null,
      countdownInterval: null,
      chooserInterval: null,
      players: new Map(),
      // Round-robin state
      playerOrder: [],
      chooserIndex: 0,
      chooserSkips: 0,
      currentChooserId: null,
      waitingForWord: false,
      config: {
        roundTime: roundTime,
        totalRounds: 0  // Calculated as 2x players when session starts
      },
      currentRoundConfig: {
        word: null,
        hint: ""
      }
    };

    // Free any previous room so creating a new one never leaves a dangling slot.
    if (socket.data.roomCode) detachFromRoom(socket);

    const host = makePlayer(socket, name, true);
    room.players.set(socket.id, host);
    rooms.set(code, room);
    socket.data.roomCode = code;
    socket.join(code);

    callback?.({ ok: true, room: getRoomSummary(room), playerId: socket.id, isHost: true, sessionId: host.sessionId });
    socket.emit("room:created", { room: getRoomSummary(room), playerId: socket.id });
  });

  // Player joins a room by code with a validated display name.
  socket.on("room:join", (payload = {}, callback) => {
    const code = String(payload.code || "").trim().toUpperCase();
    const name = String(payload.name || "").trim();
    const room = rooms.get(code);

    if (!room) return callback?.({ ok: false, error: "Room not found." });
    if (!isDisplayName(name)) return callback?.({ ok: false, error: "Name must be 3-16 characters — letters, numbers, spaces, hyphens or apostrophes." });

    // Mid-game: only RETURNING players may re-enter — reclaim a disconnected slot
    // matched by name, preserving their score, board, and chooser turn. New names
    // are turned away until the next game; an in-use (still-connected) name can't
    // be hijacked. The room key alone gets a returning player back in at any time.
    if (room.status !== "lobby") {
      if (room.status === "session-ended") {
        return callback?.({ ok: false, error: "That game has already ended." });
      }
      const sameName = Array.from(room.players.values())
        .find((p) => p.name.toLowerCase() === name.toLowerCase());
      if (sameName && !sameName.connected) {
        return callback?.(reclaimPlayerSlot(room, sameName, socket));
      }
      if (sameName) {
        return callback?.({ ok: false, error: "That name is already active in the game. If it's you, rejoin from the device you were playing on." });
      }
      return callback?.({ ok: false, error: "This game is already in progress — you can only rejoin if you were already in it. Use the exact name you played with." });
    }

    if (room.players.size >= MAX_PLAYERS) return callback?.({ ok: false, error: "Room is full." });
    if (Array.from(room.players.values()).some((player) => player.id !== socket.id && player.name.toLowerCase() === name.toLowerCase())) {
      return callback?.({ ok: false, error: "That name is already taken in this room." });
    }

    // All checks passed — free any previous room (e.g. after "Play Again" navigated
    // home without an explicit leave) so the user is never blocked as "already in a
    // room". Done only now so a rejected join never strands them out of their room.
    if (socket.data.roomCode && socket.data.roomCode !== code) detachFromRoom(socket);

    const player = makePlayer(socket, name);
    room.players.set(socket.id, player);
    socket.data.roomCode = code;
    socket.join(code);
    touchRoom(room);

    callback?.({ ok: true, room: getRoomSummary(room), playerId: socket.id, isHost: socket.id === room.hostId, sessionId: player.sessionId });
    emitPlayers(room, "room:playerJoined");
  });

  // Player reconnects mid-game with a session token to reclaim their slot.
  socket.on("room:rejoin", ({ code, sessionId } = {}, callback) => {
    const room = rooms.get(String(code || "").trim().toUpperCase());
    if (!room) return callback?.({ ok: false, error: "Room not found." });

    // Locate the existing player by their immutable session token
    const existing = Array.from(room.players.values()).find((p) => p.sessionId === sessionId);
    if (!existing) return callback?.({ ok: false, error: "Session not recognised." });

    callback?.(reclaimPlayerSlot(room, existing, socket));
  });

  // Host starts the session. Total rounds = 2 x active player count.
  // Word selection is handled per-round via round-robin.
  socket.on("game:start", (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!assertHost(socket, room)) return callback?.({ ok: false, error: "Only the host can start the session." });
    if (room.status === "playing") return callback?.({ ok: false, error: "A round is already running." });
    if (room.status === "choosing") return callback?.({ ok: false, error: "Waiting for word chooser." });
    if (room.currentRound > 0 && room.currentRound >= room.config.totalRounds) {
      return callback?.({ ok: false, error: "All rounds complete." });
    }

    const roundTime = Number(payload.roundTime || room.config.roundTime);
    if (![60, 120, 180, 300].includes(roundTime)) {
      return callback?.({ ok: false, error: "Invalid round timer." });
    }

    // Need at least 2 players: one picks the word, the other(s) guess it. A
    // solo host has no one to play against, so don't let the game start.
    const activePlayers = Array.from(room.players.values())
      .filter(p => p.active && p.connected);
    if (activePlayers.length < 2) {
      return callback?.({ ok: false, error: "You need at least 2 players to start — invite someone with the room code." });
    }
    room.config.roundTime = roundTime;

    // Lock in total rounds = 2 x active player count (min 2)
    room.config.totalRounds = Math.max(2, activePlayers.length * 2);

    startRoundRobin(room);
    callback?.({ ok: true, totalRounds: room.config.totalRounds });
  });

  // Word chooser submits the word for the current round.
  socket.on("game:submitWord", async (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!room) return callback?.({ ok: false, error: "Not in a room." });
    if (!room.waitingForWord) return callback?.({ ok: false, error: "Not waiting for a word." });
    if (socket.id !== room.currentChooserId) return callback?.({ ok: false, error: "It is not your turn to choose." });
    if (room.status !== "choosing") return callback?.({ ok: false, error: "Room is not in choosing state." });

    const word = normalizeWord(payload?.word);
    const hint = String(payload.hint || "").trim().slice(0, 140);

    if (!isFiveLetterWord(word)) {
      return callback?.({ ok: false, error: "Word must be exactly 5 alphabetic characters." });
    }

    // ONE dictionary lookup gives us BOTH the validity verdict and the meaning.
    // Tri-state: reject only a DEFINITIVE non-word. If the dictionary can't be
    // reached (null), fail-open so a missing key / MW outage never blocks play.
    // Because the definition rides along with validation, it's guaranteed
    // available to EVERY player at round end whenever the word was accepted.
    const lookup = await lookupWord(word);
    if (lookup.valid === false) {
      return callback?.({ ok: false, error: "That's not a word we recognise. Try a common English noun, verb, or adjective (no names or places)." });
    }

    room.waitingForWord = false;
    startGameRound(room, { word, hint, definition: lookup.definition || "" });
    callback?.({ ok: true });
  });

  // The current chooser is busy and hands their pick to the next player in the
  // queue. Same effect as running out of time, but on demand.
  socket.on("game:passTurn", (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!room) return callback?.({ ok: false, error: "Not in a room." });
    if (room.status !== "choosing" || !room.waitingForWord) {
      return callback?.({ ok: false, error: "There's no pick to pass right now." });
    }
    if (socket.id !== room.currentChooserId) {
      return callback?.({ ok: false, error: "Only the current picker can pass the turn." });
    }
    if (room.playerOrder.filter(id => room.players.has(id)).length <= 1) {
      return callback?.({ ok: false, error: "There's no one else to pass to." });
    }
    const me = room.players.get(socket.id);
    passChooserTurn(room, me ? me.name : "The picker", "voluntary");
    callback?.({ ok: true });
  });

  // The host moves a stalled picker along without waiting for the pick clock.
  // Same rotation semantics as a timeout or a voluntary pass: the skipped player
  // keeps their slot and is asked again on the next full cycle.
  socket.on("game:skipChooser", (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!room) return callback?.({ ok: false, error: "Not in a room." });
    if (!assertHost(socket, room)) {
      return callback?.({ ok: false, error: "Only the host can skip the picker." });
    }
    if (room.status !== "choosing" || !room.waitingForWord) {
      return callback?.({ ok: false, error: "There's no pick to skip right now." });
    }
    if (room.playerOrder.filter(id => room.players.has(id)).length <= 1) {
      return callback?.({ ok: false, error: "There's no one else to pass to." });
    }
    const chooser = room.players.get(room.currentChooserId);
    const skippedName = chooser ? chooser.name : "The picker";
    // A host skipping their own turn is just a pass — keep the wording honest.
    const reason = socket.id === room.currentChooserId ? "voluntary" : "host";
    passChooserTurn(room, skippedName, reason);
    callback?.({ ok: true });
  });

  // Host broadcasts a 10-second force-start countdown, then the server starts the round.
  socket.on("game:forceStart", (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!assertHost(socket, room)) return callback?.({ ok: false, error: "Only the host can force-start." });
    if (room.status === "playing") return callback?.({ ok: false, error: "A round is already running." });

    let remaining = 10;
    clearRoomTimers(room);
    room.status = "countdown";
    io.to(room.code).emit("game:countdown", { remaining });

    room.countdownInterval = setInterval(() => {
      remaining -= 1;
      io.to(room.code).emit("game:countdown", { remaining });
      if (remaining <= 0) {
        clearInterval(room.countdownInterval);
        room.countdownInterval = null;
        startRoundRobin(room);
      }
    }, 1000);

    callback?.({ ok: true });
  });

  // Player submits a guess; validation, feedback, solving, and scoring all happen server-side.
  socket.on("game:guess", async (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    const player = room ? findPlayer(room, socket.id) : null;
    const guess = normalizeWord(payload.guess);

    if (!room || !player) return callback?.({ ok: false, error: "You are not in a room." });
    if (room.status !== "playing") return callback?.({ ok: false, error: "No round is currently active." });
    if (!player.active) return callback?.({ ok: false, error: "Inactive players cannot guess." });
    if (socket.id === room.currentChooserId) {
      return callback?.({ ok: false, error: "You chose the word this round — you cannot guess." });
    }
    if (player.solved) return callback?.({ ok: false, error: "You already solved this round." });
    if (player.guesses.length >= MAX_GUESSES) return callback?.({ ok: false, error: "No guesses remaining." });
    if (!isFiveLetterWord(guess)) return callback?.({ ok: false, error: "Guess must be exactly 5 alphabetic characters." });

    // Re-entrancy guard: the dictionary await below opens a race window where a
    // rapid second guess from the same player could double-count. Reject overlap.
    if (player.guessing) return callback?.({ ok: false, error: "Still checking your last guess — one sec." });
    player.guessing = true;
    try {
      // Dictionary check (tri-state): reject only a DEFINITIVE non-word. A missing
      // key or MW outage returns null → fail-open so play never stalls.
      const valid = await isValidWord(guess);
      if (valid === false) return callback?.({ ok: false, error: "Not a valid word. Try again." });

      // Re-check terminal conditions in case the round ended during the await.
      if (room.status !== "playing") return callback?.({ ok: false, error: "No round is currently active." });
      if (player.solved) return callback?.({ ok: false, error: "You already solved this round." });
      if (player.guesses.length >= MAX_GUESSES) return callback?.({ ok: false, error: "No guesses remaining." });

      const answer = room.currentRoundConfig.word;
      const feedback = makeFeedback(guess, answer);
      player.guesses.push({ guess, feedback });
      touchRoom(room);

      const solved = guess === answer;
      if (solved) {
        const breakdown = calculateScore(room, player);
        player.solved = true;
        player.solvedAt = Math.max(0, nowSeconds() - room.roundStartedAt);
        player.roundScore = breakdown.score;
        player.cumulativeScore += breakdown.score;
        player.breakdown = breakdown;

        io.to(room.code).emit("game:playerSolved", {
          id: player.id,
          name: player.name,
          time: player.solvedAt,
          score: player.roundScore,
          guessesUsed: player.guesses.length,
          leaderboard: getLiveLeaderboard(room),
          progress: getRoundProgress(room)
        });
      }

      // Live spectator feed: every guess updates the attempt counts the room is
      // watching, and redraws the picker's tile-by-tile view of every board.
      emitProgress(room);

      callback?.({
        ok: true,
        guess,
        feedback,
        solved,
        guessesUsed: player.guesses.length,
        maxGuesses: MAX_GUESSES
      });

      if (allActivePlayersFinished(room)) {
        finishRound(room, finishedReason(room));
      }
    } finally {
      player.guessing = false;
    }
  });

  // Player opts in to reveal the chooser's hint. The cost is computed and locked
  // in server-side at reveal time — it decays the longer the player has battled
  // on their own — and is subtracted from their round score only when they solve.
  // The numeric cost is never sent to the client.
  socket.on("game:useHint", (_payload, callback) => {
    const room = getRoomForSocket(socket);
    const player = room ? findPlayer(room, socket.id) : null;

    if (!room || !player) return callback?.({ ok: false });
    if (room.status !== "playing") return callback?.({ ok: false });
    if (!room.currentRoundConfig.hint) return callback?.({ ok: false });

    // Idempotent: a second reveal never recomputes or re-applies the cost.
    if (player.usedHint) {
      return callback?.({ ok: true, hint: room.currentRoundConfig.hint });
    }

    player.hintPenalty = Math.max(
      0,
      HINT_PENALTY_BASE - HINT_PENALTY_STEP * player.guesses.length
    );
    player.usedHint = true;
    touchRoom(room);

    callback?.({ ok: true, hint: room.currentRoundConfig.hint });

    // Let the word-picker (and anyone already done) see who took the hint right
    // away, instead of waiting for this player's next guess to refresh the feed.
    // The hint TEXT never rides along — only the flag.
    emitProgress(room);
  });

  // Host force-ends the active round.
  socket.on("game:endRound", (_payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    if (!assertHost(socket, room)) return callback?.({ ok: false, error: "Only the host can end the round." });
    if (room.status !== "playing") return callback?.({ ok: false, error: "No active round to end." });
    finishRound(room, "host");
    callback?.({ ok: true });
  });

  // Host removes a player from the room while preserving server authority over room membership.
  socket.on("room:kick", (payload = {}, callback) => {
    const room = getRoomForSocket(socket);
    const targetId = String(payload.playerId || "");

    if (!assertHost(socket, room)) return callback?.({ ok: false, error: "Only the host can kick players." });
    if (targetId === room.hostId) return callback?.({ ok: false, error: "The host cannot kick themselves." });
    const target = room.players.get(targetId);
    if (!target) return callback?.({ ok: false, error: "Player not found." });

    const targetSocket = io.sockets.sockets.get(targetId);
    if (targetSocket) {
      targetSocket.emit("room:kicked", { reason: "You were removed by the host." });
      targetSocket.leave(room.code);
      targetSocket.data.roomCode = null;
    }
    room.players.delete(targetId);
    touchRoom(room);
    emitPlayers(room, "room:playerLeft");
    callback?.({ ok: true });
  });

  // Any player can voluntarily leave the room at any time.
  socket.on("room:leave", (_payload = {}, callback) => {
    if (!socket.data.roomCode) return callback?.({ ok: false, error: "Not in a room." });
    detachFromRoom(socket);
    callback?.({ ok: true });
  });

  // A disconnected player is marked inactive; if the host leaves, the next connected player is promoted.
  socket.on("disconnect", () => {
    const room = getRoomForSocket(socket);
    if (!room) return;

    const player = findPlayer(room, socket.id);
    if (player) {
      player.connected = false;
      player.active = false;
    }
    touchRoom(room);

    if (socket.id === room.hostId) {
      // Fix 15: grace window before ending the game, in case of a brief drop.
      startHostGrace(room);
    }

    // If the disconnected player was the current chooser
    if (player && socket.id === room.currentChooserId) {
      if (room.waitingForWord && room.status === "choosing") {
        room.waitingForWord = false;
        room.playerOrder = room.playerOrder.filter(id => id !== socket.id);
        if (room.playerOrder.length > 0) {
          setTimeout(() => {
            if (rooms.has(room.code) && room.status !== "session-ended") startRoundRobin(room);
          }, 1500);
        }
      }
    }

    if (rooms.has(room.code)) {
      emitPlayers(room, "room:playerLeft");
      // Only one player left in a live game → close it and show the leaderboard.
      if (endSessionIfAbandoned(room)) return;
      if (room.status === "playing" && allActivePlayersFinished(room)) {
        finishRound(room, finishedReason(room));
      }
    }
  });
});

// ===== PRACTICE MODE START =====
// Solo "Play vs Claude AI" practice mode. Fully self-contained: its own session
// store (keyed by socket.id), its own feedback + scoring, its own Anthropic calls,
// and its own io.on("connection") block. It does not touch any multiplayer code,
// the `rooms` map, or the existing handlers above.

const PRACTICE_MODEL = "claude-sonnet-4-6";
const PRACTICE_MAX_ATTEMPTS = 6;
const PRACTICE_GUESS_DEBOUNCE_MS = 300;
const PRACTICE_NEXT_ROUND_DELAY_MS = 2600; // pause on the round-end card before the next word

// Anthropic client. Loaded defensively: a missing package or unset key must never
// crash the server — practice simply falls back to the hardcoded word lists.
let anthropic = null;
try {
  if (process.env.ANTHROPIC_API_KEY) {
    const AnthropicSDK = require("@anthropic-ai/sdk");
    const Anthropic = AnthropicSDK.default || AnthropicSDK;
    anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    logEvent("info", "practice_anthropic_ready", {});
  } else {
    logEvent("warn", "practice_anthropic_disabled", {
      message: "ANTHROPIC_API_KEY not set — Play vs AI uses the fallback word lists."
    });
  }
} catch (err) {
  logEvent("error", "practice_anthropic_load_failed", { message: err && err.message });
}

// Fallback words (≥20 per tier) with crossword-style hints — used whenever the
// Anthropic call fails or no key is configured.
const FALLBACK_WORDS = {
  easy: [
    { word: "CHAIR", hint: "You pull this up to a table" },
    { word: "BREAD", hint: "A baker pulls it from the oven each morning" },
    { word: "CLOCK", hint: "Its hands never hold anything" },
    { word: "RIVER", hint: "It runs but never walks" },
    { word: "HOUSE", hint: "Four walls you come home to" },
    { word: "TRAIN", hint: "It runs on rails between stations" },
    { word: "LIGHT", hint: "A switch on the wall summons it" },
    { word: "MUSIC", hint: "Headphones deliver it to your ears" },
    { word: "PLANT", hint: "It drinks sunlight through green leaves" },
    { word: "BEACH", hint: "Waves meet sand here" },
    { word: "CLOUD", hint: "It drifts overhead and may bring rain" },
    { word: "SMILE", hint: "It spreads across a happy face" },
    { word: "WATER", hint: "Fish breathe in it" },
    { word: "BRAVE", hint: "How a hero faces danger" },
    { word: "HORSE", hint: "A cowboy's four-legged ride" },
    { word: "PHONE", hint: "It rings in your pocket" },
    { word: "STORM", hint: "Thunder announces its arrival" },
    { word: "NIGHT", hint: "The moon lights the sky during it" },
    { word: "STORY", hint: "You turn a book's pages to follow it" },
    { word: "GRASS", hint: "Green blades you mow in summer" }
  ],
  medium: [
    { word: "FLAIR", hint: "A natural, stylish talent" },
    { word: "TROVE", hint: "A hidden hoard of treasure" },
    { word: "SHRUB", hint: "A bush shorter than a tree" },
    { word: "PRISM", hint: "It splits light into a rainbow" },
    { word: "EMBER", hint: "A glowing remnant of a dying fire" },
    { word: "QUILT", hint: "Stitched squares keep you warm at night" },
    { word: "GLOBE", hint: "A desktop model of the world" },
    { word: "MARSH", hint: "Soggy ground where reeds grow" },
    { word: "VAULT", hint: "A bank keeps riches behind its heavy door" },
    { word: "THORN", hint: "A rose defends itself with this" },
    { word: "PLUME", hint: "A feather rising in smoke or on a hat" },
    { word: "STEED", hint: "A spirited horse fit for a knight" },
    { word: "FROST", hint: "It etches patterns on winter windows" },
    { word: "WAGER", hint: "Money placed on an outcome" },
    { word: "NUDGE", hint: "A gentle push to get someone moving" },
    { word: "BRINE", hint: "Salty water for pickling" },
    { word: "KNACK", hint: "A clever talent for doing something" },
    { word: "GLEAM", hint: "A brief flash of reflected light" },
    { word: "BRISK", hint: "A quick, lively pace on a cold walk" },
    { word: "CRISP", hint: "How fresh autumn air or a good chip feels" }
  ],
  hard: [
    { word: "GLYPH", hint: "A carved symbol on ancient stone" },
    { word: "NADIR", hint: "The lowest point of one's fortunes" },
    { word: "QUALM", hint: "A sudden pang of doubt or unease" },
    { word: "FJORD", hint: "A narrow sea inlet between steep cliffs" },
    { word: "AXIOM", hint: "A truth accepted without proof" },
    { word: "DIRGE", hint: "A slow, mournful funeral song" },
    { word: "KNELL", hint: "The solemn toll of a funeral bell" },
    { word: "CRYPT", hint: "A stone chamber beneath a church" },
    { word: "HAVOC", hint: "Widespread destruction and chaos" },
    { word: "QUELL", hint: "To put down a rebellion or a fear" },
    { word: "HELIX", hint: "A spiral like a strand of DNA" },
    { word: "NYMPH", hint: "A minor nature spirit of old myth" },
    { word: "UMBRA", hint: "The darkest core of a shadow" },
    { word: "WALTZ", hint: "A sweeping three-step ballroom dance" },
    { word: "XENON", hint: "A rare gas glowing in bright lamps" },
    { word: "EPOCH", hint: "A notable stretch of history" },
    { word: "GAUNT", hint: "Thin and hollow from hardship" },
    { word: "VIXEN", hint: "A female fox, or a fierce woman" },
    { word: "TOPAZ", hint: "A golden-yellow gemstone" },
    { word: "JOUST", hint: "A duel of mounted, lance-bearing knights" }
  ]
};

// Practice words are always exactly 5 letters (matching the real game). Difficulty
// varies by word rarity/obscurity, NOT by length.
const PRACTICE_LENGTH_RANGE = { easy: [5, 5], medium: [5, 5], hard: [5, 5] };

const practiceSessions = new Map();

function nowMs() { return Date.now(); }

// Variable-length Wordle feedback returning the spec's colour names.
function makePracticeFeedback(guess, answer) {
  const n = answer.length;
  const colours = Array(n).fill("absent");
  const counts = {};
  for (let i = 0; i < n; i += 1) {
    if (guess[i] === answer[i]) colours[i] = "correct";
    else counts[answer[i]] = (counts[answer[i]] || 0) + 1;
  }
  for (let i = 0; i < n; i += 1) {
    if (colours[i] === "correct") continue;
    if (counts[guess[i]] > 0) { colours[i] = "present"; counts[guess[i]] -= 1; }
  }
  return colours;
}

function calcPracticeScore({ outcome, attemptsUsed, timerSeconds, timeLeft }) {
  if (outcome !== "win") return 0;
  const base = 100;
  const guessBonus = (PRACTICE_MAX_ATTEMPTS - attemptsUsed) * 10;
  const timerBonus = timerSeconds > 0 ? Math.floor(timeLeft * 0.5) : 0;
  return base + guessBonus + timerBonus;
}

function extractJson(text) {
  if (!text) return null;
  // Strip ```json fences if present, then grab the outermost JSON object/array.
  const cleaned = String(text).replace(/```(?:json)?/gi, "").trim();
  const match = cleaned.match(/[[{][\s\S]*[\]}]/);
  try { return JSON.parse(match ? match[0] : cleaned); } catch { return null; }
}

function pickFallbackWord(difficulty, usedWords) {
  const list = FALLBACK_WORDS[difficulty] || FALLBACK_WORDS.easy;
  const used = new Set((usedWords || []).map((w) => String(w).toUpperCase()));
  const fresh = list.filter((e) => !used.has(e.word));
  const pool = fresh.length ? fresh : list;
  return pool[Math.floor(Math.random() * pool.length)];
}

// Rolling memory of recently-served words ACROSS sessions. Without this, a brand
// new game's first round always sends an identical prompt to Claude (usedWords is
// empty), and the model reliably returns the same word — so the first word never
// felt random. We feed these back as an explicit "avoid" list.
const recentPracticeWords = [];
const RECENT_WORDS_MAX = 40;
function rememberPracticeWord(word) {
  if (!word) return;
  recentPracticeWords.push(String(word).toUpperCase());
  while (recentPracticeWords.length > RECENT_WORDS_MAX) recentPracticeWords.shift();
}

const WORD_SYSTEM_PROMPT = [
  "You are a word selector for a Wordle-style word game. Your job is to choose a single English word and write a contextual hint for it.",
  "Rules:",
  "- Return ONLY valid JSON matching this schema: { \"word\": string, \"hint\": string }",
  "- No markdown, no explanation, no extra keys",
  "- The word must be a real, standard English dictionary word",
  "- The hint must be a single sentence of 15 words or fewer",
  "- The hint must NOT contain the word, its root, or any direct synonym",
  "- The hint should read like a crossword clue — evocative, not definitional"
].join("\n");

function wordUserPrompt(difficulty, avoidWords) {
  const avoid = (avoidWords && avoidWords.length) ? avoidWords.join(", ") : "none";
  // Entropy: changing the prompt each call makes the word vary even when the model
  // ignores `temperature`. The random starting letter is the strongest diversifier.
  const seed = Math.floor(Math.random() * 1e9);
  const letters = "ABCDEFGHILMNOPRSTUW"; // skip rare/awkward first letters (J,K,Q,V,X,Y,Z)
  const letter = letters[Math.floor(Math.random() * letters.length)];
  return [
    `Select a ${difficulty} difficulty word.`,
    "The word MUST be EXACTLY 5 letters long — no more, no fewer — for every difficulty.",
    "Difficulty is set by word rarity, NOT length:",
    "- easy: high-frequency common English nouns or verbs (e.g. CHAIR, BRAVE, WATER)",
    "- medium: moderate frequency, less obvious (e.g. FLAIR, TROVE, SHRUB)",
    "- hard: uncommon, tricky, or literary 5-letter words (e.g. GLYPH, NADIR, QUALM)",
    `Do NOT use any of these recently-used words: ${avoid}`,
    `Variety seed: ${seed}. Prefer a fresh, non-obvious word that ideally starts with "${letter}"; if no natural ${difficulty} word fits that letter, choose any suitable one. Don't default to the most common pick.`,
    "Return JSON only."
  ].join("\n");
}

// Pick the next word+hint. Tries Claude (temperature 0.8 for variety); on any
// failure, logs and falls back to the hardcoded list so the session never breaks.
async function generateWord(difficulty, usedWords) {
  const [min, max] = PRACTICE_LENGTH_RANGE[difficulty] || PRACTICE_LENGTH_RANGE.easy;
  // Avoid both this session's words AND recently-served words across sessions, so
  // the first word of a fresh game isn't always identical.
  const avoid = Array.from(new Set(
    [...(usedWords || []), ...recentPracticeWords].map((w) => String(w).toUpperCase())
  ));
  if (anthropic) {
    try {
      const msg = await anthropic.messages.create({
        model: PRACTICE_MODEL,
        max_tokens: 256,
        temperature: 0.8,
        system: WORD_SYSTEM_PROMPT,
        messages: [{ role: "user", content: wordUserPrompt(difficulty, avoid) }]
      });
      const text = (msg.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
      const parsed = extractJson(text);
      const word = parsed && normalizeWord(parsed.word);
      const hint = parsed && String(parsed.hint || "").trim();
      const used = new Set(avoid);
      const re = new RegExp(`^[A-Z]{${min},${max}}$`);
      if (word && hint && re.test(word) && !used.has(word) &&
          !hint.toUpperCase().includes(word)) {
        rememberPracticeWord(word);
        return { word, hint, source: "ai" };
      }
      logEvent("warn", "practice_word_rejected", { difficulty, word: word || null });
    } catch (err) {
      logEvent("error", "practice_word_gen_failed", { message: err && err.message });
    }
  }
  const fb = pickFallbackWord(difficulty, avoid);
  rememberPracticeWord(fb.word);
  return { ...fb, source: "fallback" };
}

const RECAP_SYSTEM_PROMPT = [
  "You are a friendly but honest word game coach giving a player feedback on their practice session. Be concise, specific, and encouraging without being sycophantic.",
  "Return ONLY valid JSON matching this schema:",
  "{",
  "  \"recap\": string[],   // one string per round, max 20 words each",
  "  \"grade\": string      // e.g. \"A\", \"B+\", \"C\", \"Needs practice\" — one short string",
  "}",
  "No markdown, no explanation, no extra keys."
].join("\n");

function recapUserPrompt(difficulty, rounds) {
  const lines = rounds.map((r, i) =>
    `Round ${i + 1}: word="${r.word}", outcome="${r.outcome}", attempts=${r.attemptsUsed}/6, time=${r.timeTaken}s`
  ).join("\n");
  return [
    `The player completed a ${difficulty} difficulty session of ${rounds.length} rounds.`,
    "Round results:",
    lines,
    "Write one coaching note per round (reference the specific word) and assign an overall grade."
  ].join("\n");
}

function fallbackRecap(rounds) {
  const recap = rounds.map((r) => {
    if (r.outcome === "win") return `Nice work cracking ${r.word} in ${r.attemptsUsed} guess${r.attemptsUsed === 1 ? "" : "es"}.`;
    if (r.outcome === "timeout") return `Time ran out on ${r.word} — pace yourself a little faster next time.`;
    return `${r.word} got away this round — review it and you'll spot it next time.`;
  });
  const wins = rounds.filter((r) => r.outcome === "win").length;
  const ratio = rounds.length ? wins / rounds.length : 0;
  const grade = ratio >= 0.9 ? "A" : ratio >= 0.7 ? "B" : ratio >= 0.5 ? "C" : ratio > 0 ? "D" : "Needs practice";
  return { recap, grade };
}

// One Claude call at the end for personalised commentary; falls back gracefully.
async function generateRecap(difficulty, rounds) {
  if (anthropic) {
    try {
      const msg = await anthropic.messages.create({
        model: PRACTICE_MODEL,
        max_tokens: 512,
        temperature: 0.5,
        system: RECAP_SYSTEM_PROMPT,
        messages: [{ role: "user", content: recapUserPrompt(difficulty, rounds) }]
      });
      const text = (msg.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
      const parsed = extractJson(text);
      if (parsed && Array.isArray(parsed.recap) && parsed.grade) {
        // Pad/trim recap to one entry per round.
        const recap = rounds.map((_, i) => String(parsed.recap[i] || "").trim() || "Solid effort this round.");
        return { recap, grade: String(parsed.grade).trim() };
      }
      logEvent("warn", "practice_recap_rejected", {});
    } catch (err) {
      logEvent("error", "practice_recap_gen_failed", { message: err && err.message });
    }
  }
  return fallbackRecap(rounds);
}

function clearPracticeTimers(session) {
  if (!session) return;
  if (session.roundTimer) { clearTimeout(session.roundTimer); session.roundTimer = null; }
  if (session.advanceTimer) { clearTimeout(session.advanceTimer); session.advanceTimer = null; }
}

async function startPracticeRound(socket, session) {
  session.round += 1;
  session.attempts = [];
  session.roundOver = false;
  const { word, hint } = await generateWord(session.difficulty, session.usedWords);

  // The socket may have disconnected / quit during the async word generation.
  if (!practiceSessions.has(socket.id) || practiceSessions.get(socket.id) !== session) return;

  session.currentWord = word;
  session.currentHint = hint;
  session.usedWords.push(word);
  session.roundStartedAt = nowMs();

  if (session.timerSeconds > 0) {
    session.roundTimer = setTimeout(() => {
      const live = practiceSessions.get(socket.id);
      if (!live || live !== session || session.roundOver) return;
      socket.emit("practice:timeout", { word: session.currentWord });
      endPracticeRound(socket, session, "timeout");
    }, session.timerSeconds * 1000);
  }

  socket.emit("practice:round_start", {
    round: session.round,
    totalRounds: session.totalRounds,
    hint: session.currentHint,
    timerSeconds: session.timerSeconds,
    maxAttempts: PRACTICE_MAX_ATTEMPTS,
    wordLength: session.currentWord.length // the length only — never the word itself
  });
}

async function endPracticeRound(socket, session, outcome) {
  if (session.roundOver) return; // idempotent — guard double calls (solve + timer race)
  session.roundOver = true;
  clearPracticeTimers(session);

  const attemptsUsed = session.attempts.length;
  const timeTaken = Math.max(0, Math.round((nowMs() - session.roundStartedAt) / 1000));
  const timeLeft = session.timerSeconds > 0 ? Math.max(0, session.timerSeconds - timeTaken) : 0;
  const roundScore = calcPracticeScore({ outcome, attemptsUsed, timerSeconds: session.timerSeconds, timeLeft });
  session.totalScore += roundScore;

  session.history.push({
    word: session.currentWord,
    hint: session.currentHint,
    outcome,
    attemptsUsed,
    timeTaken,
    roundScore
  });

  socket.emit("practice:round_end", {
    outcome,
    word: session.currentWord,
    timeTaken,
    attemptsUsed,
    roundScore,
    totalScore: session.totalScore
  });

  if (session.round >= session.totalRounds) {
    const { recap, grade } = await generateRecap(session.difficulty, session.history);
    if (!practiceSessions.has(socket.id) || practiceSessions.get(socket.id) !== session) return;
    socket.emit("practice:game_end", {
      rounds: session.history,
      totalScore: session.totalScore,
      recap,
      grade
    });
  } else {
    session.advanceTimer = setTimeout(() => {
      const live = practiceSessions.get(socket.id);
      if (live === session) startPracticeRound(socket, session);
    }, PRACTICE_NEXT_ROUND_DELAY_MS);
  }
}

io.on("connection", (socket) => {
  socket.on("practice:start", async (payload = {}) => {
    const difficulty = ["easy", "medium", "hard"].includes(payload.difficulty) ? payload.difficulty : null;
    const rounds = [1, 3, 5, 10].includes(Number(payload.rounds)) ? Number(payload.rounds) : null;
    const timer = [60, 180, 300].includes(Number(payload.timer)) ? Number(payload.timer) : null;
    if (!difficulty || rounds == null || timer == null) {
      return socket.emit("practice:error", { message: "Invalid practice settings." });
    }

    // Reset any prior session for this socket.
    const prior = practiceSessions.get(socket.id);
    if (prior) clearPracticeTimers(prior);

    const session = {
      difficulty,
      totalRounds: rounds,
      timerSeconds: timer,
      round: 0,
      usedWords: [],
      currentWord: null,
      currentHint: null,
      attempts: [],
      roundStartedAt: 0,
      roundOver: false,
      totalScore: 0,
      history: [],
      lastGuessAt: 0,
      busy: false,
      roundTimer: null,
      advanceTimer: null
    };
    practiceSessions.set(socket.id, session);
    await startPracticeRound(socket, session);
  });

  socket.on("practice:guess", async (payload = {}) => {
    const session = practiceSessions.get(socket.id);
    if (!session || session.roundOver || !session.currentWord) return;

    // Debounce: ignore guesses arriving within 300ms of the previous one.
    const t = nowMs();
    if (t - session.lastGuessAt < PRACTICE_GUESS_DEBOUNCE_MS) return;
    session.lastGuessAt = t;
    if (session.busy) return;

    const answer = session.currentWord;
    const guess = normalizeWord(payload.guess);
    if (!new RegExp(`^[A-Z]{${answer.length}}$`).test(guess)) {
      return socket.emit("practice:error", { message: `Guess must be ${answer.length} letters.` });
    }

    session.busy = true;
    try {
      // Dictionary check (tri-state, fail-open): only reject a definitive non-word.
      const valid = await isValidWord(guess);
      if (valid === false) {
        return socket.emit("practice:error", { message: "Not a valid word. Try again." });
      }
      // Re-check liveness after the await.
      const live = practiceSessions.get(socket.id);
      if (!live || live !== session || session.roundOver) return;

      const colours = makePracticeFeedback(guess, answer);
      session.attempts.push(guess);
      const attemptsUsed = session.attempts.length;
      const attemptsLeft = PRACTICE_MAX_ATTEMPTS - attemptsUsed;
      const solved = guess === answer;

      socket.emit("practice:feedback", { guess, colours, attemptsLeft, attemptsUsed });

      if (solved) {
        await endPracticeRound(socket, session, "win");
      } else if (attemptsUsed >= PRACTICE_MAX_ATTEMPTS) {
        await endPracticeRound(socket, session, "loss");
      }
    } finally {
      session.busy = false;
    }
  });

  socket.on("practice:forfeit", () => {
    const session = practiceSessions.get(socket.id);
    if (session && !session.roundOver) endPracticeRound(socket, session, "loss");
  });

  socket.on("practice:quit", () => {
    const session = practiceSessions.get(socket.id);
    if (session) { clearPracticeTimers(session); practiceSessions.delete(socket.id); }
  });

  socket.on("disconnect", () => {
    const session = practiceSessions.get(socket.id);
    if (session) { clearPracticeTimers(session); practiceSessions.delete(socket.id); }
  });
});
// ===== PRACTICE MODE END =====

// Periodic snapshot so a hard crash loses at most a few seconds of progress.
// Only runs when persistence is enabled; unref'd so it never holds the process up.
if (redis) {
  setInterval(() => { persistState(); }, SNAPSHOT_INTERVAL_MS).unref();
}

// Graceful drain. Railway sends SIGTERM before replacing the container. We take a
// final snapshot (so the new container can restore the games) and tell connected
// players an update is rolling — they're reconnected automatically once it's up.
let shuttingDown = false;
async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logEvent("info", "shutdown", { signal });
  try { io.emit("server:restarting", { message: "Server is updating — you'll be reconnected automatically." }); } catch (_) {}
  await persistState();
  server.close(() => { logEvent("info", "server_closed", {}); process.exit(0); });
  // Force exit if open sockets keep the server from closing in time.
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Restore any persisted games before accepting traffic, then start listening.
// Guarded so the module can be required in tests without binding a port.
if (require.main === module) {
  (async () => {
    await restoreRooms();
    server.listen(PORT, "0.0.0.0", () => {
      logEvent("info", "server_listening", { port: PORT, env: NODE_ENV, persistence: redis ? "redis" : "memory" });
    });
  })();
}

// Test surface: lets a test require this module (without booting) and exercise the
// persistence layer with an injected client. Not used by the running server.
module.exports = {
  rooms, serializeRoom, deserializeRoom, persistState, restoreRooms,
  setRedisClient: (client) => { redis = client; }
};
