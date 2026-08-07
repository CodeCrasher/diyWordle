// Integration tests for the host's "skip the current picker" control
// (game:skipChooser). The host must be able to move a stalled word-picker along
// without waiting out the pick clock, handing the turn to the next player in the
// round-robin queue. Drives the real server over the shared minimal Socket.IO
// client. The server runs WITHOUT an MW_API_KEY, so word validation fails open.
const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const path = require("node:path");
const { IOClient, authedClient, waitForHealth } = require("./helpers/socket-client");

const SERVER = path.join(__dirname, "..", "server.js");
const PORT = 3991;

let server;
before(async () => {
  const env = { ...process.env, NODE_ENV: "development", PORT: String(PORT) };
  delete env.MW_API_KEY;          // fail-open validation so the test runs offline
  env.REQUIRE_HOST_AUTH = "true"; // hosting requires a signed-in account
  server = spawn(process.execPath, [SERVER], { env, stdio: ["ignore", "pipe", "pipe"] });
  await waitForHealth(PORT);
});
after(() => { if (server) server.kill("SIGKILL"); });

// Build a room with the host plus two guests, start the session, and hand the
// pick to the FIRST guest — the host is always first in the round-robin order,
// so they pass their own turn to get someone else on the clock.
async function roomWithGuestPicking(hostName, names) {
  const host = await authedClient(PORT);
  const guests = [];
  for (const _ of names) {
    const g = new IOClient(PORT);
    await g.connect();
    guests.push(g);
  }

  const created = await host.emit("room:create", { name: hostName, roundTime: 60 });
  assert.ok(created.ok, created.error);
  const code = created.room.code;

  const guestIds = [];
  for (let i = 0; i < guests.length; i++) {
    const joined = await guests[i].emit("room:join", { code, name: names[i] });
    assert.ok(joined.ok, joined.error);
    guestIds.push(joined.playerId);
  }

  const choosingP = host.once("game:choosingWord");
  const started = await host.emit("game:start", { roundTime: 60 });
  assert.ok(started.ok, started.error);
  const first = await choosingP;
  assert.equal(first.chooserId, created.playerId, "host should be the first chooser");

  // Host steps aside so a guest is the one on the clock.
  const passedP = host.once("game:choosingWord");
  const passed = await host.emit("game:passTurn", {});
  assert.ok(passed.ok, passed.error);
  const second = await passedP;
  assert.equal(second.chooserId, guestIds[0], "pick should move to the first guest");

  return { host, guests, guestIds, hostId: created.playerId };
}

test("host can skip the current picker; the turn moves to the next player in the queue", async () => {
  const { host, guests, guestIds } = await roomWithGuestPicking("Hostskip", ["Guestalpha", "Guestbeta"]);

  // The next player in the round-robin must be the one asked to pick.
  const requestP = guests[1].once("game:requestWord");
  const passedP = guests[0].once("game:chooserPassed");
  const choosingP = host.once("game:choosingWord");

  const res = await host.emit("game:skipChooser", {});
  assert.ok(res.ok, res.error);

  const passedEvt = await passedP;
  assert.equal(passedEvt.reason, "host", "clients should be told the host did the skipping");
  assert.equal(passedEvt.skippedName, "Guestalpha", "the skipped player is named");

  const choosing = await choosingP;
  assert.equal(choosing.chooserId, guestIds[1], "pick should advance to the next player in the queue");
  await requestP; // the new picker is actually prompted for a word

  host.close();
  guests.forEach((g) => g.close());
});

test("a non-host cannot skip the picker", async () => {
  const { host, guests, guestIds } = await roomWithGuestPicking("Hostdeny", ["Guestgamma", "Guestdelta"]);

  // The player who isn't picking tries to skip the one who is.
  const res = await guests[1].emit("game:skipChooser", {});
  assert.equal(res.ok, false);
  assert.match(res.error, /only the host/i);

  // The rejected attempt must not have advanced the queue: when the HOST then
  // skips, the turn lands on the next player, not the one after.
  const choosingP = host.once("game:choosingWord");
  await host.emit("game:skipChooser", {});
  const choosing = await choosingP;
  assert.equal(choosing.chooserId, guestIds[1], "the failed skip must not have advanced the queue");

  host.close();
  guests.forEach((g) => g.close());
});

test("skipping is rejected once the word is locked in", async () => {
  const { host, guests } = await roomWithGuestPicking("Hostlate", ["Guestepsilon", "Guestzeta"]);

  const startedP = host.once("game:started");
  const submit = await guests[0].emit("game:submitWord", { word: "APPLE", hint: "" });
  assert.ok(submit.ok, submit.error);
  await startedP;

  const res = await host.emit("game:skipChooser", {});
  assert.equal(res.ok, false);
  assert.match(res.error, /no pick to skip/i);

  host.close();
  guests.forEach((g) => g.close());
});
