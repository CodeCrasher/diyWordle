// Regression tests for display-name handling.
//
// The bug: room:create used `isDisplayName(name) ? name : "Host"`, so any host
// name the validator didn't like was SILENTLY replaced with "Host" and the call
// still returned ok — the host just found themselves renamed with no
// explanation. room:join had always rejected properly; create now matches it.
//
// The rule itself also widened to what real names contain (spaces, accents,
// hyphens, apostrophes) while still excluding emoji, underscores and symbols.
const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const path = require("node:path");
const { IOClient, authedClient, waitForHealth } = require("./helpers/socket-client");

const SERVER = path.join(__dirname, "..", "server.js");
const PORT = 3992;

let server;
before(async () => {
  const env = { ...process.env, NODE_ENV: "development", PORT: String(PORT) };
  delete env.MW_API_KEY;
  env.REQUIRE_HOST_AUTH = "true";
  server = spawn(process.execPath, [SERVER], { env, stdio: ["ignore", "pipe", "pipe"] });
  await waitForHealth(PORT);
});
after(() => { if (server) server.kill("SIGKILL"); });

const hostNameOf = (res) => res.room.players.find((p) => p.isHost).name;

test("a host name is never silently replaced with \"Host\"", async () => {
  const host = await authedClient(PORT);

  // The exact shape of the original bug: a perfectly reasonable name with a
  // space came back as ok:true, renamed to "Host".
  const spaced = await host.emit("room:create", { name: "Ambhoj S", roundTime: 180 });
  assert.ok(spaced.ok, spaced.error);
  assert.equal(hostNameOf(spaced), "Ambhoj S", "the host's own name must survive round-tripping");

  host.close();
});

test("an unusable host name is rejected, not quietly rewritten", async () => {
  const host = await authedClient(PORT);

  const res = await host.emit("room:create", { name: "Am", roundTime: 180 });
  assert.equal(res.ok, false, "create must fail rather than invent a name");
  assert.match(res.error, /3-16/);

  // Nothing may have been created under a substituted name.
  const after = await host.emit("room:create", { name: "Ambhoj", roundTime: 180 });
  assert.ok(after.ok, after.error);
  assert.equal(hostNameOf(after), "Ambhoj");

  host.close();
});

test("names people actually have are accepted on both create and join", async () => {
  const host = await authedClient(PORT);
  const created = await host.emit("room:create", { name: "J. Smith", roundTime: 180 });
  assert.ok(created.ok, created.error);
  assert.equal(hostNameOf(created), "J. Smith");
  const code = created.room.code;

  for (const name of ["José", "O'Brien", "Anne-Marie", "अम्भोज"]) {
    const guest = new IOClient(PORT);
    await guest.connect();
    const joined = await guest.emit("room:join", { code, name });
    assert.ok(joined.ok, `${name} should be a valid display name (got: ${joined.error})`);
    guest.close();
  }

  host.close();
});

test("emoji, underscores, padding and overlong names stay rejected", async () => {
  const host = await authedClient(PORT);
  const created = await host.emit("room:create", { name: "Roomowner", roundTime: 180 });
  assert.ok(created.ok, created.error);
  const code = created.room.code;

  const bad = ["Ambhoj_1", "Ambhoj👋", "A  B", "A--B", "<script>x", "Am", "", "ThisNameIsWayTooLongToPass"];
  for (const name of bad) {
    const guest = new IOClient(PORT);
    await guest.connect();
    const joined = await guest.emit("room:join", { code, name });
    assert.equal(joined.ok, false, `${JSON.stringify(name)} should be rejected`);
    guest.close();
  }

  host.close();
});

test("surrounding whitespace is trimmed rather than counted", async () => {
  const host = await authedClient(PORT);
  const res = await host.emit("room:create", { name: "  Ambhoj  ", roundTime: 180 });
  assert.ok(res.ok, res.error);
  assert.equal(hostNameOf(res), "Ambhoj", "padding must not survive into the display name");
  host.close();
});
