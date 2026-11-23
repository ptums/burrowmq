// Full-disk test: a runaway agent. It runs on the device.
//
// It floods the bulk lane with big log messages, as fast as the device store takes them, like a
// buggy agent stuck in a loop. Every 2 seconds it also tries to send 1 alert on the critical lane.
// It answers 3 questions:
//   1. When the store is full, does send() fail with a clear error, or does it ever lie?
//   2. Can an alert still get in while the bulk lane floods the store?
//   3. What does the error say, so an agent can tell "store full" from other errors?
//
// Stop burrow-sync first, so nothing drains while the store fills. Stop with Ctrl+C.
// Settings: BURROW_SITE (default "dell3"), BURROW_URL (default "nats://localhost:4223"),
// SIZE_KB (size of each log, default 8), STOP_AFTER_FAILS (default 200 failed sends in a row).
import { Burrow } from "@burrowmq/sdk";

const SITE = process.env.BURROW_SITE ?? "dell3";
const URL = process.env.BURROW_URL ?? "nats://localhost:4223";
const SIZE_KB = Number(process.env.SIZE_KB ?? 8);
const STOP_AFTER_FAILS = Number(process.env.STOP_AFTER_FAILS ?? 200);
const ALERT_EVERY_MS = 2000;
const FULL_FOR_MS = 15000; // after the store is full, keep going this long, then stop
let fullSince = null;
const RUN = `runaway-${Date.now()}`;

const burrow = await Burrow.connect({ site: SITE, url: URL });
const filler = "x".repeat(SIZE_KB * 1024);
console.log(`runaway agent started on ${SITE}: ${SIZE_KB} KB logs as fast as possible, 1 alert try every 2 s. Ctrl+C to stop.`);

let running = true;
process.on("SIGINT", () => {
  running = false;
});

let logsSaved = 0;
let logsFailed = 0;
let failsInARow = 0;
let alertsSaved = 0;
let alertsFailed = 0;
const errors = new Map(); // error text -> how many times
let firstFailAt = null;
let lastAlertTry = 0;
let lastReport = Date.now();

// Keep the full story of each error: its message, its code and JetStream's own description.
function remember(err) {
  const parts = [err instanceof Error ? err.message : String(err)];
  if (err && typeof err === "object") {
    if ("code" in err && err.code) {
      parts.push(`code ${err.code}`);
    }
    if ("api_error" in err && err.api_error) {
      parts.push(`jetstream ${err.api_error.err_code}: ${err.api_error.description}`);
    }
  }
  const text = parts.join(" | ");
  errors.set(text, (errors.get(text) ?? 0) + 1);
}

while (running) {
  try {
    await burrow.send("bulk", "logs.runaway", { run: RUN, n: logsSaved + logsFailed + 1, filler });
    logsSaved++;
    failsInARow = 0;
  } catch (err) {
    logsFailed++;
    failsInARow++;
    firstFailAt ??= new Date().toISOString();
    remember(err);
  }

  const now = Date.now();
  if (now - lastAlertTry >= ALERT_EVERY_MS) {
    lastAlertTry = now;
    try {
      await burrow.send("critical", "alerts.runaway", { run: RUN, at: new Date().toISOString() }, { id: `${RUN}-alert-${now}` });
      alertsSaved++;
    } catch (err) {
      alertsFailed++;
      remember(err);
    }
  }

  if (now - lastReport >= 5000) {
    lastReport = now;
    console.log(`logs saved ${logsSaved} (${Math.round((logsSaved * SIZE_KB) / 1024)} MB), failed ${logsFailed} | alerts saved ${alertsSaved}, failed ${alertsFailed}`);
  }

  // Once the store is full, keep trying for FULL_FOR_MS, so several alerts are tried while it is full.
  if (failsInARow >= STOP_AFTER_FAILS) {
    fullSince ??= now;
    if (now - fullSince >= FULL_FOR_MS) {
      console.log(`stopping: the store has refused every log for ${FULL_FOR_MS / 1000} s`);
      break;
    }
    await new Promise((done) => setTimeout(done, 50)); // do not spin while full
  }
}

await burrow.close();
console.log("\nresult:");
console.log(`  logs:   saved ${logsSaved} (${Math.round((logsSaved * SIZE_KB) / 1024)} MB), failed ${logsFailed}, first failure at ${firstFailAt ?? "never"}`);
console.log(`  alerts: saved ${alertsSaved}, failed ${alertsFailed}`);
console.log("  errors seen:");
for (const [text, n] of errors) {
  console.log(`    ${n} x ${text}`);
}
