// Power-loss test, part 1: the sender. It runs on the device.
//
// It sends test messages to the critical lane, 1 after another. Each time the device store says
// "saved", it prints that message ID on stdout. Start it from ANOTHER machine over SSH, so each
// printed ID lands on that machine at once, outside the blast radius:
//
//   ssh user@device 'cd ~/burrow-agent && node power-sender.mjs RUN1' > saved-ids.txt
//
// Then pull the device's plug. Every ID in saved-ids.txt is a message Burrow promised to keep.
// After the reboot, power-check.mjs compares that list with what reached the cloud.
//
// To test the device's own disk, stop burrow-sync first, so the messages wait on the device
// instead of leaving for the cloud within a moment. It starts again by itself after the reboot.
//
// The run name (RUN1 above) goes into the subject, so each run is checked on its own.
// Settings: BURROW_SITE (default "dell3"), BURROW_URL (default "nats://localhost:4223"),
// RATE (messages per second, default 50, so a long run does not fill the device store).
// Status lines go to stderr, so stdout holds only IDs.
import { Burrow } from "@burrowmq/sdk";

const RUN = process.argv[2];
if (!RUN || !/^[A-Za-z0-9_-]+$/.test(RUN)) {
  console.error("usage: node power-sender.mjs <run-name>   (letters, digits, - and _)");
  process.exit(2);
}
const SITE = process.env.BURROW_SITE ?? "dell3";
const URL = process.env.BURROW_URL ?? "nats://localhost:4223";
const REPORT_EVERY_MS = 5000;
const RATE = Number(process.env.RATE ?? 50);
const GAP_MS = RATE > 0 ? 1000 / RATE : 0;

const burrow = await Burrow.connect({ site: SITE, url: URL });
console.error(`power sender started: run ${RUN} on ${SITE}. Pull the plug whenever you like.`);

let running = true;
process.on("SIGINT", () => {
  running = false;
});
process.on("SIGTERM", () => {
  running = false;
});

let saved = 0;
let failed = 0;
let lastReport = Date.now();
let savedAtReport = 0;

while (running) {
  const started = Date.now();
  const id = `power-${RUN}-${saved + failed + 1}`;
  try {
    await burrow.send("critical", `test.power.${RUN}`, { run: RUN, n: saved + failed + 1 }, { id });
    // Print only after the store said "saved". On Linux, a write to a pipe is synchronous.
    process.stdout.write(`${id}\n`);
    saved++;
  } catch (err) {
    failed++;
    console.error(`send failed: ${err instanceof Error ? err.message : err}`);
  }

  const now = Date.now();
  if (now - lastReport >= REPORT_EVERY_MS) {
    const rate = ((saved - savedAtReport) * 1000) / (now - lastReport);
    console.error(`saved ${saved}, failed ${failed}, ${rate.toFixed(0)} msg/s`);
    lastReport = now;
    savedAtReport = saved;
  }

  const wait = GAP_MS - (Date.now() - started);
  if (running && wait > 0) {
    await new Promise((done) => setTimeout(done, wait));
  }
}

await burrow.close();
console.error(`power sender stopped: saved ${saved}, failed ${failed}`);
