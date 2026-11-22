// Power-loss test, part 2: the checker. It runs on your own machine, after the device is back
// and burrow-sync has drained.
//
// It reads the IDs the sender printed (saved-ids.txt) and the IDs that reached the cloud inbox,
// then reports:
//   lost       saved on the device, but never reached the cloud. The promise is broken.
//   duplicate  reached the cloud more than once. Dedupe is broken.
//   extra      reached the cloud, but not in the file. Fine: saved in the last instant before the cut.
//
//   HUB_URL=nats://localhost:14222 HUB_CREDS=~/path/admin.creds \
//     node power-check.mjs RUN1 saved-ids.txt
//
// Settings: HUB_URL (required), HUB_CREDS (a creds file, optional), HUB_STREAM (default "INGEST"),
// BURROW_SITE (default "dell3").
import { readFile } from "node:fs/promises";
import { connect, credsAuthenticator } from "nats";

const [RUN, FILE] = process.argv.slice(2);
if (!RUN || !FILE) {
  console.error("usage: node power-check.mjs <run-name> <saved-ids.txt>");
  process.exit(2);
}
if (!process.env.HUB_URL) {
  console.error("Set HUB_URL to the cloud hub, for example nats://localhost:14222");
  process.exit(2);
}
const SITE = process.env.BURROW_SITE ?? "dell3";
const STREAM = process.env.HUB_STREAM ?? "INGEST";
const SUBJECT = `ingest.${SITE}.critical.test.power.${RUN}`;

// The device's list. A last line cut in half by the power loss is not a full ID, so skip it.
const text = await readFile(FILE, "utf8");
// After the last "\n" there is either nothing or a half-written ID, so drop that last piece.
const complete = text.split("\n").slice(0, -1);
const saved = new Set(complete.filter((l) => l.startsWith(`power-${RUN}-`)));

const opts = { servers: process.env.HUB_URL };
if (process.env.HUB_CREDS) {
  opts.authenticator = credsAuthenticator(await readFile(process.env.HUB_CREDS));
}
const nc = await connect(opts);
const js = nc.jetstream();

// Read every message of this run from the cloud inbox, with a temporary ordered reader.
const seen = new Map();
const consumer = await js.consumers.get(STREAM, { filterSubjects: [SUBJECT] });
const info = await consumer.info();
let left = info.num_pending;
while (left > 0) {
  const batch = await consumer.fetch({ max_messages: Math.min(left, 1000), expires: 5000 });
  let got = 0;
  for await (const m of batch) {
    const id = m.headers?.get("Nats-Msg-Id") ?? "";
    seen.set(id, (seen.get(id) ?? 0) + 1);
    got++;
  }
  if (got === 0) {
    break;
  }
  left -= got;
}
await nc.close();

const lost = [...saved].filter((id) => !seen.has(id));
const duplicate = [...seen].filter(([, n]) => n > 1).map(([id]) => id);
const extra = [...seen.keys()].filter((id) => !saved.has(id));

console.log(`run ${RUN}: ${saved.size} saved on the device, ${seen.size} different IDs in the cloud`);
console.log(`lost:      ${lost.length}${lost.length ? `  e.g. ${lost.slice(0, 5).join(", ")}` : ""}`);
console.log(`duplicate: ${duplicate.length}${duplicate.length ? `  e.g. ${duplicate.slice(0, 5).join(", ")}` : ""}`);
console.log(`extra:     ${extra.length}  (saved in the last instant before the cut, which is fine)`);
console.log(lost.length === 0 && duplicate.length === 0 ? "PASS: every saved message reached the cloud exactly once" : "FAIL");
process.exit(lost.length === 0 && duplicate.length === 0 ? 0 : 1);
