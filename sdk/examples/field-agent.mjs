// Burrow field agent: watches this device's CPU, asks an on-site model what each reading means,
// and saves the result with Burrow. It never needs the internet to decide or to save.
//
// Each reading:
//   1. Read the CPU temperature and load from Linux.
//   2. Ask the on-site model (Ollama): normal, warning or alert?
//      Simple rules also judge every reading, and the stricter verdict wins (a guardrail):
//      the model can raise the level, but never lower it below the rules.
//   3. Save the reading in the bulk lane. If it is an alert, also save it in the critical lane.
//
// Run it next to a Burrow device server: node field-agent.mjs
// Settings (all optional), as environment variables:
//   BURROW_SITE   the device name            (default "dell3")
//   BURROW_URL    the device server           (default "nats://localhost:4223")
//   OLLAMA_URL    the on-site model server    (default "http://localhost:11434")
//   OLLAMA_MODEL  the model                   (default "qwen2.5:7b")
//   EVERY_MS      time between readings, ms   (default 5000)
//   FAKE_TEMP_C   for tests only: report this CPU temperature instead of the real one,
//                 so you can force the alert path on a device that never gets that hot
import { readFile, readdir } from "node:fs/promises";
import { cpus, loadavg } from "node:os";
import { Burrow } from "@burrowmq/sdk";

const SITE = process.env.BURROW_SITE ?? "dell3";
const URL = process.env.BURROW_URL ?? "nats://localhost:4223";
const OLLAMA_URL = process.env.OLLAMA_URL ?? "http://localhost:11434";
const MODEL = process.env.OLLAMA_MODEL ?? "qwen2.5:7b";
const EVERY_MS = Number(process.env.EVERY_MS ?? 5000);
const FAKE_TEMP_C = process.env.FAKE_TEMP_C === undefined ? null : Number(process.env.FAKE_TEMP_C);
const MODEL_TIMEOUT_MS = 15000; // a slow model is treated as no model
const VERDICTS = ["normal", "warning", "alert"];

// The rules the agent uses when the model does not answer.
const WARNING_C = 80;
const ALERT_C = 90;

// The hottest CPU sensor in /sys/class/thermal, in °C. null if the device has none.
async function cpuTempC() {
  let zones;
  try {
    zones = (await readdir("/sys/class/thermal")).filter((z) => z.startsWith("thermal_zone"));
  } catch {
    return null;
  }
  let hottest = null;
  for (const zone of zones) {
    try {
      const milli = Number(await readFile(`/sys/class/thermal/${zone}/temp`, "utf8"));
      const c = milli / 1000;
      if (Number.isFinite(c) && (hottest === null || c > hottest)) {
        hottest = c;
      }
    } catch {
      continue;
    }
  }
  return hottest;
}

async function readSensors() {
  const reading = {
    at: new Date().toISOString(),
    tempC: await cpuTempC(),
    load1m: loadavg()[0],
    cores: cpus().length,
  };
  // Fault injection at the sensor: fake the input, and test the real path behind it.
  // The reading says it is fake, so nobody in the cloud mistakes it for a real alarm.
  if (FAKE_TEMP_C !== null && Number.isFinite(FAKE_TEMP_C)) {
    reading.tempC = FAKE_TEMP_C;
    reading.fake = true;
  }
  return reading;
}

// Simple rules: hot is bad, and a load far above the number of cores is a warning.
function rules(r) {
  if (r.tempC !== null && r.tempC >= ALERT_C) {
    return "alert";
  }
  if (r.tempC !== null && r.tempC >= WARNING_C) {
    return "warning";
  }
  if (r.load1m > r.cores * 2) {
    return "warning";
  }
  return "normal";
}

// Ask the on-site model for 1 word. Returns null if it does not answer in time, or answers badly.
async function askModel(r) {
  const temp = r.tempC === null ? "unknown" : `${r.tempC.toFixed(1)} C`;
  const prompt =
    `A computer has ${r.cores} CPU cores. CPU temperature is ${temp}. ` +
    `The 1-minute load average is ${r.load1m.toFixed(2)}. ` +
    "Answer with 1 word: normal, warning or alert.";
  try {
    const res = await fetch(`${OLLAMA_URL}/api/generate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      // temperature 0: the same reading always gets the same answer.
      body: JSON.stringify({ model: MODEL, prompt, stream: false, options: { temperature: 0 } }),
      signal: AbortSignal.timeout(MODEL_TIMEOUT_MS),
    });
    if (!res.ok) {
      return null;
    }
    const body = await res.json();
    const word = String(body.response ?? "").trim().toLowerCase().replace(/[^a-z]/g, "");
    return VERDICTS.includes(word) ? word : null;
  } catch {
    return null;
  }
}

const burrow = await Burrow.connect({ site: SITE, url: URL });
console.log(`field agent started on ${SITE}: a reading every ${EVERY_MS} ms, model ${MODEL} at ${OLLAMA_URL}`);
if (FAKE_TEMP_C !== null) {
  console.log(`TEST MODE: every reading reports a fake CPU temperature of ${FAKE_TEMP_C} C`);
}

let running = true;
let readings = 0;
let alerts = 0;

async function stop() {
  running = false;
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop); // systemd stops services with SIGTERM

while (running) {
  const started = Date.now();
  const r = await readSensors();

  // The guardrail: a model must not be the last word on safety. Take the stricter verdict.
  const fromModel = await askModel(r);
  const fromRules = rules(r);
  let verdict = fromRules;
  let source = "rules";
  if (fromModel !== null && VERDICTS.indexOf(fromModel) >= VERDICTS.indexOf(fromRules)) {
    verdict = fromModel;
    source = "model";
  } else if (fromModel !== null) {
    source = "rules overrode model"; // the model said something milder than the rules allow
  }
  const reading = { ...r, verdict, source, model: fromModel, rules: fromRules };

  try {
    // Every reading is a log, in the bulk lane.
    await burrow.send("bulk", "readings.cpu", reading);
    readings++;

    // An alert also goes in the critical lane. Its ID comes from the sensor and the time,
    // so a retry of the same alert never makes a second copy in the cloud.
    if (verdict === "alert") {
      await burrow.send("critical", "alerts.cpu", reading, { id: `${SITE}-cpu-${r.at}` });
      alerts++;
    }
  } catch (err) {
    // The device store did not save the message. The agent knows, and says so.
    console.error(`send failed: ${err instanceof Error ? err.message : err}`);
  }

  const temp = r.tempC === null ? "n/a" : `${r.tempC.toFixed(1)} C`;
  console.log(`${r.at} temp ${temp}${r.fake ? " (fake)" : ""}, load ${r.load1m.toFixed(2)} -> ${verdict} (${source})`);

  const wait = EVERY_MS - (Date.now() - started);
  if (running && wait > 0) {
    await new Promise((done) => setTimeout(done, wait));
  }
}

await burrow.close();
console.log(`field agent stopped: ${readings} readings, ${alerts} alerts`);
