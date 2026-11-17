// A dummy kiosk agent. It takes an order every few seconds and writes log lines all the time.
// It never waits for the internet: Burrow saves each message on the device.
import { Burrow } from "../src/index.ts";

const ORDER_EVERY_MS = 3000; // a customer orders every 3 s
const LOG_EVERY_MS = 500; // the kiosk writes a log line every 0.5 s
const REPORT_EVERY_MS = 5000; // print a status line every 5 s

const burrow = await Burrow.connect({ site: "device01", url: "nats://localhost:4223" }); // a device NATS server on port 4223
console.log("kiosk agent started on device01. Press Ctrl+C to stop.");

let orders = 0;
let logs = 0;
let slowestSendMs = 0;

// Send 1 message and keep track of how long the agent had to wait.
async function timedSend(...args: Parameters<Burrow["send"]>): Promise<void> {
  const start = performance.now();
  try {
    await burrow.send(...args);
  } catch (err) {
    console.error("send failed:", err);
    return;
  }
  slowestSendMs = Math.max(slowestSendMs, performance.now() - start);
}

const orderTimer = setInterval(() => {
  orders++;
  const order = { order: orders, item: "coffee", total: 3.5 };
  // The order number is the ID, so a retry of the same order never makes a second copy.
  void timedSend("critical", "orders.new", order, {
    id: `kiosk-${process.pid}-order-${orders}`,
  });
}, ORDER_EVERY_MS);

const logTimer = setInterval(() => {
  logs++;
  void timedSend("bulk", "logs.app", {
    level: "info",
    text: `screen refresh ${logs}`,
  });
}, LOG_EVERY_MS);

const reportTimer = setInterval(() => {
  console.log(
    `orders ${orders}, logs ${logs}, slowest send ${slowestSendMs.toFixed(1)} ms`,
  );
  slowestSendMs = 0;
}, REPORT_EVERY_MS);

// Ctrl+C: stop the timers, then let Burrow finish its last sends.
process.on("SIGINT", async () => {
  clearInterval(orderTimer);
  clearInterval(logTimer);
  clearInterval(reportTimer);
  await burrow.close();
  console.log(`kiosk agent stopped: ${orders} orders, ${logs} logs`);
  process.exit(0);
});
