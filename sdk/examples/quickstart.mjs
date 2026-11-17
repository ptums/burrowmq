// Burrow quick start: save 1 order and 1 log line on the device, then send the same order again.
// Run a local NATS server with JetStream first (see README), then: node quickstart.mjs
import { Burrow } from "@burrowmq/sdk";

const burrow = await Burrow.connect({ site: "device01" });

const order = await burrow.send("critical", "orders.new", { order: 42, total: 12.5 }, { id: "order-42" });
console.log("order saved:", order);

const again = await burrow.send("critical", "orders.new", { order: 42, total: 12.5 }, { id: "order-42" });
console.log("same order again (kept once):", again);

const log = await burrow.send("bulk", "logs.app", { level: "info", text: "kiosk started" });
console.log("log saved:", log);

await burrow.close();
