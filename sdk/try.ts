// Try the SDK: send 1 order twice with the same ID, and 1 log line.
import { Burrow } from "./src/index.ts";

const burrow = await Burrow.connect({ site: "device01", url: "nats://localhost:4223" }); // a device NATS server on port 4223

const first = await burrow.send("critical", "orders.new", { order: 42, total: 12.5 }, { id: "order-42" });
console.log("order:", first);

const again = await burrow.send("critical", "orders.new", { order: 42, total: 12.5 }, { id: "order-42" });
console.log("same order again:", again);

const log = await burrow.send("bulk", "logs.app", { level: "info", text: "kiosk started" });
console.log("log:", log);

await burrow.close();