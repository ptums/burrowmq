# @burrowmq/sdk

Offline-first messaging for software on edge devices: gateways, controllers, kiosks, field equipment.

`send()` saves each message on the device's own disk and returns in milliseconds, with or without internet. burrow-sync then moves it to the Burrow cloud hub when the link allows: the **critical** lane first, then **bulk**. Each message keeps its ID, so a retry never makes a second copy.

Built on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). Apache 2.0.

> **Status: early preview (0.x).** The API may change. Tested results, known gaps and prices: [AGENTS.md](https://github.com/ptums/burrowmq/blob/main/AGENTS.md) and [burrowmq.com](https://burrowmq.com).

## Quick start (5 minutes)

1. Run a NATS server with JetStream on the device. With Docker:

   ```bash
   docker run -d --name burrow-device -p 4222:4222 -v burrow-device:/data nats:2.11 -js -sd /data
   ```

2. Install the SDK in your project (Node 18 or newer):

   ```bash
   npm install @burrowmq/sdk
   ```

3. Send a message:

   ```js
   import { Burrow } from "@burrowmq/sdk";

   const burrow = await Burrow.connect({ site: "device01" });
   const result = await burrow.send("critical", "orders.new", { order: 42 }, { id: "order-42" });
   console.log(result); // { id: "order-42", seq: 1, duplicate: false }
   await burrow.close();
   ```

   Send the same `id` again and you get `duplicate: true`: the device keeps 1 copy.

A full example is in `examples/quickstart.mjs`.

`examples/field-agent.mjs` is a small AI agent for a Linux device. It reads the CPU temperature and load, asks an on-site model (Ollama) whether each reading is normal, a warning or an alert, and falls back to simple rules when the model does not answer. Readings go in the bulk lane, and alerts also go in the critical lane.

## API

### `Burrow.connect(options)`

| Option | Default | What it does |
| --- | --- | --- |
| `site` | (required) | The name of this device, for example `"store042"`. Letters, digits, `-` and `_` |
| `url` | `BURROW_URL`, or `nats://localhost:4222` | The NATS server on the device |
| `setup` | `true` | Create the 2 lanes on the device if they are missing. It never changes lanes that exist |
| `bulkMaxBytes` | 5 GB | Disk cap for the bulk lane, set when the lane is made. When it is full, the oldest bulk messages are dropped. The critical lane has no cap and is never dropped. `0` means no cap |

### `burrow.send(lane, topic, data, options?)`

| Argument | What it is |
| --- | --- |
| `lane` | `"critical"` (orders, alerts) or `"bulk"` (logs, readings, photos). Critical is always sent first |
| `topic` | The rest of the subject, for example `"orders.new"`. The message goes to `site.<site>.<lane>.<topic>` |
| `data` | Any JSON value |
| `options.id` | Your own message ID. Give the same ID on a retry, and only 1 copy is kept (for 24 hours) |

Returns `{ id, seq, duplicate }`.

### `burrow.close()`

Sends anything still in flight to the device, then closes the connection.

## Disk use on the device

A message stays on the device only until the hub confirms it. The lanes use work-queue retention: once burrow-sync forwards a message and the hub acks it, it leaves the device disk. A waiting message takes about 370 bytes of disk for 200 bytes of data. A long outage fills at most `bulkMaxBytes` of bulk, plus the critical backlog.

## Upgrading from 0.1.2 or older

SDK 0.1.2 and older made the lanes with `limits` retention. Those lanes keep every message they have ever sent, so the device disk only grows (about 640 MB a day at 20 messages a second) until sends fail. NATS can't change retention on a lane that exists, and `connect()` never changes existing lanes. Instead it logs a warning that names the old lanes.

To fix a device, run `migrateLanes()` once, with burrow-sync running and the hub reachable:

```js
import { connect } from "nats";
import { migrateLanes } from "@burrowmq/sdk";

const nc = await connect({ servers: "nats://localhost:4222" });
console.log(await migrateLanes(await nc.jetstreamManager(), "device01")); // [ "CRITICAL", "BULK" ]
await nc.close();
```

For each old lane it does 3 things:

1. Stops new messages from entering the lane. While it waits, `send()` to that lane throws, so retry it with the same `id`.
2. Waits until burrow-sync has sent and the hub has confirmed every message in the lane. The default wait is 10 minutes (`timeoutMs`).
3. Deletes the lane and makes it again with work-queue retention, the bulk cap and burrow-sync's `sync` consumer.

If a lane still has messages waiting when time runs out, `migrateLanes()` puts the lane back as it was and throws. It never deletes a lane with messages waiting. Lanes already on work-queue retention are skipped, so running it twice is safe. A running burrow-sync carries on with the new lanes, with no restart.

## Changes

- **0.2.0.** Lanes use work-queue retention, so a message leaves the device disk once the hub confirms it (it was kept forever before). The bulk lane is capped at 5 GB by default (`bulkMaxBytes`), and the oldest bulk is dropped first. New: `migrateLanes()` for lanes made by 0.1.2 or older, and a warning when such lanes are found.

## How the device reaches the cloud

The SDK only talks to the device. A NATS leaf node and burrow-sync carry the messages to the hub at `hub.burrowmq.com` over TLS, with a per-device login. Design partners get their hub login and the sync setup from us.

## License

Apache 2.0. See `LICENSE`.
