# @burrowmq/sdk

Offline-first messaging for software on edge devices: gateways, controllers, kiosks, field equipment.

`send()` saves each message on the device's own disk and returns in milliseconds, with or without internet. burrow-sync then moves it to the Burrow cloud hub when the link allows: the **critical** lane first, then **bulk**. Each message keeps its ID, so a retry never makes a second copy.

Built on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). Apache 2.0.

> **Status: early preview (0.x).** The API may change. We're working with a few design partners: [burrowmq.com](https://burrowmq.com).

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

## API

### `Burrow.connect(options)`

| Option | Default | What it does |
| --- | --- | --- |
| `site` | (required) | The name of this device, for example `"store042"`. Letters, digits, `-` and `_` |
| `url` | `BURROW_URL`, or `nats://localhost:4222` | The NATS server on the device |
| `setup` | `true` | Create the 2 lanes on the device if they are missing. It never changes lanes that exist |

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

## How the device reaches the cloud

The SDK only talks to the device. A NATS leaf node and burrow-sync carry the messages to the hub at `hub.burrowmq.com` over TLS, with a per-device login. Design partners get their hub login and the sync setup from us.

## License

Apache 2.0. See `LICENSE`.
