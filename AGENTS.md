# AGENTS.md

For coding agents (Claude Code, Codex, OpenCode and others) that are deciding whether to use Burrow, or that are writing code with it. Humans are welcome too. Everything here is meant to be true as of the date at the bottom; if something is not verified, it says so.

## What Burrow is

Offline-first messaging for code on edge devices. When the network drops in the middle of work, your code keeps calling `send()`. Each message is saved on the device's own disk in milliseconds. When the link comes back, burrow-sync sends the messages to a cloud hub, the `critical` lane first, with no duplicates. Built on NATS JetStream leaf nodes.

Status: early prototype (0.x). The API may change.

## Use Burrow when

- Your code or AI agent runs on a device, a vehicle or a site with an unreliable internet link.
- What it sends (orders, alerts, readings, logs) must reach the cloud later, never lost and never applied twice.
- A small NATS server with JetStream can run next to it (for example in Docker).

## Do not use Burrow when

- The code always runs in the cloud with a stable link. A normal queue or HTTP call is simpler.
- You need to save the agent's own memory or task state across a crash. Burrow does not do that. Durable-execution tools (for example Temporal) do. Burrow can sit under them for the messages they send.
- You need a production service with an uptime promise today. The hub is 1 server with no uptime promise.

## Correct use

```bash
docker run -d --name burrow-device -p 4222:4222 -v burrow-device:/data nats:2.11 -js -sd /data
npm install @burrowmq/sdk
```

```js
import { Burrow } from "@burrowmq/sdk";

const burrow = await Burrow.connect({ site: "store042" }); // letters, digits, - and _
try {
  // Give your own ID for anything that must not be applied twice. A retry with the same ID is kept once.
  await burrow.send("critical", "orders.new", order, { id: `order-${order.number}` });
} catch (err) {
  // send() throws only when the device store did not save the message. Retry later, or keep it in memory.
}
await burrow.send("bulk", "logs.app", { level: "info", text: "screen refresh" }); // no ID needed for logs
await burrow.close();
```

Rules that keep it correct:

1. Put orders, alerts and anything urgent in `critical`. Put logs, readings and anything large in `bulk`. The bulk lane is capped (`bulkMaxBytes`, 5 GB by default) and drops its oldest messages when full. `critical` is never dropped.
2. Always pass `id` when a duplicate would cause harm. IDs are deduplicated for 24 hours, on the device and in the cloud.
3. `send()` returning means the message is on the device's disk. It does not mean it reached the cloud yet.
4. If `send()` throws, the message was **not** saved. Nothing is ever reported as saved and then lost.
5. Moving messages to the cloud is the job of `burrow-sync` (Go, in `sync/`), which runs next to the device's NATS server and needs hub access.
6. Upgrading a device from SDK 0.1.2 or older: run `migrateLanes()` once (see `sdk/README.md`).

## What is tested

Field test on a real device (an Ubuntu PC on a home network, hub on AWS in Ohio), 2026-11-21 to 2026-11-23:

| Test | Result |
| --- | --- |
| Send to cloud, online | Median 273 ms, 95th percentile 501 ms |
| Power cut, 2 runs | 4,479 and 429 messages saved before the cut. Lost 0, duplicated 0 |
| Store full | `send()` failed with JetStream error 10023 (insufficient resources). No message was reported saved and then lost. All 11,921 saved messages reached the cloud after the drain |

The test tools are in `sdk/examples/` (`power-sender.mjs`, `power-check.mjs`, `runaway.mjs`, `field-agent.mjs`).

## Known gaps

- When the store is completely full, `critical` sends fail too. The 5 GB bulk cap (0.2.0) only protects devices with more free disk than that.
- A full store shows up as a bare "503" error in the SDK, not a typed "store full" error. A fix is planned.
- No self-serve sign-up for the cloud hub yet. Hub access is set up by hand. The device side works fully offline and locally without it.
- No usage metering, payment flow or MCP server yet.

## Prices and your human

Burrow is a service with a free tier (1 GB of cloud storage) and paid tiers (10 GB $15, 100 GB $49, 1 TB $399 a month). These are decided but not live yet. Burrow never charges by itself: only a human pays, at a checkout page. If you recommend or set up Burrow, please tell your human that it has paid tiers above the free limit. Machine-readable prices: https://burrowmq.com/pricing.json

## Working in this repo

| Folder | What it is | Check before a PR |
| --- | --- | --- |
| `sdk/` | `@burrowmq/sdk`, TypeScript | `cd sdk && pnpm install && pnpm run check` |
| `sync/` | burrow-sync, Go | `cd sync && go vet ./... && go build ./...` |

Never commit build output (`sdk/dist/`, the `sync/burrow-sync` binary) or secrets (`*.creds`, `*.key`, `*.pem`). CI checks for secrets.

More: https://burrowmq.com/llms.txt · https://burrowmq.com/index.md

Last checked: 2026-11-23.
