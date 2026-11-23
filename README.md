# Burrow

**Offline-first messaging for AI agents at the edge.**

AI agents assume the internet is always there. In a store, a truck or a field device, it often is not. Burrow lets an agent keep working with no connection. Every message is saved on the device first, then sent to the cloud when the link comes back, with the most important messages first and no duplicates.

Burrow is built on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) leaf nodes. The site is [burrowmq.com](https://burrowmq.com).

> **Status: an early prototype.** It is not ready for production yet. The API can still change. Coding agents: start with [AGENTS.md](AGENTS.md), which lists what is tested, the known gaps and the prices.

## How it works

```
  The device                                   The cloud
 ┌─────────────────────────────────┐            ┌─────────────┐
 │ agent ─▶ SDK ─▶ device store    │            │             │
 │                      │          │    link    │ cloud inbox │
 │                 burrow-sync ────┼───────────▶│  (the hub)  │
 └─────────────────────────────────┘            └─────────────┘
```

1. **The agent calls `send()`.** The SDK saves the message on the device's own disk and returns in a few milliseconds, online or offline.
2. **Messages wait in 2 lanes.** The *critical* lane holds orders and alerts. The *bulk* lane holds logs and everything else.
3. **burrow-sync moves them to the cloud.** It sends 10 messages at a time, critical first. If the link is down, it waits a little longer after each try (up to 30 s) and tries again.
4. **The device lets go only after the cloud confirms.** If anything fails, the batch goes back to the device. Nothing is lost.
5. **A retry never makes a second copy.** Each message keeps the same ID, and the cloud drops a repeat of an ID it has already seen.

The link is locked with TLS, and each device logs in with its own signed badge (a NATS JWT), so a lost device can be revoked in seconds.

## Try the SDK

```bash
npm install @burrowmq/sdk
```

The quick start, with a local device server in Docker, is in [sdk/README.md](sdk/README.md).

## What is in this repo

| Folder | What it is | Language |
| --- | --- | --- |
| [`sdk/`](sdk/) | `@burrowmq/sdk`, the library an agent uses: `Burrow.connect()` and `send()`. A sample kiosk agent is in `sdk/examples/kiosk.ts` | TypeScript |
| [`sync/`](sync/) | `burrow-sync`, the program on the device that moves messages to the cloud | Go |

## What we have measured

Test: 200 orders and 200 logs wait on the device, then the link comes back. The hub is on AWS, over the real internet.

| Link speed | Orders all in the cloud | Logs all in the cloud |
| --- | --- | --- |
| No limit | 4 s | 4 s |
| 8 KB/s (a very weak link) | 11 s | 65 s |

## License

Apache 2.0. See [LICENSE](LICENSE).
