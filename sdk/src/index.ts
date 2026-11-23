// Burrow SDK: an agent calls send(). The message is saved on the device first,
// and burrow-sync moves it to the cloud when the link allows.
import { randomUUID } from "node:crypto";
import {
  AckPolicy,
  connect,
  DeliverPolicy,
  DiscardPolicy,
  headers,
  RetentionPolicy,
  StorageType,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
} from "nats";

// The lanes, most important first. burrow-sync sends critical before bulk.
export type Lane = "critical" | "bulk";
const LANES: Lane[] = ["critical", "bulk"];

export interface BurrowOptions {
  site: string; // the name of this device, for example "device01"
  url?: string; // the device's own NATS server. Default: BURROW_URL, or nats://localhost:4222
  setup?: boolean; // create the lanes on the device if they are missing. Default: true
  bulkMaxBytes?: number; // disk cap for the bulk lane on a new lane. Oldest bulk is dropped first. Default: 5 GB. 0 = no cap
}

// A repeat of the same message ID within this window is dropped, on the device and in the cloud.
const DUPLICATE_WINDOW_NS = 24 * 60 * 60 * 1_000_000_000;

// QR-2: 5 GB of waiting messages per site, oldest bulk data dropped first.
export const DEFAULT_BULK_MAX_BYTES = 5 * 1024 ** 3;

export interface LaneOptions {
  bulkMaxBytes?: number; // see BurrowOptions.bulkMaxBytes
}

// How each lane is made. Work-queue retention: a message leaves the device disk once
// burrow-sync's "sync" consumer acks it (the hub has confirmed it). CRITICAL has no cap,
// so it is never dropped. BULK is capped and drops its oldest messages when full.
function laneConfig(lane: Lane, site: string, opts: LaneOptions) {
  const bulkMax = opts.bulkMaxBytes ?? DEFAULT_BULK_MAX_BYTES;
  if (!Number.isInteger(bulkMax) || bulkMax < 0) {
    throw new Error(`Burrow: bulkMaxBytes must be a whole number of bytes, 0 or more (got ${bulkMax}).`);
  }
  return {
    name: lane.toUpperCase(),
    subjects: [`site.${site}.${lane}.>`],
    storage: StorageType.File,
    retention: RetentionPolicy.Workqueue,
    duplicate_window: DUPLICATE_WINDOW_NS,
    ...(lane === "bulk" && bulkMax > 0 ? { max_bytes: bulkMax, discard: DiscardPolicy.Old } : {}),
  };
}

async function addSyncConsumer(jsm: JetStreamManager, stream: string): Promise<void> {
  await jsm.consumers.add(stream, {
    durable_name: "sync",
    ack_policy: AckPolicy.Explicit,
    ack_wait: 30 * 1_000_000_000,
    max_ack_pending: 1000,
    deliver_policy: DeliverPolicy.All,
  });
}

// Creates the 2 lanes (streams CRITICAL and BULK) and burrow-sync's bookmark in each
// (the "sync" consumer), if they are missing. It never changes lanes that already exist.
// A lane made by SDK 0.1.2 or older has "limits" retention and keeps every message forever:
// it logs a warning, and migrateLanes() fixes it. Returns the names of such lanes.
export async function setupLanes(jsm: JetStreamManager, site: string, opts: LaneOptions = {}): Promise<string[]> {
  const old: string[] = [];
  for (const lane of LANES) {
    const cfg = laneConfig(lane, site, opts);
    const stream = cfg.name;
    try {
      const info = await jsm.streams.info(stream);
      if (info.config.retention !== RetentionPolicy.Workqueue) old.push(stream);
    } catch {
      await jsm.streams.add(cfg);
    }
    try {
      await jsm.consumers.info(stream, "sync");
    } catch {
      await addSyncConsumer(jsm, stream);
    }
  }
  if (old.length > 0) {
    console.warn(
      `Burrow: lane(s) ${old.join(", ")} use "limits" retention, so sent messages never leave the device disk ` +
        `and the disk fills up. Fix: call migrateLanes() from @burrowmq/sdk once (see the README, ` +
        `"Upgrading from 0.1.2 or older"). It waits until burrow-sync has sent everything, then remakes the lanes.`,
    );
  }
  return old;
}

export interface MigrateOptions extends LaneOptions {
  timeoutMs?: number; // how long to wait for burrow-sync to send what is waiting. Default: 10 minutes
}

// Remakes lanes that still have "limits" retention with work-queue retention (NATS cannot
// change retention on an existing stream). For each such lane it:
//   1. points the lane at a placeholder subject, so new send() calls fail and can be retried,
//      instead of landing in a lane that is about to be deleted;
//   2. waits until the "sync" consumer has 0 pending and 0 unacked, so nothing unsent is lost;
//   3. deletes the lane and makes it again, with its "sync" consumer.
// burrow-sync must be running and the hub reachable. If the lane does not drain in time, its
// subjects are put back and it throws: the lane is left as it was, with nothing deleted.
// Each remade lane starts a fresh 24-hour duplicate window on the device; the hub's INGEST
// stream still drops repeats. A running burrow-sync finds the new "sync" consumers by name, so it
// carries on without a restart.
// Returns the names of the lanes it remade.
export async function migrateLanes(jsm: JetStreamManager, site: string, opts: MigrateOptions = {}): Promise<string[]> {
  const timeoutMs = opts.timeoutMs ?? 10 * 60 * 1000;
  const done: string[] = [];
  for (const lane of LANES) {
    const cfg = laneConfig(lane, site, opts);
    const stream = cfg.name;
    let info;
    try {
      info = await jsm.streams.info(stream);
    } catch {
      continue; // missing: setupLanes() makes it
    }
    if (info.config.retention === RetentionPolicy.Workqueue) continue;

    const original = info.config;
    await jsm.streams.update(stream, { ...original, subjects: [`burrow.migrating.${site}.${lane}`] });
    try {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const ci = await jsm.consumers.info(stream, "sync");
        if (ci.num_pending === 0 && ci.num_ack_pending === 0) break;
        if (Date.now() > deadline) {
          throw new Error(
            `Burrow: ${stream} still has ${ci.num_pending} pending and ${ci.num_ack_pending} unacked messages ` +
              `after ${timeoutMs} ms. Is burrow-sync running and the hub reachable? The lane was not changed.`,
          );
        }
        await new Promise((r) => setTimeout(r, 1000));
      }
    } catch (err) {
      await jsm.streams.update(stream, original);
      throw err;
    }
    await jsm.streams.delete(stream);
    await jsm.streams.add(cfg);
    await addSyncConsumer(jsm, stream);
    done.push(stream);
  }
  return done;
}

export interface SendOptions {
  id?: string; // your own ID for this message. Give the same ID on a retry, and the cloud keeps 1 copy
}

export interface SendResult {
  id: string; // the message ID that was used
  seq: number; // the message's place in the lane on the device
  duplicate: boolean; // true if the device already had a message with this ID
}

export class Burrow {
  private constructor(
    private readonly nc: NatsConnection,
    private readonly js: JetStreamClient,
    private readonly site: string,
  ) {}

  // Connect to the device. This works with no internet, because the device's server is local.
  static async connect(opts: BurrowOptions): Promise<Burrow> {
    if (!/^[A-Za-z0-9_-]+$/.test(opts.site)) {
      throw new Error(`Burrow: site "${opts.site}" may use only letters, digits, "-" and "_".`);
    }
    const url = opts.url ?? process.env.BURROW_URL ?? "nats://localhost:4222";
    const nc = await connect({ servers: url, maxReconnectAttempts: -1 });
    if (opts.setup !== false) {
      await setupLanes(await nc.jetstreamManager(), opts.site, { bulkMaxBytes: opts.bulkMaxBytes });
    }
    return new Burrow(nc, nc.jetstream(), opts.site);
  }

  // Save 1 message on the device, in a lane. topic is the rest of the subject, for example "orders.new".
  async send(lane: Lane, topic: string, data: unknown, opts: SendOptions = {}): Promise<SendResult> {
    const subject = `site.${this.site}.${lane}.${topic}`;
    const id = opts.id ?? randomUUID();

    const h = headers();
    h.set("Burrow-Agent-Time", new Date().toISOString()); // when the agent made it

    const body = new TextEncoder().encode(JSON.stringify(data));
    const ack = await this.js.publish(subject, body, { msgID: id, headers: h });
    return { id, seq: ack.seq, duplicate: ack.duplicate };
  }

  async close(): Promise<void> {
    await this.nc.drain();
  }
}