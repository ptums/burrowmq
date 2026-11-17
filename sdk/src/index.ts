// Burrow SDK: an agent calls send(). The message is saved on the device first,
// and burrow-sync moves it to the cloud when the link allows.
import { randomUUID } from "node:crypto";
import {
  AckPolicy,
  connect,
  DeliverPolicy,
  headers,
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
}

// A repeat of the same message ID within this window is dropped, on the device and in the cloud.
const DUPLICATE_WINDOW_NS = 24 * 60 * 60 * 1_000_000_000;

// Creates the 2 lanes (streams CRITICAL and BULK) and burrow-sync's bookmark in each
// (the "sync" consumer), if they are missing. It never changes lanes that already exist.
export async function setupLanes(jsm: JetStreamManager, site: string): Promise<void> {
  for (const lane of LANES) {
    const stream = lane.toUpperCase();
    try {
      await jsm.streams.info(stream);
    } catch {
      await jsm.streams.add({
        name: stream,
        subjects: [`site.${site}.${lane}.>`],
        storage: StorageType.File,
        duplicate_window: DUPLICATE_WINDOW_NS,
      });
    }
    try {
      await jsm.consumers.info(stream, "sync");
    } catch {
      await jsm.consumers.add(stream, {
        durable_name: "sync",
        ack_policy: AckPolicy.Explicit,
        ack_wait: 30 * 1_000_000_000,
        max_ack_pending: 1000,
        deliver_policy: DeliverPolicy.All,
      });
    }
  }
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
      await setupLanes(await nc.jetstreamManager(), opts.site);
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