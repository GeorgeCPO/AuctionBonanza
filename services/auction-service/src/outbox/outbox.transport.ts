import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger } from "pino";

export interface OutboundMessage {
  topic: string;
  key: string;
  value: string;
  headers: Readonly<Record<string, string>>;
}

export interface DeliveryReceipt {
  partition: number;
  offset: string | null;
}

// Resolves only after the broker has acknowledged the message with the
// producer's configured acks; rejects on any delivery failure.
export interface MessageTransport {
  send(message: OutboundMessage): Promise<DeliveryReceipt>;
}

export interface KafkaTransportOptions {
  brokers: readonly string[];
  clientId: string;
  publishTimeoutMs: number;
  logger: Logger;
}

export class KafkaMessageTransport implements MessageTransport {
  private readonly producer: KafkaJS.Producer;
  private connected = false;

  constructor(options: KafkaTransportOptions) {
    this.producer = new KafkaJS.Kafka({}).producer({
      kafkaJS: { logger: createKafkaLogger(options.logger) },
      "bootstrap.servers": options.brokers.join(","),
      "client.id": options.clientId,
      // Wait for all in-sync replicas. The outbox marks an event published
      // only after this acknowledgement.
      acks: -1,
      // Broker-side de-duplication of the producer's own internal retries and
      // preserved ordering per partition within one producer session. It does
      // not de-duplicate a republication after the outbox transaction was
      // lost; that duplicate is a separate send with a new sequence number.
      "enable.idempotence": true,
      // Upper bound on queueing plus all internal retries for one message.
      // The publisher also races each send against its own timer because
      // librdkafka scans for expired messages periodically, not exactly.
      "message.timeout.ms": options.publishTimeoutMs,
      "request.timeout.ms": Math.min(options.publishTimeoutMs, 30_000),
      "socket.connection.setup.timeout.ms": Math.min(options.publishTimeoutMs, 10_000),
      // Keep reconnection attempts frequent so that a recovered broker is
      // noticed well within one publish timeout (librdkafka defaults to
      // backing off up to 10 seconds).
      "reconnect.backoff.ms": 100,
      "reconnect.backoff.max.ms": 1_000,
      // Java-client-compatible key hashing, so any producer or tool computing
      // the partition for an auction ID agrees with this one.
      partitioner: "murmur2_random",
      "linger.ms": 5,
    });
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    this.connected = true;
  }

  async send(message: OutboundMessage): Promise<DeliveryReceipt> {
    const [metadata] = await this.producer.send({
      topic: message.topic,
      messages: [{ key: message.key, value: message.value, headers: { ...message.headers } }],
    });
    if (metadata === undefined) {
      throw new Error("Kafka returned no delivery metadata for an acknowledged message");
    }
    return {
      partition: metadata.partition,
      offset: metadata.baseOffset ?? metadata.offset ?? null,
    };
  }

  // Waits for queued deliveries, bounded by timeoutMs, then closes the client.
  // A producer that never connected has nothing to flush; flushing it would
  // only raise a state error that hides why startup failed.
  async disconnect(timeoutMs: number): Promise<void> {
    if (!this.connected) {
      return;
    }
    this.connected = false;
    try {
      await this.producer.flush({ timeout: timeoutMs });
    } finally {
      await this.producer.disconnect();
    }
  }
}

function createKafkaLogger(logger: Logger): KafkaJS.Logger {
  const child = logger.child({ component: "kafka" });
  const kafkaLogger: KafkaJS.Logger = {
    info: (message, extra) => child.debug(extra ?? {}, message),
    debug: (message, extra) => child.trace(extra ?? {}, message),
    warn: (message, extra) => child.warn(extra ?? {}, message),
    error: (message, extra) => child.error(extra ?? {}, message),
    namespace: () => kafkaLogger,
    setLogLevel: () => {},
  };
  return kafkaLogger;
}
