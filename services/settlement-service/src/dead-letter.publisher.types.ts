import type { Logger } from "pino";

export interface KafkaDeadLetterPublisherOptions {
  brokers: readonly string[];
  clientId: string;
  topic: string;
  sendTimeoutMs: number;
  logger: Logger;
}
