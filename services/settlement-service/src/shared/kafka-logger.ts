import type { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger } from "pino";

// The same adapter as auction-service's outbox transport and the realtime
// relay, copied rather than shared. The client's info and debug output is
// chatty, so both move down one level.
export function createKafkaLogger(logger: Logger): KafkaJS.Logger {
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
