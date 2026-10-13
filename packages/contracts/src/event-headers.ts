// Kafka header names every published event record carries. The outbox
// publisher sets them and each consumer reads them, so the names live here, in
// one place: a rename in one service would otherwise be a silent mismatch in
// the others (settlement dead-letters a record that lacks them).
export const EVENT_TYPE_HEADER = "event-type";
export const EVENT_ID_HEADER = "event-id";
