-- migrate:up

-- The settlement worker's record of the events it has handled. Kafka delivers
-- at least once, so the worker sees an event again after a crash between its
-- database commit and its offset commit, or when the database commit succeeded
-- but its acknowledgement was lost. Recording the event in the same transaction
-- as its effect makes the second delivery recognisable and harmless.
--
-- It keys on the event, not the auction: that is the consumer rule in
-- OUTBOX_PUBLISHING.md. It also records events with no other effect, such as
-- an auction that closed unsold, which creates no order.
--
-- auction_version is kept for audit. AuctionClosed is the last event of an
-- auction, so settlement needs no version ordering to decide anything.
CREATE TABLE settlement_processed_events (
    event_id uuid NOT NULL,
    event_type character varying(100) NOT NULL,
    auction_id bigint NOT NULL,
    auction_version bigint NOT NULL,
    processed_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT settlement_processed_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT settlement_processed_events_auction_version_check CHECK (auction_version >= 1)
);

-- migrate:down

DROP TABLE settlement_processed_events;
