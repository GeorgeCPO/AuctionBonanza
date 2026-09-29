-- migrate:up

-- Delivery bookkeeping for the outbox publisher. None of these columns are
-- part of the immutable event record guarded by
-- outbox_events_immutable_event_data, so publishers may update them.
ALTER TABLE outbox_events
  ADD COLUMN publish_attempts integer NOT NULL DEFAULT 0,
  ADD COLUMN next_publish_attempt_at timestamp with time zone,
  ADD COLUMN last_publish_error character varying(1000),
  ADD CONSTRAINT outbox_events_publish_attempts_check CHECK (publish_attempts >= 0),
  -- A published event is never scheduled for another attempt.
  ADD CONSTRAINT outbox_events_published_retry_check CHECK (
    published_at IS NULL OR next_publish_attempt_at IS NULL
  );

-- Supports the claim query's "no earlier unpublished event for this auction"
-- check, which enforces per-auction head-of-line publication. Like
-- outbox_events_unpublished_idx, published rows leave it automatically.
CREATE INDEX outbox_events_unpublished_auction_idx
  ON outbox_events (auction_id, auction_version)
  WHERE published_at IS NULL;

-- migrate:down

DROP INDEX outbox_events_unpublished_auction_idx;

ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_events_published_retry_check,
  DROP CONSTRAINT outbox_events_publish_attempts_check,
  DROP COLUMN last_publish_error,
  DROP COLUMN next_publish_attempt_at,
  DROP COLUMN publish_attempts;
