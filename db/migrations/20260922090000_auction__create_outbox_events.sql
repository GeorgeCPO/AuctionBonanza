-- migrate:up

CREATE TABLE outbox_events (
    event_id uuid NOT NULL,
    event_type character varying(100) NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    auction_id bigint NOT NULL,
    auction_version bigint NOT NULL,
    payload jsonb NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT outbox_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT outbox_events_auction_id_fkey FOREIGN KEY (auction_id) REFERENCES auctions(id),
    CONSTRAINT outbox_events_event_type_check CHECK (event_type = btrim(event_type) AND char_length(event_type) BETWEEN 1 AND 100),
    CONSTRAINT outbox_events_auction_version_check CHECK (auction_version >= 1),
    CONSTRAINT outbox_events_payload_object_check CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT outbox_events_payload_metadata_check CHECK (
      payload ?& ARRAY['eventId', 'eventType', 'occurredAt', 'auctionId', 'auctionVersion']
      AND payload ->> 'eventId' = event_id::text
      AND payload ->> 'eventType' = event_type
      AND (payload ->> 'occurredAt')::timestamptz = occurred_at
      AND payload ->> 'auctionId' = auction_id::text
      AND payload ->> 'auctionVersion' = auction_version::text
    ),
    CONSTRAINT outbox_events_published_at_check CHECK (published_at IS NULL OR published_at >= occurred_at),
    CONSTRAINT outbox_events_auction_event_version_key UNIQUE (auction_id, event_type, auction_version)
);

CREATE FUNCTION auction_reject_outbox_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF ROW(
    NEW.event_id,
    NEW.event_type,
    NEW.occurred_at,
    NEW.auction_id,
    NEW.auction_version,
    NEW.payload
  ) IS DISTINCT FROM ROW(
    OLD.event_id,
    OLD.event_type,
    OLD.occurred_at,
    OLD.auction_id,
    OLD.auction_version,
    OLD.payload
  ) THEN
    RAISE EXCEPTION 'outbox event data is immutable'
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NEW;
END;
$$;

-- Publishers may update published_at, but event identity, ordering metadata,
-- and the payload itself remain an append-only record of the committed fact.
CREATE TRIGGER outbox_events_immutable_event_data
BEFORE UPDATE ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION auction_reject_outbox_event_mutation();

-- A publisher can claim the oldest unpublished rows in short transactions
-- with ORDER BY occurred_at, event_id FOR UPDATE SKIP LOCKED. Published rows
-- leave this small partial index automatically.
CREATE INDEX outbox_events_unpublished_idx
  ON outbox_events (occurred_at, event_id)
  WHERE published_at IS NULL;

-- migrate:down

DROP TABLE outbox_events;
DROP FUNCTION IF EXISTS auction_reject_outbox_event_mutation();
