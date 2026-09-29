-- migrate:up

-- The outbox publisher's per-auction ordering relies on two facts that were
-- previously only writer discipline:
--
-- 1. auction_version totally orders an auction's events. The previous key
--    allowed two event types at one version, whose relative order would then
--    be arbitrary. One event per aggregate version removes the tie.
ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_events_auction_event_version_key,
  ADD CONSTRAINT outbox_events_auction_version_key UNIQUE (auction_id, auction_version);

-- 2. An auction's events become visible in version order. This holds when
--    every writer appends the event for the auction's current version while
--    holding the auction row lock until commit: the writer of version n + 1
--    cannot read the auction until the writer of version n has committed.
--    The trigger takes that lock itself (a no-op for bid acceptance, which
--    already holds FOR UPDATE) and rejects any other version, so a writer
--    must bump auctions.version in the same transaction to append a new event.
CREATE FUNCTION auction_require_current_outbox_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  current_version bigint;
BEGIN
  SELECT version INTO current_version
  FROM auctions
  WHERE id = NEW.auction_id
  FOR NO KEY UPDATE;

  -- A missing auction is reported by the foreign key instead.
  IF FOUND AND current_version <> NEW.auction_version THEN
    RAISE EXCEPTION 'outbox event auction_version % does not match auction % version %',
      NEW.auction_version, NEW.auction_id, current_version
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'outbox_events_current_auction_version';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_current_auction_version
BEFORE INSERT ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION auction_require_current_outbox_version();

-- migrate:down

DROP TRIGGER outbox_events_current_auction_version ON outbox_events;
DROP FUNCTION auction_require_current_outbox_version();

ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_events_auction_version_key,
  ADD CONSTRAINT outbox_events_auction_event_version_key UNIQUE (auction_id, event_type, auction_version);
