-- migrate:up

-- Consumers of auction events detect missed events by version gaps: holding
-- version n, receiving n + 2 means n + 1 was lost. That only works when every
-- version change emits exactly one event carrying the new version. The
-- outbox's current-version trigger guarantees that every event carries the
-- current version; these two triggers guarantee the converse.
--
-- 1. Versions move forward one step at a time. A skipped version would look
--    like a lost event to every consumer, forever.
CREATE FUNCTION auction_require_single_version_step()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.version <> OLD.version + 1 THEN
    RAISE EXCEPTION 'auction % version must advance from % to %, not to %',
      NEW.id, OLD.version, OLD.version + 1, NEW.version
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'auctions_version_single_step';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER auctions_version_single_step
BEFORE UPDATE OF version ON auctions
FOR EACH ROW
WHEN (NEW.version IS DISTINCT FROM OLD.version)
EXECUTE FUNCTION auction_require_single_version_step();

-- 2. Every version change has its outbox event by commit. The check is
--    deferred because a writer bumps the version first and appends the event
--    afterwards: the outbox trigger only accepts an event for the auction's
--    current version. The unique (auction_id, auction_version) key already
--    limits each version to one event.
--
--    The creation version has no event; consumers take it from a snapshot.
CREATE FUNCTION auction_require_version_outbox_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM outbox_events
    WHERE auction_id = NEW.id
      AND auction_version = NEW.version
  ) THEN
    RAISE EXCEPTION 'auction % version % has no outbox event', NEW.id, NEW.version
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'auctions_version_outbox_event';
  END IF;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER auctions_version_outbox_event
AFTER UPDATE OF version ON auctions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.version IS DISTINCT FROM OLD.version)
EXECUTE FUNCTION auction_require_version_outbox_event();

-- migrate:down

DROP TRIGGER auctions_version_outbox_event ON auctions;
DROP FUNCTION auction_require_version_outbox_event();
DROP TRIGGER auctions_version_single_step ON auctions;
DROP FUNCTION auction_require_single_version_step();
