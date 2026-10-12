-- migrate:up

-- Retry bookkeeping for the finalizer. When closing an auction fails, the
-- finalizer records the failure and a database-clock time before which no
-- finalizer retries it, so one auction that cannot be closed does not hold up
-- the auctions due after it. Like the outbox's publication state, these are
-- not aggregate state: writing them does not advance `version`, emits no
-- event, and is not exposed by the API.
ALTER TABLE auctions
  ADD COLUMN finalization_attempts integer NOT NULL DEFAULT 0,
  ADD COLUMN next_finalization_attempt_at timestamp with time zone,
  ADD COLUMN last_finalization_error character varying(1000),
  ADD CONSTRAINT auctions_finalization_attempts_check CHECK (finalization_attempts >= 0),
  -- A closed or cancelled auction is never scheduled for another attempt.
  -- The attempt count and last error stay behind as history.
  ADD CONSTRAINT auctions_finalization_retry_terminal_check CHECK (
    (closed_at IS NULL AND cancelled_at IS NULL) OR next_finalization_attempt_at IS NULL
  );

-- migrate:down

ALTER TABLE auctions
  DROP CONSTRAINT auctions_finalization_retry_terminal_check,
  DROP CONSTRAINT auctions_finalization_attempts_check,
  DROP COLUMN last_finalization_error,
  DROP COLUMN next_finalization_attempt_at,
  DROP COLUMN finalization_attempts;
