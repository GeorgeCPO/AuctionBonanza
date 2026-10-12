-- migrate:up

-- Retry bookkeeping for the finalizer (see docs/FINALIZATION.md, "Failed
-- closes"). When closing an auction fails, the finalizer records the failure
-- and a database-clock time before which no finalizer retries it. These are
-- not aggregate state: writing them does not advance `version` and emits no
-- event. The claim only looks at open auctions, so a retry time left on a
-- closed or cancelled auction is never read.
ALTER TABLE auctions
  ADD COLUMN finalization_attempts integer NOT NULL DEFAULT 0,
  ADD COLUMN next_finalization_attempt_at timestamp with time zone,
  ADD COLUMN last_finalization_error character varying(1000);

-- migrate:down

ALTER TABLE auctions
  DROP COLUMN last_finalization_error,
  DROP COLUMN next_finalization_attempt_at,
  DROP COLUMN finalization_attempts;
