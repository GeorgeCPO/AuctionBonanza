-- migrate:up

ALTER TABLE auctions RENAME COLUMN start_date TO starts_at;
ALTER TABLE auctions RENAME COLUMN end_date TO ends_at;
ALTER TABLE auctions RENAME COLUMN starting_price TO starting_price_minor;
ALTER TABLE auctions RENAME COLUMN auction_item_description TO description;

ALTER TABLE auctions
  RENAME CONSTRAINT auctions_start_date_not_null TO auctions_starts_at_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_end_date_not_null TO auctions_ends_at_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_starting_price_not_null TO auctions_starting_price_minor_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_auction_item_description_not_null TO auctions_description_not_null;

ALTER TABLE auctions
  ALTER COLUMN description TYPE text,
  ADD COLUMN title varchar(160),
  ADD COLUMN currency_code varchar(3),
  ADD COLUMN minimum_bid_increment_minor bigint,
  ADD COLUMN version bigint,
  ADD COLUMN cancelled_at timestamptz,
  ADD COLUMN closed_at timestamptz,
  ADD COLUMN updated_at timestamptz;

-- The initial schema did not record these values. Preserve the existing
-- description, derive a readable title, and use explicit migration-only
-- defaults for currency and the smallest possible increment.
UPDATE auctions
SET description = COALESCE(NULLIF(btrim(description), ''), 'Auction ' || id::text),
    title = left(COALESCE(NULLIF(btrim(description), ''), 'Auction ' || id::text), 160),
    currency_code = 'USD',
    minimum_bid_increment_minor = 1,
    version = 1,
    updated_at = created_at;

ALTER TABLE auctions
  ALTER COLUMN title SET NOT NULL,
  ALTER COLUMN currency_code SET NOT NULL,
  ALTER COLUMN minimum_bid_increment_minor SET NOT NULL,
  ALTER COLUMN version SET DEFAULT 1,
  ALTER COLUMN version SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT now(),
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE auctions DROP CONSTRAINT auctions_dates_check;

ALTER TABLE auctions
  ADD CONSTRAINT auctions_title_check
    CHECK (title = btrim(title) AND char_length(title) BETWEEN 1 AND 160),
  ADD CONSTRAINT auctions_description_check
    CHECK (
      description = btrim(description)
      AND char_length(description) BETWEEN 1 AND 10000
    ),
  ADD CONSTRAINT auctions_currency_code_check
    CHECK (currency_code ~ '^[A-Z]{3}$'),
  ADD CONSTRAINT auctions_schedule_check
    CHECK (ends_at > starts_at),
  ADD CONSTRAINT auctions_duration_check
    CHECK (
      ends_at <= starts_at
      OR ends_at - starts_at BETWEEN interval '1 minute' AND interval '30 days'
    ),
  ADD CONSTRAINT auctions_starting_price_minor_check
    CHECK (starting_price_minor >= 0),
  ADD CONSTRAINT auctions_minimum_bid_increment_minor_check
    CHECK (minimum_bid_increment_minor > 0),
  ADD CONSTRAINT auctions_version_check
    CHECK (version >= 1),
  ADD CONSTRAINT auctions_terminal_state_check
    CHECK (cancelled_at IS NULL OR closed_at IS NULL),
  ADD CONSTRAINT auctions_closed_at_check
    CHECK (closed_at IS NULL OR closed_at >= ends_at),
  ADD CONSTRAINT auctions_updated_at_check
    CHECK (updated_at >= created_at);

DROP INDEX auctions_end_date_idx;

CREATE INDEX auctions_pending_finalization_idx
  ON auctions (ends_at, id)
  WHERE cancelled_at IS NULL AND closed_at IS NULL;

-- migrate:down

DROP INDEX auctions_pending_finalization_idx;

ALTER TABLE auctions
  DROP CONSTRAINT auctions_title_check,
  DROP CONSTRAINT auctions_description_check,
  DROP CONSTRAINT auctions_currency_code_check,
  DROP CONSTRAINT auctions_schedule_check,
  DROP CONSTRAINT auctions_duration_check,
  DROP CONSTRAINT auctions_starting_price_minor_check,
  DROP CONSTRAINT auctions_minimum_bid_increment_minor_check,
  DROP CONSTRAINT auctions_version_check,
  DROP CONSTRAINT auctions_terminal_state_check,
  DROP CONSTRAINT auctions_closed_at_check,
  DROP CONSTRAINT auctions_updated_at_check;

ALTER TABLE auctions
  DROP COLUMN title,
  DROP COLUMN currency_code,
  DROP COLUMN minimum_bid_increment_minor,
  DROP COLUMN version,
  DROP COLUMN cancelled_at,
  DROP COLUMN closed_at,
  DROP COLUMN updated_at,
  ALTER COLUMN description TYPE varchar;

ALTER TABLE auctions RENAME COLUMN starts_at TO start_date;
ALTER TABLE auctions RENAME COLUMN ends_at TO end_date;
ALTER TABLE auctions RENAME COLUMN starting_price_minor TO starting_price;
ALTER TABLE auctions RENAME COLUMN description TO auction_item_description;

ALTER TABLE auctions
  RENAME CONSTRAINT auctions_starts_at_not_null TO auctions_start_date_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_ends_at_not_null TO auctions_end_date_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_starting_price_minor_not_null TO auctions_starting_price_not_null;
ALTER TABLE auctions
  RENAME CONSTRAINT auctions_description_not_null TO auctions_auction_item_description_not_null;

ALTER TABLE auctions
  ADD CONSTRAINT auctions_dates_check
    CHECK (end_date > start_date);

CREATE INDEX auctions_end_date_idx ON auctions (end_date);
