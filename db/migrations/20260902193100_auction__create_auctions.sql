-- migrate:up

CREATE TABLE auctions (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id bigint NOT NULL,
  start_date timestamptz NOT NULL,
  end_date timestamptz NOT NULL,
  starting_price bigint NOT NULL,
  auction_item_description varchar NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT auctions_owner_id_fkey
    FOREIGN KEY (owner_id) REFERENCES users (id),
  CONSTRAINT auctions_dates_check
    CHECK (end_date > start_date)
);

CREATE INDEX auctions_owner_id_idx ON auctions (owner_id);

CREATE INDEX auctions_end_date_idx ON auctions (end_date);

-- migrate:down

DROP TABLE auctions;
