-- migrate:up

CREATE TABLE bids (
    id bigint NOT NULL,
    auction_id bigint NOT NULL,
    bidder_id bigint NOT NULL,
    amount_minor bigint NOT NULL,
    idempotency_key character varying(100) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT bids_pkey PRIMARY KEY (id),
    CONSTRAINT bids_auction_id_fkey FOREIGN KEY (auction_id) REFERENCES auctions(id),
    CONSTRAINT bids_bidder_id_fkey FOREIGN KEY (bidder_id) REFERENCES users(id),
    CONSTRAINT bids_amount_minor_check CHECK (amount_minor > 0),
    CONSTRAINT bids_idempotency_key_check CHECK (
      idempotency_key = btrim(idempotency_key)
      AND char_length(idempotency_key) BETWEEN 1 AND 100
    )
);

ALTER TABLE bids ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME bids_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Scopes idempotency-key replay detection to one bidder's requests on one
-- auction and doubles as the lookup index for the replay check. The
-- acceptance transaction locks the parent `auctions` row before ever
-- consulting this index, so it is a defensive invariant rather than the
-- primary concurrency guard.
CREATE UNIQUE INDEX bids_auction_bidder_idempotency_key_idx
  ON bids (auction_id, bidder_id, idempotency_key);

-- Supports a future keyset-paginated "list bids for an auction" read,
-- mirroring auctions_created_at_id_idx. No such endpoint exists yet.
CREATE INDEX bids_auction_id_created_at_id_idx
  ON bids (auction_id, created_at DESC, id DESC);

-- Denormalized current leading bid: a reference, not a duplicated
-- amount/bidder pair, matching the auction lifecycle's "winning-bid
-- reference" language for finalization. Nullable: new auctions have no
-- accepted bid yet. Added after `bids` exists so the two tables' foreign
-- keys point in only one direction within this migration.
ALTER TABLE auctions ADD COLUMN leading_bid_id bigint;
ALTER TABLE auctions
  ADD CONSTRAINT auctions_leading_bid_id_fkey
    FOREIGN KEY (leading_bid_id) REFERENCES bids(id);

-- migrate:down

ALTER TABLE auctions DROP CONSTRAINT auctions_leading_bid_id_fkey;
ALTER TABLE auctions DROP COLUMN leading_bid_id;

DROP TABLE bids;
