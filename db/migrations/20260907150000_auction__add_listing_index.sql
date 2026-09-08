-- migrate:up

CREATE INDEX auctions_created_at_id_idx ON auctions (created_at DESC, id DESC);

-- migrate:down

DROP INDEX auctions_created_at_id_idx;
