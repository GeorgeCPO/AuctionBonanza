-- migrate:up

-- Finalization persists the outcome of an auction as a fact: the winning bid,
-- or no winning bid when the auction closed unsold. Like leading_bid_id this is
-- a reference, not a copy of the bid's amount or bidder.
--
-- The key is composite for the same reason as auctions_leading_bid_id_fkey:
-- the database itself rejects an auction naming another auction's bid.
ALTER TABLE auctions ADD COLUMN winning_bid_id bigint;
ALTER TABLE auctions
  ADD CONSTRAINT auctions_winning_bid_id_fkey
    FOREIGN KEY (id, winning_bid_id) REFERENCES bids (auction_id, id);

-- Only a closed auction has a winner. A closed auction without one is unsold.
ALTER TABLE auctions
  ADD CONSTRAINT auctions_winning_bid_closed_check
    CHECK (winning_bid_id IS NULL OR closed_at IS NOT NULL);

-- A CHECK cannot compare a row with its previous version, so a trigger
-- guards the transition itself:
--   1. Closing copies the leading bid: the winner is exactly the bid that led
--      when the row was locked, or none if no bid was accepted.
--   2. A closed auction's outcome never changes afterwards.
-- Finalization is the only writer allowed to set closed_at, and it does so in
-- the same transaction as the version step and the AuctionClosed.v1 event,
-- which the version triggers already require.
CREATE FUNCTION auction_require_winner_on_close()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.closed_at IS NULL AND NEW.closed_at IS NOT NULL THEN
    IF NEW.winning_bid_id IS DISTINCT FROM NEW.leading_bid_id THEN
      RAISE EXCEPTION 'auction % must close with its leading bid % as winner, not %',
        NEW.id, NEW.leading_bid_id, NEW.winning_bid_id
        USING ERRCODE = 'check_violation',
              CONSTRAINT = 'auctions_winner_is_leading_bid';
    END IF;
  ELSIF OLD.closed_at IS NOT NULL
    AND ROW(NEW.closed_at, NEW.winning_bid_id, NEW.leading_bid_id)
      IS DISTINCT FROM ROW(OLD.closed_at, OLD.winning_bid_id, OLD.leading_bid_id) THEN
    RAISE EXCEPTION 'the outcome of closed auction % is immutable', NEW.id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'auctions_closed_outcome_immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER auctions_winner_on_close
BEFORE UPDATE ON auctions
FOR EACH ROW
EXECUTE FUNCTION auction_require_winner_on_close();

-- migrate:down

DROP TRIGGER auctions_winner_on_close ON auctions;
DROP FUNCTION auction_require_winner_on_close();
ALTER TABLE auctions DROP CONSTRAINT auctions_winning_bid_closed_check;
ALTER TABLE auctions DROP CONSTRAINT auctions_winning_bid_id_fkey;
ALTER TABLE auctions DROP COLUMN winning_bid_id;
