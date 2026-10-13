-- migrate:up

-- An order is what a closed auction owes its winner: one row per sold auction,
-- written by the settlement worker when it consumes AuctionClosed.v1. An
-- auction that closed unsold has no order.
--
-- Like winning_bid_id, the winner is a reference to the bid, and the buyer and
-- price are copied from it. Copying is deliberate: an order is a fact that must
-- keep its terms, and the checks below stop the copy from ever disagreeing
-- with the bid it came from.
CREATE TABLE orders (
    id bigint GENERATED ALWAYS AS IDENTITY,
    auction_id bigint NOT NULL,
    winning_bid_id bigint NOT NULL,
    buyer_id bigint NOT NULL,
    amount_minor bigint NOT NULL,
    currency_code character varying(3) NOT NULL,
    source_event_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    -- The business backstop against a second order. The event table already
    -- makes a redelivered event harmless; this makes a second, different
    -- event for the same auction fail loudly instead of settling it twice.
    CONSTRAINT orders_auction_id_key UNIQUE (auction_id),
    CONSTRAINT orders_source_event_id_key UNIQUE (source_event_id),
    CONSTRAINT orders_amount_minor_check CHECK (amount_minor > 0),
    CONSTRAINT orders_currency_code_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT orders_buyer_id_fkey FOREIGN KEY (buyer_id) REFERENCES users(id),
    -- Composite for the same reason as auctions_winning_bid_id_fkey: the
    -- database itself rejects an order naming another auction's bid.
    CONSTRAINT orders_winning_bid_id_fkey
      FOREIGN KEY (auction_id, winning_bid_id) REFERENCES bids(auction_id, id),
    -- An order exists only because an event was processed, in the same
    -- transaction.
    CONSTRAINT orders_source_event_id_fkey
      FOREIGN KEY (source_event_id) REFERENCES settlement_processed_events(event_id)
);

-- "No incorrect winner" as a database rule. The auction must be closed, and the
-- order must name the auction's recorded winning bid, its bidder, its amount,
-- and the auction's currency. A bug in the consumer, or a hand-written insert,
-- cannot create an order for anyone else.
--
-- The checks read the auction's recorded outcome, which is immutable once
-- closed (auctions_closed_outcome_immutable), so no lock on the auction is
-- needed: the order is compared with a value that cannot change afterwards.
-- The trigger runs before the foreign keys are checked, so it also rejects an
-- order for an auction that does not exist.
CREATE FUNCTION order_require_winning_bid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  auction_closed_at timestamp with time zone;
  auction_winning_bid_id bigint;
  auction_currency_code character varying(3);
  bid_bidder_id bigint;
  bid_amount_minor bigint;
BEGIN
  SELECT auctions.closed_at, auctions.winning_bid_id, auctions.currency_code,
         bids.bidder_id, bids.amount_minor
  INTO auction_closed_at, auction_winning_bid_id, auction_currency_code,
       bid_bidder_id, bid_amount_minor
  FROM auctions
  LEFT JOIN bids ON bids.auction_id = auctions.id AND bids.id = auctions.winning_bid_id
  WHERE auctions.id = NEW.auction_id;

  IF NOT FOUND OR auction_closed_at IS NULL THEN
    RAISE EXCEPTION 'order for auction % requires a closed auction', NEW.auction_id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'orders_auction_closed';
  END IF;

  IF auction_winning_bid_id IS DISTINCT FROM NEW.winning_bid_id THEN
    RAISE EXCEPTION 'order for auction % names bid %, but the winning bid is %',
      NEW.auction_id, NEW.winning_bid_id, auction_winning_bid_id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'orders_winning_bid_is_auction_winner';
  END IF;

  IF ROW(NEW.buyer_id, NEW.amount_minor, NEW.currency_code)
    IS DISTINCT FROM ROW(bid_bidder_id, bid_amount_minor, auction_currency_code) THEN
    RAISE EXCEPTION 'order for auction % does not match the terms of winning bid %',
      NEW.auction_id, NEW.winning_bid_id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'orders_terms_match_winning_bid';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER orders_winning_bid
BEFORE INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION order_require_winning_bid();

-- The facts of an order never change. Written as a comparison of the fact
-- columns, not of the whole row, so a column added later (such as a payment
-- status) can change without this trigger having to be loosened.
CREATE FUNCTION order_reject_fact_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF ROW(NEW.id, NEW.auction_id, NEW.winning_bid_id, NEW.buyer_id, NEW.amount_minor,
         NEW.currency_code, NEW.source_event_id, NEW.created_at)
    IS DISTINCT FROM ROW(OLD.id, OLD.auction_id, OLD.winning_bid_id, OLD.buyer_id,
                         OLD.amount_minor, OLD.currency_code, OLD.source_event_id,
                         OLD.created_at) THEN
    RAISE EXCEPTION 'the facts of order % are immutable', OLD.id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'orders_facts_immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER orders_immutable_facts
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION order_reject_fact_change();

-- migrate:down

DROP TRIGGER orders_immutable_facts ON orders;
DROP FUNCTION order_reject_fact_change();
DROP TRIGGER orders_winning_bid ON orders;
DROP FUNCTION order_require_winning_bid();
DROP TABLE orders;
