\restrict dbmate

-- Dumped from database version 18.6 (Debian 18.6-1.pgdg12+2)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg12+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: auction_reject_outbox_event_mutation(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.auction_reject_outbox_event_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF ROW(
    NEW.event_id,
    NEW.event_type,
    NEW.occurred_at,
    NEW.auction_id,
    NEW.auction_version,
    NEW.payload
  ) IS DISTINCT FROM ROW(
    OLD.event_id,
    OLD.event_type,
    OLD.occurred_at,
    OLD.auction_id,
    OLD.auction_version,
    OLD.payload
  ) THEN
    RAISE EXCEPTION 'outbox event data is immutable'
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: auction_require_current_outbox_version(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.auction_require_current_outbox_version() RETURNS trigger
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


--
-- Name: auction_require_single_version_step(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.auction_require_single_version_step() RETURNS trigger
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


--
-- Name: auction_require_version_outbox_event(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.auction_require_version_outbox_event() RETURNS trigger
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


--
-- Name: auction_require_winner_on_close(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.auction_require_winner_on_close() RETURNS trigger
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: auctions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.auctions (
    id bigint NOT NULL,
    owner_id bigint NOT NULL,
    starts_at timestamp with time zone NOT NULL,
    ends_at timestamp with time zone NOT NULL,
    starting_price_minor bigint NOT NULL,
    description text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    title character varying(160) NOT NULL,
    currency_code character varying(3) NOT NULL,
    minimum_bid_increment_minor bigint NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    cancelled_at timestamp with time zone,
    closed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    leading_bid_id bigint,
    winning_bid_id bigint,
    CONSTRAINT auctions_closed_at_check CHECK (((closed_at IS NULL) OR (closed_at >= ends_at))),
    CONSTRAINT auctions_currency_code_check CHECK (((currency_code)::text ~ '^[A-Z]{3}$'::text)),
    CONSTRAINT auctions_description_check CHECK (((description = btrim(description)) AND ((char_length(description) >= 1) AND (char_length(description) <= 10000)))),
    CONSTRAINT auctions_duration_check CHECK (((ends_at <= starts_at) OR (((ends_at - starts_at) >= '00:01:00'::interval) AND ((ends_at - starts_at) <= '30 days'::interval)))),
    CONSTRAINT auctions_minimum_bid_increment_minor_check CHECK ((minimum_bid_increment_minor > 0)),
    CONSTRAINT auctions_schedule_check CHECK ((ends_at > starts_at)),
    CONSTRAINT auctions_starting_price_minor_check CHECK ((starting_price_minor >= 0)),
    CONSTRAINT auctions_terminal_state_check CHECK (((cancelled_at IS NULL) OR (closed_at IS NULL))),
    CONSTRAINT auctions_title_check CHECK ((((title)::text = btrim((title)::text)) AND ((char_length((title)::text) >= 1) AND (char_length((title)::text) <= 160)))),
    CONSTRAINT auctions_updated_at_check CHECK ((updated_at >= created_at)),
    CONSTRAINT auctions_version_check CHECK ((version >= 1)),
    CONSTRAINT auctions_winning_bid_closed_check CHECK (((winning_bid_id IS NULL) OR (closed_at IS NOT NULL)))
);


--
-- Name: auctions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.auctions ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.auctions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: bids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bids (
    id bigint NOT NULL,
    auction_id bigint NOT NULL,
    bidder_id bigint NOT NULL,
    amount_minor bigint NOT NULL,
    idempotency_key character varying(100) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT bids_amount_minor_check CHECK ((amount_minor > 0)),
    CONSTRAINT bids_idempotency_key_check CHECK ((((idempotency_key)::text = btrim((idempotency_key)::text)) AND ((char_length((idempotency_key)::text) >= 1) AND (char_length((idempotency_key)::text) <= 100))))
);


--
-- Name: bids_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.bids ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.bids_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: identity_refresh_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.identity_refresh_tokens (
    id uuid NOT NULL,
    session_id uuid NOT NULL,
    parent_token_id uuid,
    generation integer NOT NULL,
    token_digest bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    consumed_at timestamp with time zone,
    CONSTRAINT identity_refresh_tokens_consumed_after_creation_check CHECK (((consumed_at IS NULL) OR (consumed_at >= created_at))),
    CONSTRAINT identity_refresh_tokens_digest_length_check CHECK ((octet_length(token_digest) = 32)),
    CONSTRAINT identity_refresh_tokens_generation_check CHECK ((generation >= 0))
);


--
-- Name: identity_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.identity_sessions (
    id uuid NOT NULL,
    user_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    last_refreshed_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    revocation_reason character varying(100),
    CONSTRAINT identity_sessions_expiry_check CHECK ((expires_at > created_at)),
    CONSTRAINT identity_sessions_last_refreshed_check CHECK (((last_refreshed_at >= created_at) AND (last_refreshed_at <= expires_at))),
    CONSTRAINT identity_sessions_revocation_check CHECK ((((revoked_at IS NULL) AND (revocation_reason IS NULL)) OR ((revoked_at IS NOT NULL) AND (revocation_reason IS NOT NULL)))),
    CONSTRAINT identity_sessions_revoked_after_creation_check CHECK (((revoked_at IS NULL) OR (revoked_at >= created_at)))
);


--
-- Name: outbox_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_events (
    event_id uuid NOT NULL,
    event_type character varying(100) NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    auction_id bigint NOT NULL,
    auction_version bigint NOT NULL,
    payload jsonb NOT NULL,
    published_at timestamp with time zone,
    publish_attempts integer DEFAULT 0 NOT NULL,
    next_publish_attempt_at timestamp with time zone,
    last_publish_error character varying(1000),
    CONSTRAINT outbox_events_auction_version_check CHECK ((auction_version >= 1)),
    CONSTRAINT outbox_events_event_type_check CHECK ((((event_type)::text = btrim((event_type)::text)) AND ((char_length((event_type)::text) >= 1) AND (char_length((event_type)::text) <= 100)))),
    CONSTRAINT outbox_events_payload_metadata_check CHECK (((payload ?& ARRAY['eventId'::text, 'eventType'::text, 'occurredAt'::text, 'auctionId'::text, 'auctionVersion'::text]) AND ((payload ->> 'eventId'::text) = (event_id)::text) AND ((payload ->> 'eventType'::text) = (event_type)::text) AND (((payload ->> 'occurredAt'::text))::timestamp with time zone = occurred_at) AND ((payload ->> 'auctionId'::text) = (auction_id)::text) AND ((payload ->> 'auctionVersion'::text) = (auction_version)::text))),
    CONSTRAINT outbox_events_payload_object_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT outbox_events_publish_attempts_check CHECK ((publish_attempts >= 0)),
    CONSTRAINT outbox_events_published_at_check CHECK (((published_at IS NULL) OR (published_at >= occurred_at))),
    CONSTRAINT outbox_events_published_retry_check CHECK (((published_at IS NULL) OR (next_publish_attempt_at IS NULL)))
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    username character varying,
    password_hash character varying NOT NULL,
    email character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: auctions auctions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auctions
    ADD CONSTRAINT auctions_pkey PRIMARY KEY (id);


--
-- Name: bids bids_auction_id_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bids
    ADD CONSTRAINT bids_auction_id_id_key UNIQUE (auction_id, id);


--
-- Name: bids bids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bids
    ADD CONSTRAINT bids_pkey PRIMARY KEY (id);


--
-- Name: identity_refresh_tokens identity_refresh_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_pkey PRIMARY KEY (id);


--
-- Name: identity_refresh_tokens identity_refresh_tokens_session_generation_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_session_generation_key UNIQUE (session_id, generation);


--
-- Name: identity_refresh_tokens identity_refresh_tokens_token_digest_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_token_digest_key UNIQUE (token_digest);


--
-- Name: identity_sessions identity_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_sessions
    ADD CONSTRAINT identity_sessions_pkey PRIMARY KEY (id);


--
-- Name: outbox_events outbox_events_auction_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_auction_version_key UNIQUE (auction_id, auction_version);


--
-- Name: outbox_events outbox_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (event_id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: auctions_created_at_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX auctions_created_at_id_idx ON public.auctions USING btree (created_at DESC, id DESC);


--
-- Name: auctions_owner_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX auctions_owner_id_idx ON public.auctions USING btree (owner_id);


--
-- Name: auctions_pending_finalization_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX auctions_pending_finalization_idx ON public.auctions USING btree (ends_at, id) WHERE ((cancelled_at IS NULL) AND (closed_at IS NULL));


--
-- Name: bids_auction_bidder_idempotency_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX bids_auction_bidder_idempotency_key_idx ON public.bids USING btree (auction_id, bidder_id, idempotency_key);


--
-- Name: bids_auction_id_created_at_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX bids_auction_id_created_at_id_idx ON public.bids USING btree (auction_id, created_at DESC, id DESC);


--
-- Name: identity_refresh_tokens_consumed_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX identity_refresh_tokens_consumed_at_idx ON public.identity_refresh_tokens USING btree (consumed_at) WHERE (consumed_at IS NOT NULL);


--
-- Name: identity_refresh_tokens_parent_token_id_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX identity_refresh_tokens_parent_token_id_key ON public.identity_refresh_tokens USING btree (parent_token_id) WHERE (parent_token_id IS NOT NULL);


--
-- Name: identity_sessions_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX identity_sessions_expiry_idx ON public.identity_sessions USING btree (expires_at);


--
-- Name: identity_sessions_revoked_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX identity_sessions_revoked_at_idx ON public.identity_sessions USING btree (revoked_at) WHERE (revoked_at IS NOT NULL);


--
-- Name: identity_sessions_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX identity_sessions_user_id_idx ON public.identity_sessions USING btree (user_id);


--
-- Name: outbox_events_unpublished_auction_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_events_unpublished_auction_idx ON public.outbox_events USING btree (auction_id, auction_version) WHERE (published_at IS NULL);


--
-- Name: outbox_events_unpublished_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_events_unpublished_idx ON public.outbox_events USING btree (occurred_at, event_id) WHERE (published_at IS NULL);


--
-- Name: users_email_case_insensitive_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX users_email_case_insensitive_key ON public.users USING btree (lower((email)::text));


--
-- Name: auctions auctions_version_outbox_event; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER auctions_version_outbox_event AFTER UPDATE OF version ON public.auctions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN ((new.version IS DISTINCT FROM old.version)) EXECUTE FUNCTION public.auction_require_version_outbox_event();


--
-- Name: auctions auctions_version_single_step; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER auctions_version_single_step BEFORE UPDATE OF version ON public.auctions FOR EACH ROW WHEN ((new.version IS DISTINCT FROM old.version)) EXECUTE FUNCTION public.auction_require_single_version_step();


--
-- Name: auctions auctions_winner_on_close; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER auctions_winner_on_close BEFORE UPDATE ON public.auctions FOR EACH ROW EXECUTE FUNCTION public.auction_require_winner_on_close();


--
-- Name: outbox_events outbox_events_current_auction_version; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER outbox_events_current_auction_version BEFORE INSERT ON public.outbox_events FOR EACH ROW EXECUTE FUNCTION public.auction_require_current_outbox_version();


--
-- Name: outbox_events outbox_events_immutable_event_data; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER outbox_events_immutable_event_data BEFORE UPDATE ON public.outbox_events FOR EACH ROW EXECUTE FUNCTION public.auction_reject_outbox_event_mutation();


--
-- Name: auctions auctions_leading_bid_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auctions
    ADD CONSTRAINT auctions_leading_bid_id_fkey FOREIGN KEY (id, leading_bid_id) REFERENCES public.bids(auction_id, id);


--
-- Name: auctions auctions_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auctions
    ADD CONSTRAINT auctions_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id);


--
-- Name: auctions auctions_winning_bid_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auctions
    ADD CONSTRAINT auctions_winning_bid_id_fkey FOREIGN KEY (id, winning_bid_id) REFERENCES public.bids(auction_id, id);


--
-- Name: bids bids_auction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bids
    ADD CONSTRAINT bids_auction_id_fkey FOREIGN KEY (auction_id) REFERENCES public.auctions(id);


--
-- Name: bids bids_bidder_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bids
    ADD CONSTRAINT bids_bidder_id_fkey FOREIGN KEY (bidder_id) REFERENCES public.users(id);


--
-- Name: identity_refresh_tokens identity_refresh_tokens_parent_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_parent_token_id_fkey FOREIGN KEY (parent_token_id) REFERENCES public.identity_refresh_tokens(id);


--
-- Name: identity_refresh_tokens identity_refresh_tokens_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.identity_sessions(id) ON DELETE CASCADE;


--
-- Name: identity_sessions identity_sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.identity_sessions
    ADD CONSTRAINT identity_sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: outbox_events outbox_events_auction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_auction_id_fkey FOREIGN KEY (auction_id) REFERENCES public.auctions(id);


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20260902193000'),
    ('20260902193100'),
    ('20260903170000'),
    ('20260907120000'),
    ('20260907150000'),
    ('20260908073852'),
    ('20260922090000'),
    ('20260923090000'),
    ('20260923100000'),
    ('20260929120000'),
    ('20261012100000');
