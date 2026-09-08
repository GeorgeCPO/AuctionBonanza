\restrict dbmate

-- Dumped from database version 18.6 (Debian 18.6-1.pgdg12+2)
-- Dumped by pg_dump version 18.1 (Postgres.app)

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
    CONSTRAINT auctions_version_check CHECK ((version >= 1))
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
-- Name: users_email_case_insensitive_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX users_email_case_insensitive_key ON public.users USING btree (lower((email)::text));


--
-- Name: auctions auctions_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auctions
    ADD CONSTRAINT auctions_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id);


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
    ('20260907150000');
