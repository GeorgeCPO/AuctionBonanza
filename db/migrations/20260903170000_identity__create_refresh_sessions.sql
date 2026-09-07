-- migrate:up

CREATE TABLE identity_sessions (
  id uuid PRIMARY KEY,
  user_id bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  last_refreshed_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  revocation_reason varchar(100),
  CONSTRAINT identity_sessions_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users (id),
  CONSTRAINT identity_sessions_expiry_check
    CHECK (expires_at > created_at),
  CONSTRAINT identity_sessions_last_refreshed_check
    CHECK (last_refreshed_at >= created_at AND last_refreshed_at <= expires_at),
  CONSTRAINT identity_sessions_revocation_check
    CHECK (
      (revoked_at IS NULL AND revocation_reason IS NULL)
      OR (revoked_at IS NOT NULL AND revocation_reason IS NOT NULL)
    ),
  CONSTRAINT identity_sessions_revoked_after_creation_check
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

CREATE INDEX identity_sessions_user_id_idx ON identity_sessions (user_id);

CREATE INDEX identity_sessions_expiry_idx ON identity_sessions (expires_at);

CREATE INDEX identity_sessions_revoked_at_idx
  ON identity_sessions (revoked_at)
  WHERE revoked_at IS NOT NULL;

CREATE TABLE identity_refresh_tokens (
  id uuid PRIMARY KEY,
  session_id uuid NOT NULL,
  parent_token_id uuid,
  generation integer NOT NULL,
  token_digest bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  consumed_at timestamptz,
  CONSTRAINT identity_refresh_tokens_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES identity_sessions (id) ON DELETE CASCADE,
  CONSTRAINT identity_refresh_tokens_parent_token_id_fkey
    FOREIGN KEY (parent_token_id) REFERENCES identity_refresh_tokens (id),
  CONSTRAINT identity_refresh_tokens_generation_check
    CHECK (generation >= 0),
  CONSTRAINT identity_refresh_tokens_digest_length_check
    CHECK (octet_length(token_digest) = 32),
  CONSTRAINT identity_refresh_tokens_consumed_after_creation_check
    CHECK (consumed_at IS NULL OR consumed_at >= created_at),
  CONSTRAINT identity_refresh_tokens_session_generation_key
    UNIQUE (session_id, generation),
  CONSTRAINT identity_refresh_tokens_token_digest_key
    UNIQUE (token_digest)
);

CREATE INDEX identity_refresh_tokens_consumed_at_idx
  ON identity_refresh_tokens (consumed_at)
  WHERE consumed_at IS NOT NULL;

CREATE UNIQUE INDEX identity_refresh_tokens_parent_token_id_key
  ON identity_refresh_tokens (parent_token_id)
  WHERE parent_token_id IS NOT NULL;

-- migrate:down

DROP TABLE identity_refresh_tokens;
DROP TABLE identity_sessions;
