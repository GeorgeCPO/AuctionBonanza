-- migrate:up

CREATE TABLE users (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  username varchar,
  password_hash varchar NOT NULL,
  email varchar NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX users_email_case_insensitive_key
  ON users (lower(email));

-- migrate:down

DROP TABLE users;
