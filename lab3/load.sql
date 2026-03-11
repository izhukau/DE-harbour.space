-- Lab03 load script: creates schema/tables and loads CSVs from /import
BEGIN;

CREATE SCHEMA IF NOT EXISTS lab03;
SET search_path = lab03, public;

-- Drop old tables if re-running
DROP TABLE IF EXISTS purchases CASCADE;
DROP TABLE IF EXISTS swipes CASCADE;
DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;

-- Table definitions
CREATE TABLE people (
  person_id   integer PRIMARY KEY,
  full_name   text    NOT NULL,
  team        text    NOT NULL,
  badge_uid   text    NOT NULL UNIQUE
);

CREATE TABLE sessions (
  session_code text PRIMARY KEY,
  session_name text NOT NULL
);

CREATE TABLE swipes (
  badge_uid    text NOT NULL,
  session_code text NOT NULL,
  ts           timestamp NOT NULL
);

CREATE TABLE purchases (
  purchase_id integer PRIMARY KEY,
  badge_uid   text NOT NULL,
  location    text NOT NULL,
  product     text NOT NULL,
  qty         integer NOT NULL,
  ts          timestamp NOT NULL
);

-- Load data from the mounted /import directory (see docker-compose.yml)
COPY people     FROM '/import/people.csv'     WITH (FORMAT csv, HEADER true);
COPY sessions   FROM '/import/sessions.csv'   WITH (FORMAT csv, HEADER true);
COPY swipes     FROM '/import/swipes.csv'     WITH (FORMAT csv, HEADER true);
COPY purchases  FROM '/import/purchases.csv'  WITH (FORMAT csv, HEADER true);

-- Helpful indexes for the investigation
CREATE INDEX ON swipes     (session_code, ts);
CREATE INDEX ON swipes     (badge_uid, ts);
CREATE INDEX ON purchases  (badge_uid, ts);
CREATE INDEX ON purchases  (product);

-- Optional: registry of detectives for the lab
CREATE TABLE IF NOT EXISTS detective_registry (
  detective_name text PRIMARY KEY,
  registered_at  timestamptz NOT NULL DEFAULT now()
);
-- TODO: поменяй на своё имя перед запуском (или выполни отдельным INSERT)
-- INSERT INTO detective_registry(detective_name) VALUES ('ИМЯ ФАМИЛИЯ');

COMMIT;
