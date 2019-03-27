-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE IF NOT EXISTS dispatcher  (
  id BIGINT PRIMARY KEY,
  dispatcher_id BIGINT NOT NULL,
  job_id BIGINT REFERENCES job(id) NOT NULL,
  registered_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
  heartbeat_at timestamp without time zone DEFAULT (now() at time zone 'utc')
);

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE IF EXISTS dispatcher;