-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE IF NOT EXISTS job  (
  id BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL, --can't be changed, delete -> create
  description VARCHAR(100) NOT NULL, --can be changed
  type VARCHAR(100) NOT NULL, --can't be changed, delete -> create
  state VARCHAR(100) NOT NULL, -- active/stopped/deprecated/archived
  queue_config json NOT NULL,  -- update version on change, even queue type can be changed
  retry_config json,  -- retry queue config. null for producer
  max_processing_time INT NOT NULL, -- max time taken be worker to process a message
  version int not null, --starts with 1 and increases monotonically
  created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
  updated_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
  unique (name, version) DEFERRABLE
);

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE IF EXISTS job;