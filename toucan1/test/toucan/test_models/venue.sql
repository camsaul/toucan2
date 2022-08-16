DROP TABLE IF EXISTS t1_venues;

CREATE TABLE t1_venues (
  id SERIAL PRIMARY KEY,
  name VARCHAR(256) UNIQUE NOT NULL,
  category VARCHAR(256) NOT NULL,
  "created-at" TIMESTAMP NOT NULL,
  "updated-at" TIMESTAMP NOT NULL
);

INSERT INTO t1_venues (name, category, "created-at", "updated-at")
VALUES
('Tempest', 'bar', '2017-01-01T00:00:00'::timestamp, '2017-01-01T00:00:00'::timestamp),
('Ho''s Tavern', 'bar', '2017-01-01T00:00:00'::timestamp, '2017-01-01T00:00:00'::timestamp),
('BevMo', 'store', '2017-01-01T00:00:00'::timestamp, '2017-01-01T00:00:00'::timestamp);
