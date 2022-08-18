DROP TABLE IF EXISTS venues;

CREATE TABLE venues (
  id SERIAL PRIMARY KEY,
  name VARCHAR(256) UNIQUE NOT NULL,
  category VARCHAR(256) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz,
  updated_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz
);

INSERT INTO venues (name, category)
VALUES
('Tempest', 'bar'),
('Ho''s Tavern', 'bar'),
('BevMo', 'store');
