DROP TABLE IF EXISTS venues;

CREATE TABLE venues (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(256) UNIQUE NOT NULL,
  category VARCHAR(256) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT timestamp '2017-01-01T00:00:00Z',
  updated_at TIMESTAMP NOT NULL DEFAULT timestamp '2017-01-01T00:00:00Z'
);

INSERT INTO venues (name, category)
VALUES
('Tempest', 'bar'),      -- 1
('Ho''s Tavern', 'bar'), -- 2
('BevMo', 'store');      -- 3
