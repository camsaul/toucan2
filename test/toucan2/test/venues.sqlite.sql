DROP TABLE IF EXISTS venues;

CREATE TABLE venues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  category TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT '2017-01-01T00:00:00Z',
  updated_at TEXT NOT NULL DEFAULT '2017-01-01T00:00:00Z'
);

INSERT INTO venues (name, category)
VALUES
('Tempest', 'bar'),
('Ho''s Tavern', 'bar'),
('BevMo', 'store');
