DROP TABLE IF EXISTS t1_venues;

CREATE TABLE t1_venues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  category TEXT NOT NULL,
  "created-at" TEXT NOT NULL,
  "updated-at" TEXT NOT NULL
);

INSERT INTO t1_venues (name, category, "created-at", "updated-at")
VALUES
('Tempest', 'bar', '2017-01-01T00:00:00', '2017-01-01T00:00:00'),
('Ho''s Tavern', 'bar', '2017-01-01T00:00:00', '2017-01-01T00:00:00'),
('BevMo', 'store', '2017-01-01T00:00:00', '2017-01-01T00:00:00');
