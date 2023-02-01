DROP TABLE IF EXISTS t1_venues;

CREATE TABLE t1_venues (
  id integer AUTO_INCREMENT PRIMARY KEY,
  name varchar(256) UNIQUE NOT NULL,
  category varchar(256) NOT NULL,
  `created-at` datetime(3) NOT NULL,
  `updated-at` datetime(3) NOT NULL
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;

INSERT INTO t1_venues (name, category, `created-at`, `updated-at`)
VALUES
('Tempest',      'bar',   timestamp '2017-01-01 00:00:00', timestamp '2017-01-01 00:00:00'),
('Ho''s Tavern', 'bar',   timestamp '2017-01-01 00:00:00', timestamp '2017-01-01 00:00:00'),
('BevMo',        'store', timestamp '2017-01-01 00:00:00', timestamp '2017-01-01 00:00:00');
