DROP TABLE IF EXISTS venues;

CREATE TABLE venues (
  id bigint AUTO_INCREMENT PRIMARY KEY,
  name varchar(256) UNIQUE NOT NULL,
  category varchar(256) NOT NULL,
  created_at datetime(3) NOT NULL DEFAULT timestamp '2017-01-01 00:00:00',
  updated_at datetime(3) NOT NULL DEFAULT timestamp '2017-01-01 00:00:00'
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;

INSERT INTO venues (name, category)
VALUES
('Tempest', 'bar'),      -- 1
('Ho''s Tavern', 'bar'), -- 2
('BevMo', 'store');      -- 3
