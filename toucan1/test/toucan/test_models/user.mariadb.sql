DROP TABLE IF EXISTS t1_users;

CREATE TABLE t1_users (
  id integer AUTO_INCREMENT PRIMARY KEY,
  `first-name` varchar(256) NOT NULL,
  `last-name` varchar(256) NOT NULL
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;

INSERT INTO t1_users (`first-name`, `last-name`)
VALUES
('Cam', 'Saul'),
('Rasta', 'Toucan'),
('Lucky', 'Bird');
