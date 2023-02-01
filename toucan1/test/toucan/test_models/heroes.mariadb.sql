DROP TABLE IF EXISTS t1_heroes;

CREATE TABLE t1_heroes (
  `ID` integer AUTO_INCREMENT PRIMARY KEY,
  `NAME` varchar(256)
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;

INSERT INTO t1_heroes (`NAME`)
VALUES
('Batman');
