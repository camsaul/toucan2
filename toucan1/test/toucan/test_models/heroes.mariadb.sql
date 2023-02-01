DROP TABLE IF EXISTS t1_heroes;

CREATE TABLE t1_heroes (
  `ID` integer AUTO_INCREMENT PRIMARY KEY,
  `NAME` varchar(256)
);

INSERT INTO t1_heroes (`NAME`)
VALUES
('Batman');
