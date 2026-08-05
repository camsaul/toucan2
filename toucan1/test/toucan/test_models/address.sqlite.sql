DROP TABLE IF EXISTS t1_address;

CREATE TABLE t1_address (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  street_name TEXT NOT NULL
);

INSERT INTO t1_address (street_name)
VALUES
('1 Toucan Drive');
