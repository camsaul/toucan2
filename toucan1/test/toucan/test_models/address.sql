DROP TABLE IF EXISTS t1_address;

CREATE TABLE t1_address (
  id SERIAL PRIMARY KEY,
  street_name text NOT NULL
);

INSERT INTO t1_address (street_name)
VALUES
('1 Toucan Drive');
