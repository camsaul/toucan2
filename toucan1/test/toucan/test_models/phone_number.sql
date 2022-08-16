DROP TABLE IF EXISTS t1_phone_numbers;

CREATE TABLE IF NOT EXISTS t1_phone_numbers (
  number TEXT PRIMARY KEY,
  country_code VARCHAR(3) NOT NULL
);
