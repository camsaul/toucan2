DROP TABLE IF EXISTS t1_phone_numbers;

CREATE TABLE IF NOT EXISTS t1_phone_numbers (
  number varchar(255) PRIMARY KEY,
  country_code varchar(3) NOT NULL
);
