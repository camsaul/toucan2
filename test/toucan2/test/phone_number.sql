DROP TABLE IF EXISTS phone_number;

CREATE TABLE IF NOT EXISTS phone_number (
  number text PRIMARY KEY NOT NULL,
  country_code varchar(3) NOT NULL
);
