DROP TABLE IF EXISTS phone_number;

CREATE TABLE IF NOT EXISTS phone_number (
  number TEXT PRIMARY KEY NOT NULL,
  country_code TEXT NOT NULL
);
