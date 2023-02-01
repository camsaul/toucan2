DROP TABLE IF EXISTS phone_number;

CREATE TABLE IF NOT EXISTS phone_number (
  number varchar(255) PRIMARY KEY NOT NULL,
  country_code varchar(3) NOT NULL
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;
