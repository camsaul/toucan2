DROP TABLE IF EXISTS t1_users;

CREATE TABLE t1_users (
  id SERIAL PRIMARY KEY,
  "first-name" VARCHAR(256) NOT NULL,
  "last-name" VARCHAR(256) NOT NULL
);

INSERT INTO t1_users ("first-name", "last-name")
VALUES
('Cam', 'Saul'),
('Rasta', 'Toucan'),
('Lucky', 'Bird');
