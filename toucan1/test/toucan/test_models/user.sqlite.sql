DROP TABLE IF EXISTS t1_users;

CREATE TABLE t1_users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  "first-name" TEXT NOT NULL,
  "last-name" TEXT NOT NULL
);

INSERT INTO t1_users ("first-name", "last-name")
VALUES
('Cam', 'Saul'),
('Rasta', 'Toucan'),
('Lucky', 'Bird');
