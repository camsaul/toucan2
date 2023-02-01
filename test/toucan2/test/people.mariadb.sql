DROP TABLE IF EXISTS people;

CREATE TABLE people (
  id integer AUTO_INCREMENT PRIMARY KEY NOT NULL,
  name longtext,
  created_at timestamp(3) NULL
)
CHARACTER SET utf8mb4
COLLATE utf8mb4_bin;

INSERT INTO people (name, created_at)
VALUES
('Cam', timestamp '2020-04-21T23:56:00.000'), -- 1
('Sam', timestamp '2019-01-11T23:56:00.000'), -- 2
('Pam', timestamp '2020-01-01T21:56:00.000'), -- 3
('Tam', timestamp '2020-05-25T19:56:00.000'); -- 4
