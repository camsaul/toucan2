DROP TABLE IF EXISTS people;

CREATE TABLE people (
  id serial PRIMARY KEY NOT NULL,
  name text,
  created_at timestamp with time zone
);

INSERT INTO people (name, created_at)
VALUES
('Cam', '2020-04-21T16:56:00.000-07:00'::timestamptz), -- 1
('Sam', '2019-01-11T15:56:00.000-08:00'::timestamptz), -- 2
('Pam', '2020-01-01T13:56:00.000-08:00'::timestamptz), -- 3
('Tam', '2020-05-25T12:56:00.000-07:00'::timestamptz); -- 4
