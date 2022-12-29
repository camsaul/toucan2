DROP TABLE IF EXISTS people;

CREATE TABLE people (
  id bigint auto_increment PRIMARY KEY NOT NULL,
  name text,
  created_at timestamp with time zone
);

INSERT INTO people (name, created_at)
VALUES
('Cam', timestamp with time zone '2020-04-21T23:56:00.000-00:00'), -- 1
('Sam', timestamp with time zone '2019-01-11T23:56:00.000-00:00'), -- 2
('Pam', timestamp with time zone '2020-01-01T21:56:00.000-00:00'), -- 3
('Tam', timestamp with time zone '2020-05-25T19:56:00.000-00:00'); -- 4
