-- The birds table has default values for all of its NOT NULL columns.

DROP TABLE IF EXISTS birds;

CREATE TABLE birds (
  id SERIAL PRIMARY KEY NOT NULL,
  name TEXT NOT NULL DEFAULT 'birb',
  bird_type TEXT NOT NULL DEFAULT 'parrot',
  good_bird BOOLEAN
);

INSERT INTO birds (name, bird_type, good_bird)
VALUES
('Reggae', 'toucan', true),           -- 1
('Lucky', 'pigeon', true),            -- 2
('Parroty', 'parakeet', true),        -- 3
('Green Friend', 'parakeet', false),  -- 4
('Parrot Hilton', 'parakeet', false), -- 5
('Egg', 'parakeet', NULL);            -- 6
