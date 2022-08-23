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
('Reggae', 'toucan', true),
('Lucky', 'pigeon', true),
('Parroty', 'parakeet', true),
('Green Friend', 'parakeet', false),
('Parrot Hilton', 'parakeet', false),
('Egg', 'parakeet', NULL);
