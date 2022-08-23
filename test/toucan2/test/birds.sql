DROP TABLE IF EXISTS birds;

CREATE TABLE birds (
  id SERIAL PRIMARY KEY NOT NULL,
  name TEXT UNIQUE NOT NULL,
  bird_type TEXT NOT NULL,
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
