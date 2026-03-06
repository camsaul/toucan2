-- The birds table has default values for all of its NOT NULL columns.

DROP TABLE IF EXISTS birds;

CREATE TABLE birds (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT NOT NULL DEFAULT 'birb',
  bird_type TEXT NOT NULL DEFAULT 'parrot',
  good_bird INTEGER
);

INSERT INTO birds (name, bird_type, good_bird)
VALUES
('Reggae', 'toucan', 1),
('Lucky', 'pigeon', 1),
('Parroty', 'parakeet', 1),
('Green Friend', 'parakeet', 0),
('Parrot Hilton', 'parakeet', 0),
('Egg', 'parakeet', NULL);
