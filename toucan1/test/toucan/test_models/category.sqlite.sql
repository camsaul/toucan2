DROP TABLE IF EXISTS t1_categories;

CREATE TABLE t1_categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  "parent-category-id" INTEGER
);

INSERT INTO t1_categories (name, "parent-category-id")
VALUES
('bar', NULL),
('dive-bar', 1),
('restaurant', NULL),
('mexican-restaurant', 3);
