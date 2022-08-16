DROP TABLE IF EXISTS t1_categories;

CREATE TABLE t1_categories (
  id SERIAL PRIMARY KEY,
  name VARCHAR(256) UNIQUE NOT NULL,
  "parent-category-id" INTEGER
);

INSERT INTO t1_categories (name, "parent-category-id")
VALUES
('bar', NULL),            -- 1
('dive-bar', 1),          -- 2
('resturaunt', NULL),     -- 3
('mxican-resturaunt', 3); -- 4
