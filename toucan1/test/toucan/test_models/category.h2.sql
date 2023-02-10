DROP TABLE IF EXISTS T1_CATEGORIES;

CREATE TABLE t1_categories (
  ID SERIAL PRIMARY KEY,
  NAME VARCHAR(256) UNIQUE NOT NULL,
  "PARENT-CATEGORY-ID" INTEGER
);

INSERT INTO T1_CATEGORIES (NAME, "PARENT-CATEGORY-ID")
VALUES
('bar', NULL),             -- 1
('dive-bar', 1),           -- 2
('restaurant', NULL),      -- 3
('mexican-restaurant', 3); -- 4
