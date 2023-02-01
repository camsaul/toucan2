DROP TABLE IF EXISTS t1_categories;

CREATE TABLE t1_categories (
  id integer AUTO_INCREMENT PRIMARY KEY,
  name varchar(256) UNIQUE NOT NULL,
  `parent-category-id` integer
);

INSERT INTO t1_categories (name, `parent-category-id`)
VALUES
('bar', NULL),             -- 1
('dive-bar', 1),           -- 2
('resturaunt', NULL),      -- 3
('mexican-resturaunt', 3); -- 4
