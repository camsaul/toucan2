DROP TABLE IF EXISTS category;

CREATE TABLE category (
  name varchar(255) PRIMARY KEY NOT NULL,
  slug text,
  parent_category varchar(255) REFERENCES category (name) ON DELETE CASCADE
);

INSERT INTO category (name, slug, parent_category)
VALUES
('bar',      'bar_01',      NULL),
('store',    'store_02',    NULL),
('dive-bar', 'dive_bar_03', 'bar');
