CREATE TABLE orders (
    id integer, region text, amount integer, owner text, card_no text
);
CREATE TABLE departments (
    owner text PRIMARY KEY, department text NOT NULL, enabled boolean NOT NULL
);
