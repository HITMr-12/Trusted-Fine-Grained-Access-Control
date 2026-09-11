\set ON_ERROR_STOP on

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'fgac_pg') THEN
    RAISE EXCEPTION 'fgac_pg must not be installed as an SQL extension';
  END IF;
END $$;

SELECT count(*) AS local_governed_placeholder_rows FROM orders;

CREATE TEMP TABLE alice_orders (
  id integer, region text, amount integer, owner text
);
\copy alice_orders FROM '/tmp/alice-orders.csv' WITH (FORMAT csv, HEADER true)

CREATE TEMP TABLE bob_orders (
  id integer, region text, amount integer, owner text
);
\copy bob_orders FROM '/tmp/bob-orders.csv' WITH (FORMAT csv, HEADER true)

CREATE TEMP TABLE alice_result AS
SELECT d.department, SUM(o.amount)::bigint AS total
FROM alice_orders o
JOIN departments d ON o.owner = d.owner
WHERE o.amount >= 1000 AND d.enabled
GROUP BY d.department;

CREATE TEMP TABLE bob_result AS
SELECT d.department, SUM(o.amount)::bigint AS total
FROM bob_orders o
JOIN departments d ON o.owner = d.owner
WHERE o.amount >= 1000 AND d.enabled
GROUP BY d.department;

DO $$
BEGIN
  IF (SELECT count(*) FROM orders) <> 0 THEN
    RAISE EXCEPTION 'Pure PostgreSQL unexpectedly sees governed storage';
  END IF;
  IF (SELECT jsonb_object_agg(department, total) FROM alice_result)
       IS DISTINCT FROM '{"engineering":1000,"finance":3000}'::jsonb THEN
    RAISE EXCEPTION 'Alice non-plugin result mismatch';
  END IF;
  IF (SELECT jsonb_object_agg(department, total) FROM bob_result)
       IS DISTINCT FROM '{"finance":2000}'::jsonb THEN
    RAISE EXCEPTION 'Bob non-plugin result mismatch';
  END IF;
END $$;

EXPLAIN (COSTS OFF)
SELECT d.department, SUM(o.amount)
FROM alice_orders o
JOIN departments d ON o.owner = d.owner
WHERE o.amount >= 1000 AND d.enabled
GROUP BY d.department;

SELECT 'alice' AS principal, * FROM alice_result
UNION ALL
SELECT 'bob', * FROM bob_result
ORDER BY principal, department;
