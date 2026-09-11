\set ON_ERROR_STOP on
LOAD '/plugins/fgac_pg.so';

SET fgac.catalog_host = 'polaris-fgac';
SET fgac.catalog_port = 8181;
SET fgac.user_token = 'alice-token';
EXPLAIN (COSTS OFF)
SELECT d.department, SUM(o.amount) FROM orders o JOIN departments d ON o.owner=d.owner
WHERE o.amount >= 1000 AND d.enabled GROUP BY d.department;
CREATE TEMP TABLE alice_result AS
SELECT d.department, SUM(o.amount)::bigint total FROM orders o JOIN departments d ON o.owner=d.owner
WHERE o.amount >= 1000 AND d.enabled GROUP BY d.department;
DO $$ BEGIN
 IF (SELECT jsonb_object_agg(department,total) FROM alice_result) IS DISTINCT FROM
    '{"engineering":1000,"finance":3000}'::jsonb THEN RAISE EXCEPTION 'Alice mismatch'; END IF;
END $$;

SET fgac.user_token = 'bob-token';
CREATE TEMP TABLE bob_result AS
SELECT d.department, SUM(o.amount)::bigint total FROM orders o JOIN departments d ON o.owner=d.owner
WHERE o.amount >= 1000 AND d.enabled GROUP BY d.department;
DO $$ BEGIN
 IF (SELECT jsonb_object_agg(department,total) FROM bob_result) IS DISTINCT FROM
    '{"finance":2000}'::jsonb THEN RAISE EXCEPTION 'Bob mismatch'; END IF;
END $$;

SELECT 'alice' principal,* FROM alice_result UNION ALL SELECT 'bob',* FROM bob_result
ORDER BY principal,department;

\ir pg_predicate_pushdown_test.sql
\ir pg_plan_contract_dml_test.sql
