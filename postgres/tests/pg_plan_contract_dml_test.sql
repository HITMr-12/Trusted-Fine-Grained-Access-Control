\set ON_ERROR_STOP on

\echo 'PLAN-CONTRACT-01: direct governed GROUP BY preserves planner references'
CREATE TEMP TABLE plan_contract_group AS
SELECT region, owner, SUM(amount)::bigint total
FROM orders
GROUP BY region, owner;
DO $$ BEGIN
 IF (SELECT jsonb_agg(to_jsonb(x) ORDER BY owner) FROM plan_contract_group x)
    IS DISTINCT FROM
    '[{"owner":"alice","region":"CN","total":1000},{"owner":"bob","region":"CN","total":3000}]'::jsonb
 THEN RAISE EXCEPTION 'direct governed GROUP BY mismatch'; END IF;
END $$;

\echo 'PLAN-CONTRACT-02: GROUP BY plus window frame'
CREATE TEMP TABLE plan_contract_window AS
SELECT region, owner, SUM(amount)::bigint total,
       DENSE_RANK() OVER (
           PARTITION BY region ORDER BY SUM(amount) DESC
       )::bigint rank_in_region
FROM orders
GROUP BY region, owner;
DO $$ BEGIN
 IF (SELECT jsonb_agg(to_jsonb(x) ORDER BY owner) FROM plan_contract_window x)
    IS DISTINCT FROM
    '[{"owner":"alice","region":"CN","total":1000,"rank_in_region":2},{"owner":"bob","region":"CN","total":3000,"rank_in_region":1}]'::jsonb
 THEN RAISE EXCEPTION 'GROUP BY plus window mismatch'; END IF;
END $$;

\echo 'PLAN-CONTRACT-03: masked output keeps grouping/window semantics'
CREATE TEMP TABLE plan_contract_mask AS
SELECT card_no, COUNT(*)::bigint n,
       ROW_NUMBER() OVER (ORDER BY card_no)::bigint rn
FROM orders
GROUP BY card_no;
DO $$ BEGIN
 IF EXISTS (
      SELECT 1 FROM plan_contract_mask
      WHERE card_no !~ '^[*]{4}[0-9]{4}$'
    ) OR (SELECT count(*) FROM plan_contract_mask) <> 2
 THEN RAISE EXCEPTION 'masked GROUP BY/window mismatch'; END IF;
END $$;

CREATE OR REPLACE FUNCTION pg_temp.assert_fgac_dml_rejected(statement text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 BEGIN
  EXECUTE statement;
  RAISE EXCEPTION 'statement unexpectedly succeeded: %', statement;
 EXCEPTION
  WHEN insufficient_privilege THEN
   IF SQLERRM NOT LIKE 'FGAC governed relations are read-only%' THEN
    RAISE;
   END IF;
 END;
END $$;

\echo 'DML-01: INSERT governed target rejected'
SELECT pg_temp.assert_fgac_dml_rejected(
  $$INSERT INTO orders(id,region,amount,owner,card_no)
    VALUES (99,'CN',9999,'alice','4111111111119999')$$);

\echo 'DML-02: UPDATE governed target rejected'
SELECT pg_temp.assert_fgac_dml_rejected(
  $$UPDATE orders SET amount=0 WHERE owner='alice'$$);

\echo 'DML-03: DELETE governed target rejected without backend crash'
SELECT pg_temp.assert_fgac_dml_rejected(
  $$DELETE FROM orders WHERE owner='alice' RETURNING id$$);

\echo 'DML-04: MERGE governed target rejected'
SELECT pg_temp.assert_fgac_dml_rejected(
  $$MERGE INTO orders o
    USING (VALUES (1,0)) AS v(id,amount) ON o.id=v.id
    WHEN MATCHED THEN UPDATE SET amount=v.amount$$);

\echo 'DML-05: governed-to-local INSERT egress rejected'
CREATE TEMP TABLE dml_sink(owner text, amount integer);
SELECT pg_temp.assert_fgac_dml_rejected(
  $$INSERT INTO dml_sink SELECT owner,amount FROM orders$$);

\echo 'DML-06: modifying CTE rejected'
SELECT pg_temp.assert_fgac_dml_rejected(
  $$WITH removed AS (DELETE FROM orders RETURNING id)
    SELECT count(*) FROM removed$$);

\echo 'DML-07: COPY and TRUNCATE utility bypasses rejected'
\set ON_ERROR_STOP off
COPY orders TO STDOUT;
\set copy_sqlstate :SQLSTATE
TRUNCATE orders;
\set truncate_sqlstate :SQLSTATE
\set ON_ERROR_STOP on
SELECT :'copy_sqlstate' = '42501' AS copy_was_rejected,
       :'truncate_sqlstate' = '42501' AS truncate_was_rejected
\gset
\if :copy_was_rejected
\else
  \echo 'COPY governed relation was not rejected with insufficient_privilege'
  \quit 1
\endif
\if :truncate_was_rejected
\else
  \echo 'TRUNCATE governed relation was not rejected with insufficient_privilege'
  \quit 1
\endif

\echo 'DML-08: ordinary local-table DML remains available'
BEGIN;
INSERT INTO departments(owner,department,enabled)
VALUES ('test-local','test-local',true);
UPDATE departments SET enabled=false WHERE owner='test-local';
DELETE FROM departments WHERE owner='test-local';
ROLLBACK;

EXPLAIN (COSTS OFF)
SELECT region, owner, SUM(amount)::bigint total,
       DENSE_RANK() OVER (
           PARTITION BY region ORDER BY SUM(amount) DESC
       )::bigint rank_in_region
FROM orders
GROUP BY region, owner;

SELECT 'PLAN_CONTRACT_AND_DML_TESTS_PASSED' AS result;
