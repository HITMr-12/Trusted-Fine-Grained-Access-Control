\set ON_ERROR_STOP on
\pset pager off

SET fgac.user_token = 'alice-token';

CREATE OR REPLACE FUNCTION pg_temp.assert_json(test_name text, actual jsonb, expected jsonb)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF actual IS DISTINCT FROM expected THEN
    RAISE EXCEPTION '%: expected %, got %', test_name, expected, actual;
  END IF;
  RAISE NOTICE 'PASS %', test_name;
END $$;

-- The plan must expose both the remotely serialized filter and the mandatory
-- PostgreSQL recheck.  This is human-readable evidence in CI logs.
EXPLAIN (COSTS OFF)
SELECT id FROM orders WHERE amount >= 2000;

-- Numeric comparisons: all six comparison operators.
SELECT pg_temp.assert_json('numeric eq',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount = 1000), '[1]');
SELECT pg_temp.assert_json('numeric neq',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount <> 1000), '[3]');
SELECT pg_temp.assert_json('numeric gt',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount > 1000), '[3]');
SELECT pg_temp.assert_json('numeric gte',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount >= 2000), '[3]');
SELECT pg_temp.assert_json('numeric lt',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount < 3000), '[1]');
SELECT pg_temp.assert_json('numeric lte',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount <= 1000), '[1]');

-- String equality is pushed; locale-sensitive ordering is deliberately not.
SELECT pg_temp.assert_json('text eq',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE owner = 'alice'), '[1]');
SELECT pg_temp.assert_json('text neq',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE owner <> 'alice'), '[3]');
SELECT pg_temp.assert_json('masked-column eq after governance',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE card_no = '****0001'), '[1]');

-- Boolean structure and SQL NULL semantics.
SELECT pg_temp.assert_json('and',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE amount >= 1000 AND owner = 'alice'), '[1]');
SELECT pg_temp.assert_json('or',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE amount = 1000 OR amount = 3000), '[1,3]');
SELECT pg_temp.assert_json('not',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE NOT (amount = 1000)), '[3]');
SELECT pg_temp.assert_json('is null',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE owner IS NULL), NULL);
SELECT pg_temp.assert_json('is not null',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE owner IS NOT NULL), '[1,3]');

-- Unsupported expressions must remain local residual quals and still be exact.
SELECT pg_temp.assert_json('LIKE residual',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE owner LIKE 'a%'), '[1]');
SELECT pg_temp.assert_json('function residual',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE lower(owner) = 'alice'), '[1]');
SELECT pg_temp.assert_json('arithmetic residual',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount + 1 > 2000), '[3]');
SELECT pg_temp.assert_json('IN residual',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount IN (1000, 9999)), '[1]');
SELECT pg_temp.assert_json('mixed pushdown and residual',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE amount >= 1000 AND owner LIKE 'a%'), '[1]');
SELECT pg_temp.assert_json('unsupported child prevents unsafe OR pushdown',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE amount = 1000 OR owner LIKE 'b%'), '[1,3]');

-- Upper-plan coverage: CTE, subquery, join, aggregate, projection and self-join.
SELECT pg_temp.assert_json('CTE predicate',
  (WITH eligible AS (SELECT id,amount FROM orders WHERE amount >= 2000)
   SELECT jsonb_agg(id ORDER BY id) FROM eligible), '[3]');
SELECT pg_temp.assert_json('nested subquery predicate',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders
    WHERE owner IN (SELECT owner FROM departments WHERE enabled)
      AND amount >= 2000), '[3]');
SELECT pg_temp.assert_json('join predicate',
  (SELECT jsonb_agg(o.id ORDER BY o.id)
     FROM orders o JOIN departments d ON o.owner=d.owner
    WHERE o.amount >= 2000 AND d.enabled), '[3]');
SELECT pg_temp.assert_json('aggregate over predicate',
  (SELECT to_jsonb(SUM(amount)) FROM orders WHERE amount >= 2000), '3000');
SELECT pg_temp.assert_json('projection over predicate',
  (SELECT jsonb_agg(amount ORDER BY amount) FROM
    (SELECT amount FROM orders WHERE amount >= 2000) projected), '[3000]');
SELECT pg_temp.assert_json('two governed scans with independent quals',
  (SELECT jsonb_agg(jsonb_build_array(a.id,b.id) ORDER BY a.id,b.id)
     FROM orders a JOIN orders b ON a.owner=b.owner
    WHERE a.amount >= 2000 AND b.amount < 2000), NULL);

-- A PL/pgSQL parameter is not serialized as a literal; local recheck must own it.
DO $$
DECLARE threshold integer := 2000; actual jsonb;
BEGIN
  SELECT jsonb_agg(id ORDER BY id) INTO actual FROM orders WHERE amount >= threshold;
  PERFORM pg_temp.assert_json('parameter residual', actual, '[3]');
END $$;

-- Identity-dependent policy remains intact after query-filter changes.
SET fgac.user_token = 'bob-token';
SELECT pg_temp.assert_json('Bob policy plus query predicate',
  (SELECT jsonb_agg(id ORDER BY id) FROM orders WHERE amount >= 2000), '[2]');
SET fgac.user_token = 'alice-token';

DROP FUNCTION pg_temp.assert_json(text,jsonb,jsonb);
SELECT 'predicate pushdown and local recheck suite passed' AS result;
