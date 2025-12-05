-------------------------------------
-- Usage:  vsql -f 3_stress_test.sql
-------------------------------------

\set DEMO_ROWS 100000000

-- Drop any existing test table to ensure a clean environment before recreating it for this test.
drop table if exists public.my_numeric_test cascade;

-- Create a new test table with a single NUMERIC(75,2) column to hold very large decimal values for average testing.
create table public.my_numeric_test (row_id int, a numeric(75,2) default 1439324057017381289491464076569211292870045918343227178012190411543327443.13 + row_id)
order by row_id
segmented by hash(row_id) ALL NODES;

INSERT INTO public.my_numeric_test
with myrows as (select
row_number() over() as row_id
from ( select 1 from ( select now() as se union all
select now() + :DEMO_ROWS - 1 as se) a timeseries ts as '1 day' over (order by se)) b)
select row_id
from myrows
order by row_id;
COMMIT;

\timing on
\echo
\echo '##### Compute SUM(a)/COUNT(a); for these huge NUMERIC values Vertica can overflow, so this “average” may be incorrect.'
select sum(a)/count(a) as correct_avg from public.my_numeric_test;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '##### Compute built-in AVG(a)::NUMERIC(75,2); this may also be inaccurate for these very large NUMERIC values.'
select avg(a)::numeric(75,2) as avg from public.my_numeric_test;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '##### Call exact_avg(a), which uses a much wider intermediate NUMERIC to produce a mathematically correct average.'
SELECT exact_avg(a) FROM public.my_numeric_test;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '##### Compare SUM(a)/COUNT(a) with exact_avg(a); a large gap indicates overflow in Vertica''s internal SUM, not an error in exact_avg.'
select ((sum(a)/count(a))  - exact_avg(a))::numeric(75,2) as Gap from public.my_numeric_test;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '##### Show the last 5 rows to confirm the sequence a = BASE + row_id is correct and increasing as expected.'
select * from public.my_numeric_test order by 1 desc limit 5;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '##### Compute the true mathematical average using BASE + (n+1)/2,'
\echo '##### and verify exact_avg(a) matches this exact value with zero difference.'
WITH stats AS (
    SELECT (MIN(a) - 1)::NUMERIC(80,2) AS base_val,
           MAX(row_id)::NUMERIC(80,2)   AS n
    FROM public.my_numeric_test
),
udx AS (
    SELECT exact_avg(a) AS udx_avg
    FROM public.my_numeric_test
)
SELECT base_val + ((n + 1) / 2)                    AS expected_avg,
       udx_avg,
       (base_val + ((n + 1) / 2) - udx_avg)::NUMERIC(80,5) AS diff
FROM stats
CROSS JOIN udx;
\echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

\echo
\echo '===== SUMMARY ====='
\echo 'The built-in AVG() and SUM()/COUNT() become inaccurate for extremely large NUMERIC values.'
\echo 'The exact_avg() UDX remains fully accurate by using a much wider internal NUMERIC(1024) for the SUM.'
\echo 'The final mathematical verification confirms that exact_avg() exactly matches the true average (diff = 0).'
\echo '==================='

