-------------------------------------------
-- Usage:  vsql -ef 2_register_and_test.sql
-------------------------------------------

-- Create or replace the library object in Vertica that points to the shared library file on disk so the UDX code can be loaded.
CREATE OR REPLACE LIBRARY exact_avg_lib AS '/tmp/exact_avg.so';

-- Create or replace the aggregate function definition in Vertica so that the exact_avg UDX can be invoked in SQL queries.
CREATE OR REPLACE AGGREGATE FUNCTION exact_avg
AS LANGUAGE 'C++'
NAME 'ExactAvgFactory'
LIBRARY exact_avg_lib;

-- Grant execute permission on the exact_avg aggregate function to all users, so everyone can call it without extra privileges.
GRANT EXECUTE ON AGGREGATE FUNCTION exact_avg(NUMERIC) TO PUBLIC;

-- Drop any existing test table to ensure a clean environment before recreating it for this test.
drop table if exists public.my_numeric_test cascade;

-- Create a new test table with a single NUMERIC(75,2) column to hold very large decimal values for average testing.
create table public.my_numeric_test (a numeric(75,2));

-- Load five test rows, one large number in each, into the table from standard input and abort if any row fails to load.
copy public.my_numeric_test from stdin delimiter ',' abort on error;
1439324057017381289491464076569211292870045918343227178012190411543327443.13,
3515992393922911434464513561313214310233044631971506505148639456185931421.49,
4914640765692112928700459183432271780121904115433274431333515992393922911.43,
7004591834322717801219041154332744313335159923939229114344645135613132143.10,
2190411543327443133351599239392291143446451356131321431023304463197150650.51,
\.

\echo '##### Compute SUM(a)/COUNT(a); for this small 5-row sample this is the true mathematical average,'
\echo '##### But in general SUM(a) can overflow when input precision p_in plus digits in the row count '
\echo '##### exceeds Vertica''s NUMERIC(1024) limit, producing a wrong result.'
select sum(a)/count(a) as correct_avg from public.my_numeric_test;
--                                           correct_avg
-- ------------------------------------------------------------------------------------------------
--  3812992118856513317445415443007946568001321189163711731972459091786692913.93200000000000000000
-- (1 row)

\echo '##### Compute built-in AVG(a)::NUMERIC(75,2); the cast hides a huge internal precision loss so it appears close to the baseline even though it is actually not.'
select avg(a)::numeric(75,2) as avg from public.my_numeric_test;
--                                      avg
-- ------------------------------------------------------------------------------
--  3812992118856513208147398257816259665489807111371965726498849801761718272.00
-- (1 row)

\echo '##### Compute (SUM(a)/COUNT(a) - AVG(a))::NUMERIC(75,2); due to the cast to NUMERIC(75,2) this prints 0.00, even though the true gap between them is enormous.'
select ((sum(a)/count(a))  - avg(a))::numeric(75,2) as Gap from public.my_numeric_test;
--  Gap
-- ------
--  0.00
-- (1 row)

\echo '##### Compute analytic AVG(a) OVER ()::NUMERIC(75,2); this uses the same internal AVG engine and therefore suffers from the same precision limits as the built-in AVG.'
select distinct avg(a) over ()::numeric(75,2) as precise_avg from public.my_numeric_test;
--                                  precise_avg
-- ------------------------------------------------------------------------------
--  3812992118856513208147398257816259665489807111371965726498849801761718272.00
-- (1 row)

\echo '##### Call exact_avg(a); it uses a much wider intermediate NUMERIC so on this 5-row test it exactly matches the true SUM(a)/COUNT(a) baseline.'
SELECT exact_avg(a) FROM public.my_numeric_test;
--                                      exact_avg
-- -----------------------------------------------------------------------------------
--  3812992118856513317445415443007946568001321189163711731972459091786692913.9320000
-- (1 row)

