-- Thanks to http://stackoverflow.com/a/17267423

BEGIN;
CREATE TEMPORARY TABLE 
 newvals (oid_nuts INTEGER, success_time TIMESTAMP WITH TIME ZONE, tmp_table TEXT, success BOOLEAN) ON COMMIT DROP;

INSERT INTO newvals (oid_nuts, success_time, tmp_table, success) VALUES (1, '2014-09-01
		15:00:00', 'test_table',  'TRUE');

SELECT * FROM newvals;

LOCK TABLE results.nuts_completed IN EXCLUSIVE MODE;

UPDATE results.nuts_completed
SET 
 oid_nuts = newvals.oid_nuts,
 success_time = newvals.success_time,
 tmp_table = newvals.tmp_table,
 success = newvals.success
FROM newvals
WHERE 
 newvals.oid_nuts = nuts_completed.oid_nuts AND
 newvals.tmp_table = nuts_completed.tmp_table AND
 newvals.success = nuts_completed.success;

INSERT INTO results.nuts_completed(
		oid_nuts, success_time, tmp_table, success) 
SELECT
 newvals.oid_nuts, newvals.success_time, newvals.tmp_table, newvals.success
FROM 
 newvals
LEFT OUTER JOIN results.nuts_completed ON
 (newvals.oid_nuts = nuts_completed.oid_nuts AND
	   	newvals.tmp_table = nuts_completed.tmp_table AND
	   	newvals.success = nuts_completed.success)
WHERE 
 nuts_completed.oid_nuts IS NULL AND
 nuts_completed.success_time IS NULL AND
 nuts_completed.tmp_table IS NULL AND
 nuts_completed.success IS NULL;

COMMIT;
