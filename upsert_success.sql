-- Thanks to http://stackoverflow.com/a/17267423

BEGIN;
CREATE TEMPORARY TABLE 
 newvals (oid_nuts INTEGER, success_time TIMESTAMP WITH TIME ZONE, tmp_table TEXT, success BOOLEAN);

INSERT INTO newvals (oid_nuts, success_time, tmp_table, success) VALUES 
				(?, ?, ?, ?);

UPDATE test.nuts_completed
SET 
 oid_nuts = newvals.oid_nuts,
 success_time = newvals.success_time,
 tmp_table = newvals.tmp_table,
 success = newvals.success
FROM newvals
WHERE 
 newvals.oid_nuts = nuts_completed.oid_nuts;


INSERT INTO test.nuts_completed(
		oid_nuts, success_time, tmp_table, success) 
SELECT
 newvals.oid_nuts, newvals.success_time, newvals.tmp_table, newvals.success
FROM 
 newvals
LEFT OUTER JOIN test.nuts_completed ON
 newvals.oid_nuts = nuts_completed.oid_nuts
WHERE 
 nuts_completed.oid_nuts IS NULL;

COMMIT;
