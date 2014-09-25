-- Thanks to http://stackoverflow.com/a/17267423
BEGIN;

CREATE TEMPORARY TABLE
 newvals(mc_run integer, crop_id integer, year integer, objectid integer);

INSERT INTO newvals(mc_run, crop_id, year, objectid) VALUES (1, 99, 1999, 13);
INSERT INTO newvals(mc_run, crop_id, year, objectid) VALUES (1, 77, 1997, 13);
INSERT INTO newvals(mc_run, crop_id, year, objectid) VALUES (1, 78, 1996, 13);
INSERT INTO newvals(mc_run, crop_id, year, objectid) VALUES (1, 78, 1995, 13);

LOCK TABLE results.crop_gen_ts IN EXCLUSIVE MODE;

UPDATE results.crop_gen_ts
SET 
 mc_run = newvals.mc_run,
 crop_id = newvals.crop_id,
 year = newvals.year,
 objectid = newvals.objectid
FROM newvals
WHERE 
 newvals.mc_run = crop_gen_ts.mc_run AND
 newvals.year = crop_gen_ts.year AND
 newvals.objectid = crop_gen_ts.objectid;

INSERT INTO results.crop_gen_ts(
		mc_run, crop_id, year, objectid) 
SELECT
 newvals.mc_run, newvals.crop_id, newvals.year, newvals.objectid
FROM 
 newvals
LEFT OUTER JOIN results.crop_gen_ts ON
 (newvals.mc_run = crop_gen_ts.mc_run AND
	   	newvals.year = crop_gen_ts.year AND
	   	newvals.objectid = crop_gen_ts.objectid)
WHERE 
 crop_gen_ts.mc_run IS NULL AND
 crop_gen_ts.crop_id IS NULL AND
 crop_gen_ts.year IS NULL AND
 crop_gen_ts.objectid IS NULL;

COMMIT;
