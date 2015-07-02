-- This query returns the newest avalailable nuts version given any nuts code that was valid between 2003 and 2010
-- Join the unique admin units from the time series ot our reference table
-- If we find that multiple version fit, we take the most recent (max) one
-- That will be the reference table for the time series.

-- get the relevant nuts that occur in the time series
WITH  dn AS(
SELECT DISTINCT ON (nuts_code)
 nuts_code,
 oid_nuts,
 year AS version
FROM data.nuts 
  WHERE parent_id IS NOT NULL
ORDER BY  nuts_code, year DESC
),

-- query processed nuts 
nuts_done AS(
SELECT DISTINCT oid_nuts
FROM 
results.nuts_completed
)

--  check which nuts 
SELECT DISTINCT ON (oid_nuts)
 dn.oid_nuts,
 dn.version
FROM
 data.time_series
INNER JOIN dn ON 
dn.oid_nuts=data.time_series.oid_nuts
LEFT OUTER JOIN nuts_done ON 
data.time_series.oid_nuts=nuts_done.oid_nuts
WHERE nuts_done.oid_nuts IS NULL 
AND 
value IS NOT NULL;

