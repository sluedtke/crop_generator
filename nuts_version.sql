-- This query returns the newest avalailable nuts version given any nuts code that was valid between 2003 and 2010
-- Join the unique admin units from the time series ot our reference table
-- If we find that multiple version fit, we take the most recent (max) one
-- That will be the reference table for the time series.

-- get the relevant nuts that occur in the time series
WITH  dn AS(
SELECT DISTINCT ON (nuts_code)
 nuts_code,
 oid_nuts,
 nuts_version_year AS version
FROM data.nuts_version_years
ORDER BY nuts_code, nuts_version_year DESC
)

SELECT DISTINCT ON (nuts_id)
 dn.nuts_code,
 dn.oid_nuts,
 dn.version
FROM
 data.time_series
INNER JOIN dn ON 
dn.oid_nuts=data.time_series.oid_nuts

