-- This query returns the newest avalailable nuts version given any nuts code that was valid between 2003 and 2010
-- Join the unique admin units from the time series ot our reference table
-- If we find that multiple version fit, we take the most recent (max) one
-- That will be the reference table for the time series.

-- get the relevant nuts that occur in the time series
SELECT DISTINCT ON (nuts_id)
 nuts_id, 
 oid_nuts
FROM
 data.time_series

