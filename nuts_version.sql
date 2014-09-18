-- This query returns the newest avalailable nuts version given any nuts code that was valid between 2003 and 2010
-- Join the unique admin units from the time series ot our reference table
-- If we find that multiple version fit, we take the most recent (max) one
-- That will be the reference table for the time series.

SELECT nuts_code, MAX(nuts_version_year)
from data.nuts_version_years
WHERE
 nuts_code='DE30'
GROUP BY nuts_code;

