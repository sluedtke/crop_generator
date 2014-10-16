-- This query returns the newest avalailable nuts version given any nuts code that was valid between 2003 and 2010
-- Join the unique admin units from the time series ot our reference table
-- If we find that multiple version fit, we take the most recent (max) one
-- That will be the reference table for the time series.
WITH 

-- get the relevant nuts that occur in the time series
unique_nuts AS (
SELECT DISTINCT nuts_id
FROM
 data.time_series)

-- and the version the correspond to
SELECT id, nuts_code, MAX(nuts_version_year)
from data.nuts_version_years
INNER JOIN unique_nuts ON
unique_nuts.nuts_id=data.nuts_version_years.nuts_code
GROUP BY nuts_code, id;

