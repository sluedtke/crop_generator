-- This query gives all the time series entries for a given nuts. 

SELECT value, year, crop_id, data_source
FROM data.time_series
WHERE
 nuts_id= ? AND
 value IS NOT NULL
ORDER BY crop_id, year;

