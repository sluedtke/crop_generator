-- This query gives all the time series entries for a given nuts. 

SELECT *
FROM data.time_series
WHERE
 nuts_id='DE30' AND
 value IS NOT NULL AND
ORDER BY crop_id, year;

