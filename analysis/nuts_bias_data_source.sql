WITH 
-- Get per crop, year, nuts the number of cells with this crop
crops_per_year AS ( 
	SELECT crop_id, COUNT(crop_id) AS crop, year, results.crop_gen_stat.oid_nuts, data.nuts_version_years.id, data.nuts_version_years.nuts_code
	FROM results.crop_gen_stat INNER JOIN data.nuts_version_years 
	ON data.nuts_version_years.oid_nuts=results.crop_gen_stat.oid_nuts
--	WHERE nuts_version_years.nuts_code LIKE 'AT%'
	GROUP BY year, results.crop_gen_stat.oid_nuts, crop_id, data.nuts_version_years.id, nuts_version_years.nuts_code
	ORDER BY year 
),   
-- Calculate simulated share: crop/sum(crops) per year and nuts. That is the ratio of cells containing one crop and the number of all cells in this nuts per year.
sim_share AS ( 
	SELECT crop_id, year, oid_nuts, id, crop/SUM(crop) OVER (PARTITION BY year, oid_nuts) AS sim, nuts_code 
	FROM crops_per_year 
),  
-- Get 'observations' from time series for each crop, year, nuts.
join_obs_share AS ( 
	SELECT sim_share.crop_id,  sim_share.year,  sim_share.id,  sim_share.oid_nuts,  sim_share.sim,  value AS obs, nuts_code 
	FROM data.time_series INNER JOIN sim_share 
	ON data.time_series.crop_id=sim_share.crop_id AND  data.time_series.year=sim_share.year  AND  data.time_series.oid_nuts=sim_share.oid_nuts 
	WHERE data.time_series.value IS NOT NULL  AND  data.time_series.value !=0  
),
-- Caclulate percent bias.
results AS ( 
	SELECT  oid_nuts,  crop_id,  year, sim,  obs,  (100*((sim-obs)/obs)) AS pbias, data.rt_crops.crop_short, nuts_code
	FROM join_obs_share  INNER JOIN data.rt_crops  
	ON join_obs_share.crop_id=data.rt_crops.id 
),
bias AS (
	SELECT oid_nuts, crop_id, year, pbias, crop_short, nuts_code 
	FROM results  
	ORDER BY oid_nuts, year 
),
bias_source AS (
	SELECT bias.*, time_series.data_source
	FROM bias
	INNER JOIN data.time_series
	ON bias.nuts_code = time_series.nuts_id
	AND bias.oid_nuts = time_series.oid_nuts
	AND bias.year = time_series.year
	AND bias.crop_id = time_series.crop_id
	),
avg_bias AS (
	--SELECT avg(pbias) as avg_pbias, nuts_id_2010 as nuts_id, data_source
	SELECT pbias, nuts_code as nuts_id, data_source, year, crop_id
	FROM bias_source
	GROUP BY nuts_code, year, crop_id, data_source, pbias
	ORDER BY nuts_code, year, crop_id, data_source
	),
with_crops AS (
	SELECT avg_bias.*, rt_crops.crop_short
	FROM avg_bias, data.rt_crops
	WHERE avg_bias.crop_id=rt_crops.id
)
SELECT with_crops.*, countries.name
FROM with_crops INNER JOIN data.countries
ON substring(with_crops.nuts_id, 1, 2) = countries.code;
