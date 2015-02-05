SELECT time_series.*, countries.name, rt_crops.crop_short 
FROM data.time_series, data.countries, data.rt_crops 
WHERE countries.code=substring(time_series.nuts_id from 1 for 2) 
AND rt_crops.id=time_series.crop_id;
