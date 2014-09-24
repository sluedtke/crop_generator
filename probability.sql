-- This document gives a short explanation of the single temporary tables and the final
-- query. Eventually it will produce a query in pure R code that can be loaded by a
-- script. The question mark (?) in the query is a placeholder that is used by a variable.
-- All queries will be combined with a WITH statement in PostGreSQL.

WITH 

-- get crops that are relevant for the current nuts_id

listed_crops AS (
SELECT DISTINCT crop_id
FROM 
 data.time_series
WHERE data.time_series.nuts_id='DE41'
),

-- The first temporary table will hold all ids from the EU reference grid that intersect
-- with the nuts ID (spatial query).

refgrid_nuts AS (
SELECT 
 euref_grid_points.objectid, euref_grid_points.geom 
FROM 
 data.euref_grid_points, data.nuts_2006
WHERE nuts_id = 'DE41' AND 
 data.euref_grid_points.geom && data.nuts_2006.geom AND
 ST_Intersects(data.euref_grid_points.geom, data.nuts_2006.geom)
),

-- Again a spatial query. That on returns all soil mapping units (smu) that are covered by
-- the admin units. I uses the temporary table created before to sample the soil data for
-- each reference grid cell.

smu AS (
SELECT
 data.sgdbe.smu AS refgrid_smu, refgrid_nuts.objectid
FROM
 data.sgdbe, refgrid_nuts WHERE data.sgdbe.geom && refgrid_nuts.geom AND
 ST_Intersects(data.sgdbe.geom, refgrid_nuts.geom)
),

-- At very first, we need all stu's for each smu, that is the soil topographic unit.
-- We do a join given by a reference table (smu_stu). 

refgrid_stu AS (
SELECT
 smu.refgrid_smu AS smu, smu.objectid, stu, pcarea
FROM smu 
INNER JOIN data.smu_stu ON
 smu.refgrid_smu=data.smu_stu.smu 
),

-- Get only the stu's that are suitable from suitability_cgms.

refgrid_stu_suit AS (
SELECT  * 
FROM refgrid_stu
 INNER JOIN data.suitability_cgms ON
		refgrid_stu.stu=data.suitability_cgms.stu_no
),

-- Get crop_groups of stu with biggest share.
-- If a crop_group is not present at the selected stu, take the next stu with max share for this crop group.
-- The DISTINCT ON is based on [that
-- link](http://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564#7630564)
stu_crop_suit_max AS (
	SELECT DISTINCT ON (objectid, cropgroup_no)
	smu, objectid, stu, pcarea, cropgroup_no
	FROM refgrid_stu_suit
	GROUP BY objectid, cropgroup_no, smu, stu, pcarea
	ORDER BY objectid, cropgroup_no, pcarea DESC
),

-- After the crop group, we merge the crop id based on the table crop_to_crop_group.

stu_crop_id AS (
SELECT objectid, stu, crop_id
FROM stu_crop_suit_max
INNER JOIN data.crop_to_crop_group ON
 stu_crop_suit_max.cropgroup_no=data.crop_to_crop_group.crop_group_id
),

-- Now, we subset by the crops that occur in the time series only.

stu_crop_id_listed AS (
SELECT objectid, stu, stu_crop_id.crop_id
FROM stu_crop_id
INNER JOIN listed_crops ON 
 stu_crop_id.crop_id=listed_crops.crop_id
),

-- ------------------------------------------------------------------
-- get slope probabilities

soil_probs_slope AS (
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS slope_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_dom_slope, 
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_dom_slope.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.slope1 = rt_dom_slope.id
),

-- get vs (volume of stones) probabilities

soil_probs_stone AS(
SELECT 
 stu_ptrb.stu AS stu_no,
 soil_crop_probability.probability AS stone_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_stoniness,
 data.rt_limiting_soil_props, 
 data.stu_ptrb,
 data.soil_crop_probability
WHERE 
 rt_stoniness.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 stu_ptrb.vs = rt_stoniness.stoniness_code
),

-- get (roo) rooting depth probabilities

soil_probs_roo AS(
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS rooting_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_rooting_depth,
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_rooting_depth.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.calculated_rooting_depth = rt_rooting_depth.id
),

-- get (texture) texture probabilities

soil_probs_text AS (
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS texture_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_texture,
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_texture.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.text1 = rt_texture.id
),

-- get  alkalinity probabilities

soil_probs_alk AS (
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS alkalinity_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_alkalinity,
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_alkalinity.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.alkalinity = rt_alkalinity.alkalinity_code
),

-- get  salinity probabilities

soil_probs_sal AS (
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS salinity_prob, 
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_salinity,
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_salinity.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.salinity = rt_salinity.salinity_code
),

-- get  drainage class probabilities

soil_probs_drain AS (
SELECT 
 soil_typologic_unit_cgms.stu_no, 
 soil_crop_probability.probability AS drainage_prob,
 soil_crop_probability.actual_crop_id
FROM 
 data.rt_drainage,
 data.rt_limiting_soil_props, 
 data.soil_typologic_unit_cgms, 
 data.soil_crop_probability
WHERE 
 rt_drainage.rt_limiting_soil_props_id = rt_limiting_soil_props.id AND
 rt_limiting_soil_props.id = soil_crop_probability.rt_soil_props_id AND
 soil_typologic_unit_cgms.drainage = rt_drainage.drainage_code
),

-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- soil_probs_slope, soil_probs_stone, soil_probs_roo, soil_probs_alk, soil_probs_sal,
-- soil_probs_drain, soil_probs_text, stu_crop_id_listed

-- That is the first big merge. We merge the probabilities for each crop given the soil
-- conditions on the reference centroid.
stu_cropid_probs AS (
SELECT 
 soil_probs_slope.stu_no AS stu,
 soil_probs_slope.actual_crop_id AS crop_id, 
 (soil_probs_slope.slope_prob *
 soil_probs_stone.stone_prob *
 soil_probs_roo.rooting_prob * 
 soil_probs_alk.alkalinity_prob *
 soil_probs_sal.salinity_prob *
 soil_probs_text.texture_prob *
 soil_probs_drain.drainage_prob) AS current_soil_prob
 
FROM 
 soil_probs_slope

 INNER JOIN soil_probs_stone ON soil_probs_slope.stu_no=soil_probs_stone.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_stone.actual_crop_id
 INNER JOIN soil_probs_roo ON soil_probs_slope.stu_no=soil_probs_roo.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_roo.actual_crop_id
 INNER JOIN soil_probs_alk ON soil_probs_slope.stu_no=soil_probs_alk.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_alk.actual_crop_id
 INNER JOIN soil_probs_sal ON soil_probs_slope.stu_no=soil_probs_sal.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_sal.actual_crop_id
 INNER JOIN soil_probs_drain ON soil_probs_slope.stu_no=soil_probs_drain.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_drain.actual_crop_id
 INNER JOIN soil_probs_text ON soil_probs_slope.stu_no=soil_probs_text.stu_no AND 
 		soil_probs_slope.actual_crop_id=soil_probs_text.actual_crop_id

),

soil_probs AS (
SELECT 
 stu_crop_id_listed.stu,
 stu_crop_id_listed.objectid, 
 stu_crop_id_listed.crop_id, 
 stu_cropid_probs.current_soil_prob
FROM
 stu_crop_id_listed
INNER JOIN stu_cropid_probs ON 
 stu_crop_id_listed.stu = stu_cropid_probs.stu AND
 stu_crop_id_listed.crop_id = stu_cropid_probs.crop_id
),

refgrid_crops AS (
 SELECT DISTINCT crop_id, objectid
FROM soil_probs
GROUP BY objectid, crop_id
), 

-- Getting the follow up crop probabilities based on the reference table.
follow_up_probs AS (
SELECT * 
FROM
 soil_probs
INNER JOIN data.crop_sequence_probability
ON
 soil_probs.crop_id=data.crop_sequence_probability.actual_crop_id
),

-- Getting the follow up crop probabilities based on the reference table.

crop_probs AS (
SELECT 
 follow_up_probs.stu, 
 follow_up_probs.objectid, 
 follow_up_probs.crop_id AS current_crop_id,
 follow_up_probs.next_crop_id AS follow_up_crop_id,
 follow_up_probs.current_soil_prob, 
 follow_up_probs.probability AS follow_up_crop_prob
FROM follow_up_probs, refgrid_crops WHERE 
 refgrid_crops.crop_id=follow_up_probs.next_crop_id 
AND
 refgrid_crops.objectid=follow_up_probs.objectid
)

-- Now we subset the potential follow up crops for each pixel based on the table created
-- before.

SELECT 
 crop_probs.objectid, 
 crop_probs.current_crop_id,
 crop_probs.current_soil_prob, 
 crop_probs.follow_up_crop_id,
 crop_probs.follow_up_crop_prob,
 stu_cropid_probs.current_soil_prob AS follow_up_soil_prob
FROM 
 crop_probs
INNER JOIN stu_cropid_probs ON
crop_probs.stu=stu_cropid_probs.stu AND 
 		crop_probs.follow_up_crop_id=stu_cropid_probs.crop_id
ORDER BY 
 crop_probs.follow_up_crop_prob,
 stu_cropid_probs.current_soil_prob;
