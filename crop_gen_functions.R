################################################################################################

#       Filename: crop_gen_functions.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:10:15 CEST

#       Last modified: Friday 29 August 2014 16:24:04 CEST

################################################################################################

#	PURPOSE		########################################################################
#
#	Outsource the function for the probabilistic crop generator

################################################################################################
#setting variables to access the DB
# RODBCext must be configured properly!!!
library(RODBCext)
conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")

nuts_id = "ITI4"
nuts=data.frame('nuts_id'=nuts_id)

# select crop, cropgroup. smu, stu for each reference grid cell in nuts
nuts_query= 
'
WITH
-- This subquery outputs the grid cells of the EU reference grid intersecting with the nuts under consideration 
refgrid_nuts AS (SELECT euref_grid_points.objectid, euref_grid_points.eurefgrdid,
				 euref_grid_points.geom FROM data.euref_grid_points, data.nuts_2010_3035
				 WHERE nuts_id = ? AND data.euref_grid_points.geom &&
				 data.nuts_2010_3035.geom AND ST_Intersects(data.euref_grid_points.geom,
															data.nuts_2010_3035.geom)), 

smu AS (SELECT data.sgdbe.smu, refgrid_nuts.eurefgrdid, euref_grid_points.objectid FROM
		data.sgdbe, refgrid_nuts WHERE data.sgdbe.geom && refgrid_nuts.geom AND
		ST_Intersects(data.sgdbe.geom, refgrid_nuts.geom)), 

stu_cgms AS (SELECT smu.* FROM data.soil_typologic_unit_cgms INNER JOIN smu ON
			 soil_typologic_unit_cgms.stu_no = smu.stu_dom),

crop_group AS (SELECT data.suitability_cgms.cropgroup_no, stu_cgms.* FROM
			   data.suitability_cgms INNER JOIN stu_cgms ON suitability_cgms.stu_no =
			   stu_cgms.stu_dom),

ref_grid AS (SELECT crop_to_crop_group.crop_id, crop_group.* FROM data.crop_to_crop_group
			 INNER JOIN crop_group ON crop_to_crop_group.crop_group_id =
			 crop_group.cropgroup_no), 

ts_data AS (SELECT * FROM data.time_series WHERE nuts_id=
			(SELECT DISTINCT nuts_id from refgrid_nuts) AND value IS NOT NULL ORDER BY
			year, value ASC)

SELECT * FROM ref_grid INNER JOIN ts_data ON ts_data.crop_id=ref_grid.crop_id;
'
sqlPrepare(conn, nuts_query)
sqlExecute(conn, NULL, nuts)
crop_on_refgrid=sqlGetResults(conn)

odbcClose(conn)
merge=merge(crop_on_refgrid, crop_ts, by.x = "crop_id", by.y = "crop_id")


