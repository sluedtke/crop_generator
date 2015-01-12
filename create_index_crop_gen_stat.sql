-- Creates index on results.crop_gen_stat
-- Author: Richard Redweik
-- 2015-01-12

-- Add primary key sonstraint and thus an index to table
	--ALTER TABLE results.crop_gen_stat ADD PRIMARY KEY (objectid, year, oid_nuts);
	CREATE INDEX crop_gen_stat_idx ON results.crop_gen_stat (objectid, year, oid_nuts);

