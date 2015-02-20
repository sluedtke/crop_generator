-- Drops the index crop_gen_stat_idx if existing.
-- Author: Richard Redweik
-- 2015-01-12

--DROP INDEX IF EXISTS results.crop_gen_stat_idx;
SELECT test.drop_indeces();
