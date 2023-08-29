CREATE TABLE "analytics"."unique_jd_skills" AS (
	SELECT
	    u.major_category,
	    u.middle_category,
	    u.platform,
	    u.job_id,
	    js.unnested_skill as skill
	FROM "analytics"."unique_jds" AS u
	JOIN "raw_data_external"."jd_skills" AS js
	ON u.platform = js.platform AND u.job_id = js.job_id
);