CREATE TABLE "analytics"."unique_preferred_english" AS (
	SELECT
	    u.major_category,
	    u.middle_category,
	    u.sub_category,
	    u.platform,
	    u.job_id,
	    pe.unnested_preferred_english_nouns AS preferred
	FROM "analytics"."unique_jds" AS u
	JOIN "raw_data_external"."jd_preferred_english_nouns" AS pe
	ON u.platform = pe.platform AND u.job_id = pe.job_id
);