CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_skills" AS
SELECT DISTINCT job_id, platform, unnested_skill
FROM "de1_1_database"."2nd_processed_data_total", UNNEST(skills) AS t(unnested_skill);