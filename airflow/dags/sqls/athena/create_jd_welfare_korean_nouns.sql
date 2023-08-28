CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_welfare_korean_nouns" AS
SELECT DISTINCT job_id, platform, unnested_welfare_korean_nouns
FROM "de1_1_database"."2nd_processed_data", UNNEST(welfare_korean_nouns) AS t(unnested_welfare_korean_nouns);