CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_preferred_korean_nouns" AS
SELECT DISTINCT job_id, platform, unnested_preferred_korean_nouns
FROM "de1_1_database"."2nd_processed_data", UNNEST(preferred_korean_nouns) AS t(unnested_preferred_korean_nouns);