CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_primary_responsibility_english_nouns" AS
SELECT DISTINCT job_id, platform, unnested_primary_responsibility_english_nouns
FROM "de1_1_database"."2nd_processed_data",
UNNEST(primary_responsibility_english_nouns) AS t(unnested_primary_responsibility_english_nouns);