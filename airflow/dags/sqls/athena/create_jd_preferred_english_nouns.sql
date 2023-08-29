CREATE TABLE IF NOT EXISTS "de1_1_database"."jd_preferred_english_nouns" AS
SELECT DISTINCT job_id, platform, unnested_preferred_english_nouns
FROM "de1_1_database"."2nd_processed_data_total",
UNNEST(preferred_english_nouns) AS t(unnested_preferred_english_nouns);