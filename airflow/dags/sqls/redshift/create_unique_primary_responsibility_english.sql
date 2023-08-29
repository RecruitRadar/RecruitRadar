CREATE TABLE "analytics"."unique_primary_responsibility_english" AS (
    SELECT
        u.major_category,
        u.middle_category,
        u.sub_category,
        u.platform,
        u.job_id,
        pre.unnested_primary_responsibility_english_nouns AS primary_responsibility
    FROM "analytics"."unique_jds" AS u
    JOIN "raw_data_external"."jd_primary_responsibility_english_nouns" AS pre
    ON u.platform = pre.platform AND u.job_id = pre.job_id
);