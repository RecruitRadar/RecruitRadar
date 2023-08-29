CREATE TABLE "analytics"."unique_welfare_english" AS (
    SELECT u.major_category, u.middle_category, u.sub_category, u.platform, u.job_id, wen.unnested_welfare_english_nouns
    FROM "analytics"."unique_jds" as u
    JOIN "raw_data_external"."jd_welfare_english_nouns" AS wen
    ON u.platform = wen.platform AND u.job_id = wen.job_id
);