CREATE TABLE "analytics"."unique_required_english" AS (
    SELECT
        u.major_category,
        u.middle_category,
        u.sub_category,
        u.platform,
        u.job_id,
        re.unnested_required_english_nouns AS required
    FROM "analytics"."unique_jds" AS u
    JOIN "raw_data_external"."jd_required_english_nouns" AS re
    ON u.platform = re.platform AND u.job_id = re.job_id
);