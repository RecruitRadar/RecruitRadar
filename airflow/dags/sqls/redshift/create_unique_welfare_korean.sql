CREATE TABLE "analytics"."unique_welfare_korean" AS (
    SELECT u.major_category, u.middle_category, u.sub_category, u.platform, u.job_id, wkn.unnested_welfare_korean_nouns
    FROM "analytics"."unique_jds" as u
    JOIN "raw_data_external"."jd_welfare_korean_nouns" AS wkn
    ON u.platform = wkn.platform AND u.job_id = wkn.job_id
);