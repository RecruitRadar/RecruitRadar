CREATE TABLE "analytics"."unique_jds" AS (
    SELECT DISTINCT platform, job_id, title, company, major_category, middle_category, sub_category
    FROM "raw_data_external"."daily_jd_table"
);