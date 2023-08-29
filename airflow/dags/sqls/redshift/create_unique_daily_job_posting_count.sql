CREATE TABLE "analytics"."unique_daily_job_posting_count" AS (
    SELECT
        date_str::DATE,
        COUNT(*) AS count
    FROM "raw_data_external"."daily_jd_table"
    GROUP BY date_str
);