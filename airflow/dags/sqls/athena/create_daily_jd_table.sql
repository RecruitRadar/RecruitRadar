CREATE TABLE IF NOT EXISTS "de1_1_database"."daily_jd_table" AS
SELECT DISTINCT
    year,
    month,
    day,
    CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str,
    platform,
    job_id,
    company,
    title,
    major_category,
    middle_category,
    sub_category
FROM
    "de1_1_database"."2nd_processed_data_total";