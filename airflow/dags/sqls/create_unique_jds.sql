create table analytics.unique_jds as (
    select distinct major_category, middle_category, platform, job_id
    from raw_data_external.daily_jd_table
);