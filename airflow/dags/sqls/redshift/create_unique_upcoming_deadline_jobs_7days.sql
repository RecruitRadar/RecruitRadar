CREATE TABLE "analytics"."unique_upcoming_deadline_jobs_7days" AS (
    SELECT DISTINCT 
        company,
        title,
        url,
        end_at,
        primary_responsibility,
        company_description,
        major_category,
        middle_category
    FROM "raw_data_external"."2nd_processed_data_total"
    WHERE
        end_at::DATE >= CURRENT_DATE
        AND end_at::DATE <= CURRENT_DATE + 7
);