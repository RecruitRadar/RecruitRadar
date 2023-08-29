CREATE TABLE "analytics"."unique_category_count_major" AS (
    SELECT
        major_category,
        COUNT(*) AS count
    FROM "analytics"."unique_jds"
    GROUP BY 1
);