CREATE TABLE "analytics"."unique_category_count_major_middle" AS (
    SELECT
        major_category,
        middle_category,
        COUNT(*) AS count
    FROM "analytics"."unique_jds"
    GROUP BY 1, 2
);