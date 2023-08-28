CREATE TABLE IF NOT EXISTS "de1_1_database"."company_detail" AS
SELECT DISTINCT
    company,
    CASE WHEN coordinate IS NOT NULL THEN CAST(coordinate[1] AS DOUBLE) ELSE NULL END AS lat,
    CASE WHEN coordinate IS NOT NULL THEN CAST(coordinate[2] AS DOUBLE) ELSE NULL END AS lon,
    company_description
FROM
    "de1_1_database"."2nd_processed_data";