CREATE TABLE "analytics"."unique_company_coordinates" AS (
    SELECT DISTINCT u.major_category, u.middle_category, u.sub_category, c.company, c.lat, c.lon
    FROM "analytics".unique_jds AS u
    JOIN "raw_data_external".company_detail AS c ON u.company = c.company
    WHERE c.lat IS NOT NULL AND c.lon IS NOT NULL
);