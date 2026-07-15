{{ config(materialized='table') }}
SELECT
    a."id" AS "Id",
    COALESCE(NULLIF(TRIM(a."name"), ''), 'Unknown') AS "Name",
    TRIM(a."serial") AS "Serial_Number__c",
    CASE
        WHEN TRIM(a."warranty") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(a."warranty")
        WHEN TRIM(a."warranty") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a."warranty"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(a."warranty") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a."warranty"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(a."warranty") ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(a."warranty"), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(
        (SELECT acc."id" FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc WHERE acc."name" = TRIM(a."client") LIMIT 1),
        (SELECT acc."id" FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc WHERE acc."id" = TRIM(a."client") LIMIT 1),
        (SELECT acc."id" FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} op ON op."customer_number" = acc."id" WHERE op."id" = TRIM(a."client") LIMIT 1)
    ) AS "Account__c",
    COALESCE(
        (SELECT p."id" FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p WHERE p."name" = TRIM(a."project") LIMIT 1),
        (SELECT p."id" FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p WHERE p."id" = TRIM(a."project") LIMIT 1)
    ) AS "Project__c",
    a."id" AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a