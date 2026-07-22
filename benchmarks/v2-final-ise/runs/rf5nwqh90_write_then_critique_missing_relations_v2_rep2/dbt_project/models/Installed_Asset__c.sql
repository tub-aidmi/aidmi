{{ config(materialized='table') }}

SELECT
    a."id" AS "Id",
    a."name" AS "Name",
    a."serial" AS "Serial_Number__c",
    CASE
        WHEN a."warranty" IS NULL OR TRIM(a."warranty") = '' THEN NULL
        WHEN a."warranty" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a."warranty", 'DD.MM.YYYY')::TEXT
        WHEN a."warranty" ~ '^\d{4}-\d{2}-\d{2}$' THEN a."warranty"
        WHEN a."warranty" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(a."warranty", 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc."id" AS "Account__c",
    proj."id" AS "Project__c",
    a."id" AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON a."client" = acc."id"
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON a."project" = proj."id"