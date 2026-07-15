{{ config(materialized='table') }}

SELECT
    a."id" AS "Id",
    a."name" AS "Name",
    a."serial" AS "Serial_Number__c",
    CASE 
        WHEN a."warranty" IS NOT NULL THEN 
            TO_CHAR(TO_DATE(a."warranty", 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN a."client" ~ '^ACC-' THEN a."client"
        ELSE ac."id"
    END AS "Account__c",
    p."id" AS "Project__c",
    a."id" AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac 
    ON a."client" = ac."name"
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON a."project" = p."id"
WHERE NOT (a."client" ~ '^ACC-' AND ac."id" IS NULL) OR a."client" ~ '^ACC-'
