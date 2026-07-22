{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    COALESCE(
        CASE 
            WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN a.warranty
            ELSE NULL 
        END,
        NULL
    ) AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(a.client) = TRIM(acc.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON TRIM(a.project) = TRIM(p.id)
