{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Untitled Asset') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(a.warranty) ~ '\d{4}-\d{2}-\d{2}' THEN TRIM(a.warranty)
        WHEN TRIM(a.warranty) ~ '\d{2}/\d{2}/\d{4}' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(a.warranty) ~ '\d{2}\.\d{2}\.\d{4}' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(a.warranty) ~ '\d{8}' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc.id, NULL) AS "Account__c",
    COALESCE(p.id, NULL) AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(a.client) = TRIM(acc.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON TRIM(a.project) = TRIM(p.id)
