{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    CASE 
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON a.client = acc.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON a.project = p.id
