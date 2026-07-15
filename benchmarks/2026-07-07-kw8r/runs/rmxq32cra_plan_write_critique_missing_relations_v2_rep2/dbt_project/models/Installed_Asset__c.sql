{{ config(materialized='table') }}

SELECT 
    TRIM(a.id) AS "Id",
    INITCAP(TRIM(COALESCE(a.name, ''))) AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE 
        WHEN a.warranty IS NULL OR TRIM(a.warranty) = '' THEN NULL
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY')::TEXT
        WHEN a.warranty ~ '^\d{8}$' THEN TO_DATE(TRIM(a.warranty), 'YYYYMMDD')::TEXT
        WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(UPPER(acc.id)) AS "Account__c",
    TRIM(UPPER(proj.id)) AS "Project__c",
    TRIM(a.id) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(UPPER(a.client)) = TRIM(UPPER(acc.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj 
    ON TRIM(UPPER(a.project)) = TRIM(UPPER(proj.id))