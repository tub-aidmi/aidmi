{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    COALESCE(TRIM(a.name), 'Unknown') AS "Name",
    TRIM(UPPER(a.serial)) AS "Serial_Number__c",
    CASE 
        WHEN a.warranty IS NULL OR TRIM(a.warranty) = '' THEN NULL
        WHEN TRIM(a.warranty) = '0000-00-00' THEN NULL
        WHEN TRIM(a.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(a.warranty)
        WHEN TRIM(a.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ac.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac 
    ON UPPER(TRIM(a.client)) = UPPER(TRIM(ac.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON UPPER(TRIM(a.project)) = UPPER(TRIM(p.id))