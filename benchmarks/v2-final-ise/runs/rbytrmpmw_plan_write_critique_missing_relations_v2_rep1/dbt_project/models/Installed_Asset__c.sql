{{ config(materialized='table') }}

SELECT 
    TRIM(a.id) AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Asset - ' || COALESCE(UPPER(TRIM(a.serial)), '')) AS "Name",
    UPPER(TRIM(COALESCE(a.serial, ''))) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(a.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(a.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')  
        WHEN TRIM(a.warranty) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acct.id AS "Account__c",
    proj.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON TRIM(a.client) = TRIM(acct.id)
    OR TRIM(COALESCE(NULLIF(TRIM(a.client), ''), '')) = TRIM(acct.name)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON TRIM(a.project) = TRIM(proj.id)