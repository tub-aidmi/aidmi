{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    INITCAP(a.name) AS "Name",
    a.serial AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.warranty, 'DD.MM.YYYY')::TEXT
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    acct.id AS "Account__c",
    proj.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON TRIM(LOWER(a.client)) = TRIM(LOWER(acct.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON TRIM(LOWER(a.project)) = TRIM(LOWER(proj.id))