{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(a.id)) AS "Id",
    COALESCE(INITCAP(TRIM(a.name)), 'Unknown Asset') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    TRIM(UPPER(acc.id)) AS "Account__c",
    TRIM(UPPER(a.project)) AS "Project__c",
    TRIM(a.id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON (a.client ~ '^ACC-\d{4}$' AND TRIM(UPPER(a.client)) = TRIM(UPPER(acc.id)))
    OR (a.client !~ '^ACC-\d{4}$' AND TRIM(a.client) = TRIM(acc.name))