{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE 
        WHEN a.client ~ '^ACC-\d+$' THEN a.client
        ELSE acc.id
    END AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON (a.client ~ '^ACC-\d+$' AND a.client = acc.id)
    OR (NOT a.client ~ '^ACC-\d+$' AND UPPER(TRIM(a.client)) = UPPER(TRIM(acc.name)))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON p.id = 'PROJ-' || LPAD(SUBSTRING(a.project FROM '\d+'), 5, '0')