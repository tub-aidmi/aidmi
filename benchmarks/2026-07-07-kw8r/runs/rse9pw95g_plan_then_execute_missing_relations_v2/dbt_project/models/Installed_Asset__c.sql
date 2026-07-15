{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    serial AS "Serial_Number__c",
    CASE 
        WHEN warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty, 'DD.MM.YYYY')::TEXT
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    REGEXP_REPLACE(TRIM(client), '^[^0-9]*', '') AS "Account__c",
    REGEXP_REPLACE(TRIM(project), '^[^0-9]*', '') AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}