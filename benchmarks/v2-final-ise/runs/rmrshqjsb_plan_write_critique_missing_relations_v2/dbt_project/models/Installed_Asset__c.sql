{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unnamed Asset') AS "Name",
    serial AS "Serial_Number__c",
    CASE 
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(warranty, 'YYYY-MM-DD')::TEXT
        WHEN warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty, 'DD.MM.YYYY')::TEXT
        WHEN warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(warranty, 'MM/DD/YYYY')::TEXT
        WHEN warranty ~ '^\d{8}$' THEN TO_DATE(warranty, 'YYYYMMDD')::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    TRIM(UPPER(client)) AS "Account__c",
    TRIM(UPPER(project)) AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}