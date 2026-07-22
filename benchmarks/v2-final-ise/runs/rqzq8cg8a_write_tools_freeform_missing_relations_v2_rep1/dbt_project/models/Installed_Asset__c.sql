{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    serial AS "Serial_Number__c",
    CASE 
        WHEN warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty, 'DD.MM.YYYY')::TEXT
        WHEN warranty ~ '^\d{8}$' THEN SUBSTRING(warranty, 1, 4) || '-' || SUBSTRING(warranty, 5, 2) || '-' || SUBSTRING(warranty, 7, 2)
        WHEN warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(warranty, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    client AS "Account__c",
    project AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}
