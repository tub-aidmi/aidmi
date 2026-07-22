{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Untitled Asset') AS "Name", -- Name is NOT NULL
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(warranty AS DATE)::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    client AS "Account__c",
    project AS "Project__c",
    NULL AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Asset') }}
