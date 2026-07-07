-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty
        ELSE NULL
    END AS "Warranty_End_Date__c", -- Assuming YYYY-MM-DD based on inspection
    client AS "Account__c", -- Assuming client maps to Account Id
    project AS "Project__c", -- Assuming project maps to Project Id
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }}
