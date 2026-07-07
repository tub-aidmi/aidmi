-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Asset') AS "Name",
    src.serial AS "Serial_Number__c",
    CASE
        WHEN src.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN src.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    src.client AS "Account__c",
    src.project AS "Project__c",
    src.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS src