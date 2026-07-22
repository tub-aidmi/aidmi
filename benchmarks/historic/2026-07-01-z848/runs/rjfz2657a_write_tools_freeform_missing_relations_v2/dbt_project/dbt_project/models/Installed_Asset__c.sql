{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty::date::text
        ELSE NULL
    END AS "Warranty_End_Date__c",
    client AS "Account__c",
    project AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }}
