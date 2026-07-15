{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        id,
        name,
        serial,
        warranty,
        client,
        project
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}
),
account_name_lookup AS (
    SELECT id, name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)
SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    CASE 
        WHEN a.client ~ '^ACC-\d+$' THEN a.client
        ELSE (SELECT anl.id FROM account_name_lookup anl WHERE anl.name = a.client LIMIT 1)
    END AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data a
