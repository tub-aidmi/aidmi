{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    CASE 
        WHEN a.client LIKE 'ACC-%' THEN a.client
        ELSE acc.id
    END AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON (a.client LIKE 'ACC-%' AND acc.id = a.client)
    OR (acc.name = a.client)