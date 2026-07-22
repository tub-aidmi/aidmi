{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    COALESCE(a_id.id, a_name.id) AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a_id ON a.client = a_id.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a_name ON a.client = a_name.name
