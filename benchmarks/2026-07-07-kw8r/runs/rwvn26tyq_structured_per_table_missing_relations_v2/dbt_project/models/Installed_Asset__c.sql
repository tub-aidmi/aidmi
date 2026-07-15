{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    COALESCE(acc.id, NULL) AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    NULL AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON a.client = acc.id OR a.client = acc.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON a.project = p.id