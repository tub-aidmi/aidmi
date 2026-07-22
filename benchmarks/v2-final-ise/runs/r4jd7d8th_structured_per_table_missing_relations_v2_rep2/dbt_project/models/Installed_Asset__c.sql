{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty 
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    ac.id AS "Account__c",
    p.id AS "Project__c",
    a.serial AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac 
    ON a.client = ac.id OR a.client = ac.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON a.project = p.id