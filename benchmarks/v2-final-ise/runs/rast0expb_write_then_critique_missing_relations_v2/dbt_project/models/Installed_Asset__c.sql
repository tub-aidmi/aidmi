{{ config(materialized='table') }}
SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    CASE WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty ELSE NULL END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON acc.id = a.client OR acc.name = a.client
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON p.id = a.project