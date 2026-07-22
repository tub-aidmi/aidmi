{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    COALESCE(
        CASE WHEN a.client ~ '^ACC-' THEN a.client ELSE acc.id END,
        NULL
    ) AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON (a.client ~ '^ACC-' AND acc.id = a.client)
       OR (NOT a.client ~ '^ACC-' AND TRIM(acc.name) = TRIM(a.client))