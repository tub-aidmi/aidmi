{{ config(materialized='table') }}

SELECT
    CAST(a.id AS TEXT) AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    -- Warranty dates are already in YYYY-MM-DD format
    a.warranty AS "Warranty_End_Date__c",
    -- Map to Account: direct match on ACC- prefix or join by name
    CASE
        WHEN a.client LIKE 'ACC-%' THEN a.client
        ELSE acct.id
    END AS "Account__c",
    CAST(a.project AS TEXT) AS "Project__c",
    -- Legacy key is the source natural id
    a.id AS "Legacy_Asset_ID__c",
    -- No creation/modification dates in source
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct
    ON (a.client LIKE 'ACC-%' AND acct.id = a.client)
       OR (acct.name = a.client)