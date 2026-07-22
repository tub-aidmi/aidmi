{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    CASE
        WHEN a.project ~ '^PROJ-' THEN a.project
        ELSE NULL
    END AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON (
        a.client = acc.id
        OR (
            NOT a.client ~ '^ACC-' 
            AND UPPER(TRIM(a.client)) = UPPER(TRIM(acc.name))
        )
    )
