{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    a.warranty AS "Warranty_End_Date__c",
    COALESCE(
        CASE WHEN a.client LIKE 'ACC-%' THEN a.client ELSE NULL END,
        acc.id,
        p.client_id
    ) AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc ON a.client = acc.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p ON a.project = p.id