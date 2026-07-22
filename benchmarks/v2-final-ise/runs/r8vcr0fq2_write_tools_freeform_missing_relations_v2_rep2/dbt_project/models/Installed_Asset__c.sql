{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    ast.name AS "Name",
    ast.serial AS "Serial_Number__c",
    ast.warranty AS "Warranty_End_Date__c",
    COALESCE(
        a.id,
        (SELECT a2.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a2 WHERE a2.name = ast.client LIMIT 1)
    ) AS "Account__c",
    prj.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} ast
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON ast.client = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} prj ON ast.project = prj.id
