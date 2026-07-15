{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    ast.name AS "Name",
    ast.serial AS "Serial_Number__c",
    ast.warranty AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    p.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} ast
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON 
    ast.client = a.id OR ast.client = a.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p ON ast.project = p.id
