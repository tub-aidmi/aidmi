{{ config(materialized='table') }}
SELECT
    ast.id AS "Id",
    ast.name AS "Name",
    ast.serial AS "Serial_Number__c",
    CASE WHEN ast.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN ast.warranty ELSE NULL END AS "Warranty_End_Date__c",
    COALESCE(
        acc.id,
        (SELECT acc2.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc2 WHERE acc2.name = ast.client ORDER BY acc2.id LIMIT 1)
    ) AS "Account__c",
    prj.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} ast
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc ON ast.client = acc.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} prj ON ast.project = prj.id