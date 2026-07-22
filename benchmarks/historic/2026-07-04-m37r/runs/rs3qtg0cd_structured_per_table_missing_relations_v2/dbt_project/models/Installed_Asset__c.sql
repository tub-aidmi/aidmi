-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    COALESCE(ast.name, 'Unknown Asset') AS "Name",
    ast.serial AS "Serial_Number__c",
    CASE WHEN ast.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN ast.warranty ELSE NULL END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    proj.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS ast
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON (ast.client = acc.id OR ast.client = acc.name)
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON ast.project = proj.id
