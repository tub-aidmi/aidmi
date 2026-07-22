-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    COALESCE(ast.name, 'Unknown Asset') AS "Name", -- Name is NOT NULL
    ast.serial AS "Serial_Number__c",
    ast.warranty AS "Warranty_End_Date__c", -- Already in YYYY-MM-DD format
    acc.id AS "Account__c", -- Map asset.client (name) to account.id
    proj.id AS "Project__c", -- Map asset.project (id) to project.id
    ast.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS ast
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    ast.client = acc.name -- Joining on name, as discovered during introspection
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
ON
    ast.project = proj.id
