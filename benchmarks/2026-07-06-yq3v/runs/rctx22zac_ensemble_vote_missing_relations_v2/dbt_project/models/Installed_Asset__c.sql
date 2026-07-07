{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    COALESCE(ast.name, '') AS "Name",
    ast.serial AS "Serial_Number__c",
    ast.warranty::text AS "Warranty_End_Date__c",
    COALESCE(acc_id_match.id, acc_name_match.id) AS "Account__c",
    proj.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS ast
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_id_match
    ON ast.client = acc_id_match.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_name_match
    ON ast.client = acc_name_match.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON ast.project = proj.id
