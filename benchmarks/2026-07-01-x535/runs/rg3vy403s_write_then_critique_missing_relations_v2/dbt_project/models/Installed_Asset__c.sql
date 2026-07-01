{{
    config(materialized='table')
}}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    asset.warranty AS "Warranty_End_Date__c",
    COALESCE(account_by_id.id, account_by_name.id) AS "Account__c",
    proj.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_by_id
    ON asset.client = account_by_id.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_by_name
    ON asset.client = account_by_name.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON asset.project = proj.id