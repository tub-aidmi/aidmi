-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(TRIM(asset.name), 'Unknown Asset') AS "Name",
    TRIM(asset.serial) AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account.id AS "Account__c",
    project.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON asset.client = account.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON asset.project = project.id