{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_CHAR(TO_DATE(asset.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE
        WHEN account_by_id.id IS NOT NULL THEN account_by_id.id
        WHEN account_by_name.id IS NOT NULL THEN account_by_name.id
        ELSE NULL
    END AS "Account__c",
    project.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_by_id
    ON asset.client = account_by_id.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_by_name
    ON asset.client = account_by_name.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON asset.project = project.id
