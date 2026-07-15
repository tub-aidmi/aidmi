{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(NULLIF(TRIM(asset.name), ''), 'Unknown') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.warranty
        WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account.id AS "Account__c",
    project.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON asset.client = account.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON asset.project = project.name