{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(TRIM(asset.name), 'Unknown Asset') AS "Name",
    TRIM(asset.serial) AS "Serial_Number__c",
    CASE
        WHEN asset.warranty IS NULL THEN NULL
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.warranty -- YYYY-MM-DD
        WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_source.id AS "Account__c",
    project_source.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account_source
    ON TRIM(asset.client) = TRIM(account_source.id)
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project_source
    ON TRIM(asset.project) = TRIM(project_source.id)