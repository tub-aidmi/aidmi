{{ config(materialized='table') }}

SELECT
    TRIM(asset.id) AS "Id",
    COALESCE(TRIM(asset.name), 'Unknown Asset Name') AS "Name",
    TRIM(asset.serial) AS "Serial_Number__c",
    CASE
        WHEN asset.warranty IS NULL THEN NULL
        WHEN TRIM(asset.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(asset.warranty)
        WHEN TRIM(asset.warranty) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(asset.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(asset.warranty) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(asset.warranty), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(account.id) AS "Account__c",
    TRIM(project.id) AS "Project__c",
    TRIM(asset.id) AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON TRIM(asset.client) = TRIM(account.id)
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON TRIM(asset.project) = TRIM(project.id)
