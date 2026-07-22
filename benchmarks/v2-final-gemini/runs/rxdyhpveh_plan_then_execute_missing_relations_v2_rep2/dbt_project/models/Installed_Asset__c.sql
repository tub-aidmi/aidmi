{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(TRIM(asset.name), 'Unnamed Asset') AS "Name",
    TRIM(asset.serial) AS "Serial_Number__c",
    TO_CHAR(COALESCE(
        CASE WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(asset.warranty, 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN asset.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(asset.warranty, 'DD.MM.YYYY') ELSE NULL END,
        CASE WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(asset.warranty, 'MM/DD/YYYY') ELSE NULL END
    ), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    proj.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON asset.client = acc.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS proj
    ON asset.project = proj.id