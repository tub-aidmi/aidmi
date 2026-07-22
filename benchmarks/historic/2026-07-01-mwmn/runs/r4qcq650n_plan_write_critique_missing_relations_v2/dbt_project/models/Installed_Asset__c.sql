{{ config(materialized='table') }}
WITH resolved_account AS (
    SELECT 
        asset.id AS asset_id,
        asset.name AS asset_name,
        asset.serial AS asset_serial,
        asset.warranty AS asset_warranty,
        asset.project AS asset_project,
        COALESCE(
            (SELECT account.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} AS account WHERE account.id = asset.client LIMIT 1),
            (SELECT account.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} AS account WHERE account.name = asset.client LIMIT 1)
        ) AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
)
SELECT 
    ra.asset_id AS "Id",
    INITCAP(TRIM(ra.asset_name)) AS "Name",
    ra.asset_serial AS "Serial_Number__c",
    CASE 
        WHEN ra.asset_warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN ra.asset_warranty
        WHEN ra.asset_warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ra.asset_warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ra.asset_warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ra.asset_warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN ra.asset_warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ra.asset_warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ra.account_id AS "Account__c",
    project.id AS "Project__c",
    ra.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM resolved_account AS ra
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} AS project ON ra.asset_project = project.id