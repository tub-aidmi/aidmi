{{ config(materialized='table') }}

SELECT
    asset.asset_kennung AS "Id",
    COALESCE(TRIM(asset.asset_name), asset.asset_kennung) AS "Name",
    TRIM(asset.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(asset.garantieende, 'YYYY-MM-DD')::TEXT
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(asset.garantieende, 'DD.MM.YYYY')::TEXT
        WHEN asset.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(asset.garantieende, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset.kunden_kennung AS "Account__c",
    asset.projekt_kennung AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset