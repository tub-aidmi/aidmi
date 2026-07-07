{{ config(materialized='table') }}

SELECT
    MD5(asset.asset_kennung) AS "Id",
    COALESCE(TRIM(asset.asset_name), 'Untitled Asset') AS "Name",
    TRIM(asset.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(asset.kunden_kennung) AS "Account__c",
    MD5(asset.projekt_kennung) AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset