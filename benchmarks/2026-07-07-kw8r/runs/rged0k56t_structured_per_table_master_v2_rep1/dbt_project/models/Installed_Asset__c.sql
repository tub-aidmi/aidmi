{{ config(materialized='table') }}
SELECT
    MD5(ma.asset_kennung || 'installed_asset') AS "Id",
    COALESCE(TRIM(ma.asset_name), '') AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(COALESCE(ma.kunden_kennung, '')) AS "Account__c",
    MD5(COALESCE(ma.projekt_kennung, '')) AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma