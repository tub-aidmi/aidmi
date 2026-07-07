{{ config(materialized='table') }}

SELECT
    MD5(TRIM(asset_kennung)) AS "Id",
    COALESCE(TRIM(asset_name), TRIM(asset_kennung)) AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(garantieende, 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantieende, 'DD.MM.YYYY')
        WHEN garantieende ~ '^\d{8}$' THEN TO_DATE(garantieende, 'YYYYMMDD')
        WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(garantieende, 'MM/DD/YYYY')
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c",
    MD5(TRIM(kunden_kennung)) AS "Account__c",
    MD5(TRIM(projekt_kennung)) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }}
