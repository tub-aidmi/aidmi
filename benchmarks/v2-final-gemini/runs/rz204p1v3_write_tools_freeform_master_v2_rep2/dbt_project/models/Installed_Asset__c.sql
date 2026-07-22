{{ config(materialized='table') }}

WITH cleaned_assets AS (
    SELECT
        TRIM(asset_kennung) AS asset_kennung,
        TRIM(asset_name) AS asset_name,
        TRIM(serien_nummer) AS serien_nummer,
        TRIM(garantieende) AS garantieende,
        TRIM(kunden_kennung) AS kunden_kennung,
        TRIM(projekt_kennung) AS projekt_kennung
    FROM
        {{ source('fixture_master_v2_src', 'master_assets') }}
)
SELECT
    MD5(asset_kennung) AS "Id",
    COALESCE(asset_name, asset_kennung) AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(kunden_kennung) AS "Account__c",
    MD5(projekt_kennung) AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_assets
