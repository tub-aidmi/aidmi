{{ config(materialized='table') }}

SELECT
    'a2X' || UPPER(TRIM(asset_kennung)) AS "Id",
    COALESCE(INITCAP(TRIM(asset_name)), 'Unknown Asset') AS "Name",
    NULLIF(TRIM(serien_nummer), '') AS "Serial_Number__c",
    CASE
        WHEN TRIM(garantieende) IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN UPPER(TRIM(garantieende)) IN ('N/A', '0000-00-00') THEN NULL
        WHEN TRIM(garantieende) ~ '\d{2}\.\d{2}\.\d{4}' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$' AND TRIM(garantieende) != '0000-00-00' THEN TO_DATE(TRIM(garantieende), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(garantieende) ~ '\d{1,2}/\d{1,2}/\d{4}' THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    'A00' || UPPER(TRIM(kunden_kennung)) AS "Account__c",
    'a1X' || UPPER(TRIM(projekt_kennung)) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}