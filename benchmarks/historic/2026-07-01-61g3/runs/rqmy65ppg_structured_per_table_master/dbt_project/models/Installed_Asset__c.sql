{{ config(materialized='table') }}

SELECT
    CAST(asset_kennung AS TEXT) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name",
    CAST(serien_nummer AS TEXT) AS "Serial_Number__c",
    CASE 
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN UPPER(TRIM(COALESCE(garantieende, ''))) = 'N/A' THEN NULL
        WHEN TRIM(garantieende) = '0000-00-00' THEN NULL
        WHEN TRIM(garantieende) ~ '^\d{8}$' THEN TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT
        WHEN TRIM(garantieende) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(garantieende)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(kunden_kennung AS TEXT) AS "Account__c",
    CAST(projekt_kennung AS TEXT) AS "Project__c",
    CAST(asset_kennung AS TEXT) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_assets') }}