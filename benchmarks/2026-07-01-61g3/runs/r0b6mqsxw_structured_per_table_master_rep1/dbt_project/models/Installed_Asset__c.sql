{{ config(materialized='table') }}

SELECT
    CAST(asset_kennung AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(asset_name)), 'Unnamed Asset') AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN garantieende IS NULL THEN NULL
        WHEN garantieende ~ '^N/A$' THEN NULL
        WHEN garantieende = '0000-00-00' THEN NULL
        WHEN garantieende ~ '^\d{8}$' THEN TO_DATE(garantieende, 'YYYYMMDD')::TEXT
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantieende, 'DD.MM.YYYY')::TEXT
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(garantieende AS DATE)::TEXT
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE(garantieende, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(projekt_kennung) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_assets') }}