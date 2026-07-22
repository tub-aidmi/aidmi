{{ config(materialized='table') }}

SELECT
    CAST(UPPER(TRIM(a.asset_kennung)) AS TEXT) AS "Id",
    INITCAP(TRIM(a.asset_name)) AS "Name",
    TRIM(a.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN a.garantieende IS NULL OR TRIM(a.garantieende) = '' THEN NULL
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_DATE(a.garantieende, 'YYYYMMDD')::TEXT
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantieende, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(UPPER(TRIM(k.kundennummer)) AS TEXT) AS "Account__c",
    CAST(UPPER(TRIM(p.projekt_kennung)) AS TEXT) AS "Project__c",
    TRIM(a.asset_kennung) AS "Legacy_Asset_ID__c",
    '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
    ON TRIM(a.projekt_kennung) = TRIM(p.projekt_kennung)
JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(p.kunden_kennung) = TRIM(k.kundennummer)
