{{ config(materialized='table') }}

SELECT
    '00I' || LPAD(SUBSTRING(a.asset_kennung FROM '\d+')::INTEGER::TEXT, 8, '0') AS "Id",
    COALESCE(TRIM(a.asset_name), 'Unknown Asset') AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende IS NULL OR LOWER(TRIM(a.garantieende)) IN ('n/a', '') THEN NULL
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.garantieende, 'YYYY-MM-DD')::TEXT
        WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(a.garantieende, 'DD.MM.YYYY')::TEXT
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(a.garantieende, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE
        WHEN k.kundennummer IS NOT NULL
        THEN '001' || LPAD(SUBSTRING(k.kundennummer FROM '\d+')::INTEGER::TEXT, 8, '0')
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN p.projekt_kennung IS NOT NULL
        THEN '00I' || LPAD(SUBSTRING(p.projekt_kennung FROM '\d+')::INTEGER::TEXT, 8, '0')
        ELSE NULL
    END AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON a.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
    ON a.projekt_kennung = p.projekt_kennung