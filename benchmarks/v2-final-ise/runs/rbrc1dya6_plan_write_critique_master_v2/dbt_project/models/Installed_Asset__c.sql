{{ config(materialized='table') }}

SELECT 
    CONCAT('I0XX', TRIM(a.asset_kennung)) AS "Id",
    TRIM(a.asset_name) AS "Name",
    TRIM(a.serien_nummer) AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende IS NULL THEN NULL
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantieende, 'DD.MM.YYYY')::TEXT
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_DATE(
            SUBSTRING(a.garantieende FROM 1 FOR 4) || '-' || 
            SUBSTRING(a.garantieende FROM 5 FOR 2) || '-' || 
            SUBSTRING(a.garantieende FROM 7 FOR 2), 
            'YYYY-MM-DD'
        )::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CONCAT('A0XX', TRIM(k.kundennummer)) AS "Account__c",
    CONCAT('P0XX', TRIM(p.projekt_kennung)) AS "Project__c",
    TRIM(a.asset_kennung) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON TRIM(a.kunden_kennung) = TRIM(k.kundennummer)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p 
    ON TRIM(a.projekt_kennung) = TRIM(p.projekt_kennung)