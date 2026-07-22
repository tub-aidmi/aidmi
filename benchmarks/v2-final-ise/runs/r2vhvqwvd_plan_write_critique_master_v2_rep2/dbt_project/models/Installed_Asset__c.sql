{{ config(materialized='table') }}

SELECT 
    TRIM(a.asset_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Unknown Asset') AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN TRIM(a.garantieende) ~ '^\d{8}$' THEN TO_DATE(TRIM(a.garantieende), 'YYYYMMDD')::VARCHAR
        WHEN TRIM(a.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY')::VARCHAR
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    LOWER(TRIM(k.kundennummer)) AS "Account__c",
    LOWER(TRIM(p.projekt_kennung)) AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON LOWER(TRIM(a.kunden_kennung)) = LOWER(TRIM(k.kundennummer))
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p 
    ON LOWER(TRIM(a.projekt_kennung)) = LOWER(TRIM(p.projekt_kennung))