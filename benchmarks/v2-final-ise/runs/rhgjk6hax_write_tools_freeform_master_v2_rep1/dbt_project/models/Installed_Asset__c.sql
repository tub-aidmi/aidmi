{{ config(materialized='table') }}

SELECT
    '02i' || SUBSTRING(MD5(a.asset_kennung) FROM 1 FOR 15) AS "Id",
    COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Untitled Asset') AS "Name",
    NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",
    CASE 
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende IS NULL THEN NULL
        WHEN UPPER(TRIM(a.garantieende)) = 'N/A' THEN NULL
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || SUBSTRING(MD5(c.kundennummer) FROM 1 FOR 15) AS "Account__c",
    CASE 
        WHEN p.projekt_kennung IS NOT NULL THEN '701' || SUBSTRING(MD5(p.projekt_kennung) FROM 1 FOR 15)
        ELSE NULL
    END AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON a.kunden_kennung = c.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
    ON a.projekt_kennung = p.projekt_kennung
