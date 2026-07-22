{{ config(materialized='table') }}

SELECT
    '00Q' || LEFT(MD5(TRIM(a.asset_kennung)), 15) AS "Id",

    COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Asset_' || TRIM(a.asset_kennung)) AS "Name",

    NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",

    CASE
        WHEN a.garantieende IS NULL OR TRIM(a.garantieende) = '' THEN NULL
        -- DD.MM.YYYY (strict European, unambiguous two-digit parts with dots)
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY')::TEXT
        -- YYYYMMDD compact numeric (8 digits)
        WHEN a.garantieende ~ '^\d{8}$' THEN REGEXP_REPLACE(a.garantieende, '(\d{4})(\d{2})(\d{2})', '\1-\2-\3')
        -- YYYY-MM-DD or YYYY/MM/DD (year first — unambiguous)
        WHEN a.garantieende ~ '^\d{4}[-/]\d{1,2}[-/]\d{1,2}$' THEN TO_DATE(REGEXP_REPLACE(TRIM(a.garantieende), '[./-]', '-', 'g'), 'YYYY-MM-DD')::TEXT
        -- DD-MM-YYYY or MM-DD-YYYY with hyphen/slash separators — default to DD-MM-YYYY for German data
        WHEN a.garantieende ~ '^\d{2}[-/.]\d{1,2}[-/.]\d{4}$' THEN TO_DATE(TRIM(a.garantieende), 'DD-MM-YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: reference Salesforce-style Account Id (not raw customer number)
    CASE WHEN TRIM(k.kundennummer) IS NOT NULL THEN '001' || LEFT(MD5(TRIM(k.kundennummer)), 15) END AS "Account__c",

    -- Project__c: reference Salesforce-style Project Id
    CASE WHEN TRIM(p.projekt_kennung) IS NOT NULL THEN 'a00' || LEFT(MD5(TRIM(p.projekt_kennung)), 15) END AS "Project__c",

    TRIM(a.asset_kennung) AS "Legacy_Asset_ID__c",

    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON TRIM(a.kunden_kennung) = TRIM(k.kundennummer)
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
    ON TRIM(a.projekt_kennung) = TRIM(p.projekt_kennung)