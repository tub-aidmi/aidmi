{{ config(materialized='table') }}

SELECT
    -- Id: Transform AST-00001 → AS00001 (Salesforce-style asset ID)
    'AS' || RIGHT('00000' || REGEXP_REPLACE(a.asset_kennung, '^AST-(\d+)$', '\1'), 5) AS "Id",

    -- Name: Init-capped and trimmed
    INITCAP(TRIM(a.asset_name)) AS "Name",

    -- Serial_Number__c: Trimmed serial number from source
    TRIM(a.serien_nummer) AS "Serial_Number__c",

    -- Warranty_End_Date__c: Parse multiple date formats to ISO YYYY-MM-DD
    CASE
        WHEN a.garantieende IS NULL OR LOWER(TRIM(COALESCE(a.garantieende, ''))) IN ('n/a', '', 'null') THEN NULL
        WHEN TRIM(a.garantieende) = '0000-00-00' THEN NULL
        -- ISO format: YYYY-MM-DD
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN TO_CHAR(TO_DATE(TRIM(a.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        -- European format: DD.MM.YYYY
        WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
            THEN TO_CHAR(TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- US format: MM/DD/YYYY (handles single-digit months/days)
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
            THEN TO_CHAR(TO_DATE(TRIM(a.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- Compact format: YYYYMMDD
        WHEN a.garantieende ~ '^\d{8}$' 
            THEN TO_CHAR(TO_DATE(TRIM(a.garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: Salesforce Account.Id via LEFT JOIN to master_kunden
    CASE WHEN k.kundennummer IS NOT NULL 
         THEN 'CU' || RIGHT('0000' || REGEXP_REPLACE(k.kundennummer, '^CUST-M(\d+)$', '\1'), 4)
         ELSE NULL
    END AS "Account__c",

    -- Project__c: Salesforce Project ID — only where projekt_kennung matches master_projekte format (no M-prefix)
    CASE 
        WHEN a.projekt_kennung ~ '^PROJ-\d{5}$' 
        THEN 'PR' || RIGHT('00000' || REGEXP_REPLACE(a.projekt_kennung, '^PROJ-(\d+)$', '\1'), 5)
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: Original source key for traceability
    a.asset_kennung AS "Legacy_Asset_ID__c",

    -- CreatedDate / LastModifiedDate: Placeholder (not in source)
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",

    -- IsDeleted: Default to 0
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON a.kunden_kennung = k.kundennummer