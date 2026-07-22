{{ config(materialized='table') }}

SELECT
    -- Primary key from asset_kennung (preserved as-is for legacy mapping)
    CAST(asset_kennung AS TEXT) AS "Id",
    
    -- Asset name, trimmed; default to NULL if empty
    COALESCE(NULLIF(TRIM(asset_name), ''), NULL) AS "Name",
    
    -- Serial number
    CAST(serien_nummer AS TEXT) AS "Serial_Number__c",
    
    -- Warranty end date: parse multiple formats and normalize to YYYY-MM-DD
    -- Formats observed in data:
    --   YYYYMMDD (e.g., '20250828')
    --   DD.MM.YYYY (e.g., '09.09.2023')
    --   YYYY-MM-DD (e.g., '2025-06-28')
    --   MM/DD/YYYY (e.g., '2/26/2027')
    -- Invalid values to exclude: '0000-00-00', 'N/A', ''
    CASE
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN TRIM(garantieende) = 'N/A' THEN NULL
        WHEN TRIM(garantieende) = '0000-00-00' THEN NULL
        -- YYYYMMDD format (8 digits only)
        WHEN garantieende ~ '^\d{8}$' THEN 
            TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT
        -- DD.MM.YYYY format
        WHEN garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        -- YYYY-MM-DD format (ISO)
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            CAST(TO_DATE(TRIM(garantieende), 'YYYY-MM-DD') AS TEXT)
        -- MM/DD/YYYY format (month/day can be 1 or 2 digits, year is 4 digits)
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Foreign key to Account (matches master_kunden.kundennummer)
    CAST(kunden_kennung AS TEXT) AS "Account__c",
    
    -- Foreign key to Project__c (matches master_projekte.projekt_kennung)
    CAST(projekt_kennung AS TEXT) AS "Project__c",
    
    -- Legacy asset ID is the same as the source key
    CAST(asset_kennung AS TEXT) AS "Legacy_Asset_ID__c",
    
    -- Standard Salesforce audit fields (not present in source data)
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_assets') }}
WHERE TRIM(asset_kennung) != '' -- Exclude any rows with empty keys