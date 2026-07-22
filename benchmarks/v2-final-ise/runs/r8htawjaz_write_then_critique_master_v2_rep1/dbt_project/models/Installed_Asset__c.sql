{{ config(materialized='table') }}

-- Installed_Asset__c model
-- Maps source assets to Salesforce-style Asset records with proper FK references.

SELECT 
     -- Primary key: deterministic SFDC-style ID from asset_kennung
    'a1T' || LOWER(REGEXP_REPLACE(ma.asset_kennung, '[^a-z0-9]', '', 'g')) AS "Id",

     -- Asset name (NOT NULL constraint satisfied with fallback)
    COALESCE(NULLIF(TRIM(ma.asset_name), ''), 'Unknown Asset') AS "Name",

     -- Serial number (trimmed to reasonable length)
    TRIM(SUBSTR(ma.serien_nummer, 1, 50)) AS "Serial_Number__c",

     -- Warranty end date: parse multiple input formats into ISO YYYY-MM-DD; return NULL for invalid/missing
    CASE 
        WHEN TRIM(ma.garantieende) IS NULL THEN NULL
        WHEN TRIM(ma.garantieende) = 'N/A' THEN NULL
        WHEN TRIM(ma.garantieende) ~ '^0+$' THEN NULL
        WHEN TRIM(ma.garantieende) = '0000-00-00' THEN NULL
         -- Compact format YYYYMMDD (e.g. 20280601)
        WHEN TRIM(ma.garantieende) ~ '^\d{8}$' 
            THEN TO_DATE(TRIM(ma.garantieende), 'YYYYMMDD')::TEXT
         -- ISO format already (YYYY-MM-DD) — cast to date and back to text for consistency
        WHEN TRIM(ma.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN TO_DATE(TRIM(ma.garantieende), 'YYYY-MM-DD')::TEXT
         -- European dot-separated (DD.MM.YYYY)
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
            THEN TO_DATE(TRIM(ma.garantieende), 'DD.MM.YYYY')::TEXT
         -- US slash-separated (MM/DD/YYYY)  
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
            THEN TO_DATE(TRIM(ma.garantieende), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

     -- Account reference: use the same transform as Account.Id for FK resolution
    '001' || LOWER(REGEXP_REPLACE(ma.kunden_kennung, '[^a-z0-9]', '', 'g')) AS "Account__c",

     -- Project reference: use the same transform as Project__c.Id for FK resolution
    'a0Q' || REGEXP_REPLACE(ma.projekt_kennung, '[^0-9]', '') AS "Project__c",

     -- Legacy asset ID: natural key from source
    ma.asset_kennung AS "Legacy_Asset_ID__c",

     -- Audit columns (current timestamp as text)
    NOW()::TEXT AS "CreatedDate",
    NOW()::TEXT AS "LastModifiedDate",

     -- Soft-delete flag (0 = not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma