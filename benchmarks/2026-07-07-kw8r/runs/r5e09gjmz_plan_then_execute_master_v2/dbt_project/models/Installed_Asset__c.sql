{{ config(materialized='table') }}

SELECT
    -- Id: canonical Salesforce-style ID with consistent prefix
    'ASSET-' || TRIM(asset_kennung) AS "Id",
    
    -- Name: INITCAP of asset_name, default to 'Unnamed Asset' if NULL/empty
    COALESCE(
        NULLIF(TRIM(asset_name), ''),
        'Unnamed Asset'
    ) AS "Name",
    
    -- Serial_Number__c: uppercased and trimmed
    CASE 
        WHEN TRIM(serien_nummer) IS NULL OR TRIM(serien_nummer) = '' THEN NULL
        ELSE UPPER(TRIM(serien_nummer))
    END AS "Serial_Number__c",
    
    -- Warranty_End_Date__c: parse multiple date formats to ISO YYYY-MM-DD
    CASE 
        WHEN garantieende IS NULL OR TRIM(garantieende) IN ('', 'N/A') THEN NULL
        WHEN TRIM(garantieende) = '0000-00-00' THEN NULL
        -- ISO format YYYY-MM-DD (already valid)
        WHEN TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(garantieende)
        -- European DD.MM.YYYY format
        WHEN TRIM(garantieende) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
            THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        -- US MM/DD/YYYY format
        WHEN TRIM(garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        -- YYYYMMDD compact format
        WHEN TRIM(garantieende) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account__c: reference canonical Account.Id via consistent key transform
    'CUS-' || TRIM(kunden_kennung) AS "Account__c",
    
    -- Project__c: reference canonical Project__c.Id via consistent key transform  
    'PRJ-' || TRIM(projekt_kennung) AS "Project__c",
    
    -- Legacy_Asset_ID__c: direct copy of source natural key for row-level verification
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    
    -- CreatedDate: constant since source has no creation timestamp
    '2024-01-01T00:00:00Z' AS "CreatedDate",
    
    -- LastModifiedDate: same as CreatedDate (no update tracking in source)
    '2024-01-01T00:00:00Z' AS "LastModifiedDate",
    
    -- IsDeleted: integer literal 0 (no soft-delete concept in source)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}