{{ config(materialized='table') }}

SELECT
    -- Id: Transform asset_kennung to Salesforce-style asset ID (15-char prefix with 'a00' prefix)
    CASE 
        WHEN TRIM(asset_kennung) = '' OR asset_kennung IS NULL THEN NULL
        ELSE 'a00' || SUBSTRING(TRIM(asset_kennung) FROM 1 FOR 15)
    END AS "Id",

    -- Name: Asset name, fallback to 'Unknown Asset' if null/empty
    CASE 
        WHEN TRIM(asset_name) = '' OR asset_name IS NULL THEN 'Unknown Asset'
        ELSE INITCAP(TRIM(asset_name))
    END AS "Name",

    -- Serial Number
    TRIM(serien_nummer) AS "Serial_Number__c",

    -- Warranty End Date: Parse multiple date formats (DD.MM.YYYY, DD-MM-YYYY, MM/DD/YYYY, YYYY-MM-DD)
    CASE 
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        -- US format: MM/DD/YYYY
        WHEN TRIM(garantieende) ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        -- German/European format: DD.MM.YYYY or DD-MM-YYYY
        WHEN TRIM(garantieende) ~ '^\d{2}[./-]\d{2}[./-]\d{4}$' THEN 
            TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        -- ISO format: YYYY-MM-DD or YYYY/MM/DD
        WHEN TRIM(garantieende) ~ '^\d{4}[-/]\d{2}[-/]\d{2}$' THEN 
            TO_DATE(TRIM(garantieende), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: Transform customer key to Salesforce Account Id format ('001' prefix)
    CASE 
        WHEN kunden_kennung IS NULL OR TRIM(kunden_kennung) = '' THEN NULL
        ELSE '001' || SUBSTRING(TRIM(kunden_kennung) FROM 1 FOR 15)
    END AS "Account__c",

    -- Project__c: Transform project key to Salesforce custom object format ('a17' prefix for custom objects)
    CASE 
        WHEN projekt_kennung IS NULL OR TRIM(projekt_kennung) = '' THEN NULL
        ELSE 'a17' || SUBSTRING(TRIM(projekt_kennung) FROM 1 FOR 15)
    END AS "Project__c",

    -- Legacy_Asset_ID__c: Store the original natural key for traceability
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",

    -- Audit dates
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",

    -- IsDeleted: 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}
WHERE asset_kennung IS NOT NULL AND TRIM(asset_kennung) != ''