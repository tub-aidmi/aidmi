{{ config(materialized='table') }}

WITH assets AS (
    SELECT
        -- Asset Id: Transform to Salesforce-style asset ID (18-char format starting with 'a00')
        CASE 
            WHEN asset_kennung IS NULL THEN NULL
            ELSE 'a00' || SUBSTRING(asset_kennung FROM 1 FOR 15)
        END AS "Id",

        -- Name: Asset name, fallback to 'Unknown Asset' if null/empty
        COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name",

        -- Serial Number
        TRIM(serien_nummer) AS "Serial_Number__c",

        -- Warranty End Date: Parse German date formats (DD.MM.YYYY, DD-MM-YYYY, YYYY-MM-DD)
        CASE 
            WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
            WHEN TRIM(garantieende) ~ '^\d{2}[./-]\d{2}[./-]\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
            WHEN TRIM(garantieende) ~ '^\d{4}[./-]\d{2}[./-]\d{2}$' THEN TO_DATE(TRIM(garantieende), 'YYYY-MM-DD')::TEXT
            ELSE NULL
        END AS "Warranty_End_Date__c",

        -- Account__c: Transform customer key to Salesforce Account Id format ('001' prefix)
        CASE 
            WHEN kunden_kennung IS NULL THEN NULL
            ELSE '001' || SUBSTRING(kunden_kennung FROM 1 FOR 15)
        END AS "Account__c",

        -- Project__c: Transform project key to Salesforce custom object format ('a17' prefix for custom objects, or keep as-is if already prefixed)
        CASE 
            WHEN projekt_kennung IS NULL THEN NULL
            ELSE 'a17' || SUBSTRING(projekt_kennung FROM 1 FOR 15)
        END AS "Project__c",

        -- Legacy_Asset_ID__c: Store the original natural key for traceability
        asset_kennung AS "Legacy_Asset_ID__c",

        -- Audit dates: Use a consistent timestamp from source or current time
        CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
        CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",

        -- IsDeleted: 0 (not deleted)
        0 AS "IsDeleted"

    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
    WHERE asset_kennung IS NOT NULL AND TRIM(asset_kennung) != ''
)

SELECT * FROM assets;