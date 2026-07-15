{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Asset Id: prefix with 'A' for cross-table FK consistency
    'A' || TRIM("asset_kennung") AS "Id",
    COALESCE(TRIM("asset_name"), 'Asset ' || TRIM("asset_kennung")) AS "Name",
    TRIM("serien_nummer") AS "Serial_Number__c",
    -- Parse garantieende (multiple possible formats) into ISO YYYY-MM-DD
    CASE
        WHEN TRIM("garantieende") IS NULL OR TRIM("garantieende") = '' THEN NULL
        WHEN TRIM("garantieende") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM("garantieende"), 'DD.MM.YYYY')::TEXT
        WHEN TRIM("garantieende") ~ '^\d{4}-\d{2}-\d{2}$' THEN SUBSTRING(TRIM("garantieende") FROM 1 FOR 10)
        WHEN TRIM("garantieende") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM("garantieende"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM("garantieende") ~ '^\d{8}$' THEN
            CASE
                WHEN SUBSTRING(TRIM("garantieende") FROM 1 FOR 4)::INTEGER BETWEEN 1900 AND 2099
                    THEN TO_DATE(TRIM("garantieende"), 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Account__c: Salesforce-style, match Account.Id = 'C' || kunden_kennung
    CASE
        WHEN TRIM("kunden_kennung") IS NOT NULL THEN 'C' || TRIM("kunden_kennung")
        ELSE NULL
    END AS "Account__c",
    -- Project__c: Salesforce-style, match Project__c.Id = 'P' || projekt_kennung
    CASE
        WHEN TRIM("projekt_kennung") IS NOT NULL THEN 'P' || TRIM("projekt_kennung")
        ELSE NULL
    END AS "Project__c",
    -- Legacy key from source natural key
    TRIM("asset_kennung") AS "Legacy_Asset_ID__c",
    -- Fixed dates
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}
