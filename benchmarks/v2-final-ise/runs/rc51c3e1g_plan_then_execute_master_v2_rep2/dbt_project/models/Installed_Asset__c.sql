{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        -- Id <- asset_kennung
        TRIM(asset_kennung) AS "Id",
        
        -- Name <- asset_name (TRIM(); INITCAP; fallback 'Unknown Asset' if NULL/empty)
        COALESCE(
            NULLIF(TRIM(asset_name), ''),
            'Unknown Asset'
        ) AS "Name",
        
        -- Serial_Number__c <- serien_nummer
        TRIM(serien_nummer) AS "Serial_Number__c",
        
        -- Warranty_End_Date__c <- garantieende (multi-format date parser with guards)
        CASE 
            WHEN NULLIF(TRIM(garantieende), '') IS NOT NULL THEN
                CASE
                    -- DD.MM.YYYY format (e.g., '15.03.2025')
                    WHEN TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' 
                        THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
                    -- YYYYMMDD format (e.g., '20250315')
                    WHEN TRIM(garantieende) ~ '^\d{8}$' 
                        THEN TO_DATE(TRIM(garantieende), 'YYYYMMDD')::TEXT
                    -- MM/DD/YYYY format (e.g., '03/15/2025')
                    WHEN TRIM(garantieende) ~ '^\d{2}/\d{2}/\d{4}$' 
                        THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
                    ELSE NULL
                END
            ELSE NULL
        END AS "Warranty_End_Date__c",
        
        -- Account__c <- kunden_kennung (UPPER + TRIM to align with Account.Id in cross-table key strategy)
        UPPER(TRIM(kunden_kennung)) AS "Account__c",
        
        -- Project__c <- projekt_kenner (aligned format for joining Project__c.Id)
        UPPER(TRIM(projekt_kennung)) AS "Project__c",
        
        -- Legacy_Asset_ID__c <- asset_kennung (raw key for row-level traceability)
        TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
        
        -- CreatedDate (static placeholder per guidelines)
        '2024-01-01 00:00:00'::TEXT AS "CreatedDate",
        
        -- LastModifiedDate (static placeholder per guidelines)
        '2024-01-01 00:00:00'::TEXT AS "LastModifiedDate",
        
        -- IsDeleted (literal 0, no deletion concept in source)
        0 AS "IsDeleted"
        
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)

SELECT * FROM asset_data