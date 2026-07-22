{{ config(materialized='table') }}

SELECT
    TRIM(asset_kennung) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    
    -- Warranty End Date: handle DD.MM.YYYY and YYYY-MM-DD formats; prefer NULL over sentinel dates
    CASE
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TRIM(garantieende)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account reference: consistent transform matching Account model's Id generation
    CASE WHEN TRIM(kunden_kennung) = '' THEN NULL
         ELSE INITCAP(TRIM(kunden_kennung))
    END AS "Account__c",
    
    -- Project reference: same consistent key transform applied across sibling models
    CASE WHEN TRIM(projekt_kennung) = '' THEN NULL
         ELSE INITCAP(TRIM(projekt_kennung))
    END AS "Project__c",
    
    -- Legacy natural key for row-level verification
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    
    -- System fields not present in raw source
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    
    -- Default non-deleted record
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}