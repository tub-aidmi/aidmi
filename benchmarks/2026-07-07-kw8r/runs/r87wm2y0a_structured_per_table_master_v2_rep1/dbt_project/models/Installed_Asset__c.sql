{{ config(materialized='table') }}

SELECT
    -- Primary key and legacy ID from asset source
    "asset_kennung" AS "Id",
    "asset_kennung" AS "Legacy_Asset_ID__c",
    
    -- Asset name
    "asset_name" AS "Name",
    
    -- Serial number
    CAST("serien_nummer" AS TEXT) AS "Serial_Number__c",
    
    -- Warranty end date - parse multiple formats to ISO YYYY-MM-DD
    CASE 
        WHEN "garantieende" IS NULL OR TRIM("garantieende") = '' THEN NULL
        WHEN "garantieende" ~ '^\d{4}-\d{2}-\d{2}$' THEN "garantieende"  -- YYYY-MM-DD
        WHEN "garantieende" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE("garantieende", 'MM/DD/YYYY')::TEXT  -- MM/DD/YYYY
        WHEN "garantieende" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE("garantieende", 'DD.MM.YYYY')::TEXT  -- DD.MM.YYYY
        WHEN "garantieende" ~ '^\d{8}$' THEN 
            SUBSTR("garantieende", 1, 4) || '-' || SUBSTR("garantieende", 5, 2) || '-' || SUBSTR("garantieende", 7, 2)  -- YYYYMMDD
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account ID - reference to Account model using customer key
    "kunden_kennung" AS "Account__c",
    
    -- Project ID - join with master_projekte to get standard project format
    CASE 
        WHEN "projekt_kennung" ~ '^PROJ-\d{5}$' THEN "projekt_kennung"  -- Standard format: PROJ-00001
        WHEN "projekt_kennung" ~ '^PROJ-M-\d{5}$' THEN 'MISSING-' || RIGHT("projekt_kennung", 5)  -- Map missing/bad projects to MISSING-XXXXX prefix
        ELSE NULL
    END AS "Project__c",
    
    -- Static fields for Salesforce compliance
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}