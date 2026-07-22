{{ config(materialized='table') }}

SELECT 
    a.asset_kennung AS "Id",
    INITCAP(TRIM(a.asset_name)) AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    
     -- Warranty end date: handle multiple formats and sentinel values
    CASE 
        WHEN a.garantieende IS NULL THEN NULL
        WHEN TRIM(LOWER(a.garantieende)) IN ('n/a', '-', '0000-00-00') THEN NULL
        
         -- YYYY-MM-DD format (ISO) — only accept valid dates
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN CASE 
                WHEN TRY_CAST(a.garantieende AS DATE) IS NOT NULL 
                    THEN a.garantieende::DATE::text
                ELSE NULL
            END
            
         -- DD.MM.YYYY format (German locale)
        WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' 
            THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            
         -- YYYYMMDD format
        WHEN a.garantieende ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
            
        ELSE NULL
    END AS "Warranty_End_Date__c",

     -- Account Id: deterministic numeric-padding from kundennummer (matches Account model)
    '001' || lpad(CAST(SUBSTRING(k.kundennummer FROM 8) AS INTEGER)::text, 15, '0') AS "Account__c",
    
     -- Project Id: direct key pass-through (matches Project__c model)
    p.projekt_kennung AS "Project__c",
    
    a.asset_kennung AS "Legacy_Asset_ID__c",
    
     -- Default timestamps — source lacks asset-level audit fields
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a

LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON a.kunden_kennung = k.kundennummer

LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p 
    ON a.projekt_kennung = p.projekt_kennung