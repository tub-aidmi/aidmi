{{ config(materialized='table') }}

SELECT 
    -- Id: Salesforce-style unique ID for Installed Asset (custom object)
    CONCAT('00T', SUBSTR(MD5(LOWER(TRIM(a.asset_kennung))), 1, 14)) AS "Id",
    
    -- Name from asset_name
    TRIM(a.asset_name) AS "Name",
    
    -- Serial Number
    TRIM(a.serien_nummer) AS "Serial_Number__c",
    
    -- Warranty End Date: parse DD.MM.YYYY, YYYY-MM-DD, or YYYYMMDD formats
    CASE 
        WHEN a.garantieende IS NULL OR TRIM(a.garantieende) = '' THEN NULL
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(LOWER(TRIM(a.garantieende)), 'DD.MM.YYYY')::TEXT
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN LOWER(TRIM(a.garantieende))
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_DATE(LOWER(TRIM(a.garantieende)), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account__c: Salesforce-style Account ID (consistent with Account.Id transform)
    CONCAT('001', SUBSTR(MD5(LOWER(TRIM(k.kundennummer))), 1, 14)) AS "Account__c",
    
    -- Project__c: Salesforce-style Project ID (consistent with Project__c.Id transform)
    CONCAT('a00', SUBSTR(MD5(LOWER(TRIM(p.projekt_kennung))), 1, 14)) AS "Project__c",
    
    -- Legacy_Asset_ID__c: source natural key for row-level verification
    LOWER(TRIM(a.asset_kennung)) AS "Legacy_Asset_ID__c",
    
    -- CreatedDate and LastModifiedDate: defaults since source lacks timestamps
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    
    -- IsDeleted: 0 = false (no deletes in source)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON LOWER(TRIM(a.kunden_kennung)) = LOWER(TRIM(k.kundennummer))
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p 
    ON LOWER(TRIM(a.projekt_kennung)) = LOWER(TRIM(p.projekt_kennung))