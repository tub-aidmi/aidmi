{{ config(materialized='table') }}

SELECT 
    -- Id: Prepend SFDC custom object prefix 'a00' to source key
    'a00' || TRIM(asset_id) AS "Id",
    
    -- Name: Normalized product/asset name with INITCAP; default fallback for missing names
    COALESCE(TRIM(INITCAP(bezeichnung)), 'Unknown Asset') AS "Name",
    
    -- Serial_Number__c: Trimmed serial number from source
    TRIM(seriennr) AS "Serial_Number__c",
    
    -- Warranty_End_Date__c: Safe multi-format date parser (DD.MM.YYYY, YYYYMMDD, or YYYY-MM-DD); NULL for unparseable
    CASE 
        WHEN TRIM(garantie_bis) IS NULL OR TRIM(garantie_bis) = '' THEN NULL
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{8}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYYMMDD')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account__c: Transform kd_ref using SFDC Account prefix '001' for FK alignment with Account.Id
    CASE 
        WHEN TRIM(kd_ref) IS NOT NULL AND TRIM(kd_ref) != '' THEN '001' || TRIM(kd_ref)
        ELSE NULL
    END AS "Account__c",
    
    -- Project__c: Transform projekt_ref using SFDC custom object prefix 'a00' for FK alignment with Project__c.Id
    CASE 
        WHEN TRIM(projekt_ref) IS NOT NULL AND TRIM(projekt_ref) != '' THEN 'a00' || TRIM(projekt_ref)
        ELSE NULL
    END AS "Project__c",
    
    -- Legacy_Asset_ID__c: Raw source asset_id for row-level audit/verification
    TRIM(asset_id) AS "Legacy_Asset_ID__c",
    
    -- CreatedDate: Audit field populated with current timestamp
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    
    -- LastModifiedDate: Audit field populated with current timestamp
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    
    -- IsDeleted: Static default 0 (no soft-delete semantics in source)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
WHERE TRIM(asset_id) IS NOT NULL AND TRIM(asset_id) != '';