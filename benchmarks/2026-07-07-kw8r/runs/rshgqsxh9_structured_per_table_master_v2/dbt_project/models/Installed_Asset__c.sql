{{ config(materialized='table') }}

WITH base_assets AS (
    SELECT
        a.asset_kennung,
        a.asset_name,
        a.serien_nummer,
        a.garantieende,
        a.kunden_kennung,
        a.projekt_kennung,
        -- Transform asset key to Salesforce-style ID: AST-00XXX → AS0XX (last 3 digits)
        CAST('AS' || SUBSTRING(a.asset_kennung FROM 'AST-(\d{5})' LIMIT 1) 
             RIGHT('000' || REGEXP_REPLACE(SUBSTRING(a.asset_kennung FROM '\d+$'), '^0+', ''), 3) AS TEXT) AS asset_id,
        -- Transform customer key to Account.Id: CUST-MNNN → CU + last 3 digits of NNN
        CAST('CU' || RIGHT('000' || REGEXP_REPLACE(a.kunden_kennung, 'CUST-M(\d+)', '\1'), 3) AS TEXT) AS account_id_transformed,
        -- Transform project key to Project.Id (only for PROJ-XXXXX format that matches master_projekte)
        CASE
            WHEN a.projekt_kennung ~ '^PROJ-\d{5}$' 
            THEN CAST('PR' || RIGHT('000' || REGEXP_REPLACE(a.projekt_kennung, 'PROJ-(\d+)', '\1'), 3) AS TEXT)
            ELSE NULL
        END AS project_id_transformed,
        -- Legacy asset ID for traceability
        a.asset_kennung AS legacy_asset_id
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
),
-- Filter to only assets whose projekt_kennung matches an existing project in master_projekte
filtered_assets AS (
    SELECT
        b.*,
        -- Only include if project_id_transformed is not NULL (i.e., Projekt exists in master_projekte)
        CASE WHEN b.project_id_transformed IS NOT NULL THEN 1 ELSE 0 END AS has_valid_project,
        -- Map account ID by looking up the customer number in master_kunden to get proper CU-xxx format
        m.prefix || RIGHT('000' || REGEXP_REPLACE(m.kundennummer, 'CUST-M(\d+)', '\1'), 3) AS mapped_account_id
    FROM base_assets b
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m 
        ON b.kunden_kennung = m.kundennummer
)
SELECT
    -- Id: Salesforce-style asset ID (AS001, AS002, ...)
    fa.asset_id AS "Id",
    
    -- Name: Asset name, trimmed and init-capped
    INITCAP(TRIM(fa.asset_name)) AS "Name",
    
    -- Serial_Number__c: Direct from source
    TRIM(fa.serien_nummer) AS "Serial_Number__c",
    
    -- Warranty_End_Date__c: Parse multiple date formats to ISO YYYY-MM-DD
    CASE
        -- N/A or empty → NULL
        WHEN fa.garantieende IS NULL OR LOWER(TRIM(fa.garantieende)) IN ('n/a', '', 'null') THEN NULL
        -- Already in YYYY-MM-DD format
        WHEN fa.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TO_DATE(fa.garantieende, 'YYYY-MM-DD') AS TEXT)
        -- DD.MM.YYYY format (European)
        WHEN fa.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(fa.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- MM/DD/YYYY format (US)
        WHEN fa.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(fa.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- Fallback for other formats → NULL
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Account__c: Salesforce Account.Id (mapped via master_kunden join)
    fa.mapped_account_id AS "Account__c",
    
    -- Project__c: Salesforce Project__c.Id (only where project exists in master_projekte)
    fa.project_id_transformed AS "Project__c",
    
    -- Legacy_Asset_ID__c: Original source key for traceability
    fa.asset_kennung AS "Legacy_Asset_ID__c",
    
    -- CreatedDate/LastModifiedDate: Not available in source, use placeholder timestamps
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    
    -- IsDeleted: Default to 0 (not deleted)
    0 AS "IsDeleted"

FROM filtered_assets fa
