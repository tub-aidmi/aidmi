{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(COALESCE(name, 'Unknown')) AS "Name",
    serial_number__c AS "Serial_Number__c",
    
    -- Parse warranty_end_date__c handling multiple formats and invalid dates
    CASE
        WHEN warranty_end_date__c IS NULL 
            OR TRIM(warranty_end_date__c) = '' 
            OR TRIM(LOWER(warranty_end_date__c)) IN ('n/a', 'na', '-') THEN NULL
        WHEN warranty_end_date__c ~ '^0{4}-0{2}-0{2}$' THEN NULL  -- Sentinel date
        WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
        WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN 
            TO_DATE(
                SUBSTRING(warranty_end_date__c FROM 1 FOR 4) || '-' ||
                SUBSTRING(warranty_end_date__c FROM 5 FOR 2) || '-' ||
                SUBSTRING(warranty_end_date__c FROM 7 FOR 2),
                'YYYY-MM-DD'
            )::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    
    -- Foreign keys mapped to Salesforce-style IDs (Account.id format is CUST-XXXX, Project__c.id format is PROJ-XXXXX)
    account__c AS "Account__c",
    project__c AS "Project__c",
    
    -- Legacy key from source natural id
    id AS "Legacy_Asset_ID__c",
    
    -- Dates not present in source, mark as NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    
    -- Default to 0 (not deleted) since source doesn't track this
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}