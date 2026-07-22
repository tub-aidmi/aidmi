{{ config(materialized='table') }}

SELECT 
    p.id AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unnamed Project') AS "Name",
    
    -- Map project_status__c to enum values
    CASE 
        WHEN TRIM(LOWER(p.project_status__c)) IN ('active', 'in progress', 'running') THEN 'Active'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('completed', 'finished', 'done') THEN 'Completed'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('planning', 'in planning', 'preparation') THEN 'In Planning'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('on hold', 'paused', 'suspended') THEN 'On Hold'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('cancelled', 'canceled', 'aborted') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    -- Parse go_live_date__c to ISO format (YYYY-MM-DD)
    CASE 
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_date__c
        WHEN p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    
    -- Account__c: Use the Salesforce-style Account Id from the joined account table
    a.id AS "Account__c",
    
    -- Opportunity__c: Use the Salesforce-style Opportunity Id from the joined opportunity table
    o.id AS "Opportunity__c",
    
    -- Legacy_Project_ID__c: Populate from the source natural key (id)
    p.id AS "Legacy_Project_ID__c",
    
    -- CreatedDate and LastModifiedDate: Use NULL as default for now
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    
    -- IsDeleted: Default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p

-- Join to account to resolve Account__c to Salesforce-style Account Id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
    ON p.account__c = a.id 
    OR p.account__c = a.erp_number__c 
    OR p.account__c = a.legacy_customer_id__c

-- Join to opportunity to resolve Opportunity__c to Salesforce-style Opportunity Id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o 
    ON p.opportunity__c = o.id