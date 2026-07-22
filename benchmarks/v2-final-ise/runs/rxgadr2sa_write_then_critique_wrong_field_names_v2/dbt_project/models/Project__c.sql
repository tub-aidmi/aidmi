{{ config(materialized='table') }}

SELECT
     -- Primary key: transform proj_id to Salesforce-style ID with standard prefix
    'a00' || LOWER(TRIM(p.proj_id)) AS "Id",
    
     -- Project name (NOT NULL)
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unknown Project') AS "Name",
    
     -- Status mapping: map German source values to English enum values
    CASE 
        WHEN LOWER(TRIM(p.status)) IN ('aktiv', 'active', 'in arbeit', 'running') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('abgeschlossen', 'completed', 'fertig', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('planung', 'planning', 'in planung', 'planned', 'entwurf') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('pausiert', 'on hold', 'gehalten', 'held', 'gestoppt') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('storniert', 'cancelled', 'abgebrochen', 'cancel', 'beendet') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
     -- Go Live Date: parse DD.MM.YYYY or YYYY-MM-DD format to ISO text
    CASE 
        WHEN p.go_live IS NULL OR TRIM(p.go_live) = '' THEN NULL
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live)
        ELSE NULL
    END AS "Go_Live_Date__c",
    
     -- Account reference: transform kd to match Account model's Id transformation (001 prefix for accounts)
    '001' || TRIM(p.kd) AS "Account__c",
    
     -- Opportunity reference: transform opp to match Opportunity model's Id transformation (006 prefix for opportunities)
    '006' || TRIM(p.opp) AS "Opportunity__c",
    
     -- Legacy Project ID from source natural key
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    
     -- System fields - not available in source, set defaults
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p