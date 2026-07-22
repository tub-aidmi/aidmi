{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE 
        WHEN UPPER(TRIM(status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(status)) LIKE '%PLANNING%' THEN 'In Planning'
        WHEN UPPER(TRIM(status)) LIKE '%HOLD%' OR UPPER(TRIM(status)) IN ('ON HOLD', 'PAUSED', 'SUSPENDED') THEN 'On Hold'
        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
        -- European DD.MM.YYYY format (most specific first to avoid misparse)
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live), 'DD.MM.YYYY')::TEXT
        -- US MM/DD/YYYY format  
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(go_live), 'MM/DD/YYYY')::TEXT
        -- ISO YYYY-MM-DD format (already correct)
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live)
        -- Compact YYYYMMDD format
        WHEN go_live ~ '^\d{8}$' THEN 
            LEFT(TRIM(go_live), 4) || '-' || SUBSTRING(TRIM(go_live) FROM 5 FOR 2) || '-' || RIGHT(TRIM(go_live), 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}