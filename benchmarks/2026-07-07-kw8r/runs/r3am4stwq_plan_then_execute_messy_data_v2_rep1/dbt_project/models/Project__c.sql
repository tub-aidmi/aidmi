{{ config(materialized='table') }}

SELECT 
    CAST(id AS TEXT) AS "Id",
    TRIM(name) AS "Name",
    CASE 
        WHEN TRIM(project_status__c) IS NULL OR TRIM(project_status__c) = '' THEN NULL
        WHEN LOWER(TRIM(project_status__c)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'angehalten', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'storniert', 'abgebrochen') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}