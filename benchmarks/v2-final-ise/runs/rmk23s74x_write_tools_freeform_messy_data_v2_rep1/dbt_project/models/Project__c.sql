{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'in progress', 'ongoing', 'running') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'finished', 'done', 'complete') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planning', 'not started', 'pending', 'preparation') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'paused', 'stopped', 'suspended', 'hold') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_date__c) IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live_date__c)
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN SUBSTR(TRIM(go_live_date__c), 1, 4) || '-' || SUBSTR(TRIM(go_live_date__c), 5, 2) || '-' || SUBSTR(TRIM(go_live_date__c), 7, 2)
        WHEN TRIM(go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD-MM-YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
