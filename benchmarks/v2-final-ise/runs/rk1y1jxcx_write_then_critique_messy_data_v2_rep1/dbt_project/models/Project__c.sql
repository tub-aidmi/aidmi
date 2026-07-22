{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'held', 'paused') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'cancel', 'closed') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{8}$' THEN 
            SUBSTR(go_live_date__c, 1, 4) || '-' || SUBSTR(go_live_date__c, 5, 2) || '-' || SUBSTR(go_live_date__c, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}