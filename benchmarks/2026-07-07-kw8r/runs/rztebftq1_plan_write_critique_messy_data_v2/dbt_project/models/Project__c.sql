{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(go_live_date__c) IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}