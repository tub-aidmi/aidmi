{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(id)) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled') 
        THEN INITCAP(TRIM(project_status__c)) 
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(go_live_date__c) = '' THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL 
    END AS "Go_Live_Date__c",
    TRIM(UPPER(account__c)) AS "Account__c",
    TRIM(UPPER(opportunity__c)) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}