{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(id)) AS "Id",
    CASE 
        WHEN name IS NULL OR TRIM(name) = '' THEN 'Unknown'
        ELSE INITCAP(TRIM(name))
    END AS "Name",
    CASE 
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled') 
        THEN INITCAP(LOWER(TRIM(project_status__c)))
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(go_live_date__c) IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND (SPLIT_PART(TRIM(go_live_date__c), '/', 1)::INT <= 12) THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(UPPER(account__c)) AS "Account__c",
    TRIM(UPPER(opportunity__c)) AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}