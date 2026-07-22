{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(TRIM(INITCAP(p.name)), 'Unnamed Project') AS "Name",
    CASE 
        WHEN TRIM(LOWER(p.project_status__c)) IN ('active', 'a') THEN 'Active'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('completed', 'done') THEN 'Completed'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('on hold', 'paused') THEN 'On Hold'
        WHEN TRIM(LOWER(p.project_status__c)) IN ('cancelled', 'closed', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_date__c IS NULL THEN NULL
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_date__c
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_DATE(p.go_live_date__c, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON p.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o ON p.opportunity__c = o.id