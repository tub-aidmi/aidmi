{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(NULLIF(TRIM(src.name), ''), 'Unnamed Project') AS "Name",
    CASE 
        WHEN UPPER(TRIM(src.project_status__c)) IN ('ACTIVE', 'ACTIVE ') THEN 'Active'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('COMPLETED', 'COMPLETE') THEN 'Completed'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('IN PLANNING', 'PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('ON HOLD', 'HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('CANCELLED', 'CANCELED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN src.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN src.go_live_date__c
        WHEN src.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src.account__c AS "Account__c",
    src.opportunity__c AS "Opportunity__c",
    src.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} src