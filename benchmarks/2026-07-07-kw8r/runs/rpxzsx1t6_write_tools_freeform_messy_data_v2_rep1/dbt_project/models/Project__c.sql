{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN UPPER(TRIM(project_status__c)) IN ('ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(project_status__c)) IN ('COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(project_status__c)) IN ('IN PLANNING', 'IN_PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(project_status__c)) IN ('ON HOLD', 'ON_HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(project_status__c)) IN ('CANCELLED', 'CANCELED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    NULLIF(TRIM(id), '') AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
