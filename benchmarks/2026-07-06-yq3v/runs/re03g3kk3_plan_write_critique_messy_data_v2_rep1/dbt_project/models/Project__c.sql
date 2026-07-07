-- dbt model for Project__c
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'N/A') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}