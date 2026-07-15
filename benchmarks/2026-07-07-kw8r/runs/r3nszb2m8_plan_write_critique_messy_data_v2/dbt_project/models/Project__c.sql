{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NOT NULL AND go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_date__c != '0000-00-00' THEN TO_DATE(go_live_date__c, 'YYYY-MM-DD')::TEXT
        WHEN go_live_date__c IS NOT NULL AND go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        WHEN go_live_date__c IS NOT NULL AND go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c IS NOT NULL AND go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    UPPER(TRIM(account__c)) AS "Account__c",
    UPPER(TRIM(opportunity__c)) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}