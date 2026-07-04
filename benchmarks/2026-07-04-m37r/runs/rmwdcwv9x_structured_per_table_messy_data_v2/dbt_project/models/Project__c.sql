{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'in planung', 'planung') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD') -- YYYY-MM-DD
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- D.M.YYYY or DD.MM.YYYY
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
