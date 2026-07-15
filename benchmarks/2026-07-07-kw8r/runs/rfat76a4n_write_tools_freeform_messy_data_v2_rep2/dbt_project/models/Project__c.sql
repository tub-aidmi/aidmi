{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(TRIM(p.name), 'Untitled Project') AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.project_status__c)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_date__c = 'N/A' OR p.go_live_date__c = '0000-00-00' THEN NULL
        WHEN p.go_live_date__c ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_date__c
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    p.opportunity__c AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON p.account__c = a.id
