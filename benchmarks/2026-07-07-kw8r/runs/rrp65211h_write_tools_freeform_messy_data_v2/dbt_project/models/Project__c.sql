{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    TRIM(name) AS "Name",
    CASE 
        WHEN UPPER(TRIM(project_status__c)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
        WHEN UPPER(TRIM(project_status__c)) IN ('COMPLETED', 'COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(project_status__c)) IN ('PLANUNG', 'IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(project_status__c)) IN ('STORNIERT', 'ON HOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(project_status__c)) IN ('CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c ~ '^[0-9]{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN go_live_date__c
        WHEN go_live_date__c IS NULL OR go_live_date__c = 'N/A' THEN NULL
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
