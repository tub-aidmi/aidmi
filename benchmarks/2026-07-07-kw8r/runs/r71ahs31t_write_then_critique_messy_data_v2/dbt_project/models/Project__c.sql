{{ config(materialized='table') }}
SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE 
        WHEN UPPER(TRIM(p.project_status__c)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('PLANUNG', 'IN PLANNING', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('ON HOLD', 'ONHOLD') THEN 'On Hold'
        WHEN UPPER(TRIM(p.project_status__c)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_date__c
        WHEN p.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON p.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o ON p.opportunity__c = o.id