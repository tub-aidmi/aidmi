{{ config(materialized='table') }}
SELECT 
    src.id AS "Id",
    TRIM(src.name) AS "Name",
    CASE 
        WHEN LOWER(TRIM(src.project_status__c)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(src.project_status__c)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(src.project_status__c)) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(src.project_status__c)) IN ('on hold', 'angehalten') THEN 'On Hold'
        WHEN LOWER(TRIM(src.project_status__c)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL 
    END AS "Project_Status__c",
    CASE 
        WHEN src.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN src.go_live_date__c
        WHEN LOWER(TRIM(src.go_live_date__c)) = 'n/a' THEN NULL
        ELSE NULL 
    END AS "Go_Live_Date__c",
    acc.id AS "Account__c",
    opp.id AS "Opportunity__c",
    src.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} AS src
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} AS acc ON src.account__c = acc.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp ON src.opportunity__c = opp.id