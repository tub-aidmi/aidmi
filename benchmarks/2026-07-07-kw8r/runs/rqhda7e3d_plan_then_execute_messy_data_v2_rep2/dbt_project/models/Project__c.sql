{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown') AS "Name",
    CASE
        WHEN UPPER(TRIM(src.project_status__c)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('PLANUNG', 'IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(TRIM(src.project_status__c)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(src.go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(src.go_live_date__c)
        WHEN TRIM(src.go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src.account__c AS "Account__c",
    src.opportunity__c AS "Opportunity__c",
    src.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS src