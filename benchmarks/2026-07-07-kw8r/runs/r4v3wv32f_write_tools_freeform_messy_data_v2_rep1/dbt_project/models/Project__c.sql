{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE
        WHEN UPPER(NULLIF(TRIM(project_status__c), '')) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(NULLIF(TRIM(project_status__c), '')) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(NULLIF(TRIM(project_status__c), '')) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(NULLIF(TRIM(project_status__c), '')) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
        WHEN UPPER(NULLIF(TRIM(project_status__c), '')) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN NULLIF(TRIM(go_live_date__c), '') IS NULL THEN NULL
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
