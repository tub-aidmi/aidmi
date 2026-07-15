{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    name AS "Name",
    CASE UPPER(TRIM(project_status__c))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'IN PLANUNG' THEN 'In Planning'
        WHEN 'PLANUNG' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'PAUSIERT' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' OR TRIM(go_live_date__c) = 'N/A' THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}