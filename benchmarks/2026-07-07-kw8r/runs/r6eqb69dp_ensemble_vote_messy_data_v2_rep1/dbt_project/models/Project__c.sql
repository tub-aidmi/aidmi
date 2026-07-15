{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN go_live_date__c ~ '^\d{8}$' AND go_live_date__c != '00000000' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        WHEN go_live_date__c ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}