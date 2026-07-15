{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE LOWER(TRIM(project_status__c))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'on hold' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')::TEXT
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TO_DATE(go_live_date__c, 'YYYY-MM-DD') AS TEXT)
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
        WHEN go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}