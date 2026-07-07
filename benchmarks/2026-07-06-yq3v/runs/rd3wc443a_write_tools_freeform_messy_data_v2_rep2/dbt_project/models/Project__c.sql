{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name", -- Name is NOT NULL, fallback to id
    CASE LOWER(TRIM(project_status__c))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    (CASE
        WHEN TRIM(go_live_date__c) IN ('N/A', '0000-00-00', '') THEN NULL
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_date__c, 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_DATE(go_live_date__c, 'YYYYMMDD')
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')
        ELSE NULL
    END)::TEXT AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c", -- Using id as the natural key
    NULL::TEXT AS "CreatedDate", -- Placeholder
    NULL::TEXT AS "LastModifiedDate", -- Placeholder
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
