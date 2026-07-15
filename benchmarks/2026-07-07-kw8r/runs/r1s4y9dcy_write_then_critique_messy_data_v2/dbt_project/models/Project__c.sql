{{ config(materialized='table') }}
SELECT
    p.id AS "Id",
    TRIM(p.name) AS "Name",
    CASE
        WHEN LOWER(TRIM(p.project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.project_status__c)) IN ('in planning', 'in-planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.project_status__c)) IN ('on hold', 'on-hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_date__c
        WHEN p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON p.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o ON p.opportunity__c = o.id