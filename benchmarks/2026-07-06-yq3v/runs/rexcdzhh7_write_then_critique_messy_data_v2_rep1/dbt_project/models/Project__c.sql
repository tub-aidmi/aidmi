{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(s.name, 'Unknown Project') AS "Name",
    CASE
        WHEN TRIM(s.project_status__c) ILIKE 'Active' THEN 'Active'
        WHEN TRIM(s.project_status__c) ILIKE 'Completed' THEN 'Completed'
        WHEN TRIM(s.project_status__c) ILIKE 'In Planning' THEN 'In Planning'
        WHEN TRIM(s.project_status__c) ILIKE 'On Hold' THEN 'On Hold'
        WHEN TRIM(s.project_status__c) ILIKE 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN s.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    s.account__c AS "Account__c",
    s.opportunity__c AS "Opportunity__c",
    s.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS s