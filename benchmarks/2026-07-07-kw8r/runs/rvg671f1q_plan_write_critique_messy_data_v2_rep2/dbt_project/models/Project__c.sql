{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(INITCAP(NULLIF(TRIM(p.name), '')), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(p.project_status__c))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(p.go_live_date__c) IS NULL OR TRIM(p.go_live_date__c) = '' THEN NULL
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'DD.MM.YYYY')::TEXT
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN p.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_DATE(TRIM(p.go_live_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    NULLIF(TRIM(UPPER(p.opportunity__c)), '') AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} p
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
    ON TRIM(UPPER(p.account__c)) = TRIM(UPPER(a.id))