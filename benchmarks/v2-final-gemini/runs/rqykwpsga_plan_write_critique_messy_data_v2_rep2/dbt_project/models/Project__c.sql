{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(p.project_status__c) = 'active' THEN 'Active'
        WHEN LOWER(p.project_status__c) = 'completed' THEN 'Completed'
        WHEN LOWER(p.project_status__c) = 'in planning' THEN 'In Planning'
        WHEN LOWER(p.project_status__c) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.project_status__c) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.account__c AS "Account__c",
    p.opportunity__c AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    TO_CHAR(NOW()::DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW()::DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS p
