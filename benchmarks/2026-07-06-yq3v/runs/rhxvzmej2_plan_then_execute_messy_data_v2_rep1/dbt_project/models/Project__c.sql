{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        WHEN LOWER(project_status__c) = 'aktiv' THEN 'Active'
        WHEN LOWER(project_status__c) = 'completed' THEN 'Completed'
        WHEN LOWER(project_status__c) = 'planung' THEN 'In Planning'
        WHEN LOWER(project_status__c) = 'in planung' THEN 'In Planning'
        WHEN LOWER(project_status__c) = 'on hold' THEN 'On Hold'
        WHEN LOWER(project_status__c) = 'storniert' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c", -- Assuming account__c directly maps to Salesforce Account Id
    opportunity__c AS "Opportunity__c", -- Assuming opportunity__c directly maps to Salesforce Opportunity Id
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
