{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(src.project_status__c) = 'active' THEN 'Active'
        WHEN LOWER(src.project_status__c) = 'completed' THEN 'Completed'
        WHEN LOWER(src.project_status__c) = 'in planning' THEN 'In Planning'
        WHEN LOWER(src.project_status__c) = 'on hold' THEN 'On Hold'
        WHEN LOWER(src.project_status__c) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src.go_live_date__c IS NULL THEN NULL
        WHEN src.go_live_date__c = '0000-00-00' THEN NULL
        WHEN src.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src.account__c AS "Account__c",
    src.opportunity__c AS "Opportunity__c",
    src.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS src
