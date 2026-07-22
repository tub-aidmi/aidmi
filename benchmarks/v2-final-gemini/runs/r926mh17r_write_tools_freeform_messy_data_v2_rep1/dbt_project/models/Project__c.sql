{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN UPPER(TRIM(project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CASE
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(go_live_date__c, 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')
        WHEN go_live_date__c ~ '^\d{2}.\d{2}.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')
        ELSE NULL
    END, 'YYYY-MM-DD') AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
