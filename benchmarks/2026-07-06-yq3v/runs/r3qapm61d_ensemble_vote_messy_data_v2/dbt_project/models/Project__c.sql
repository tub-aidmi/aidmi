{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        WHEN UPPER(TRIM(project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_date__c) = '0000-00-00' THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD') -- YYYY-MM-DD
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN TRIM(go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        ELSE NULL
    END AS "Go_Live_Date__c",
    account__c AS "Account__c",
    opportunity__c AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}