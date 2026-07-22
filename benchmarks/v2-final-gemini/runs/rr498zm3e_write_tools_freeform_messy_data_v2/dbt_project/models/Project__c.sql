{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN -- YYYY-MM-DD
            TRIM(go_live_date__c)
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN -- DD.MM.YYYY
            TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN -- YYYYMMDD
            TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN -- MM/DD/YYYY
            TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    id AS "Legacy_Project_ID__c", -- Using source id as legacy ID
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
