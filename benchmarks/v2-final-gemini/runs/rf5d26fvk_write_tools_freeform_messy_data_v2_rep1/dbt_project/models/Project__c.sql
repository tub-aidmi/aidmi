{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), TRIM(id)) AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live_date__c) -- YYYY-MM-DD
        WHEN TRIM(go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(opportunity__c) AS "Opportunity__c",
    TRIM(id) AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}
