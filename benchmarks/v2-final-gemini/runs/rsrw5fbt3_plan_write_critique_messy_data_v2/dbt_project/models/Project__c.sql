{{ config(materialized='table') }}

SELECT
    TRIM(src.id) AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(src.project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(src.project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(src.project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(src.project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(src.project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(src.go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(src.account__c) AS "Account__c",
    TRIM(src.opportunity__c) AS "Opportunity__c",
    TRIM(src.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS src
