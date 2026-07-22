-- This dbt model transforms the raw project data into the target Project__c schema.
{{ config(materialized='table') }}

SELECT
    TRIM(src.id) AS "Id",
    COALESCE(src.name, 'Unknown Project Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(src.project_status__c)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(src.project_status__c)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(src.project_status__c)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(src.project_status__c)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(src.project_status__c)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(src.go_live_date__c) IS NULL THEN NULL
        WHEN TRIM(src.go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(src.go_live_date__c)
        WHEN TRIM(src.go_live_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'DD-MM-YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(src.go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(src.go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
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