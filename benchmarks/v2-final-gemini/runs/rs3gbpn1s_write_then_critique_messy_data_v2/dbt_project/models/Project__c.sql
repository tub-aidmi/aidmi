-- depends_on: fixture_messy_data_v2_src.project__c
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'N/A') AS "Name",
    CASE
        WHEN TRIM(project_status__c) ILIKE 'Active' THEN 'Active'
        WHEN TRIM(project_status__c) ILIKE 'Completed' THEN 'Completed'
        WHEN TRIM(project_status__c) ILIKE 'In Planning' THEN 'In Planning'
        WHEN TRIM(project_status__c) ILIKE 'On Hold' THEN 'On Hold'
        WHEN TRIM(project_status__c) ILIKE 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_date__c
        WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_date__c ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_CHAR(TO_DATE(go_live_date__c, 'YYYY/MM/DD'), 'YYYY-MM-DD')
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