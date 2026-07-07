-- depends_on: {{ source('fixture_messy_data_v2_src', 'project__c') }}

{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(TRIM("name"), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM("project_status__c")) IN ('active', 'कार्यशील') THEN 'Active'
        WHEN LOWER(TRIM("project_status__c")) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM("project_status__c")) IN ('in planning', ' नियोजन') THEN 'In Planning'
        WHEN LOWER(TRIM("project_status__c")) IN ('on hold', 'निष्क्रिय') THEN 'On Hold'
        WHEN LOWER(TRIM("project_status__c")) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN "go_live_date__c" ~ '^\d{4}-\d{2}-\d{2}$' THEN "go_live_date__c" -- YYYY-MM-DD
        WHEN "go_live_date__c" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("go_live_date__c", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "go_live_date__c" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("go_live_date__c", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    "account__c" AS "Account__c",
    "opportunity__c" AS "Opportunity__c",
    "id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }}