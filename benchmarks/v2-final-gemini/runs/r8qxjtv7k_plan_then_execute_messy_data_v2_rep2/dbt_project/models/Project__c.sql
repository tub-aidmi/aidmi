{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        project_status__c,
        go_live_date__c,
        account__c,
        opportunity__c
    FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
)

SELECT
    TRIM(source_data.id) AS "Id",
    TRIM(COALESCE(source_data.name, 'Unknown Project')) AS "Name",
    CASE
        WHEN TRIM(INITCAP(source_data.project_status__c)) = 'Active' THEN 'Active'
        WHEN TRIM(INITCAP(source_data.project_status__c)) = 'Completed' THEN 'Completed'
        WHEN TRIM(INITCAP(source_data.project_status__c)) = 'In Planning' THEN 'In Planning'
        WHEN TRIM(INITCAP(source_data.project_status__c)) = 'On Hold' THEN 'On Hold'
        WHEN TRIM(INITCAP(source_data.project_status__c)) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN source_data.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN source_data.go_live_date__c
        WHEN source_data.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' AND TO_DATE(source_data.go_live_date__c, 'DD.MM.YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(source_data.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN source_data.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' AND TO_DATE(source_data.go_live_date__c, 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(source_data.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN source_data.go_live_date__c ~ '^\d{8}$' AND TO_DATE(source_data.go_live_date__c, 'YYYYMMDD') IS NOT NULL THEN TO_CHAR(TO_DATE(source_data.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(source_data.account__c) AS "Account__c",
    TRIM(source_data.opportunity__c) AS "Opportunity__c",
    TRIM(source_data.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_data
