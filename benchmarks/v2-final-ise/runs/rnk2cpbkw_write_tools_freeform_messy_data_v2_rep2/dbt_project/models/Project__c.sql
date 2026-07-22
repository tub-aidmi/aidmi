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
),

transformed AS (
    SELECT
        id AS "Id",
        COALESCE(TRIM(INITCAP(name)), '') AS "Name",
        CASE
            WHEN LOWER(TRIM(project_status__c)) IN ('active', 'in progress', 'live') THEN 'Active'
            WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'complete', 'finished', 'done') THEN 'Completed'
            WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planning') THEN 'In Planning'
            WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'paused', 'suspended', 'hold') THEN 'On Hold'
            WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'cancel', 'cancelled') THEN 'Cancelled'
            ELSE NULL
        END AS "Project_Status__c",
        CASE
            WHEN go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live_date__c, 'DD.MM.YYYY')::TEXT
            WHEN go_live_date__c ~ '^\d{8}$' THEN SUBSTR(go_live_date__c, 1, 4) || '-' || SUBSTR(go_live_date__c, 5, 2) || '-' || SUBSTR(go_live_date__c, 7, 2)
            WHEN go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live_date__c, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "Go_Live_Date__c",
        CAST(account__c AS TEXT) AS "Account__c",
        CAST(opportunity__c AS TEXT) AS "Opportunity__c",
        id AS "Legacy_Project_ID__c",
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM source_data
)

SELECT * FROM transformed
