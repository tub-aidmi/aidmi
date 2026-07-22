{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE
        WHEN LOWER(TRIM(project_status__c)) IN ('active', 'in_progress', 'ongoing') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('completed', 'complete', 'finished', 'closed') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('in planning', 'planning', 'planned', 'not started') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'paused', 'suspended', 'stopped') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('cancelled', 'canceled', 'cancelled', 'void') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live_date__c IS NOT NULL AND TRIM(go_live_date__c) <> '' THEN
            COALESCE(
                TRY_CAST(TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD') AS TEXT),
                TRY_CAST(TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY') AS TEXT),
                TRY_CAST(TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY') AS TEXT),
                TRY_CAST(TO_DATE(REGEXP_REPLACE(go_live_date__c, '[^0-9]', '', 'g'), 'YYYYMMDD') AS TEXT)
            )
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(opportunity__c AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}