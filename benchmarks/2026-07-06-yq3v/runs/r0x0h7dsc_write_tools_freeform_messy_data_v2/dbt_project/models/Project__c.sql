{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(project.project_status__c) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(project.project_status__c) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(project.project_status__c) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(project.project_status__c) = 'on hold' THEN 'On Hold'
        WHEN LOWER(project.project_status__c) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    project.account__c AS "Account__c",
    project.opportunity__c AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c", -- Source natural key
    '2023-01-01' AS "CreatedDate", -- Default value
    '2023-01-01' AS "LastModifiedDate", -- Default value
    0 AS "IsDeleted" -- Default value
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project
