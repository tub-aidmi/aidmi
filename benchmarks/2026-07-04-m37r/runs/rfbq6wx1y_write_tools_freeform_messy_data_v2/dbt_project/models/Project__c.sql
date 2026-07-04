{{ config(materialized='table') }}

SELECT
    TRIM(project.id) AS "Id",
    COALESCE(TRIM(project.name), 'Unnamed Project ' || TRIM(project.id)) AS "Name",
    CASE
        WHEN LOWER(TRIM(project.project_status__c)) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(TRIM(project.project_status__c)) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(TRIM(project.project_status__c)) IN ('in planning', 'planung', 'in planung') THEN 'In Planning'
        WHEN LOWER(TRIM(project.project_status__c)) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(TRIM(project.project_status__c)) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(project.go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(project.go_live_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(project.go_live_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(project.go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(project.account__c) AS "Account__c",
    TRIM(project.opportunity__c) AS "Opportunity__c",
    TRIM(project.id) AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project
WHERE
    project.id IS NOT NULL
