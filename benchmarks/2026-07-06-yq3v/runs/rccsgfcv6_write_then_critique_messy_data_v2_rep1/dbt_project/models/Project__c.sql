-- depends_on: {{ ref('Account') }} {{ ref('Opportunity') }}
{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(TRIM(project.name), 'Untitled Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(project.project_status__c)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
        WHEN UPPER(TRIM(project.project_status__c)) IN ('COMPLETED') THEN 'Completed'
        WHEN UPPER(TRIM(project.project_status__c)) IN ('IN PLANNING', 'PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(project.project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project.project_status__c)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    project.account__c AS "Account__c",
    project.opportunity__c AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project