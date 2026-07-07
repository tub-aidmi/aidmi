-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    TRIM(project_c.id) AS "Id",
    TRIM(project_c.name) AS "Name",
    CASE
        WHEN TRIM(UPPER(project_c.project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN TRIM(UPPER(project_c.project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN TRIM(UPPER(project_c.project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN TRIM(UPPER(project_c.project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN TRIM(UPPER(project_c.project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project_c.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN project_c.go_live_date__c -- Assuming YYYY-MM-DD as primary format
        WHEN project_c.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(project_c.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project_c.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project_c.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(project_c.account__c) AS "Account__c",
    TRIM(project_c.opportunity__c) AS "Opportunity__c",
    TRIM(project_c.id) AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project_c