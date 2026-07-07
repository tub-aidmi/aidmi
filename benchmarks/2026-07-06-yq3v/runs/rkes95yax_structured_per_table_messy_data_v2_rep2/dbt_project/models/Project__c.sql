-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    project.name AS "Name",
    CASE
        WHEN UPPER(TRIM(project.project_status__c)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project.project_status__c)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project.project_status__c)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project.project_status__c)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project.project_status__c)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(project.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(project.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(project.go_live_date__c, 'DD/MM/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    project.account__c AS "Account__c",
    project.opportunity__c AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS project