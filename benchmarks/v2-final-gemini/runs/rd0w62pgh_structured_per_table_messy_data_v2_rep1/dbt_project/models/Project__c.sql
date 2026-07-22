-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    T1.id AS "Id",
    COALESCE(T1.name, 'Unknown Project') AS "Name",
    CASE
        WHEN T1.project_status__c = 'Active' THEN 'Active'
        WHEN T1.project_status__c = 'Completed' THEN 'Completed'
        WHEN T1.project_status__c = 'In Planning' THEN 'In Planning'
        WHEN T1.project_status__c = 'On Hold' THEN 'On Hold'
        WHEN T1.project_status__c = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN T1.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN T1.go_live_date__c
        WHEN T1.go_live_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(T1.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    T1.account__c AS "Account__c",
    T1.opportunity__c AS "Opportunity__c",
    T1.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'project__c') }} AS T1