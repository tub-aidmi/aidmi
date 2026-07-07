-- depends_on: 
{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, '') AS "Name",
    CASE
        WHEN TRIM(project.status) = 'Active' THEN 'Active'
        WHEN TRIM(project.status) = 'Completed' THEN 'Completed'
        WHEN TRIM(project.status) = 'In Planning' THEN 'In Planning'
        WHEN TRIM(project.status) = 'On Hold' THEN 'On Hold'
        WHEN TRIM(project.status) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(project.go_live AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    project.client_id AS "Account__c",
    project.opportunity_ref AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project