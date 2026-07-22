-- depends_on: {{ source('fixture_missing_relations_v2_src', 'project') }}
{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    project.name AS "Name",
    CASE
        WHEN project.status = 'Active' THEN 'Active'
        WHEN project.status = 'Completed' THEN 'Completed'
        WHEN project.status = 'In Planning' THEN 'In Planning'
        WHEN project.status = 'On Hold' THEN 'On Hold'
        WHEN project.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    project.go_live AS "Go_Live_Date__c",
    project.client_id AS "Account__c",
    project.opportunity_ref AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project