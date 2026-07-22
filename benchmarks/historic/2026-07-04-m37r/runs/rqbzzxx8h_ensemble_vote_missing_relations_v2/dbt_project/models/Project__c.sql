-- dbt model for Project__c
{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unknown Project') AS "Name",
    CASE
        WHEN project.status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN project.status
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