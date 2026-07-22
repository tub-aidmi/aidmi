
{{ config(materialized='table') }}

SELECT
    P.id AS "Id",
    COALESCE(P.name, 'Untitled Project') AS "Name",
    CASE
        WHEN P.status = 'Active' THEN 'Active'
        WHEN P.status = 'Completed' THEN 'Completed'
        WHEN P.status = 'In Planning' THEN 'In Planning'
        WHEN P.status = 'On Hold' THEN 'On Hold'
        WHEN P.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    P.go_live AS "Go_Live_Date__c",
    P.client_id AS "Account__c",
    P.opportunity_ref AS "Opportunity__c",
    P.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Project') }} AS P
