-- dbt model for Project__c

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        WHEN status = 'Active' THEN 'Active'
        WHEN status = 'Completed' THEN 'Completed'
        WHEN status = 'In Planning' THEN 'In Planning'
        WHEN status = 'On Hold' THEN 'On Hold'
        WHEN status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}