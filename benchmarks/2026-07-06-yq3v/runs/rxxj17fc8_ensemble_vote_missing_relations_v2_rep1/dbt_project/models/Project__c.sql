{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE
        WHEN p.status = 'Active' THEN 'Active'
        WHEN p.status = 'Completed' THEN 'Completed'
        WHEN p.status = 'In Planning' THEN 'In Planning'
        WHEN p.status = 'On Hold' THEN 'On Hold'
        WHEN p.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.client_id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
