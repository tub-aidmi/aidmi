{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE 
        WHEN LOWER(p.status) = 'active' THEN 'Active'
        WHEN LOWER(p.status) = 'completed' THEN 'Completed'
        WHEN LOWER(p.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(p.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    CASE 
        WHEN p.client_id ~ '^ACC-\d+$' THEN p.client_id
        ELSE (SELECT acc.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc WHERE acc.name = p.client_id LIMIT 1)
    END AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
