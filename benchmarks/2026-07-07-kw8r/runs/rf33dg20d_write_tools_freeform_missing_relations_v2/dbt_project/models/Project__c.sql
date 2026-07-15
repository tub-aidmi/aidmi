{{ config(materialized='table') }}

SELECT 
    p.id AS "Id",
    p.name AS "Name",
    CASE 
        WHEN p.status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN p.status
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.client_id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
