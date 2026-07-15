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
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON p.client_id = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o ON p.opportunity_ref = o.id
