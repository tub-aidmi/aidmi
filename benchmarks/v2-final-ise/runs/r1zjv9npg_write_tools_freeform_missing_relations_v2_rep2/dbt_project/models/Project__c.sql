{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE 
        WHEN LOWER(p.status) IN ('active') THEN 'Active'
        WHEN LOWER(p.status) IN ('completed') THEN 'Completed'
        WHEN LOWER(p.status) IN ('in planning') THEN 'In Planning'
        WHEN LOWER(p.status) IN ('on hold') THEN 'On Hold'
        WHEN LOWER(p.status) IN ('cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON p.client_id = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
    ON p.opportunity_ref = o.id
