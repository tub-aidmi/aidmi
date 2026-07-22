{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN TRIM(LOWER(p.status)) = 'active' THEN 'Active'
        WHEN TRIM(LOWER(p.status)) = 'completed' THEN 'Completed'
        WHEN TRIM(LOWER(p.status)) = 'in planning' THEN 'In Planning'
        WHEN TRIM(LOWER(p.status)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
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
WHERE a.id IS NOT NULL AND o.id IS NOT NULL
