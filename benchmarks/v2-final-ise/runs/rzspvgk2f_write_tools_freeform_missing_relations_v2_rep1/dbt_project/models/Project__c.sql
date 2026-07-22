{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.status)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('on hold', 'hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        CASE 
            WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN p.go_live
            ELSE NULL 
        END,
        NULL
    ) AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(p.client_id) = TRIM(a.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(p.opportunity_ref) = TRIM(o.id)
