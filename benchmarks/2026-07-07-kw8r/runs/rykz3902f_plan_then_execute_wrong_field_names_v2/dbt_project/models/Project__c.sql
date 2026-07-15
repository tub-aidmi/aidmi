{{ config(materialized='table') }}

WITH proj_data AS (
    SELECT
        p.proj_id,
        p.name AS project_name,
        p.status AS project_status,
        p.go_live,
        p.kd AS customer_ref,
        p.opp AS opportunity_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
)

SELECT
    p.proj_id AS "Id",
    p.project_name AS "Name",
    CASE 
        WHEN p.project_status = 'Active' THEN 'Active'
        WHEN p.project_status = 'Completed' THEN 'Completed'
        WHEN p.project_status = 'In Planning' THEN 'In Planning'
        WHEN p.project_status = 'On Hold' THEN 'On Hold'
        WHEN p.project_status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL 
    END AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM proj_data p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.customer_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opportunity_ref = c.chance_id