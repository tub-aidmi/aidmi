{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(INITCAP(TRIM(p.name)), 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(p.status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NULL THEN NULL
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(a.id, p.client_id) AS "Account__c",
    COALESCE(o.id, p.opportunity_ref) AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON LOWER(TRIM(p.client_id)) = LOWER(TRIM(a.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON LOWER(TRIM(p.opportunity_ref)) = LOWER(TRIM(o.id))