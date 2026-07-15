{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE 
        WHEN LOWER(p.status) = 'active' THEN 'Active'
        WHEN LOWER(p.status) IN ('completed', 'done', 'finished', 'closed') THEN 'Completed'
        WHEN LOWER(p.status) IN ('in planning', 'planning', 'planned') THEN 'In Planning'
        WHEN LOWER(p.status) IN ('on hold', 'paused', 'pending') THEN 'On Hold'
        WHEN LOWER(p.status) IN ('cancelled', 'canceled', 'terminated') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(p.go_live, 'YYYY-MM-DD')::TEXT
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live, 'MM/DD/YYYY')::TEXT
        WHEN p.go_live ~ '^\d{8}$' THEN TO_DATE(p.go_live, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    acc.id AS "Account__c",
    opp.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON p.client_id = acc.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} opp
    ON p.opportunity_ref = opp.id
