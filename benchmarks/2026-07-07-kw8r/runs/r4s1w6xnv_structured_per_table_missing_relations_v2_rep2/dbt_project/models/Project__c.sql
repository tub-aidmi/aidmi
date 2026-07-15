{{ config(materialized='table') }}

SELECT
    CAST(p.id AS TEXT) AS "Id",
    COALESCE(TRIM(p.name), 'Unnamed Project') AS "Name",
    CASE INITCAP(TRIM(COALESCE(p.status, '')))
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'Complete' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        WHEN 'Planned' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NULL OR TRIM(p.go_live) = '' THEN NULL
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON CAST(p.client_id AS TEXT) = a.id