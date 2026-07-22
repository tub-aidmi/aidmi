{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Untitled Project') AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.status)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('on hold', 'paused') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN TRIM(p.go_live) ~ '\d{4}-\d{2}-\d{2}' THEN TRIM(p.go_live)
        WHEN TRIM(p.go_live) ~ '\d{2}/\d{2}/\d{4}' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live) ~ '\d{2}\.\d{2}\.\d{4}' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live) ~ '\d{8}' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    COALESCE(a.id, NULL) AS "Account__c",
    COALESCE(o.id, NULL) AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(p.client_id) = TRIM(a.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(p.opportunity_ref) = TRIM(o.id)
