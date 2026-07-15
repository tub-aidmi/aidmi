{{ config(materialized='table') }}

SELECT
    TRIM(p.id) AS "Id",
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('active', 'ongoing', 'in progress') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'finished', 'done') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planning', 'planned', 'setup') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('on hold', 'paused', 'hold') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled', 'closed', 'withdrawn') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    TRIM(p.id) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(UPPER(p.client_id)) = TRIM(UPPER(a.id))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
    ON TRIM(UPPER(p.opportunity_ref)) = TRIM(UPPER(o.id))