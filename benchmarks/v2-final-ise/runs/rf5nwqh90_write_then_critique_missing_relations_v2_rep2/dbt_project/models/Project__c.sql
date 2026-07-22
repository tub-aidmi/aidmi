{{ config(materialized='table') }}

WITH src_project AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
),
src_account AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)
SELECT
    CAST(p.id AS TEXT) AS "Id",
    INITCAP(TRIM(p.name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'complete') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('on hold', 'paused') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != ''
            THEN TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    CAST(p.id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM src_project p
LEFT JOIN src_account a
    ON TRIM(a.id) = TRIM(p.client_id)