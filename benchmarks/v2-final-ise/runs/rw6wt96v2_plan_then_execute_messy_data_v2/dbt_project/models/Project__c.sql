{{ config(materialized='table') }}

WITH project_source AS (
    SELECT
        id,
        name,
        project_status__c,
        go_live_date__c,
        account__c,
        opportunity__c
    FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
)

SELECT
    ps.id AS "Id",
    INITCAP(TRIM(ps.name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(ps.project_status__c)) IN ('active') THEN 'Active'
        WHEN LOWER(TRIM(ps.project_status__c)) IN ('completed') THEN 'Completed'
        WHEN LOWER(TRIM(ps.project_status__c)) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(TRIM(ps.project_status__c)) IN ('on hold', 'hold') THEN 'On Hold'
        WHEN LOWER(TRIM(ps.project_status__c)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN ps.go_live_date__c ~ '^\d{4}\d{2}\d{2}$' THEN
            TO_CHAR(TO_DATE(ps.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN ps.go_live_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(ps.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ps.go_live_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(ps.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ps.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN
            ps.go_live_date__c
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    ps.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM project_source ps
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON ps.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
    ON ps.opportunity__c = o.id