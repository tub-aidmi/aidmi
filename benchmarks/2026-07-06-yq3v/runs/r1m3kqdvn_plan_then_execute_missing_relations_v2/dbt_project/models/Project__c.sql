{{ config(materialized='table') }}

SELECT
    TRIM(project.id) AS "Id",
    COALESCE(TRIM(project.name), 'Unknown Project Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(project.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(project.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(TRIM(project.go_live) AS DATE), 'YYYY-MM-DD')
        WHEN TRIM(project.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(project.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(project.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(project.client_id) AS "Account__c",
    TRIM(project.opportunity_ref) AS "Opportunity__c",
    TRIM(project.id) AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
