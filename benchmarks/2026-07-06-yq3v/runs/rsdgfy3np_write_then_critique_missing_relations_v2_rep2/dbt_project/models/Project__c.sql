{{ config(materialized='table') }}

SELECT
    TRIM(project.id) AS "Id",
    COALESCE(TRIM(project.name), TRIM(project.id)) AS "Name",
    CASE
        WHEN LOWER(project.status) IN ('active', 'in progress') THEN 'Active'
        WHEN LOWER(project.status) IN ('completed', 'closed') THEN 'Completed'
        WHEN LOWER(project.status) IN ('in planning', 'planning') THEN 'In Planning'
        WHEN LOWER(project.status) IN ('on hold', 'paused') THEN 'On Hold'
        WHEN LOWER(project.status) IN ('cancelled', 'canceled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN project.go_live
        WHEN project.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(project.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(account.id) AS "Account__c",
    TRIM(opportunity.id) AS "Opportunity__c",
    TRIM(project.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON TRIM(project.client_id) = TRIM(account.id)
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity
    ON TRIM(project.opportunity_ref) = TRIM(opportunity.id)