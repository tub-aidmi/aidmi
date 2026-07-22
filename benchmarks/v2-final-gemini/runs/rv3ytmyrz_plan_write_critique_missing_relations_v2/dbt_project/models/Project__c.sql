{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(project.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(project.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(project.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(project.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(project.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN project.go_live
        WHEN project.go_live ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN project.go_live ~ '^\\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account.id AS "Account__c",
    opportunity.id AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
ON
    project.client_id = account.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity
ON
    project.opportunity_ref = opportunity.id
