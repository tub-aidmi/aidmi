{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'N/A') AS "Name",
    CASE
        WHEN LOWER(project.status) = 'active' THEN 'Active'
        WHEN LOWER(project.status) = 'completed' THEN 'Completed'
        WHEN LOWER(project.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(project.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(project.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_CHAR(project.go_live::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    account.id AS "Account__c",
    opportunity.id AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON project.client_id = account.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity
    ON project.opportunity_ref = opportunity.id
