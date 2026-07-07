{{ config(materialized='table') }}

SELECT
    project.id AS "Id",
    COALESCE(project.name, 'Untitled Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(project.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        -- YYYY-MM-DD format already
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN project.go_live
        -- DD.MM.YYYY format
        WHEN project.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        -- MM/DD/YYYY format
        WHEN project.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(project.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- YYYYMMDD format
        WHEN project.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(project.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
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
