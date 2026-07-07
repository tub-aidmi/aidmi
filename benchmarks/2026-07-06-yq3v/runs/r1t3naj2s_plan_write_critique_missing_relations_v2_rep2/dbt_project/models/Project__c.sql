{{ config(materialized='table') }}

SELECT
    MD5(project.id) AS "Id",
    COALESCE(project.name, 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(project.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(project.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(project.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(project.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(project.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN project.go_live::DATE
        WHEN project.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(project.go_live, 'MM/DD/YYYY')
        WHEN project.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(project.go_live, 'DD.MM.YYYY')
        ELSE NULL
    END::TEXT AS "Go_Live_Date__c",
    MD5(project.client_id) AS "Account__c",
    MD5(project.opportunity_ref) AS "Opportunity__c",
    project.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
