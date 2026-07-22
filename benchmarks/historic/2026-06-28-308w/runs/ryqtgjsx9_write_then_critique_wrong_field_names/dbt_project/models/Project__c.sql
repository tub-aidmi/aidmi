
{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, 'Unknown Project') AS "Name",
    CASE
        WHEN proj.status = 'Active' THEN 'Active'
        WHEN proj.status = 'Completed' THEN 'Completed'
        WHEN proj.status = 'In Planning' THEN 'In Planning'
        WHEN proj.status = 'On Hold' THEN 'On Hold'
        WHEN proj.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(proj.go_live AS DATE)::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS proj
