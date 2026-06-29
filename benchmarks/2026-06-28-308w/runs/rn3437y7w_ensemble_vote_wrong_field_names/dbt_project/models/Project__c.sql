
{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    proj.name AS "Name",
    CASE
        WHEN proj.status = 'Active' THEN 'Active'
        WHEN proj.status = 'Completed' THEN 'Completed'
        WHEN proj.status = 'In Planning' THEN 'In Planning'
        ELSE NULL
    END AS "Project_Status__c",
    proj.go_live::text AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS proj
