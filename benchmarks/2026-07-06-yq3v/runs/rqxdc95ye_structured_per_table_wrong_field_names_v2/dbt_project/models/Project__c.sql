{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    proj.name AS "Name",
    CASE
        WHEN proj.status IN ('Active', 'Completed', 'In Planning', 'On Hold', 'Cancelled') THEN proj.status
        ELSE NULL -- Fallback for unexpected status values
    END AS "Project_Status__c",
    proj.go_live AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
