-- depends_on: {{ source('fixture_missing_relations_v2_src', 'project') }}

{{ config(materialized='table') }}

SELECT
    src_project.id AS "Id",
    COALESCE(src_project.name, 'Unknown Project') AS "Name",
    src_project.status AS "Project_Status__c",
    CASE
        WHEN src_project.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(src_project.go_live AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    src_project.client_id AS "Account__c",
    src_project.opportunity_ref AS "Opportunity__c",
    src_project.id AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS src_project