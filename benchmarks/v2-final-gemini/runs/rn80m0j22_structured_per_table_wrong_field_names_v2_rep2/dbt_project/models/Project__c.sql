-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'proj') }}

{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project Name') AS "Name",
    proj.status AS "Project_Status__c",
    proj.go_live AS "Go_Live_Date__c",
    MD5(proj.kd) AS "Account__c",
    MD5(proj.opp) AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj