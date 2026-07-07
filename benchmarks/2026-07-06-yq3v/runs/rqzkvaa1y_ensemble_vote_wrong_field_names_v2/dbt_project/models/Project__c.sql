-- depends_on: {{ ref('Account') }} -- depends_on: {{ ref('Opportunity') }}
{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, 'Unknown Project ' || proj.proj_id) AS "Name",
    proj.status AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live
        ELSE NULL
    END AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj