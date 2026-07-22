-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'proj') }}

{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    COALESCE(proj.name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(proj.status) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(proj.status) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(proj.status) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(proj.status) IN ('on hold', 'pausiert') THEN 'On Hold'
        WHEN LOWER(proj.status) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
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