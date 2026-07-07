-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, proj.proj_id) AS "Name",
    CASE
        WHEN LOWER(TRIM(proj.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(proj.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(proj.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(proj.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(proj.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(NULLIF(TO_DATE(proj.go_live, 'YYYY-MM-DD'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(proj.go_live, 'DD.MM.YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(proj.go_live, 'MM/DD/YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(proj.go_live, 'YYYYMMDD'), '0001-01-01'), 'YYYY-MM-DD'),
        NULL
    ) AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
