{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, 'Unknown Project') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(TRIM(proj.status)) = 'active' THEN 'Active'
            WHEN LOWER(TRIM(proj.status)) IN ('completed', 'closed', 'done') THEN 'Completed'
            WHEN LOWER(TRIM(proj.status)) IN ('planning', 'in planning') THEN 'In Planning'
            WHEN LOWER(TRIM(proj.status)) IN ('on hold', 'hold') THEN 'On Hold'
            WHEN LOWER(TRIM(proj.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
            ELSE NULL
        END,
        'In Planning'
    ) AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live -- Already YYYY-MM-DD
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
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
