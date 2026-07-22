{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(INITCAP(TRIM(proj.name)), 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(proj.status) = 'active' THEN 'Active'
        WHEN LOWER(proj.status) = 'completed' THEN 'Completed'
        WHEN LOWER(proj.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(proj.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(proj.status) = 'cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NULL or unmapped values
    END AS "Project_Status__c",
    -- Robust date parsing for Go_Live_Date__c
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live -- Already in YYYY-MM-DD format
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    proj.kd AS "Account__c",
    proj.opp AS "Opportunity__c",
    proj.proj_id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj