{{ config(materialized='table') }}

SELECT
    proj.proj_id AS "Id",
    COALESCE(proj.name, proj.proj_id) AS "Name",
    CASE
        WHEN LOWER(proj.status) = 'active' THEN 'Active'
        WHEN LOWER(proj.status) = 'completed' THEN 'Completed'
        WHEN LOWER(proj.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(proj.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(proj.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    -- Attempt to parse go_live date into YYYY-MM-DD format
    CASE
        WHEN proj.go_live IS NULL THEN NULL
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN proj.go_live -- Already YYYY-MM-DD
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
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