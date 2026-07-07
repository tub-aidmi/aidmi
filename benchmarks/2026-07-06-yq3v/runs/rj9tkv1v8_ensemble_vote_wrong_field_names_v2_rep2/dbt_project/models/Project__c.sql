{{ config(materialized='table') }}

SELECT
    MD5(TRIM(proj.proj_id)) AS "Id",
    COALESCE(TRIM(proj.name), 'Unknown Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(proj.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(proj.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(proj.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(proj.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(proj.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(TRIM(proj.kd)) AS "Account__c",
    MD5(TRIM(proj.opp)) AS "Opportunity__c",
    TRIM(proj.proj_id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
