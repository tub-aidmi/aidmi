{{ config(materialized='table') }}

SELECT
    MD5(proj.proj_id) AS "Id",
    TRIM(INITCAP(COALESCE(proj.name, 'Unknown Project Name'))) AS "Name",
    CASE
        WHEN TRIM(UPPER(proj.status)) = 'ACTIVE' THEN 'Active'
        WHEN TRIM(UPPER(proj.status)) = 'COMPLETED' THEN 'Completed'
        WHEN TRIM(UPPER(proj.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN TRIM(UPPER(proj.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN TRIM(UPPER(proj.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN proj.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN proj.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(proj.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
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