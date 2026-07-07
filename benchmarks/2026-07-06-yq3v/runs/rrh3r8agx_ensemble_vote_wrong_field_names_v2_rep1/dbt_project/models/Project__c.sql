{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    COALESCE(p.name, 'Unknown Project Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}.\d{2}.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(p.kd) AS "Account__c",
    MD5(p.opp) AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
