{{ config(materialized='table') }}

SELECT
    CAST(proj_id AS TEXT) AS "Id",
    TRIM(name) AS "Name",
    CASE
        WHEN LOWER(TRIM(status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(status)) = 'cancelled' THEN 'Cancelled'
        WHEN LOWER(TRIM(status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(status)) = 'on hold' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(kd AS TEXT) AS "Account__c",
    CAST(opp AS TEXT) AS "Opportunity__c",
    CAST(proj_id AS TEXT) AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
WHERE TRIM(proj_id) != ''