{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE
        WHEN TRIM(status) = 'Active' THEN 'Active'
        WHEN TRIM(status) = 'Completed' THEN 'Completed'
        WHEN TRIM(status) = 'In Planning' THEN 'In Planning'
        WHEN TRIM(status) = 'On Hold' THEN 'On Hold'
        WHEN TRIM(status) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
