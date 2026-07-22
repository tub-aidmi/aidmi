-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    name AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(status) = 'active' THEN 'Active'
            WHEN LOWER(status) = 'completed' THEN 'Completed'
            WHEN LOWER(status) = 'in planning' THEN 'In Planning'
            WHEN LOWER(status) = 'on hold' THEN 'On Hold'
            WHEN LOWER(status) = 'cancelled' THEN 'Cancelled'
            ELSE NULL
        END,
        'In Planning' -- Default for Project_Status__c
    ) AS "Project_Status__c",
    COALESCE(
        CASE
            WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '2000-01-01' -- Default date for Go_Live_Date__c
    ) AS "Go_Live_Date__c",
    kd AS "Account__c", -- Raw kunden_nr
    opp AS "Opportunity__c", -- Raw chance_id
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
