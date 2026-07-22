{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE
        WHEN status = 'Active' THEN 'Active'
        WHEN status = 'Completed' THEN 'Completed'
        WHEN status = 'In Planning' THEN 'In Planning'
        WHEN status = 'On Hold' THEN 'On Hold'
        WHEN status = 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for Project_Status__c
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    kd AS "Account__c", -- Corresponds to Account.Id (kunden_nr)
    opp AS "Opportunity__c", -- Corresponds to Opportunity.Id (chance_id)
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
