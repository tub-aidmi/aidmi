{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Untitled Project') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(TRIM(status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(status)) = 'in planning' THEN 'In Planning'
        ELSE 'In Planning' -- Default for NOT NULL Project_Status__c
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(go_live AS DATE)::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    NULL AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Project') }}
