-- models/Project__c.sql
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(status) = 'active' THEN 'Active'
        WHEN LOWER(status) = 'completed' THEN 'Completed'
        WHEN LOWER(status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        ELSE NULL
    END AS "Go_Live_Date__c", -- Assuming YYYY-MM-DD based on inspection
    client_id AS "Account__c", -- Assuming client_id maps to Account Id
    opportunity_ref AS "Opportunity__c", -- Assuming opportunity_ref maps to Opportunity Id
    id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }}
