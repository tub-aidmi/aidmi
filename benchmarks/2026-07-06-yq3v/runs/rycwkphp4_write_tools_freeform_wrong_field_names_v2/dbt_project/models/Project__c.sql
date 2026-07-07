-- models/Project__c.sql
{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL -- Fallback for Project_Status__c
    END AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    kd AS "Account__c", -- Maps to kunden.kunden_nr
    opp AS "Opportunity__c", -- Maps to chancen.chance_id
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
