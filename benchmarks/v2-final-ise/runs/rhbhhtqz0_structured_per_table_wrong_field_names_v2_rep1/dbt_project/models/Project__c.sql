{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    INITCAP(name) AS "Name",
    CASE
        WHEN LOWER(TRIM(status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}