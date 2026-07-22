{{ config(materialized='table') }}

SELECT 
    p."proj_id" AS "Id",
    COALESCE(p."name", 'Unknown') AS "Name",
    CASE 
        WHEN p."status" = 'Active' THEN 'Active'
        WHEN p."status" = 'Completed' THEN 'Completed'
        WHEN p."status" = 'In Planning' THEN 'In Planning'
        WHEN p."status" = 'On Hold' THEN 'On Hold'
        WHEN p."status" = 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    p."go_live" AS "Go_Live_Date__c",
    p."kd" AS "Account__c",
    p."opp" AS "Opportunity__c",
    p."proj_id" AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p