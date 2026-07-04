{{ config(materialized='table') }}

SELECT
    "proj_id" AS "Id",
    "name" AS "Name",
    CASE
        WHEN UPPER("status") = 'ACTIVE' THEN 'Active'
        WHEN UPPER("status") = 'COMPLETED' THEN 'Completed'
        WHEN UPPER("status") = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER("status") = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER("status") = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    "go_live" AS "Go_Live_Date__c",
    "kd" AS "Account__c",
    "opp" AS "Opportunity__c",
    "proj_id" AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
