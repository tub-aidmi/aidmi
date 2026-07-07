-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    name AS "Name",
    CASE
        WHEN lower(status) = 'active' THEN 'Active'
        WHEN lower(status) = 'completed' THEN 'Completed'
        WHEN lower(status) = 'in planning' THEN 'In Planning'
        WHEN lower(status) = 'on hold' THEN 'On Hold'
        WHEN lower(status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CAST(go_live AS DATE), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
