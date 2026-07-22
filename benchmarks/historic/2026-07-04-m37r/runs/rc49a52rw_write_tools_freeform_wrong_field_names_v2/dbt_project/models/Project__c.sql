{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    COALESCE(name, 'Unknown Project') AS "Name",
    CASE
        WHEN status = 'Active' THEN 'Active'
        WHEN status = 'Completed' THEN 'Completed'
        WHEN status = 'In Planning' THEN 'In Planning'
        WHEN status = 'On Hold' THEN 'On Hold'
        WHEN status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(CAST(go_live AS DATE), 'YYYY-MM-DD') AS "Go_Live_Date__c",
    kd AS "Account__c",
    opp AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }}