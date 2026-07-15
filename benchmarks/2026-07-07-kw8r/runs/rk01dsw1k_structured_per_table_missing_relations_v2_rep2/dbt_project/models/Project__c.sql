{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    name AS "Name",
    CASE
        WHEN LOWER(TRIM(status)) IN ('active', 'completed', 'in planning', 'on hold', 'cancelled') THEN INITCAP(TRIM(status))
        ELSE NULL
    END AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}