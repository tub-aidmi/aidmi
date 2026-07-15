{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CAST(name AS TEXT) AS "Name",
    CASE 
        WHEN INITCAP(LOWER(TRIM(status))) = 'Active' THEN 'Active'
        WHEN INITCAP(LOWER(TRIM(status))) = 'Completed' THEN 'Completed'
        WHEN INITCAP(LOWER(TRIM(status))) = 'In Planning' THEN 'In Planning'
        WHEN INITCAP(LOWER(TRIM(status))) = 'On Hold' THEN 'On Hold'
        WHEN INITCAP(LOWER(TRIM(status))) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CAST(go_live AS TEXT) AS "Go_Live_Date__c",
    CAST(client_id AS TEXT) AS "Account__c",
    CAST(opportunity_ref AS TEXT) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}