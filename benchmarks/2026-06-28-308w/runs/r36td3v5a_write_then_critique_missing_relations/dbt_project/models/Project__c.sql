
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CAST(
        CASE
            WHEN status = 'Active' THEN 'Active'
            WHEN status = 'Completed' THEN 'Completed'
            WHEN status = 'In Planning' THEN 'In Planning'
            WHEN status = 'On Hold' THEN 'On Hold'
            WHEN status = 'Cancelled' THEN 'Cancelled'
            ELSE NULL
        END AS TEXT
    ) AS "Project_Status__c",
    go_live AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Project') }}
