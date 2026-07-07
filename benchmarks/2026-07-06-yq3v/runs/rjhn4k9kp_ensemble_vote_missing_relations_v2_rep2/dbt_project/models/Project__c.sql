{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE
        WHEN p.status = 'Active' THEN 'Active'
        WHEN p.status = 'Completed' THEN 'Completed'
        WHEN p.status = 'In Planning' THEN 'In Planning'
        WHEN p.status = 'On Hold' THEN 'On Hold'
        WHEN p.status = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    TO_CHAR(p.go_live::DATE, 'YYYY-MM-DD') AS "Go_Live_Date__c",
    p.client_id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p