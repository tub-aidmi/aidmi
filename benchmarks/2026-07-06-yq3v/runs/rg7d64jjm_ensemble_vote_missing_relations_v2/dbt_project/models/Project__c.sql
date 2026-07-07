{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE
        WHEN TRIM(p.status) ILIKE 'Active' THEN 'Active'
        WHEN TRIM(p.status) ILIKE 'Completed' THEN 'Completed'
        WHEN TRIM(p.status) ILIKE 'In Planning' THEN 'In Planning'
        WHEN TRIM(p.status) ILIKE 'On Hold' THEN 'On Hold'
        WHEN TRIM(p.status) ILIKE 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(p.go_live::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    p.client_id AS "Account__c",
    p.opportunity_ref AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
