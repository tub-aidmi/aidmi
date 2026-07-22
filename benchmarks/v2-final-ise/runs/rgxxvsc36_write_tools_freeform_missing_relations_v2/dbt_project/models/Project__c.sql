{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE
        WHEN UPPER(p.status) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(p.status) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(p.status) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(p.status) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(p.status) = 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c",
    a.id AS "Account__c",
    o.id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON p.client_id = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
    ON p.opportunity_ref = o.id
