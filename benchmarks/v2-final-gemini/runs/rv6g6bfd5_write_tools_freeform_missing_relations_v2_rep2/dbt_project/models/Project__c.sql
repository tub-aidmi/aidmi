-- models/Project__c.sql

{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name", -- Name is NOT NULL
    CASE p.status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE 'In Planning' -- Default to a valid enum value if status is unmapped/NULL
    END AS "Project_Status__c",
    p.go_live AS "Go_Live_Date__c", -- Already in YYYY-MM-DD format
    a.id AS "Account__c", -- Map client_id to Account.Id
    o.id AS "Opportunity__c", -- Map opportunity_ref to Opportunity.Id
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    p.client_id = a.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
ON
    p.opportunity_ref = o.id
