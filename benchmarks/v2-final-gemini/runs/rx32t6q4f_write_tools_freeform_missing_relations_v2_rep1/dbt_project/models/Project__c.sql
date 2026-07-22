{{ config(materialized='table') }}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(p.status) = 'active' THEN 'Active'
        WHEN LOWER(p.status) = 'completed' THEN 'Completed'
        WHEN LOWER(p.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(p.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live::DATE::TEXT
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(p.go_live, 'DD.MM.YYYY')::TEXT
        WHEN p.go_live ~ '^\d{8}$' THEN TO_DATE(p.go_live, 'YYYYMMDD')::TEXT
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(p.go_live, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.id AS "Account__c", -- Join to account table to get Salesforce-style Account Id
    p.opportunity_ref AS "Opportunity__c", -- Assuming opportunity_ref is the Salesforce Opportunity Id
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    p.client_id = a.id -- Assuming client_id maps to account.id
