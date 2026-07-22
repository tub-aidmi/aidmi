{{ config(materialized='table', depends_on=['Account', 'Opportunity'])
}}

SELECT
    p.id AS "Id",
    p.name AS "Name",
    CASE
        WHEN LOWER(p.status) = 'active' THEN 'Active'
        WHEN LOWER(p.status) = 'completed' THEN 'Completed'
        WHEN LOWER(p.status) = 'in planning' THEN 'In Planning'
        WHEN LOWER(p.status) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    a.Id AS "Account__c",
    o.Id AS "Opportunity__c",
    p.id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ ref('Account') }} AS a ON p.client_id = a."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Opportunity') }} AS o ON p.opportunity_ref = o."Legacy_Opportunity_ID__c"
