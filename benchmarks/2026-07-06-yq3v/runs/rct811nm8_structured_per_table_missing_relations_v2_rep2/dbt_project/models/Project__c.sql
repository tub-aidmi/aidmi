{{
    config(materialized='table')
}}

SELECT
    p.id AS "Id",
    COALESCE(p.name, 'Unnamed Project') AS "Name",
    CASE
        WHEN LOWER(p.status) IN ('active', 'in progress') THEN 'Active'
        WHEN LOWER(p.status) = 'completed' THEN 'Completed'
        WHEN LOWER(p.status) IN ('planning', 'in planning') THEN 'In Planning'
        WHEN LOWER(p.status) IN ('on hold', 'hold') THEN 'On Hold'
        WHEN LOWER(p.status) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN p.go_live
        WHEN p.go_live ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\\d{{4}}\\d{{2}}\\d{{2}}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
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