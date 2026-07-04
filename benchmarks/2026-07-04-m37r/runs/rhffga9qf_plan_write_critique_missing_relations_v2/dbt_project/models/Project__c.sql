{{ config(materialized='table') }}

SELECT
    TRIM(p.id) AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    CASE
        WHEN TRIM(p.status) = 'Active' THEN 'Active'
        WHEN TRIM(p.status) = 'Completed' THEN 'Completed'
        WHEN TRIM(p.status) = 'In Planning' THEN 'In Planning'
        WHEN TRIM(p.status) = 'On Hold' THEN 'On Hold'
        WHEN TRIM(p.status) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(a.id) AS "Account__c",
    TRIM(o.id) AS "Opportunity__c",
    TRIM(p.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(p.client_id) = TRIM(a.id)
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
    ON TRIM(p.opportunity_ref) = TRIM(o.id)
