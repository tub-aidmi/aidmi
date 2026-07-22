{{ config(materialized='table') }}

SELECT
    TRIM(p.id) AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project Name') AS "Name",
    CASE
        WHEN INITCAP(TRIM(p.status)) = 'Active' THEN 'Active'
        WHEN INITCAP(TRIM(p.status)) = 'Completed' THEN 'Completed'
        WHEN INITCAP(TRIM(p.status)) = 'In Planning' THEN 'In Planning'
        WHEN INITCAP(TRIM(p.status)) = 'On Hold' THEN 'On Hold'
        WHEN INITCAP(TRIM(p.status)) = 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live)
        WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    TRIM(p.client_id) AS "Account__c",
    TRIM(p.opportunity_ref) AS "Opportunity__c",
    TRIM(p.id) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
