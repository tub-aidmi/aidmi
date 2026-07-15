{{ config(materialized='table') }}

SELECT
    'PROJ-' || SUBSTRING(p.proj_id FROM '\d+') AS "Id",
    p.name AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN SUBSTRING(p.go_live FROM 1 FOR 4) || '-' || SUBSTRING(p.go_live FROM 5 FOR 2) || '-' || SUBSTRING(p.go_live FROM 7 FOR 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    'ACCT-' || SUBSTRING(k.kunden_nr FROM '\d+') AS "Account__c",
    p.opp AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON p.kd = k.kunden_nr