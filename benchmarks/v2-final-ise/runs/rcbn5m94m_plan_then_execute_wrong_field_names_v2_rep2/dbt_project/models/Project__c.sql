{{ config(materialized='table') }}

SELECT
    'a0X' || TRIM(p.proj_id) AS "Id",
    INITCAP(TRIM(p.name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(p.status)) IN ('active', 'open', 'running', 'in progress') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('completed', 'finished', 'done', 'closed') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) IN ('planning', 'planned', 'preparation') THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('hold', 'paused', 'suspended') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'canceled', 'dead') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != '' THEN
            COALESCE(
                TO_CHAR(TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
                TO_CHAR(TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
                TO_CHAR(TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            )
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kunden_nr IS NOT NULL AS '001' || TRIM(k.kunden_nr) AS "Account__c",
    c.chance_id IS NOT NULL AS '006' || TRIM(c.chance_id) AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP()::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(k.kunden_nr) = TRIM(p.kd)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON TRIM(c.chance_id) = TRIM(p.opp)