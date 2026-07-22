{{ config(materialized='table') }}
SELECT
    LEFT(MD5(TRIM(p.proj_id)), 18) AS "Id",
    TRIM(COALESCE(NULLIF(p.name, ''), 'Unnamed Project')) AS "Name",
    CASE
        WHEN TRIM(LOWER(p.status)) = 'active' THEN 'Active'
        WHEN TRIM(LOWER(p.status)) = 'completed' THEN 'Completed'
        WHEN TRIM(LOWER(p.status)) = 'in planning' THEN 'In Planning'
        WHEN TRIM(LOWER(p.status)) = 'on hold' THEN 'On Hold'
        WHEN TRIM(LOWER(p.status)) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    LEFT(MD5(TRIM(k.kunden_nr)), 18) AS "Account__c",
    LEFT(MD5(TRIM(c.chance_id)), 18) AS "Opportunity__c",
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON TRIM(p.opp) = TRIM(c.chance_id)