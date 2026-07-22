{{ config(materialized='table') }}
SELECT
    p.proj_id AS "Id",
    p.name AS "Name",
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
    CASE WHEN k.kunden_nr IS NOT NULL THEN '001' || SUBSTRING(MD5(k.kunden_nr), 1, 15) ELSE NULL END AS "Account__c",
    CASE WHEN c.chance_id IS NOT NULL THEN '001' || SUBSTRING(MD5(c.kd_nr), 1, 15) ELSE NULL END AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr